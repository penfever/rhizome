package channel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"rhizome/internal/config"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Optional hook fields preserve Codex's JSON null values.
type codexHook struct {
	SessionID      string  `json:"session_id"`
	Cwd            string  `json:"cwd"`
	Event          string  `json:"hook_event_name"`
	TranscriptPath *string `json:"transcript_path,omitempty"`
	Source         *string `json:"source,omitempty"`
	AgentID        *string `json:"agent_id,omitempty"`
	AgentType      *string `json:"agent_type,omitempty"`
	ToolName       *string `json:"tool_name,omitempty"`
}

func (h codexHook) hook() Hook {
	value := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	return Hook{SessionID: h.SessionID, Cwd: h.Cwd, Event: h.Event, Source: value(h.Source), AgentID: value(h.AgentID), AgentType: value(h.AgentType), ToolName: value(h.ToolName)}
}

var threadUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Codex's app server may share a connection. Never infer the caller from the
// most recent hook, process environment, working directory, or SDK session.
func codexCaller(r *mcp.CallToolRequest) (string, error) {
	if r == nil || r.Params == nil {
		return "", errors.New("Codex caller metadata missing")
	}
	thread, ok := r.Params.Meta["threadId"].(string)
	if !ok || !threadUUID.MatchString(thread) {
		return "", errors.New("Codex requires verified _meta.threadId (UUID); update Codex if caller metadata is unavailable")
	}
	return thread, nil
}

type codexNodes struct {
	mu             sync.Mutex
	ctx            context.Context
	nodes          map[string]*Node
	binary, socket string
	opts           Options
	history        *historyService // shared read-only catalog client for every thread node
	recentWindow   time.Duration
	peerActivity   config.PeerActivityPolicy
	details        bool // [sessions].share_prompts_and_tools
	debug          bool
	steer          bool // codex.delivery = "steer"
	// cliOwned reports whether the parent process is a live standalone Codex
	// CLI rather than the App Server daemon; see codexCLIParent.
	cliOwned func() bool
}

func (m *codexNodes) resolve(r *mcp.CallToolRequest) (*Node, error) {
	thread, err := codexCaller(r)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if n := m.nodes[thread]; n != nil {
		return n, nil
	}
	if len(m.nodes) >= 64 {
		return nil, errors.New("Rhizome process thread limit reached; restart MCP to release inactive bindings")
	}
	n := newNode(m.ctx)
	n.identity.AgentType = "codex"
	n.ready = true
	n.external = true
	n.archive = m.history
	n.recentWindow = m.recentWindow
	n.details = m.details
	n.peerActivity = m.peerActivity
	n.debug = m.debug
	n.collectionState = "waiting for session hook"
	n.codex = newCodexCollector(thread, &appReader{socket: m.socket})
	n.codexOut = newCodexOutbox(m.steer, m.cliOwned, func(ctx context.Context, message string) error {
		return queueCodex(ctx, m.binary, m.socket, thread, message)
	})
	n.notify = n.holdForCodex
	if err := n.start(m.opts); err != nil {
		n.close()
		return nil, err
	}
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.deliver(n.ctx) }()
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.runCodexOutbox(n.ctx, thread) }()
	m.nodes[thread] = n
	return n, nil
}
func (m *codexNodes) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, n := range m.nodes {
		n.close()
	}
}
func (n *Node) observeCodex(ctx context.Context, h Hook) error {
	if h.AgentID != "" {
		return nil
	}
	if h.SessionID != n.codex.thread {
		return errors.New("hook session_id conflicts with Codex caller _meta.threadId")
	}
	if strings.Contains(h.ToolName, "mcp__rhizome__") || strings.HasPrefix(h.ToolName, "rhizome.") {
		return nil
	}
	switch h.Event {
	case "SessionStart", "UserPromptSubmit", "PreToolUse", "Stop":
	default:
		return fmt.Errorf("unsupported Codex hook event %q", h.Event)
	}
	if !filepath.IsAbs(h.Cwd) {
		return errors.New("Codex hook requires absolute cwd")
	}
	n.gate.Lock()
	defer n.gate.Unlock()
	n.mu.Lock()
	first := n.identity.SessionId == ""
	n.mu.Unlock()
	var nativeTitle *string
	if first {
		var info struct {
			Thread struct {
				ID     string  `json:"id"`
				Parent *string `json:"parentThreadId"`
				Name   *string `json:"name"`
			} `json:"thread"`
		}
		if err := n.codex.reader.call(ctx, "thread/read", map[string]any{"threadId": h.SessionID}, &info); err != nil {
			return fmt.Errorf("waiting for Codex thread verification: %w", err)
		}
		if info.Thread.ID != h.SessionID || info.Thread.Parent != nil {
			return errors.New("child or mismatched Codex thread cannot bind")
		}
		nativeTitle = info.Thread.Name
	}
	n.mu.Lock()
	if first {
		lease, err := n.claimCollection(h.SessionID)
		if err != nil {
			n.mu.Unlock()
			return err
		}
		n.collectionLease = lease
		n.identity.SessionId = h.SessionID
		n.identity.Generation++
		n.names = nameState{explicit: loadOverride(n.runtime, h.SessionID)}
		if nativeTitle != nil {
			n.names.custom = cleanName(*nativeTitle)
		}
	}
	n.identity.Project = clip(h.Cwd, 180)
	n.identity.DisplayName = clip(filepath.Base(h.Cwd), 40)
	renamed := n.resolveNameLocked()
	gen := n.identity.Generation
	n.mu.Unlock()
	if renamed && !first {
		n.advertise()
	}
	if first {
		// Finish the baseline attempt before returning the hook. An unavailable
		// reader stays explicitly paused and takes a baseline when it reconnects.
		n.codex.scan(ctx, n, gen)
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.codex.run(n.ctx, n, gen) }()
		if n.members != nil {
			_ = n.members.UpdateNode(time.Second)
		}
		slog.Info("binding", "agent", "codex", "session_id", h.SessionID, "generation", gen)
	} else {
		select {
		case n.codex.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// Queue output is drained but never allowed to allocate unbounded memory or
// escape to MCP stdout. No retries: a timeout can mean uncertain acceptance.
type limitedCapture struct{ b []byte }

func (w *limitedCapture) Write(p []byte) (int, error) {
	n := len(p)
	left := 4096 - len(w.b)
	if left > 0 {
		if len(p) > left {
			p = p[:left]
		}
		w.b = append(w.b, p...)
	}
	return n, nil
}

// codexCLIParent returns a check that the gateway's parent, pid, is still a
// live standalone Codex CLI. The parent's command line is read once: a gateway
// started by the App Server daemon ("codex app-server ...") never qualifies.
// The check fails closed when the command line cannot be read.
func codexCLIParent(pid int) func() bool {
	out, err := exec.Command("ps", "-o", "args=", "-p", strconv.Itoa(pid)).Output()
	cli := err == nil && pid > 1 && isCodexCLICommand(string(out))
	slog.Info("codex_parent", "pid", pid, "standalone_cli", cli)
	return func() bool { return cli && os.Getppid() == pid }
}

// isCodexCLICommand reports whether args is a Codex executable not running as
// the App Server.
func isCodexCLICommand(args string) bool {
	fields := strings.Fields(args)
	if len(fields) == 0 || !strings.HasPrefix(filepath.Base(fields[0]), "codex") {
		return false
	}
	return !slices.Contains(fields[1:], "app-server")
}

// queueCodex queues message, already wrapped in peer envelopes, as one item.
func queueCodex(ctx context.Context, binary, socket, thread, message string) error {
	if !filepath.IsAbs(binary) || !filepath.IsAbs(socket) || !threadUUID.MatchString(thread) {
		return errors.New("invalid local Codex delivery configuration")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// The endpoint is local configuration, never supplied by a peer. Pinning it
	// also prevents a CLI default from queuing into another daemon.
	cmd := exec.CommandContext(ctx, binary, "queue", "--remote", "unix://"+socket, "--thread", thread, "--message", message)
	cmd.WaitDelay = time.Second
	output := &limitedCapture{}
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Run(); err != nil {
		// CLI diagnostics can contain message text or credentials. Keep only the
		// process error in status/logs; the bounded capture is intentionally private.
		return fmt.Errorf("codex queue failed (acceptance unknown; not retried): %w", err)
	}
	return nil
}

// RunCodex serves the Codex channel. c supplies the binary, Codex home and
// runtime directory; o carries the LAN options.
func RunCodex(c config.Config, o Options) error {
	o.Runtime = c.RuntimeDir
	o.Hostname = c.Hostname
	o.UpdateNotice = c.AutoUpdate.NotifySessions
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, LogOptions(c.Debug))))
	resolved, err := exec.LookPath(c.Codex.Bin)
	if err != nil {
		return err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return err
	}
	home := c.Codex.Home
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	history := newHistoryService(historyScope{Runtime: c.RuntimeDir, ClaudeRoot: c.Claude.ProjectsDir, CodexRoot: c.Codex.SessionsDir, OpenCodeRoot: c.OpenCode.DataDir, PiRoot: c.Pi.SessionsDir})
	history.details = c.Sessions.SharePromptsAndTools
	history.peerActivity = c.Sessions.SharePeerTriggeredActivity
	manager := &codexNodes{ctx: ctx, nodes: map[string]*Node{}, binary: resolved, socket: filepath.Join(home, "app-server-control", "app-server-control.sock"), opts: o, history: history, recentWindow: c.RecentWindow, details: c.Sessions.SharePromptsAndTools, peerActivity: c.Sessions.SharePeerTriggeredActivity, debug: c.Debug, steer: c.Codex.Delivery == config.CodexDeliverySteer, cliOwned: codexCLIParent(os.Getppid())}
	slog.Info("codex_delivery", "path", c.Codex.Delivery)
	defer manager.close()
	server := makeServer(manager.resolve, agentCodex, func() {}, history)
	session, err := server.Connect(ctx, &mcp.StdioTransport{}, nil)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- session.Wait() }()
	select {
	case err := <-done:
		cancel()
		return err
	case <-ctx.Done():
		_ = session.Close()
		err := <-done
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
}
