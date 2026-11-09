package channel

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	pb "rhizome/proto"
)

const threadA = "11111111-1111-7111-8111-111111111111"
const threadB = "22222222-2222-7222-8222-222222222222"

type fakeCodex struct {
	mu             sync.Mutex
	thread, status string
	name           *string
	turns          []codexTurn
	items          map[string][]codexItem
	fail           bool
	calls          []string
	steerErr       error
	steered        []steerCall
}
type steerCall struct{ turn, text string }

func (f *fakeCodex) close() {}
func (f *fakeCodex) steer(_ context.Context, thread, turn, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "turn/steer")
	if f.steerErr != nil {
		return f.steerErr
	}
	if thread != f.thread || f.status != "active" || len(f.turns) == 0 || f.turns[0].ID != turn || f.turns[0].Status != "inProgress" {
		return &appRPCError{-32600, "expected active turn id", "turn/steer"}
	}
	f.steered = append(f.steered, steerCall{turn, text})
	return nil
}
func (f *fakeCodex) call(_ context.Context, method string, params, out any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, method)
	if f.fail {
		return errors.New("disconnected")
	}
	p := params.(map[string]any)
	var result any
	switch method {
	case "thread/read":
		result = map[string]any{"thread": map[string]any{"id": f.thread, "name": f.name, "status": map[string]string{"type": f.status}}}
	case "thread/turns/list":
		result = turnPage{Data: f.turns}
	case "thread/items/list":
		var entries []itemEntry
		for _, item := range f.items[p["turnId"].(string)] {
			entries = append(entries, itemEntry{p["turnId"].(string), item})
		}
		result = itemPage{Data: entries}
	default:
		panic("mutating method " + method)
	}
	b, _ := json.Marshal(result)
	return json.Unmarshal(b, out)
}
func TestCodexCollectionBaselineDeltasAndIsolation(t *testing.T) {
	n := node(t)
	n.identity.SessionId = threadA
	n.identity.Generation = 1
	f := &fakeCodex{thread: threadA, status: "idle", turns: []codexTurn{{"turn1", "inProgress"}}, items: map[string][]codexItem{"turn1": {{ID: "a", Type: "agentMessage", Text: "old"}}}}
	c := newCodexCollector(threadA, f)
	n.codex = c
	if !c.scan(n.ctx, n, 1) || len(n.history) != 0 {
		t.Fatal("baseline replayed history")
	}
	f.items["turn1"] = []codexItem{{ID: "a", Type: "agentMessage", Text: "old appended"}, {ID: "r", Type: "reasoning", Text: "secret"}, {ID: "u", Type: "userMessage", Text: "human prompt"}, {ID: "m", Type: "mcpToolCall", Server: "rhizome", Tool: "send", Status: "completed"}, {ID: "cmd", Type: "commandExecution", Status: "inProgress"}}
	if !c.scan(n.ctx, n, 1) {
		t.Fatal("scan failed")
	}
	if len(n.history) != 2 || n.history[0].Text != " appended" {
		t.Fatalf("unexpected events: %v", n.history)
	}
	c.scan(n.ctx, n, 1)
	if len(n.history) != 2 {
		t.Fatal("duplicate events")
	}
	f.items["turn1"][4].Status = "completed"
	f.turns[0].Status = "completed"
	c.scan(n.ctx, n, 1)
	if len(n.history) != 4 {
		t.Fatal("missing tool/turn completion")
	}
	f.status = "notLoaded"
	if c.scan(n.ctx, n, 1) || n.deliveryReady {
		t.Fatal("unloaded thread enabled delivery")
	}
	f.status = "idle"
	f.fail = true
	c.scan(n.ctx, n, 1)
	if n.deliveryReady {
		t.Fatal("disconnected reader enabled delivery")
	}
	f.fail = false
	f.items["turn1"] = nil
	c.scan(n.ctx, n, 1)
	if n.history[len(n.history)-1].Kind != "gap" {
		t.Fatal("rollback did not report gap")
	}
	f.thread = threadB
	if c.scan(n.ctx, n, 1) || n.deliveryReady {
		t.Fatal("wrong thread accepted")
	}
}
func TestCodexCallerAndSharedConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := &codexNodes{ctx: ctx, nodes: map[string]*Node{}, opts: Options{Runtime: filepath.Join(t.TempDir(), "run")}, binary: "/bin/false", socket: "/nonexistent.sock"}
	defer manager.close()
	st, ct := mcp.NewInMemoryTransports()
	ss, err := makeServer(manager.resolve, agentCodex, func() {}, nil).Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "codex-test", Version: "1"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	if cs.InitializeResult().Capabilities.Experimental["claude/channel"] != nil {
		t.Fatal("Codex advertised Claude channels")
	}
	call := func(thread, name string, args any) *mcp.CallToolResult {
		t.Helper()
		meta := mcp.Meta{}
		if thread != "" {
			meta["threadId"] = thread
		}
		r, err := cs.CallTool(ctx, &mcp.CallToolParams{Meta: meta, Name: name, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if !call("", "list_sessions", map[string]any{}).IsError {
		t.Fatal("missing caller accepted")
	}
	for _, thread := range []string{threadA, threadB} {
		if r := call(thread, "list_sessions", map[string]any{}); r.IsError {
			t.Fatalf("tool error: %+v", r.Content[0])
		}
		manager.nodes[thread].codex.reader = &fakeCodex{thread: thread, status: "idle", items: map[string][]codexItem{}}
		if !call(thread, "observe_session", Hook{SessionID: "wrong", Cwd: t.TempDir(), Event: "UserPromptSubmit"}).IsError {
			t.Fatal("conflicting hook accepted")
		}
		if call(thread, "observe_session", map[string]any{"session_id": thread, "cwd": t.TempDir(), "hook_event_name": "UserPromptSubmit", "transcript_path": nil, "agent_id": nil, "source": nil}).IsError {
			t.Fatal("binding rejected")
		}
	}
	var wg sync.WaitGroup
	for _, thread := range []string{threadA, threadB} {
		wg.Add(1)
		go func(thread string) {
			defer wg.Done()
			for i := 0; i < 4; i++ {
				r, e := cs.CallTool(ctx, &mcp.CallToolParams{Meta: mcp.Meta{"threadId": thread}, Name: "publish", Arguments: textInput{Text: thread}})
				if e != nil || r.IsError {
					t.Errorf("publish failed %v %v", r, e)
				}
			}
		}(thread)
	}
	wg.Wait()
	for thread, n := range manager.nodes {
		n.mu.Lock()
		for _, e := range n.history {
			if e.SessionId != thread || e.Text != thread {
				t.Error("cross-thread publication")
			}
		}
		n.mu.Unlock()
	}
}
func TestCodexQueueArgumentsTimeoutAndBoundedOutput(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "fake-codex")
	args := filepath.Join(dir, "args")
	t.Setenv("RHIZOME_TEST_ARGS", args)
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$RHIZOME_TEST_ARGS\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	text := "$(touch /should-never-run); `whoami`\n--remote wss://peer"
	if err := queueCodex(context.Background(), binary, "/local.sock", threadA, text); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(args)
	s := string(b)
	if !strings.HasPrefix(s, "queue\n--remote\nunix:///local.sock\n--thread\n"+threadA+"\n--message\n") || !strings.Contains(s, "$(touch /should-never-run)") {
		t.Fatal("argument boundaries changed")
	}
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := queueCodex(context.Background(), binary, "/local.sock", threadA, "hi"); err == nil {
		t.Fatal("failed CLI reported success")
	}
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexec sleep 10\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if queueCodex(ctx, binary, "/local.sock", threadA, "hi") == nil || time.Since(start) > 2*time.Second {
		t.Fatal("queue did not cancel")
	}
	w := &limitedCapture{}
	p := make([]byte, 10000)
	n, err := w.Write(p)
	if err != nil || n != len(p) || len(w.b) != 4096 {
		t.Fatal("output capture unbounded")
	}
	if err := queueCodex(context.Background(), binary, "/local.sock", "not-uuid", "hi"); err == nil {
		t.Fatal("non-UUID accepted")
	}
}
func TestCodexRPCRejectsMutations(t *testing.T) {
	a := &appReader{socket: "/does-not-exist"}
	for _, method := range []string{"thread/resume", "turn/start", "turn/steer", "thread/start"} {
		if err := a.call(context.Background(), method, nil, nil); err == nil || !strings.Contains(err.Error(), "not read-only") {
			t.Fatal("mutation not rejected")
		}
	}
}

type pagingCodex struct {
	fakeCodex
	itemCalls, turnCalls int
	endless              bool
}

func (f *pagingCodex) call(ctx context.Context, method string, params, out any) error {
	p := params.(map[string]any)
	if method == "thread/items/list" {
		f.itemCalls++
		next := "next"
		page := itemPage{Data: []itemEntry{{"t", codexItem{ID: "first", Type: "agentMessage", Text: "history"}}}, Next: &next}
		cursor, _ := p["cursor"].(*string)
		if cursor != nil && !f.endless {
			page = itemPage{Data: []itemEntry{{"t", codexItem{ID: "second", Type: "agentMessage", Text: "also history"}}}}
		}
		b, _ := json.Marshal(page)
		return json.Unmarshal(b, out)
	}
	if method == "thread/turns/list" {
		f.turnCalls++
	}
	return f.fakeCodex.call(ctx, method, params, out)
}
func TestCodexPaginationBaselineAndCap(t *testing.T) {
	n := node(t)
	n.identity.SessionId = threadA
	n.identity.Generation = 1
	f := &pagingCodex{fakeCodex: fakeCodex{thread: threadA, status: "idle", turns: []codexTurn{{"t", "completed"}}}}
	c := newCodexCollector(threadA, f)
	n.codex = c
	if !c.scan(n.ctx, n, 1) || f.itemCalls != 2 || len(n.history) != 0 {
		t.Fatal("baseline did not consume both pages")
	}
	if !c.scan(n.ctx, n, 1) || len(n.history) != 0 {
		t.Fatal("pagination duplicated history")
	}
	f.endless = true
	f.itemCalls = 0
	// An oversized turn is skipped with one gap; delivery stays open because
	// the thread itself is verified idle.
	if !c.scan(n.ctx, n, 1) || f.itemCalls != 4 || !n.deliveryReady {
		t.Fatal("unbounded pagination or delivery closed by collection cap")
	}
	if len(n.history) != 1 || n.history[0].Kind != "gap" {
		t.Fatal("missing pagination gap")
	}
	f.itemCalls = 0
	if !c.scan(n.ctx, n, 1) || f.itemCalls != 0 || len(n.history) != 1 || !n.deliveryReady {
		t.Fatalf("oversized turn re-paged or re-gapped: calls=%d history=%d", f.itemCalls, len(n.history))
	}
}

// oversizedCodex pages one turn without end; other turns use fakeCodex.
type oversizedCodex struct {
	fakeCodex
	huge string
}

func (f *oversizedCodex) call(ctx context.Context, method string, params, out any) error {
	p := params.(map[string]any)
	if method == "thread/items/list" && p["turnId"] == f.huge {
		next := "next"
		page := itemPage{Data: []itemEntry{{f.huge, codexItem{ID: "x", Type: "agentMessage", Text: "noise"}}}, Next: &next}
		b, _ := json.Marshal(page)
		return json.Unmarshal(b, out)
	}
	return f.fakeCodex.call(ctx, method, params, out)
}

func TestCodexOversizedTurnKeepsDeliveryAndLaterTurnsCollect(t *testing.T) {
	n := node(t)
	n.identity.SessionId = threadA
	n.identity.Generation = 1
	f := &oversizedCodex{fakeCodex: fakeCodex{thread: threadA, status: "idle", turns: []codexTurn{{"huge", "completed"}}, items: map[string][]codexItem{}}, huge: "huge"}
	c := newCodexCollector(threadA, f)
	n.codex = c
	for i := 0; i < 3; i++ {
		if !c.scan(n.ctx, n, 1) || !n.deliveryReady {
			t.Fatalf("scan %d closed delivery after an oversized turn", i)
		}
	}
	n.mu.Lock()
	gaps := n.stats.CollectionGaps
	n.mu.Unlock()
	if gaps != 1 {
		t.Fatalf("want one gap for the oversized turn, got %d", gaps)
	}
	f.mu.Lock()
	f.turns = []codexTurn{{"next", "inProgress"}, {"huge", "completed"}}
	f.items["next"] = []codexItem{{ID: "a", Type: "agentMessage", Text: "fresh work"}}
	f.mu.Unlock()
	if !c.scan(n.ctx, n, 1) {
		t.Fatal("scan failed after a new turn")
	}
	if got := activityTexts(n); len(got) != 1 || got[0] != "fresh work" {
		t.Fatalf("later turn not collected: %q", got)
	}
}

func TestCodexWakeEndsBackoff(t *testing.T) {
	n := node(t)
	n.identity.SessionId = threadA
	n.identity.Generation = 1
	f := &fakeCodex{thread: threadA, status: "idle", fail: true}
	c := newCodexCollector(threadA, f)
	n.codex = c
	n.wg.Add(1)
	go func() { defer n.wg.Done(); c.run(n.ctx, n, 1) }()
	// Let the failing reader push the backoff past one second.
	eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.calls) >= 2
	})
	f.mu.Lock()
	f.fail = false
	before := len(f.calls)
	f.mu.Unlock()
	c.wake <- struct{}{}
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		ready := n.deliveryReady
		n.mu.Unlock()
		if ready {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t.Fatalf("wake did not end backoff (calls %d -> %d)", before, len(f.calls))
}
func TestCodexUnavailableRetainsDelivery(t *testing.T) {
	n := node(t)
	n.identity.SessionId = threadA
	n.identity.Generation = 1
	n.codex = newCodexCollector(threadA, &fakeCodex{})
	n.external = true
	n.ready = true
	n.deliveryReady = true
	n.notify = func(context.Context, string, map[string]string) error {
		n.mu.Lock()
		n.deliveryReady = false
		n.mu.Unlock()
		return errDestinationUnavailable
	}
	n.mu.Lock()
	n.enqueueLocked(&pb.Event{SessionId: threadB, NodeId: "peer", MessageId: "one", Kind: "direct", Text: "hello"}, "", "", 1)
	n.mu.Unlock()
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.deliver(n.ctx) }()
	eventually(t, func() bool { n.mu.Lock(); defer n.mu.Unlock(); return !n.deliveryReady })
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.incoming) != 1 || n.stats.Dropped != 0 || n.stats.QueueHandoffs != 0 {
		t.Fatal("unavailable destination lost event")
	}
}

func TestCodexCapturedCallerContract(t *testing.T) {
	b, err := os.ReadFile("testdata/codex-0.163.0-call.json")
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Params *mcp.CallToolParamsRaw `json:"params"`
	}
	if err := json.Unmarshal(b, &wire); err != nil {
		t.Fatal(err)
	}
	thread, err := codexCaller(&mcp.CallToolRequest{Params: wire.Params})
	if err != nil || thread != threadA {
		t.Fatal("captured caller contract rejected", thread, err)
	}
	for _, bad := range []any{nil, 1, "name-not-uuid", map[string]any{"threadId": threadA}} {
		wire.Params.Meta["threadId"] = bad
		if _, err := codexCaller(&mcp.CallToolRequest{Params: wire.Params}); err == nil {
			t.Fatal("invalid metadata accepted")
		}
	}
}
