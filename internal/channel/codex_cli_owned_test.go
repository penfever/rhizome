package channel

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func cliOwnedNode(t *testing.T, owned bool) (*Node, *fakeCodex, *queueRecorder) {
	t.Helper()
	f := &fakeCodex{thread: threadA, status: codexNotLoaded}
	n, q := codexOutboxNode(t, true, f)
	n.codexOut.cliOwned = func() bool { return owned }
	return n, f, q
}

// Reproduces the 2026-11-14 backlog: a thread owned by a standalone Codex
// process (`codex exec … resume`) reads as notLoaded in the daemon, and the
// gateway handed events to `codex queue`. The owning process never reads the
// daemon's queue store, so items reached its 100-item cap and every later
// handoff failed while the outbox filled. Such a thread is never queued; its
// next tool-call hook carries the held events.
func TestCodexCLIOwnedThreadDeliversThroughHooks(t *testing.T) {
	n, f, q := cliOwnedNode(t, true)
	if !n.codex.scan(n.ctx, n, 1) && !n.deliveryReady {
		t.Fatalf("collector kept delivery closed: %q", n.collectionState)
	}
	if n.collectionState != codexCLIOwnedState {
		t.Fatalf("state %q", n.collectionState)
	}
	hold(t, n, "m1", "m2")
	n.flushCodex(n.ctx, threadA)
	if len(q.sent()) != 0 || outboxHeld(n) != 2 {
		t.Fatalf("CLI-owned thread was queued: sent %q held %d", q.sent(), outboxHeld(n))
	}
	for _, c := range f.calls {
		if c == "turn/steer" {
			t.Fatal("steer must not be tried on a thread the daemon has not loaded")
		}
	}
	got := hookContext(n.codexHookOutput(context.Background(), "PreToolUse", time.Now()))
	if !strings.Contains(got, "m1") || !strings.Contains(got, "m2") || !strings.Contains(got, "<rhizome-peer-event>") {
		t.Fatalf("hook context %q", got)
	}
	if outboxHeld(n) != 0 || n.stats.HookInjected != 2 {
		t.Fatalf("held %d injected %d", outboxHeld(n), n.stats.HookInjected)
	}
	// Each event is injected once.
	if again := n.codexHookOutput(context.Background(), "PreToolUse", time.Now()); len(again) != 0 {
		t.Fatalf("second hook re-delivered: %v", again)
	}
	n.mu.Lock()
	last := n.codexOut.last
	n.mu.Unlock()
	if last == nil || last.Mode != "hook" || last.Events != 2 {
		t.Fatalf("last handoff %+v", last)
	}
}

// A large backlog drains over several hooks, each within codexHookChunk.
func TestCodexHookOutputChunksBacklog(t *testing.T) {
	n, _, _ := cliOwnedNode(t, true)
	body := strings.Repeat("x", 6*1024)
	for i := 0; i < 10; i++ {
		if err := n.holdForCodex(context.Background(), body, map[string]string{"message_id": fmt.Sprint("m", i)}); err != nil {
			t.Fatal(err)
		}
	}
	hooks := 0
	for outboxHeld(n) > 0 {
		got := hookContext(n.codexHookOutput(context.Background(), "UserPromptSubmit", time.Now()))
		if got == "" || len(got) > codexHookChunk {
			t.Fatalf("hook %d context of %d bytes", hooks, len(got))
		}
		hooks++
		if hooks > 10 {
			t.Fatal("backlog did not drain")
		}
	}
	if hooks < 2 || n.stats.HookInjected != 10 {
		t.Fatalf("hooks %d injected %d", hooks, n.stats.HookInjected)
	}
}

// Hooks that cannot carry context, spent budgets, and in-flight flushes leave
// held events alone.
func TestCodexHookOutputLeavesEventsHeld(t *testing.T) {
	n, _, _ := cliOwnedNode(t, true)
	hold(t, n, "m1")
	if out := n.codexHookOutput(context.Background(), "Stop", time.Now()); len(out) != 0 {
		t.Fatalf("Stop hook drained: %v", out)
	}
	if out := n.codexHookOutput(context.Background(), "PreToolUse", time.Now().Add(-hookDeliveryBudget)); len(out) != 0 {
		t.Fatalf("late hook drained: %v", out)
	}
	n.mu.Lock()
	n.codexOut.flushing = true
	n.mu.Unlock()
	if out := n.codexHookOutput(context.Background(), "PreToolUse", time.Now()); len(out) != 0 {
		t.Fatalf("hook drained during a flush: %v", out)
	}
	if outboxHeld(n) != 1 {
		t.Fatalf("held %d", outboxHeld(n))
	}
}

// A daemon-loaded idle thread is still queued; a hook in the same window
// finds nothing left, so the event goes out once.
func TestCodexIdleThreadQueuedNotDuplicatedByHook(t *testing.T) {
	f := &fakeCodex{thread: threadA, status: "idle"}
	n, q := codexOutboxNode(t, true, f)
	hold(t, n, "m1")
	n.flushCodex(n.ctx, threadA)
	if len(q.sent()) != 1 || outboxHeld(n) != 0 {
		t.Fatalf("sent %d held %d", len(q.sent()), outboxHeld(n))
	}
	if out := n.codexHookOutput(context.Background(), "PreToolUse", time.Now()); len(out) != 0 {
		t.Fatalf("hook re-delivered a queued event: %v", out)
	}
}

// A notLoaded thread with no live owning CLI stays held, visibly.
func TestCodexNotLoadedWithoutCLIStaysHeld(t *testing.T) {
	n, _, q := cliOwnedNode(t, false)
	n.codex.scan(n.ctx, n, 1)
	if n.deliveryReady || n.collectionState != codexNotLoaded {
		t.Fatalf("ready %v state %q", n.deliveryReady, n.collectionState)
	}
	hold(t, n, "m1")
	n.flushCodex(n.ctx, threadA)
	n.mu.Lock()
	st := n.codexOut.status()
	n.mu.Unlock()
	if len(q.sent()) != 0 || st["held"] != 1 || !strings.Contains(st["state"].(string), codexNotLoaded) {
		t.Fatalf("sent %q status %v", q.sent(), st)
	}
}

func TestIsCodexCLICommand(t *testing.T) {
	for args, want := range map[string]bool{
		"/opt/homebrew/lib/node_modules/@openai/codex/vendor/aarch64-apple-darwin/bin/codex --yolo\n": true,
		"codex resume 01a16e8e-0a38-7f50-8a42-ab2d5e4ce139":                                           true,
		"/Users/x/.codex/packages/app-server-daemon/bin/codex app-server --listen unix://":            false,
		"/usr/bin/node /usr/lib/codex.js":                                                             false,
		"":                                                                                            false,
	} {
		if got := isCodexCLICommand(args); got != want {
			t.Errorf("%q: got %v want %v", args, got, want)
		}
	}
}

// A sender sees that a message to a Codex thread is held, not just accepted.
func TestReceiverDeliveryReportsHeldCodex(t *testing.T) {
	n, _, _ := cliOwnedNode(t, false)
	n.external = true
	n.codex.scan(n.ctx, n, 1)
	n.mu.Lock()
	state := n.receiverDeliveryLocked()
	n.mu.Unlock()
	if !strings.HasPrefix(state, "held") || !strings.Contains(state, codexNotLoaded) {
		t.Fatalf("state %q", state)
	}
}
