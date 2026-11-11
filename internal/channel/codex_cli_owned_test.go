package channel

import (
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

// Reproduces the 2026-11-11 hold: a thread owned by a standalone `codex` CLI
// reads as notLoaded in the managed daemon, and every message stayed held with
// zero handoff attempts. A CLI-owned thread now gets the shared-queue handoff.
func TestCodexNotLoadedCLIOwnedThreadIsDelivered(t *testing.T) {
	n, f, q := cliOwnedNode(t, true)
	if !n.codex.scan(n.ctx, n, 1) && !n.deliveryReady {
		t.Fatalf("collector kept delivery closed: %q", n.collectionState)
	}
	if n.collectionState != codexCLIOwnedState {
		t.Fatalf("state %q", n.collectionState)
	}
	hold(t, n, "m1")
	n.flushCodex(n.ctx, threadA)
	if got := q.sent(); len(got) != 1 || !strings.Contains(got[0], "m1") {
		t.Fatalf("want one queue handoff, got %q", got)
	}
	for _, c := range f.calls {
		if c == "turn/steer" {
			t.Fatal("steer must not be tried on a thread the daemon has not loaded")
		}
	}
	// Later events within the interval coalesce into one later handoff instead
	// of one CLI turn each.
	hold(t, n, "m2", "m3")
	n.flushCodex(n.ctx, threadA)
	if len(q.sent()) != 1 || outboxHeld(n) != 2 {
		t.Fatalf("want m2, m3 held, sent %d held %d", len(q.sent()), outboxHeld(n))
	}
	n.mu.Lock()
	n.codexOut.cliQueuedAt = time.Now().Add(-codexCLIQueueInterval)
	n.mu.Unlock()
	n.flushCodex(n.ctx, threadA)
	got := q.sent()
	if len(got) != 2 || !strings.Contains(got[1], "m2") || !strings.Contains(got[1], "m3") || outboxHeld(n) != 0 {
		t.Fatalf("want m2+m3 in one handoff, got %q", got)
	}
	if n.stats.QueueHandoffs != 3 {
		t.Fatalf("QueueHandoffs %d", n.stats.QueueHandoffs)
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
