package channel

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	pb "rhizome/proto"
)

type queueRecorder struct {
	mu       sync.Mutex
	messages []string
	err      error
}

func (q *queueRecorder) queue(_ context.Context, message string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	q.messages = append(q.messages, message)
	return nil
}
func (q *queueRecorder) sent() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.messages...)
}

func codexOutboxNode(t *testing.T, steer bool, f *fakeCodex) (*Node, *queueRecorder) {
	t.Helper()
	n := node(t)
	n.identity.SessionId = threadA
	n.identity.Generation = 1
	n.codex = newCodexCollector(threadA, f)
	q := &queueRecorder{}
	n.codexOut = newCodexOutbox(steer, nil, q.queue)
	return n, q
}

func hold(t *testing.T, n *Node, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := n.holdForCodex(context.Background(), "[direct] "+id, map[string]string{"message_id": id}); err != nil {
			t.Fatal(err)
		}
	}
}

func outboxHeld(n *Node) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.codexOut.held)
}

// Reproduces the lost-message report: four sends during one long Codex turn.
// Before the outbox each became its own queue item, so the last waited four
// turns. Now the first is queued for the turn and the rest go together at the
// next turn boundary.
func TestCodexQueueModeOneItemPerTurn(t *testing.T) {
	f := &fakeCodex{thread: threadA, status: "active", turns: []codexTurn{{"t1", "inProgress"}}}
	n, q := codexOutboxNode(t, false, f)
	hold(t, n, "m1")
	n.flushCodex(n.ctx, threadA)
	hold(t, n, "m2", "m3", "m4")
	n.flushCodex(n.ctx, threadA)
	n.flushCodex(n.ctx, threadA)
	if got := q.sent(); len(got) != 1 || !strings.Contains(got[0], "m1") {
		t.Fatalf("want one item for turn t1, got %q", got)
	}
	n.mu.Lock()
	st := n.codexOut.status()
	n.mu.Unlock()
	if st["held"] != 3 || !strings.Contains(st["state"].(string), "t1") {
		t.Fatalf("held events not visible: %v", st)
	}
	f.mu.Lock()
	f.turns = []codexTurn{{"t2", "inProgress"}}
	f.mu.Unlock()
	n.flushCodex(n.ctx, threadA)
	got := q.sent()
	if len(got) != 2 || outboxHeld(n) != 0 {
		t.Fatalf("turn change did not flush the backlog as one item: %q", got)
	}
	for _, id := range []string{"m2", "m3", "m4"} {
		if !strings.Contains(got[1], id) {
			t.Fatalf("coalesced item lost %s", id)
		}
	}
	if strings.Count(got[1], "<rhizome-peer-event>") != 3 {
		t.Fatal("each event must keep its own envelope")
	}
	f.mu.Lock()
	f.status, f.turns = "idle", nil
	f.mu.Unlock()
	hold(t, n, "m5")
	n.flushCodex(n.ctx, threadA)
	if len(q.sent()) != 3 || n.stats.QueueHandoffs != 5 {
		t.Fatalf("idle thread not queued at once: %d handoffs", n.stats.QueueHandoffs)
	}
}

func TestCodexSteerModeInjectsIntoRunningTurn(t *testing.T) {
	f := &fakeCodex{thread: threadA, status: "active", turns: []codexTurn{{"t1", "inProgress"}}}
	n, q := codexOutboxNode(t, true, f)
	hold(t, n, "m1", "m2")
	n.flushCodex(n.ctx, threadA)
	if len(f.steered) != 1 || f.steered[0].turn != "t1" || !strings.Contains(f.steered[0].text, "m1") || !strings.Contains(f.steered[0].text, "m2") {
		t.Fatalf("not steered as one input: %+v", f.steered)
	}
	if len(q.sent()) != 0 || n.stats.Steered != 2 || outboxHeld(n) != 0 {
		t.Fatal("steer also queued or miscounted")
	}
	n.mu.Lock()
	last := n.codexOut.last
	n.mu.Unlock()
	if last == nil || last.Mode != "steer" || len(last.MessageIDs) != 2 {
		t.Fatalf("last handoff not recorded: %+v", last)
	}
	// An idle thread has no turn to steer; queueing starts one.
	f.mu.Lock()
	f.status, f.turns = "idle", nil
	f.mu.Unlock()
	hold(t, n, "m3")
	n.flushCodex(n.ctx, threadA)
	if len(q.sent()) != 1 || len(f.steered) != 1 {
		t.Fatal("idle thread was not queued")
	}
}

func TestCodexSteerFailureRetainsThenQueues(t *testing.T) {
	f := &fakeCodex{thread: threadA, status: "active", turns: []codexTurn{{"t1", "inProgress"}}, steerErr: &appRPCError{-32600, "turn is not steerable", "turn/steer"}}
	n, q := codexOutboxNode(t, true, f)
	hold(t, n, "m1")
	n.flushCodex(n.ctx, threadA)
	if outboxHeld(n) != 1 || len(q.sent()) != 0 || n.stats.SteerFailures != 1 || n.stats.Dropped != 0 {
		t.Fatal("failed steer did not retain the event")
	}
	n.mu.Lock()
	n.codexOut.held[0].heldAt = time.Now().Add(-codexSteerFallback)
	n.mu.Unlock()
	n.flushCodex(n.ctx, threadA)
	if outboxHeld(n) != 0 || len(q.sent()) != 1 {
		t.Fatal("persistent steer failure did not fall back to the queue")
	}
}

func TestCodexQueueFailures(t *testing.T) {
	f := &fakeCodex{thread: threadA, status: "idle"}
	n, q := codexOutboxNode(t, false, f)
	exitErr := exec.Command("/bin/sh", "-c", "exit 3").Run()
	var exit *exec.ExitError
	if !errors.As(exitErr, &exit) {
		t.Fatal("no exit error")
	}
	q.err = exitErr
	hold(t, n, "m1")
	n.flushCodex(n.ctx, threadA)
	if outboxHeld(n) != 1 || n.stats.QueueFailures != 1 || n.deliveryError == "" {
		t.Fatal("rejected queue command lost the event")
	}
	q.err = context.DeadlineExceeded
	n.flushCodex(n.ctx, threadA)
	n.mu.Lock()
	st := n.codexOut.status()
	n.mu.Unlock()
	if st["held"] != 0 || st["uncertain"] == nil {
		t.Fatalf("uncertain handoff must be reported, not retried: %v", st)
	}
	q.err = nil
	n.flushCodex(n.ctx, threadA)
	if len(q.sent()) != 0 {
		t.Fatal("uncertain handoff was retried")
	}
}

func TestCodexOutboxRetainsWhileThreadUnreadable(t *testing.T) {
	f := &fakeCodex{thread: threadA, status: "idle", fail: true}
	n, q := codexOutboxNode(t, true, f)
	hold(t, n, "m1")
	n.flushCodex(n.ctx, threadA)
	n.mu.Lock()
	state := n.codexOut.state
	n.mu.Unlock()
	if outboxHeld(n) != 1 || len(q.sent()) != 0 || !strings.Contains(state, "unreadable") {
		t.Fatalf("unreadable thread: held %d state %q", outboxHeld(n), state)
	}
	f.mu.Lock()
	f.fail = false
	f.mu.Unlock()
	n.flushCodex(n.ctx, threadA)
	if outboxHeld(n) != 0 || len(q.sent()) != 1 {
		t.Fatal("event not delivered after the thread became readable")
	}
}

func TestCodexOutboxFullKeepsEventsInGatewayQueue(t *testing.T) {
	n, _ := codexOutboxNode(t, false, &fakeCodex{thread: threadA, status: "active"})
	big := strings.Repeat("x", codexOutboxLimit/2)
	if err := n.holdForCodex(context.Background(), big, nil); err != nil {
		t.Fatal(err)
	}
	if err := n.holdForCodex(context.Background(), big, nil); !errors.Is(err, errDestinationUnavailable) {
		t.Fatalf("full outbox accepted an event: %v", err)
	}
}

// Through the real delivery loop: sends during one turn are never dropped and
// reach Codex in at most one queue item per turn.
func TestCodexDeliveryLoopCoalescesDirectSends(t *testing.T) {
	f := &fakeCodex{thread: threadA, status: "active", turns: []codexTurn{{"t1", "inProgress"}}}
	n, q := codexOutboxNode(t, false, f)
	n.external, n.ready, n.deliveryReady = true, true, true
	n.notify = n.holdForCodex
	n.mu.Lock()
	for _, id := range []string{"a1", "a2", "a3", "a4"} {
		n.enqueueLocked(&pb.Event{SessionId: threadB, NodeId: "peer", MessageId: id, Kind: "direct", Text: id}, "", "", 1)
	}
	n.mu.Unlock()
	n.wg.Add(2)
	go func() { defer n.wg.Done(); n.deliver(n.ctx) }()
	go func() { defer n.wg.Done(); n.runCodexOutbox(n.ctx, threadA) }()
	eventually(t, func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		return len(n.incoming) == 0 && n.stats.QueueHandoffs > 0
	})
	time.Sleep(100 * time.Millisecond)
	if len(q.sent()) != 1 {
		t.Fatalf("more than one item queued during turn t1: %d", len(q.sent()))
	}
	f.mu.Lock()
	f.turns = []codexTurn{{"t2", "inProgress"}}
	f.mu.Unlock()
	eventually(t, func() bool { return outboxHeld(n) == 0 })
	all := strings.Join(q.sent(), "\n")
	for _, id := range []string{"a1", "a2", "a3", "a4"} {
		if !strings.Contains(all, id) {
			t.Fatalf("%s never delivered", id)
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(q.messages) > 2 || n.stats.Dropped != 0 || n.stats.QueueHandoffs != 4 {
		t.Fatalf("items %d dropped %d handoffs %d", len(q.messages), n.stats.Dropped, n.stats.QueueHandoffs)
	}
}
