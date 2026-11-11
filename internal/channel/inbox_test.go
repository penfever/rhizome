package channel

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "rhizome/proto"
)

// A direct send is mirrored to the receiver's durable inbox at acceptance, and
// the sender learns how the receiver will deliver it.
func TestDirectSendMirroredToDurableInbox(t *testing.T) {
	a, b := pair(t)
	ctx := context.Background()
	r, err := a.sendDetailed(ctx, "session-b", "please check the run", "")
	if err != nil || !r.Accepted {
		t.Fatalf("send %v %v", r, err)
	}
	if !strings.Contains(r.ReceiverDelivery, "queued for delivery") || !strings.Contains(r.ReceiverDelivery, "durable inbox") {
		t.Fatalf("receiver_delivery %q", r.ReceiverDelivery)
	}
	if n, err := UnreadInboxCount(b.runtime, "session-b"); err != nil || n != 1 {
		t.Fatalf("unread %d %v", n, err)
	}
	if st := inboxState(b.runtime, "session-b", r.MessageID); st != "unread" {
		t.Fatalf("inbox state %q", st)
	}
	msgs, err := ReadInbox(b.runtime, "session-b", false, true)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read %v %v", msgs, err)
	}
	m := msgs[0]
	if m.Text != "please check the run" || m.FromSession != "session-a" || m.Kind != "direct" || m.MessageID != r.MessageID {
		t.Fatalf("record %+v", m)
	}
	if st := inboxState(b.runtime, "session-b", r.MessageID); st != "read" {
		t.Fatalf("inbox state after read %q", st)
	}
	if again, _ := ReadInbox(b.runtime, "session-b", false, true); len(again) != 0 {
		t.Fatalf("read twice: %v", again)
	}
	// The log is never consumed: --all still shows it, and native delivery still
	// queued it.
	if all, _ := ReadInbox(b.runtime, "session-b", true, false); len(all) != 1 {
		t.Fatalf("all %v", all)
	}
	b.mu.Lock()
	queued := len(b.incoming)
	b.mu.Unlock()
	if queued != 1 {
		t.Fatalf("native queue %d", queued)
	}
	st, err := os.Stat(filepath.Join(b.runtime, inboxDirName, "session-b.jsonl"))
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("inbox file %v %v", st, err)
	}
}

// Activity feed events are not mail; only direct messages and announcements
// reach the inbox. A full native queue still records the message.
func TestInboxRecordsMailOnly(t *testing.T) {
	n := node(t)
	n.runtime = t.TempDir()
	n.identity.SessionId = "receiver"
	n.identity.Generation = 1
	n.mu.Lock()
	n.enqueueLocked(&pb.Event{SessionId: "peer", NodeId: "p", StreamId: "s", Sequence: 1, Kind: "activity", Text: "edited", MessageId: id()}, "", "", 1)
	n.enqueueLocked(&pb.Event{SessionId: "peer", NodeId: "p", StreamId: "s", Sequence: 2, Kind: "announcement", Text: "done", MessageId: id()}, "", "", 1)
	for len(n.incoming) < maxQueue {
		n.incoming = append(n.incoming, pending{})
	}
	accepted := n.enqueueLocked(&pb.Event{SessionId: "peer", NodeId: "p", StreamId: "s", Sequence: 3, Kind: "direct", Text: "overflow", MessageId: id()}, "", "", 1)
	n.mu.Unlock()
	if accepted {
		t.Fatal("full queue accepted")
	}
	msgs, err := ReadInbox(n.runtime, "receiver", false, false)
	if err != nil || len(msgs) != 2 || msgs[0].Text != "done" || msgs[1].Text != "overflow" {
		t.Fatalf("inbox %+v %v", msgs, err)
	}
}

func TestWaitInbox(t *testing.T) {
	runtime := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := WaitInbox(ctx, runtime, "s1", 10*time.Millisecond, nil); !errors.Is(err, ErrInboxTimeout) {
		t.Fatalf("empty inbox: %v", err)
	}
	done := make(chan int, 1)
	go func() {
		n, err := WaitInbox(context.Background(), runtime, "s1", 10*time.Millisecond, nil)
		if err != nil {
			t.Error(err)
		}
		done <- n
	}()
	time.Sleep(50 * time.Millisecond)
	if err := appendInbox(runtime, "s1", InboxMessage{MessageID: "m1", Kind: "direct", Text: "wake"}); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-done:
		if n != 1 {
			t.Fatalf("count %d", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait-inbox did not wake on new mail")
	}
	// Unread mail present at start returns at once; read mail does not.
	if n, err := WaitInbox(context.Background(), runtime, "s1", time.Hour, nil); err != nil || n != 1 {
		t.Fatalf("unread at start: %d %v", n, err)
	}
	if _, err := ReadInbox(runtime, "s1", false, true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := WaitInbox(ctx, runtime, "s1", 10*time.Millisecond, nil); !errors.Is(err, ErrInboxTimeout) {
		t.Fatalf("read inbox: %v", err)
	}
}

// A record being written (no trailing newline yet) is not reported or skipped.
func TestInboxPartialLine(t *testing.T) {
	runtime := t.TempDir()
	if err := appendInbox(runtime, "s1", InboxMessage{MessageID: "m1", Text: "one"}); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(runtime, inboxDirName, "s1.jsonl")
	f, err := os.OpenFile(log, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"message_id":"m2","text":"tw`)
	if msgs, _ := ReadInbox(runtime, "s1", false, true); len(msgs) != 1 {
		t.Fatalf("partial: %v", msgs)
	}
	f.WriteString(`o"}` + "\n")
	f.Close()
	msgs, _ := ReadInbox(runtime, "s1", false, true)
	if len(msgs) != 1 || msgs[0].Text != "two" {
		t.Fatalf("completed: %v", msgs)
	}
}

func TestInboxRejectsPathSessionIDs(t *testing.T) {
	for _, s := range []string{"../x", "a/b", "", strings.Repeat("a", 129)} {
		if _, err := ReadInbox(t.TempDir(), s, false, false); err == nil {
			t.Errorf("%q accepted", s)
		}
	}
}

func TestWaitInboxWatchedProcessExit(t *testing.T) {
	calls := 0
	alive := func() bool { calls++; return calls < 3 }
	if _, err := WaitInbox(context.Background(), t.TempDir(), "s1", time.Millisecond, alive); !errors.Is(err, ErrWatchedExited) {
		t.Fatalf("got %v", err)
	}
}
