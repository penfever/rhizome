package channel

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// Codex starts one turn per `codex queue` item, and only after the running
// turn ends. Handing every peer event to the queue separately therefore made
// the N-th message wait N turns; with long turns that was hours, while send
// still reported success. The gateway instead holds events in a bounded
// per-thread outbox and flushes them as one coalesced message:
//
//   - idle thread: `codex queue`, which starts a turn at once;
//   - running turn, codex.delivery = "steer": turn/steer, so the model reads it
//     at its next step, as when the user types during a turn;
//   - running turn, codex.delivery = "queue": one Rhizome item per turn at
//     most; later events wait in the outbox and go together.
//
// Held events are visible in list_sessions (codex_outbox). A handoff whose
// acceptance is unknown (a timed-out CLI) is reported there, never retried.
const (
	codexOutboxLimit   = 64 * 1024
	codexFlushInterval = 2 * time.Second
	// codexSteerFallback bounds how long steering may fail (compaction, review,
	// approval prompts) before held events are queued for the next turn instead.
	codexSteerFallback = time.Minute
	codexUncertainKept = 32
)

type codexHeld struct {
	envelope  string
	messageID string
	heldAt    time.Time
}

// codexHandoff records one flush for list_sessions.
type codexHandoff struct {
	Mode       string    `json:"mode"`
	At         time.Time `json:"at"`
	TurnID     string    `json:"turn_id,omitempty"`
	Events     int       `json:"events"`
	MessageIDs []string  `json:"message_ids,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// codexOutbox is guarded by Node.mu.
type codexOutbox struct {
	steer bool
	queue func(ctx context.Context, message string) error
	wake  chan struct{}

	held  []codexHeld
	bytes int
	// queuedTurn is the turn that was active at the last queue handoff ("" when
	// the thread was idle); queued reports whether any handoff happened yet.
	queuedTurn string
	queued     bool
	state      string
	last       *codexHandoff
	uncertain  []codexHandoff
}

func newCodexOutbox(steer bool, queue func(context.Context, string) error) *codexOutbox {
	return &codexOutbox{steer: steer, queue: queue, wake: make(chan struct{}, 1), state: "empty"}
}

func (o *codexOutbox) mode() string {
	if o.steer {
		return "steer"
	}
	return "queue"
}

// holdForCodex is the Codex notify function. A full outbox reports the
// destination unavailable, so the delivery loop retains the event.
func (n *Node) holdForCodex(_ context.Context, text string, meta map[string]string) error {
	envelope := peerEnvelope(text, meta)
	n.mu.Lock()
	defer n.mu.Unlock()
	o := n.codexOut
	if o.bytes+len(envelope) > codexOutboxLimit {
		o.state = "outbox full; later events wait in the gateway queue"
		return errDestinationUnavailable
	}
	o.held = append(o.held, codexHeld{envelope: envelope, messageID: meta["message_id"], heldAt: time.Now()})
	o.bytes += len(envelope)
	select {
	case o.wake <- struct{}{}:
	default:
	}
	return nil
}

func (n *Node) runCodexOutbox(ctx context.Context, thread string) {
	ticker := time.NewTicker(codexFlushInterval)
	defer ticker.Stop()
	n.mu.Lock()
	wake := n.codexOut.wake
	n.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
		n.flushCodex(ctx, thread)
	}
}

// codexActiveTurn reports the thread status type and, while active, the id of
// its in-progress turn ("" when none is listed yet).
func codexActiveTurn(ctx context.Context, r codexReader, thread string) (string, string, error) {
	var read struct {
		Thread struct {
			Status struct {
				Type string `json:"type"`
			} `json:"status"`
		} `json:"thread"`
	}
	if err := r.call(ctx, "thread/read", map[string]any{"threadId": thread}, &read); err != nil {
		return "", "", err
	}
	status := read.Thread.Status.Type
	if status != "active" {
		return status, "", nil
	}
	var page turnPage
	if err := r.call(ctx, "thread/turns/list", map[string]any{"threadId": thread, "limit": 1, "itemsView": "notLoaded", "sortDirection": "desc"}, &page); err != nil {
		return "", "", err
	}
	if len(page.Data) == 0 || page.Data[0].Status != "inProgress" {
		return status, "", nil
	}
	return status, page.Data[0].ID, nil
}

// flushCodex hands the held batch to Codex once, as one message.
func (n *Node) flushCodex(ctx context.Context, thread string) {
	n.mu.Lock()
	o := n.codexOut
	if len(o.held) == 0 {
		n.mu.Unlock()
		return
	}
	batch := append([]codexHeld(nil), o.held...)
	reader := n.codex.reader
	n.mu.Unlock()

	status, turn, err := codexActiveTurn(ctx, reader, thread)
	if err != nil {
		n.holdCodex(fmt.Sprintf("Codex thread unreadable (%v); %d held", err, len(batch)))
		return
	}
	envelopes := make([]string, len(batch))
	for i, h := range batch {
		envelopes[i] = h.envelope
	}
	message := strings.Join(envelopes, "\n\n")
	mode := "queue"
	switch {
	case status == "idle":
		turn = ""
		err = o.queue(ctx, message)
	case status != "active":
		n.holdCodex(fmt.Sprintf("Codex thread status %q; %d held", status, len(batch)))
		return
	case turn == "":
		n.holdCodex(fmt.Sprintf("Codex thread active without a listed turn; %d held", len(batch)))
		return
	case o.steer:
		mode = "steer"
		err = reader.steer(ctx, thread, turn, message)
		if err != nil && time.Since(batch[0].heldAt) >= codexSteerFallback && n.mayQueueFor(turn) {
			mode = "queue"
			err = o.queue(ctx, message)
		}
	case n.mayQueueFor(turn):
		err = o.queue(ctx, message)
	default:
		n.holdCodex(fmt.Sprintf("%d held for the end of Codex turn %s (one Rhizome item is already queued for it)", len(batch), turn))
		return
	}
	n.finishCodex(batch, mode, turn, err)
}

func (n *Node) mayQueueFor(turn string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.codexOut.queued || n.codexOut.queuedTurn != turn
}

func (n *Node) holdCodex(state string) {
	n.mu.Lock()
	n.codexOut.state = state
	n.mu.Unlock()
}

// finishCodex removes a delivered batch. A failed steer, or a queue command
// that exited with an error, keeps the batch for the next flush; a queue
// command whose outcome is unknown (timeout, kill) moves it to uncertain.
func (n *Node) finishCodex(batch []codexHeld, mode, turn string, err error) {
	ids := make([]string, 0, len(batch))
	for _, h := range batch {
		if h.messageID != "" {
			ids = append(ids, h.messageID)
		}
	}
	record := &codexHandoff{Mode: mode, At: time.Now(), TurnID: turn, Events: len(batch), MessageIDs: ids}
	n.mu.Lock()
	defer n.mu.Unlock()
	o := n.codexOut
	var exit *exec.ExitError
	if err != nil {
		record.Error = err.Error()
		n.deliveryError = err.Error()
		if mode == "steer" {
			n.stats.SteerFailures++
		} else {
			n.stats.QueueFailures++
		}
		if mode == "steer" || errors.As(err, &exit) {
			o.state = fmt.Sprintf("%s failed, %d held for retry: %v", mode, len(batch), err)
			return
		}
		o.uncertain = append(o.uncertain, *record)
		if len(o.uncertain) > codexUncertainKept {
			o.uncertain = o.uncertain[1:]
		}
		o.state = "last queue handoff has unknown acceptance and was not retried; see uncertain"
	} else {
		n.deliveryError = ""
		if mode == "steer" {
			n.stats.Steered += uint64(len(batch))
		} else {
			n.stats.QueueHandoffs += uint64(len(batch))
			o.queued, o.queuedTurn = true, turn
		}
		o.state = "delivered"
		o.last = record
	}
	o.held = o.held[len(batch):]
	o.bytes = 0
	for _, h := range o.held {
		o.bytes += len(h.envelope)
	}
	if len(o.held) > 0 {
		select {
		case o.wake <- struct{}{}:
		default:
		}
	}
}

// codexOutboxStatus is the list_sessions view; the caller holds n.mu.
func (o *codexOutbox) status() map[string]any {
	ids := []string{}
	for _, h := range o.held {
		if h.messageID != "" {
			ids = append(ids, h.messageID)
		}
	}
	out := map[string]any{"mode": o.mode(), "held": len(o.held), "held_message_ids": ids, "state": o.state}
	if len(o.held) > 0 {
		out["oldest_held_at"] = o.held[0].heldAt
	}
	if o.last != nil {
		out["last_handoff"] = o.last
	}
	if len(o.uncertain) > 0 {
		out["uncertain"] = o.uncertain
	}
	return out
}
