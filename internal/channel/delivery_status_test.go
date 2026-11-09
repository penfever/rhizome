package channel

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeliveryStatusFindsEnvelopeInReceiverLog(t *testing.T) {
	hh := newHistoryHarness(t)
	dir := filepath.Join(hh.o.opts.CodexRoot, "2026", "11", "09")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	const delivered, held = "07af44afa8f00517c99b8b7fdd091dc4", "567db27fbc783a4dafbea6648a69f7b6"
	envelope := peerEnvelope("[direct] hello", map[string]string{"message_id": delivered, "session_id": "sender"})
	// Padding larger than one scan chunk puts the match across chunk reads.
	padding := codexAssistant("pad", "2026-11-09T20:00:00Z", strings.Repeat("p", deliveryScanChunk+17))
	appendLog(t, filepath.Join(dir, "rollout-r.jsonl"), codexMeta(threadA, "/work", "2026-11-09T19:00:00Z")+codexUser("u1", "2026-11-09T21:07:01.390Z", envelope)+padding)
	hh.index()
	check := func(id string) map[string]any {
		t.Helper()
		out, e := hh.h.deliveryStatus(context.Background(), deliveryStatusInput{SessionID: threadA, AgentType: "codex", MessageID: id})
		if e != nil {
			t.Fatal(e)
		}
		return out.(map[string]any)
	}
	if got := check(delivered); got["state"] != "in_transcript" || got["recorded_at"] != "2026-11-09T21:07:01.390Z" {
		t.Fatalf("delivered message: %v", got)
	}
	if got := check(held); got["state"] != "not_in_transcript" {
		t.Fatalf("held message: %v", got)
	}
	if _, e := hh.h.deliveryStatus(context.Background(), deliveryStatusInput{SessionID: threadA, AgentType: "codex", MessageID: "not-hex"}); e == nil || e.Code != "INVALID_ARGUMENT" {
		t.Fatal("malformed message_id accepted")
	}
	if _, e := hh.h.deliveryStatus(context.Background(), deliveryStatusInput{SessionID: threadB, AgentType: "codex", MessageID: held}); e == nil || e.Code != "SESSION_NOT_FOUND" {
		t.Fatal("unknown session not reported")
	}
}
