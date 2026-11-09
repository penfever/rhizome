package channel

import (
	"bytes"
	"context"
	"io"
	"os"
	"regexp"
	"time"
)

// send reports acceptance by the receiver's gateway, which can hold an event
// (Codex outbox, Claude hook inbox, Codex's own queue) long before the model
// sees it. delivery_status answers the sender's real question from the
// receiver's native log on this device: a delivered event's envelope, with its
// unique message_id, is in the transcript; a held one is not.
const (
	deliveryScanLimit = 128 << 20
	deliveryScanChunk = 1 << 20
)

var messageIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
var lineTimestamp = regexp.MustCompile(`"timestamp"\s*:\s*"([^"]{10,40})"`)

type deliveryStatusInput struct {
	SessionID string `json:"session_id" jsonschema:"exact receiver session_id"`
	AgentType string `json:"agent_type" jsonschema:"receiver agent type: claude, codex, opencode or pi"`
	MessageID string `json:"message_id" jsonschema:"message_id returned by send"`
}

func (h *historyService) deliveryStatus(ctx context.Context, in deliveryStatusInput) (any, *historyError) {
	if !messageIDPattern.MatchString(in.MessageID) {
		return nil, herr("INVALID_ARGUMENT", "message_id must be the 32-hex id returned by send", false)
	}
	if in.SessionID == "" || len(in.SessionID) > 128 || !validHistoryAgent(in.AgentType) {
		return nil, herr("INVALID_ARGUMENT", "session_id and agent_type (claude, codex, opencode or pi) are required", false)
	}
	c, running, e := h.catalog.load()
	if e != nil {
		return nil, e
	}
	s := c.sessions()[sessionKey{in.AgentType, in.SessionID}]
	if s == nil {
		return nil, &historyError{Code: "SESSION_NOT_FOUND", Message: "no cataloged native log for this session on this device; delivery cannot be confirmed here", Extra: map[string]any{"catalog": catalogState(c, running)}}
	}
	src := s.source()
	if src == nil {
		return nil, herr("SESSION_AMBIGUOUS", "the session has no single readable native log", false)
	}
	if e := h.scope.confine(in.AgentType, src.Path); e != nil {
		return nil, e
	}
	at, scanned, found, err := findInTail(ctx, src.Path, []byte(in.MessageID), deliveryScanLimit)
	if err != nil {
		return nil, herr("HISTORY_UNAVAILABLE", "the native log could not be read", true)
	}
	out := map[string]any{"message_id": in.MessageID, "session_id": in.SessionID, "scanned_bytes": scanned}
	if found {
		out["state"] = "in_transcript"
		if at != "" {
			out["recorded_at"] = at
		}
		return out, nil
	}
	out["state"] = "not_in_transcript"
	out["hint"] = "accepted but not yet seen by the receiving agent; the receiver's list_sessions codex_outbox or hook_queue_depth shows held events. Codex queue items are read only when a turn starts."
	return out, nil
}

// findInTail searches the last limit bytes of path, newest first, for needle
// and returns the timestamp of the line that contains it, if any.
func findInTail(ctx context.Context, path string, needle []byte, limit int64) (string, int64, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, false, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", 0, false, err
	}
	end := st.Size()
	floor := max(0, end-limit)
	overlap := int64(len(needle) - 1)
	buf := make([]byte, deliveryScanChunk+overlap)
	deadline := time.Now().Add(historyDeadline)
	for end > floor {
		if ctx.Err() != nil || time.Now().After(deadline) {
			return "", st.Size() - end, false, nil
		}
		start := max(floor, end-deliveryScanChunk)
		stop := min(st.Size(), end+overlap)
		chunk := buf[:stop-start]
		if _, err := f.ReadAt(chunk, start); err != nil && err != io.EOF {
			return "", 0, false, err
		}
		if i := bytes.LastIndex(chunk, needle); i >= 0 {
			return lineTime(f, start+int64(i)), st.Size() - start, true, nil
		}
		end = start
	}
	return "", st.Size() - floor, false, nil
}

// lineTime returns the timestamp field of the JSONL line holding offset,
// searching at most 64 KiB on either side of it.
func lineTime(f *os.File, offset int64) string {
	const window = 64 << 10
	start := max(0, offset-window)
	b := make([]byte, 2*window)
	n, err := f.ReadAt(b[:offset-start+window], start)
	if err != nil && err != io.EOF {
		return ""
	}
	b = b[:n]
	at := int(offset - start)
	lineStart := bytes.LastIndexByte(b[:at], '\n') + 1
	lineEnd := len(b)
	if i := bytes.IndexByte(b[at:], '\n'); i >= 0 {
		lineEnd = at + i
	}
	if m := lineTimestamp.FindSubmatch(b[lineStart:lineEnd]); m != nil {
		return string(m[1])
	}
	return ""
}
