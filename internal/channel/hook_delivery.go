package channel

import (
	"context"
	"strings"
	"time"
)

// Hook delivery (claude.delivery = "hooks") serves Claude accounts whose org
// policy discards notifications/claude/channel. Events are held in a bounded
// inbox and returned as additionalContext by the next main-thread
// UserPromptSubmit or PreToolUse hook. An event leaves the inbox only in a
// response produced within hookDeliveryBudget, so a slow or cancelled hook
// (Claude's hook timeout is 5 s) leaves it for the next hook.
const (
	hookInboxLimit     = 64 * 1024
	hookDeliveryBudget = 3 * time.Second
)

// holdForHook is the notify function for hook delivery. A full inbox reports
// the destination unavailable, so the delivery loop retains the event.
func (n *Node) holdForHook(_ context.Context, text string, meta map[string]string) error {
	envelope := peerEnvelope(text, meta)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.hookInboxBytes+len(envelope) > hookInboxLimit {
		return errDestinationUnavailable
	}
	n.hookInbox = append(n.hookInbox, envelope)
	n.hookInboxBytes += len(envelope)
	return nil
}

// hookOutput drains held events into Claude hook JSON. It returns an empty
// object for other events, in channel mode, and when the hook's context is
// done or its budget since started is spent; held events then stay queued.
func (n *Node) hookOutput(ctx context.Context, event string, started time.Time) map[string]any {
	if event != "UserPromptSubmit" && event != "PreToolUse" {
		return map[string]any{}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.hookDelivery || len(n.hookInbox) == 0 || ctx.Err() != nil || time.Since(started) >= hookDeliveryBudget {
		return map[string]any{}
	}
	held := n.hookInbox
	n.hookInbox, n.hookInboxBytes = nil, 0
	n.stats.HookInjected += uint64(len(held))
	return map[string]any{"hookSpecificOutput": map[string]any{"hookEventName": event, "additionalContext": strings.Join(held, "\n\n")}}
}
