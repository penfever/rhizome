package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"rhizome/internal/channel"
	"rhizome/internal/config"
)

// exitCode is an error that sets the process exit status without a log line.
type exitCode int

func (e exitCode) Error() string { return "exit " + strconv.Itoa(int(e)) }

// waitInboxTimeoutExit is wait-inbox's status when the timeout passes with no
// unread mail, distinct from 1 (error) so shell watchers can tell them apart.
const waitInboxTimeoutExit = exitCode(2)

func inboxFlags(name string) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	config.Bind(fs, "runtime-dir")
	session := fs.String("session", "", "exact session_id whose inbox to read (required)")
	return fs, session
}

func inboxRuntime(fs *flag.FlagSet, session string) (string, error) {
	if fs.NArg() != 0 {
		return "", fmt.Errorf("unexpected %s arguments", fs.Name())
	}
	if session == "" {
		return "", fmt.Errorf("%s: --session is required", fs.Name())
	}
	c, err := config.FromFlags(fs)
	if err != nil {
		return "", err
	}
	return c.RuntimeDir, nil
}

// inboxCommand prints unread durable-inbox messages and marks them read.
func inboxCommand(args []string, w io.Writer) error {
	fs, session := inboxFlags("inbox")
	all := fs.Bool("all", false, "print every message, not only unread ones")
	peek := fs.Bool("peek", false, "do not mark printed messages read")
	asJSON := fs.Bool("json", false, "print one JSON record per line")
	if err := fs.Parse(args); err != nil {
		return err
	}
	runtime, err := inboxRuntime(fs, *session)
	if err != nil {
		return err
	}
	msgs, err := channel.ReadInbox(runtime, *session, *all, !*peek)
	if err != nil {
		return err
	}
	if !*asJSON {
		channel.FormatInbox(w, *session, msgs)
		return nil
	}
	enc := json.NewEncoder(w)
	for _, m := range msgs {
		if err := enc.Encode(m); err != nil {
			return err
		}
	}
	return nil
}

// parseTimeout accepts whole seconds ("600") or a Go duration ("10m").
func parseTimeout(s string) (time.Duration, error) {
	if n, err := strconv.Atoi(s); err == nil && n >= 0 {
		return time.Duration(n) * time.Second, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("invalid --timeout %q: use seconds or a duration such as 10m", s)
	}
	return d, nil
}

// waitInboxCommand blocks until the session has unread mail (exit 0) or the
// timeout passes (exit 2). Unread mail present at start returns immediately.
func waitInboxCommand(args []string, w io.Writer) error {
	fs, session := inboxFlags("wait-inbox")
	timeout := fs.String("timeout", "0", "give up after this many seconds or this duration (0 waits forever)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	runtime, err := inboxRuntime(fs, *session)
	if err != nil {
		return err
	}
	d, err := parseTimeout(*timeout)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}
	n, err := channel.WaitInbox(ctx, runtime, *session, channel.InboxPoll)
	if errors.Is(err, channel.ErrInboxTimeout) {
		fmt.Fprintf(w, "rhizome wait-inbox: no unread messages for %s after %s\n", *session, d)
		return waitInboxTimeoutExit
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "rhizome wait-inbox: %d unread message(s) for %s; read them with: rhizome inbox --session %s\n", n, *session, *session)
	return nil
}
