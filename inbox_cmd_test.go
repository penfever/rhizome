package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseTimeout(t *testing.T) {
	for in, want := range map[string]time.Duration{"0": 0, "600": 10 * time.Minute, "90s": 90 * time.Second, "1h": time.Hour} {
		if got, err := parseTimeout(in); err != nil || got != want {
			t.Errorf("%q: %v %v", in, got, err)
		}
	}
	for _, in := range []string{"-1", "soon", "-5m"} {
		if _, err := parseTimeout(in); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
}

// wait-inbox exits 2 on timeout and 0 once mail is present; inbox prints and
// marks it read.
func TestInboxCommands(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	runtime := filepath.Join(home, "run")
	if err := os.MkdirAll(filepath.Join(runtime, "inbox"), 0o700); err != nil {
		t.Fatal(err)
	}
	args := []string{"--runtime-dir", runtime, "--session", "s1"}
	var out bytes.Buffer
	err := waitInboxCommand(append(args, "--timeout", "1"), &out)
	var code exitCode
	if !errors.As(err, &code) || code != waitInboxTimeoutExit {
		t.Fatalf("timeout: %v", err)
	}
	line := `{"received_at":"2026-11-11T10:20:46Z","message_id":"m1","kind":"direct","from_session":"peer","from_node":"n","text":"hello"}` + "\n"
	if err := os.WriteFile(filepath.Join(runtime, "inbox", "s1.jsonl"), []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := waitInboxCommand(append(args, "--timeout", "5"), &out); err != nil || !strings.Contains(out.String(), "1 unread") {
		t.Fatalf("mail: %v %q", err, out.String())
	}
	out.Reset()
	if err := inboxCommand(args, &out); err != nil || !strings.Contains(out.String(), "hello") || !strings.Contains(out.String(), "Untrusted") {
		t.Fatalf("inbox: %v %q", err, out.String())
	}
	out.Reset()
	if err := inboxCommand(args, &out); err != nil || !strings.Contains(out.String(), "No unread") {
		t.Fatalf("second read: %v %q", err, out.String())
	}
	// --pid: a watched job that finishes first ends the wait with exit 3.
	job := exec.Command("sleep", "0.2")
	if err := job.Start(); err != nil {
		t.Fatal(err)
	}
	go job.Wait()
	err = waitInboxCommand(append(args, "--timeout", "10", "--pid", strconv.Itoa(job.Process.Pid)), &out)
	if !errors.As(err, &code) || code != waitInboxPidExit {
		t.Fatalf("pid exit: %v", err)
	}
	if err := inboxCommand([]string{"--runtime-dir", runtime}, &out); err == nil {
		t.Fatal("missing --session accepted")
	}
}
