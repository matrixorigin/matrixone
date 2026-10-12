// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0

package optools

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLightLinkGateKernelLeases(t *testing.T) {
	dir := t.TempDir()
	gate, err := filepath.Abs("ut_link_gate.sh")
	if err != nil {
		t.Fatal(err)
	}
	tool := filepath.Join(dir, "link")
	if err := os.WriteFile(tool, []byte(`#!/bin/bash
if [[ "${1:-}" == -V=full ]]; then printf 'link version go1.26.4\n'; exit 0; fi
printf 'ready %s\n' "$$"
read -r status
exit "$status"
`), 0755); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(dir, "admission.log")
	environment := append(os.Environ(), "UT_LINK_DIR="+dir, "UT_LINK_PARALLEL=2", "UT_LINK_LOG="+log)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	type child struct {
		command *exec.Cmd
		input   io.WriteCloser
		output  *bufio.Reader
		waited  bool
	}
	start := func() *child {
		cmd := exec.CommandContext(ctx, "bash", gate, tool)
		cmd.Env = environment
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
		input, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = input.Close() })
		output, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		c := &child{command: cmd, input: input, output: bufio.NewReader(output)}
		t.Cleanup(func() {
			if !c.waited {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				_ = cmd.Wait()
			}
		})
		return c
	}
	ready := func(c *child) {
		line, err := c.output.ReadString('\n')
		if err != nil || line != fmt.Sprintf("ready %d\n", c.command.Process.Pid) {
			t.Fatalf("tool did not exec with original pid: %q, %v", line, err)
		}
	}
	finish := func(c *child, status int) {
		if _, err := fmt.Fprintln(c.input, status); err != nil {
			t.Fatal(err)
		}
		err := c.command.Wait()
		c.waited = true
		if c.command.ProcessState.ExitCode() != status {
			t.Fatalf("tool status changed: want %d, got %v", status, err)
		}
	}
	first, second := start(), start()
	ready(first)
	ready(second) // proves independent slots can overlap
	waiter := start()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	waitForContention := func(c *child) {
		for {
			data, err := os.ReadFile(log)
			if err == nil && strings.Contains(string(data), fmt.Sprintf("event=waiting pid=%d\n", c.command.Process.Pid)) {
				if strings.Contains(string(data), fmt.Sprintf("event=acquired pid=%d ", c.command.Process.Pid)) {
					t.Fatal("third linker exceeded the two-slot bound")
				}
				return
			}
			select {
			case <-ticker.C: // observe the real admission phase, not a scheduler delay
			case <-ctx.Done():
				t.Fatal("linker never reported contention")
			}
		}
	}
	waitForContention(waiter)
	// Version queries bypass even a completely occupied gate, with exact bytes.
	version := exec.CommandContext(ctx, "bash", gate, tool, "-V=full")
	version.Env = environment
	if out, err := version.Output(); err != nil || string(out) != "link version go1.26.4\n" {
		t.Fatalf("tool version passthrough: %q, %v", out, err)
	}
	// Killing a waiter must not affect either owner or leave a stale reservation.
	if err := syscall.Kill(-waiter.command.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = waiter.command.Wait()
	waiter.waited = true
	waiter = start()
	waitForContention(waiter)
	// SIGKILL of an owner releases its kernel lease without shell cleanup.
	if err := first.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = first.command.Wait()
	first.waited = true
	ready(waiter)
	finish(second, 7) // failure is propagated and releases the other slot
	replacement := start()
	ready(replacement)
	finish(waiter, 0)
	finish(replacement, 0)
}
