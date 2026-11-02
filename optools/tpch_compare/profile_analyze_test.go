// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestFoldedSymbolsPreserveRawNames(t *testing.T) {
	raw := "123 124 12.000000: 100 cpu-clock:u:\n    abcd generic[A int; B *T] (worker)\n    ef01 parent%3Bname (worker)\n\n" +
		"123 124 12.100000: 200 cpu-clock:u:\n    abcd generic[A int%3B B *T] (worker)\n    ef01 parent%3Bname (worker)\n"
	evidence, err := parsePerfScript(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	for key, weight := range map[string]uint64{
		"pid=123;parent%253Bname;generic[A int%3B B *T]":   100,
		"pid=123;parent%253Bname;generic[A int%253B B *T]": 200,
	} {
		if evidence.Stacks[key] != weight || strings.Count(key, ";") != 2 {
			t.Fatalf("display stack %q lost its weight or frame boundaries: %+v", key, evidence)
		}
	}
	if len(evidence.Stacks) != 2 || evidence.RawSymbols["generic[A int%3B B *T]"] != "generic[A int; B *T]" ||
		evidence.RawSymbols["parent%253Bname"] != "parent%3Bname" || evidence.Hot["pid=123 generic[A int; B *T]"] != 100 {
		t.Fatalf("raw symbols or distinct stacks were lost: %+v", evidence)
	}
}

func TestPerfLeafHeadersAndCallchains(t *testing.T) {
	for _, chain := range []string{"", "    abcd duckdb::Probe+0x20 (worker)\n    ef01 parent (worker)\n"} {
		t.Run(fmt.Sprintf("callchain=%t", chain != ""), func(t *testing.T) {
			raw := "123 124 12.000000: 100 cpu-clock:u: abcd duckdb::Probe+0x20 (worker)\n" + chain +
				"\n234 235 12.100000: 100 cpu-clock:u: 0 [unknown] (unknown)\n"
			evidence, err := parsePerfScript(strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			key := "pid=123;duckdb::Probe"
			if chain != "" {
				key = "pid=123;parent;duckdb::Probe"
			}
			if evidence.Samples != 2 || evidence.WeightNS != 200 || evidence.KnownRatio != 0.5 || evidence.Stacks[key] != 100 {
				t.Fatalf("leaf attribution or callchain duplicated: %+v", evidence)
			}
		})
	}
}

func TestPerfAnalysisBoundsAndCancellation(t *testing.T) {
	for _, raw := range []string{
		"123 124 12.000000: 100 cpu-clock:u: abcd leaf (worker)\n",
		"invalid 124 12.000000: 100 cpu-clock:u:\n",
		"123 124 12.000000: 0 cpu-clock:u:\n",
	} {
		if _, err := parsePerfScriptContext(t.Context(), strings.NewReader(raw), 128); err == nil {
			t.Fatalf("out-of-budget or invalid record accepted: %q", raw)
		}
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("analysis cancelled")
	cancel(cause)
	if _, err := parsePerfScriptContext(ctx, strings.NewReader("123 124 12.000000: 100 cpu-clock:u:\n"), 1024); !errors.Is(err, cause) {
		t.Fatalf("lost cancellation cause: %v", err)
	}
}

func TestPerfConversionRecovery(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		fullFailure, leafFailure, stop bool
	}{
		{name: "full callgraph"},
		{name: "bounded leaf recovery", fullFailure: true},
		{name: "recovery failure", fullFailure: true, leafFailure: true},
		{name: "caller cancellation", fullFailure: true, stop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "perf.record"), []byte("raw recording"), 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("TPCH_COMPARE_FLAMEGRAPH", "test-renderer.pl")
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			leafCause := errors.New("leaf decoder failed")
			stopCause := errors.New("caller stopped profiling")
			var calls []string
			command := func(ctx context.Context, output string, _ int64, args ...string) error {
				if ctx.Err() != nil {
					t.Fatal("expired conversion context reused")
				}
				kind, budget := "full", 30*time.Second
				if slices.Contains(args, "-G") {
					kind, budget = "leaf", 15*time.Second
				}
				if args[0] == "perl" {
					kind, budget = "render", 5*time.Second
				}
				calls = append(calls, kind)
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > budget {
					t.Fatal("postprocessing sub-budget missing")
				}
				if kind == "full" && tc.fullFailure {
					if err := os.WriteFile(output, []byte("partial full callgraph"), 0600); err != nil {
						return err
					}
					if tc.stop {
						cancel(stopCause)
						return stopCause
					}
					return context.DeadlineExceeded
				}
				if kind == "leaf" && tc.leafFailure {
					return leafCause
				}
				if kind == "render" {
					folded, err := os.ReadFile(filepath.Join(root, "stacks.folded"))
					if err != nil || !strings.Contains(string(folded), "generic[A int%3B B *T] 100") {
						t.Fatalf("renderer did not receive escaped evidence: %v %s", err, folded)
					}
					return os.WriteFile(output, []byte("<svg/>"), 0600)
				}
				raw := "123 124 12.000000: 100 cpu-clock:u: abcd generic[A int; B *T] (worker)\n"
				if kind == "full" {
					raw += "    abcd generic[A int; B *T] (worker)\n    ef01 parent (worker)\n"
				}
				return os.WriteFile(output, []byte(raw), 0600)
			}
			err := analyzePerfWithCommand(ctx, root, command)
			want := []string{"full", "render"}
			if tc.fullFailure {
				want = []string{"full", "leaf", "render"}
				if tc.leafFailure {
					want = want[:2]
					if !errors.Is(err, leafCause) || !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("lost decoder failures: %v", err)
					}
				} else if tc.stop {
					want = want[:1]
					if !errors.Is(err, stopCause) {
						t.Fatalf("lost caller cancellation: %v", err)
					}
				} else if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("recovery incorrectly claimed complete callgraphs: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(calls, want) {
				t.Fatalf("commands %v, want %v", calls, want)
			}
			if !tc.leafFailure && !tc.stop {
				evidence, err := readJSON[StackEvidence](filepath.Join(root, "stacks.json"))
				if err != nil {
					t.Fatal(err)
				}
				if evidence.Samples != 1 || evidence.Hot["pid=123 generic[A int; B *T]"] != 100 || evidence.RawSymbols["generic[A int%3B B *T]"] != "generic[A int; B *T]" {
					t.Fatal(evidence)
				}
				if tc.fullFailure && (evidence.Quality != "partial_callgraph" || len(evidence.Limitations) != 1 || !strings.Contains(evidence.CallGraph, "leaf-only")) {
					t.Fatal("leaf recovery lost its coverage limitation", evidence)
				}
			}
			raw, err := os.ReadFile(filepath.Join(root, "perf.record"))
			if err != nil || string(raw) != "raw recording" {
				t.Fatal("raw recording changed", err)
			}
		})
	}
}

func TestProfileCommandCancellationStopsDescendants(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	root := t.TempDir()
	fifo := filepath.Join(root, "ready")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	guard, err := os.OpenFile(fifo, os.O_RDWR|syscall.O_NONBLOCK, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = guard.Close() })
	if err := guard.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var commandErr error
	go func() {
		commandErr = commandToFile(ctx, filepath.Join(root, "output"), 1024, "sh", "-c", `exec 3>"$1"; sleep 30 & child=$!; printf '%s\n' "$child" >&3; wait "$child"`, "sh", fifo)
		close(done)
	}()
	t.Cleanup(func() {
		cancel(context.Canceled)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("profile command did not join")
		}
	})
	line, err := bufio.NewReader(guard).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || pid <= 1 {
		t.Fatal("invalid owned child identity", line, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	pgid, err := syscall.Getpgid(pid)
	if err != nil || pgid == syscall.Getpgrp() {
		t.Fatal("offline command lacks its own process group", err)
	}
	read, err := os.OpenFile(fifo, os.O_RDONLY|syscall.O_NONBLOCK, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = read.Close() })
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	if err := read.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("cancel offline symbolization")
	cancel(cause)
	// Both the shell and native helper inherited FD 3. EOF requires both to
	// release it; killing only the direct child leaves this read blocked.
	if _, err := io.Copy(io.Discard, read); err != nil {
		t.Fatal("symbolizer descendant survived cancellation", err)
	}
	select {
	case <-done:
		if !errors.Is(commandErr, cause) {
			t.Fatal("lost command cancellation cause", commandErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled offline command did not join")
	}
}

func TestProfileCleanupRetainsUnjoinedCollectorFiles(t *testing.T) {
	for _, joined := range []bool{false, true} {
		t.Run(fmt.Sprintf("joined=%t", joined), func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "record"), []byte("owned capture"), 0600); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			if joined {
				close(done)
			}
			profile := &parentProfile{tmp: root, captures: []*perfCapture{{process: &managedProcess{done: done}}}, record: ProfileRecord{Status: "complete"}}
			profile.finishCleanup()
			_, err := os.Stat(filepath.Join(root, "record"))
			if joined {
				if !errors.Is(err, os.ErrNotExist) || profile.record.Cleanup != "complete" {
					t.Fatal("joined collector did not release its files", err, profile.record)
				}
			} else if err != nil || profile.record.Cleanup != "failed" || profile.record.Status != "partial" || profile.record.Retained != root {
				t.Fatal("unjoined writer lost its capture or claimed cleanup", err, profile.record)
			}
		})
	}
}
