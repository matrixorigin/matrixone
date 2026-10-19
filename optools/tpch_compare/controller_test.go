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
	"context"
	"errors"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestBalancedFiniteSchedule(t *testing.T) {
	c := Campaign{Mode: "full", Seed: 7, MemoryBytes: 32 << 30}
	trials := schedule(c)
	if len(trials) != 295 {
		t.Fatal(len(trials))
	}
	if !reflect.DeepEqual(trials, schedule(c)) {
		t.Fatal("schedule is not reproducible")
	}
	seen := map[string]bool{}
	first := map[string]int{}
	for _, trial := range trials {
		if seen[trial.ID] {
			t.Fatal("duplicate trial")
		}
		seen[trial.ID] = true
		if trial.Experiment == "warm" && trial.Arms[0] == "mo" {
			first[trial.Scale]++
		}
		if trial.Clients > 1 && (trial.MemoryBytes != 32<<30 || trial.Cores != 8) {
			t.Fatal("concurrent client received an independent budget")
		}
	}
	for _, sf := range []string{"1", "10", "100"} {
		if first[sf] != 5 {
			t.Fatal(first)
		}
	}
	c.Mode = "warm"
	if warm := schedule(c); len(warm) != 30 || !reflect.DeepEqual(warm, trials[:30]) {
		t.Fatal("warm campaign must retain the balanced primary cells")
	}
	for _, raw := range []string{"0-7", "0,1,2,3,4,5,6,7"} {
		cpus, err := parseCPUSet(raw)
		if err != nil || len(cpus) != 8 {
			t.Fatal(cpus, err)
		}
	}
}

func TestBootstrapPreservesPairedWaves(t *testing.T) {
	blocks := make([][]pairedTimes, 2)
	for range 20 {
		blocks[0] = append(blocks[0], pairedTimes{1, 1})
		blocks[1] = append(blocks[1], pairedTimes{1, 3})
	}
	ratio, low, high := pairedRatio(blocks, 1)
	if ratio != 2 || low != 1 || high != 3 {
		t.Fatal(ratio, low, high)
	}
	_, low, high = pairedRatio(blocks[:1], 1)
	if !math.IsNaN(low) || !math.IsNaN(high) {
		t.Fatal("one wave incorrectly became twenty independent blocks")
	}
}

func TestReportUsesExecutionStatusWithoutReadingResults(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "different results are not verified"
		if partial {
			name = "incomplete execution is rejected"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			write := func(path string, value any) FileID {
				t.Helper()
				path = filepath.Join(root, path)
				if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
					t.Fatal(err)
				}
				if err := publishJSON(path, value); err != nil {
					t.Fatal(err)
				}
				pin, err := artifact(root, path)
				if err != nil {
					t.Fatal(err)
				}
				return pin
			}
			c := Campaign{Version: formatVersion, Root: root, Mode: "sf1-smoke", Datasets: map[string]string{"1": root}, MOSource: root, MOBinary: filepath.Join(root, "mo"), MORevision: "test", MOBuild: "test", CPUs: []int{0, 1, 2, 3, 4, 5, 6, 7}, ControllerCPUs: []int{8}, MemoryBytes: 32 << 30, DiskBytes: 1 << 30, ReserveBytes: 8 << 30, ProfileBytes: 1 << 20, PortBase: 24000, ExclusiveHost: true}
			config := write("campaign.json", c)
			p := Preflight{Worker: FileID{SHA256: "worker"}}
			f := Frozen{Version: formatVersion, Preflight: write("preflight.json", p), Trials: schedule(c)}
			pair := PairRecord{Version: formatVersion, Trial: f.Trials[0], Status: "pass", Arms: map[string]ArmRecord{}}
			for index, engine := range []string{"mo", "duckdb"} {
				prefix := filepath.Join("runs", "baseline", pair.Trial.ID, engine)
				r := WorkerReceipt{Version: formatVersion, Binary: p.Worker, Controlled: true, WaveNS: int64(time.Second)}
				for _, q := range pair.Trial.Queries {
					// Captures deliberately do not exist, and the engines return
					// different row counts. Reporting reads timing receipts only.
					r.Queries = append(r.Queries, QueryReceipt{Query: q, Capture: Capture{Rows: int64(index), ElapsedNS: int64(time.Millisecond)}})
				}
				if partial && engine == "duckdb" {
					r.Queries = r.Queries[:len(r.Queries)-1]
				}
				pair.Arms[engine] = ArmRecord{Trial: pair.Trial, Engine: engine, Status: "pass", Cleanup: "complete", Request: write(filepath.Join(prefix, "request.json"), runRequest(pair.Trial, true)), Worker: write(filepath.Join(prefix, "receipt.json"), r), After: Usage{CPUUsec: 1000, Raw: map[string]string{"memory.max": "34359738368", "memory.swap.max": "0", "cpuset.cpus.effective": "0-7"}}}
				arm := pair.Arms[engine]
				arm.Request.Path = "request.json"
				arm.Worker.Path = "receipt.json"
				pair.Arms[engine] = arm
			}
			pin := write(filepath.Join("runs", "baseline", pair.Trial.ID, "pair.json"), pair)
			pin.Path = filepath.Join(pair.Trial.ID, "pair.json")
			write("runs/baseline/run.json", RunManifest{Version: formatVersion, Frozen: write("frozen.json", f), Pairs: []FileID{pin}})
			output := filepath.Join(root, "report")
			err := reportCampaign(filepath.Join(root, config.Path), filepath.Join(root, "runs", "baseline"), output)
			if partial {
				if err == nil || !strings.Contains(err.Error(), "partial query stream") {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			body, err := os.ReadFile(filepath.Join(output, "report.md"))
			if err != nil || !strings.Contains(string(body), "Run status: **complete**") || !strings.Contains(string(body), "does not perform correctness verification") {
				t.Fatal(string(body), err)
			}
		})
	}
}

func TestIndependentProgressDeadlines(t *testing.T) {
	now := time.Unix(100, 0)
	active := map[int]activeQuery{}
	if err := advanceProgress(active, "start 0 3 1000000000", now); err != nil {
		t.Fatal(err)
	}
	if !active[0].deadline.Equal(now.Add(31 * time.Second)) {
		t.Fatal(active)
	}
	if err := advanceProgress(active, "start 0 4 1000000000", now); err == nil {
		t.Fatal("duplicate admission")
	}
	if err := advanceProgress(active, "end 0 4 0", now); err == nil {
		t.Fatal("wrong completion")
	}
	if err := advanceProgress(active, "end 0 3 0", now); err != nil || len(active) != 0 {
		t.Fatal(active, err)
	}
	if err := advanceProgress(active, "start 0 3 999999999999999999", now); err == nil {
		t.Fatal("unbounded child-supplied deadline")
	}
}

func TestControllerGateCancelJoinsReader(t *testing.T) {
	g, err := newControllerGate()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.close)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("injected controller cancellation")
	cancel(cause)
	if err := g.wait(ctx, "ready"); !errors.Is(err, cause) {
		t.Fatal(err)
	}
}

func TestControllerRejectsOversizeEvent(t *testing.T) {
	g, err := newControllerGate()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.close)
	joined := make(chan struct{})
	go func() { defer close(joined); _, _ = g.child[1].Write([]byte(strings.Repeat("x", 512) + "\n")) }()
	ctx, cancel := context.WithTimeoutCause(context.Background(), 3*time.Second, errors.New("test guard"))
	defer cancel()
	if err := g.wait(ctx, "ready"); err == nil {
		t.Fatal("unbounded event accepted")
	}
	<-joined
}

func TestManagedProcessHelper(t *testing.T) {
	if os.Getenv("TPCH_COMPARE_TEST_BLOCKED") != "1" {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	f := os.NewFile(3, "ready")
	if _, err := f.Write([]byte{'r'}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	select {}
}

func TestManagedTerminationDoesNotWaitForDataPath(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 5*time.Second, errors.New("test process guard"))
	defer cancel()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = read.Close(); _ = write.Close() })
	p, err := startManaged(ctx, []string{exe, "-test.run=^TestManagedProcessHelper$", "-test.timeout=30s"}, t.TempDir(), []string{"TPCH_COMPARE_TEST_BLOCKED=1"}, nil, filepath.Join(t.TempDir(), "child.log"), []*os.File{write})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.stop(nil, 0) })
	_ = write.Close()
	if err := read.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var ready [1]byte
	if _, err := io.ReadFull(read, ready[:]); err != nil {
		t.Fatal(err)
	}
	if err := p.stop(nil, 0); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	default:
		t.Fatal("child survived termination")
	}
}

func TestConcurrentStreamsShareOneDuckDBInstance(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 15*time.Second, errors.New("stream fixture guard"))
	defer cancel()
	e := Engine{Name: "duckdb", Database: filepath.Join(t.TempDir(), "shared.db"), Threads: 1, MemoryBytes: 256 << 20}
	db, err := openEngine(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	first, err := configureConn(ctx, db, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	if _, err := first.ExecContext(ctx, "CREATE TABLE shared_fixture(i INTEGER); INSERT INTO shared_fixture VALUES (7)"); err != nil {
		t.Fatal(err)
	}
	d := Dataset{Queries: []Query{{Number: 1, Columns: []Column{{Kind: "int"}}, Statements: []string{"SELECT i FROM shared_fixture"}}}}
	r := WorkerRequest{Engine: e, Queries: []int{1}, Repetitions: 2, Clients: 4, OutputRoot: t.TempDir(), QueryDeadlineNS: int64(5 * time.Second)}
	queries, wave, err := runQueryStreams(ctx, db, first, r, d, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) != 8 || wave <= 0 {
		t.Fatal(len(queries), wave)
	}
	for _, q := range queries {
		if q.Capture.Rows != 1 {
			t.Fatal(q)
		}
	}
}
