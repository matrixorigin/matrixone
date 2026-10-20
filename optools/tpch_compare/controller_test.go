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

func campaignFixture(root string) Campaign {
	return Campaign{
		Version: formatVersion, Root: root, Mode: "sf1-smoke", Datasets: map[string]string{"1": root},
		MOSource: root, MOBinary: filepath.Join(root, "mo"), MORevision: "test", MOBuild: "test",
		CPUs: []int{0, 1, 2, 3, 4, 5, 6, 7}, ControllerCPUs: []int{8}, MemoryBytes: 32 << 30,
		DiskBytes: 1 << 30, ReserveBytes: 8 << 30, ProfileBytes: 1 << 20, PortBase: 24000, ExclusiveHost: true,
	}
}

func TestReportUsesExecutionStatusWithoutReadingResults(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mutate       func(*WorkerRequest, *WorkerReceipt)
		changeConfig bool
		wantError    string
	}{
		{name: "different results are not verified"},
		{name: "incomplete execution is rejected", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Queries = r.Queries[:len(r.Queries)-1]
		}, wantError: "partial query stream"},
		{name: "duplicate query cannot replace missing query", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Queries[len(r.Queries)-1].Query = r.Queries[0].Query
		}, wantError: "query identities"},
		{name: "unexpected stream", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Queries[0].Stream = 1
		}, wantError: "query identities"},
		{name: "unexpected repetition", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Queries[0].Repetition = 1
		}, wantError: "query identities"},
		{name: "request cannot change frozen repetitions", mutate: func(request *WorkerRequest, r *WorkerReceipt) {
			request.Repetitions = 2
			for _, q := range append([]QueryReceipt(nil), r.Queries...) {
				q.Repetition = 1
				r.Queries = append(r.Queries, q)
			}
		}, wantError: "worker request differs"},
		{name: "request cannot change frozen queries", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.Queries = append([]int(nil), request.Queries...)
			request.Queries[0], request.Queries[1] = request.Queries[1], request.Queries[0]
		}, wantError: "worker request differs"},
		{name: "report cannot change source attribution", changeConfig: true, wantError: "configuration differs from preflight"},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			c := campaignFixture(root)
			config := write("campaign.json", c)
			configPath := filepath.Join(root, config.Path)
			config.Path = configPath
			p := Preflight{Version: formatVersion, Config: config, Worker: FileID{SHA256: "worker"}}
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
				request := runRequest(pair.Trial, true)
				if tc.mutate != nil {
					tc.mutate(&request, &r)
				}
				pair.Arms[engine] = ArmRecord{Trial: pair.Trial, Engine: engine, Status: "pass", Cleanup: "complete", Request: write(filepath.Join(prefix, "request.json"), request), Worker: write(filepath.Join(prefix, "receipt.json"), r), After: Usage{CPUUsec: 1000, Raw: map[string]string{"memory.max": "34359738368", "memory.swap.max": "0", "cpuset.cpus.effective": "0-7"}}}
				arm := pair.Arms[engine]
				arm.Request.Path = "request.json"
				arm.Worker.Path = "receipt.json"
				pair.Arms[engine] = arm
			}
			pin := write(filepath.Join("runs", "baseline", pair.Trial.ID, "pair.json"), pair)
			pin.Path = filepath.Join(pair.Trial.ID, "pair.json")
			write("runs/baseline/run.json", RunManifest{Version: formatVersion, Frozen: write("frozen.json", f), Pairs: []FileID{pin}})
			if tc.changeConfig {
				c.MORevision = "NEVER-MEASURED-REVISION"
				configPath = filepath.Join(root, write("different-campaign.json", c).Path)
			}
			output := filepath.Join(root, "report")
			err := reportCampaign(configPath, filepath.Join(root, "runs", "baseline"), output)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatal(err)
				}
				if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("invalid evidence created report output", err)
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

func TestCampaignPinRejectsDifferentConfig(t *testing.T) {
	root := t.TempDir()
	c := campaignFixture(root)
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "campaign.json")
	if err := publishJSON(config, c); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.MOBinary, []byte("native engine"), 0600); err != nil {
		t.Fatal(err)
	}
	identifyFile := func(path string) FileID {
		t.Helper()
		id, err := identify(path)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := Preflight{Version: formatVersion, Config: identifyFile(config), MO: identifyFile(c.MOBinary), Worker: identifyFile(exe)}
	if err := campaignPin(c, p); err != nil {
		t.Fatal("unchanged campaign rejected", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Campaign)
	}{
		{"executable", func(c *Campaign) { c.MOBinary = filepath.Join(root, "unqualified-mo") }},
		{"CPU set", func(c *Campaign) { c.CPUs = []int{16, 17, 18, 19, 20, 21, 22, 23} }},
		{"source attribution", func(c *Campaign) { c.MORevision = "unmeasured-revision" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := c
			tc.mutate(&changed)
			if err := changed.validate(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "different-campaign.json")
			if err := publishJSON(path, changed); err != nil {
				t.Fatal(err)
			}
			actual, err := readJSON[Campaign](path)
			if err != nil {
				t.Fatal(err)
			}
			if err := campaignPin(actual, p); err == nil || !strings.Contains(err.Error(), "configuration differs from preflight") {
				t.Fatal("unqualified configuration accepted", err)
			}
		})
	}
	if err := os.WriteFile(config, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := campaignPin(c, p); err == nil {
		t.Fatal("changed frozen configuration accepted", err)
	}
}

func TestFrozenConcurrentQueryIdentities(t *testing.T) {
	trial := Trial{Experiment: "concurrency", Clients: 2, Queries: []int{1, 2}}
	for _, name := range []string{"complete", "duplicate identity", "negative stream", "unexpected query", "unexpected repetition", "changed clients", "changed repetitions", "missing frozen wave"} {
		t.Run(name, func(t *testing.T) {
			frozen := Frozen{WaveRepetitions: map[int]int{2: 2}}
			request := runRequest(trial, false)
			request.Repetitions = 2
			var receipt WorkerReceipt
			for stream := range 2 {
				for repetition := range 2 {
					for _, query := range trial.Queries {
						receipt.Queries = append(receipt.Queries, QueryReceipt{Query: query, Stream: stream, Repetition: repetition})
					}
				}
			}
			switch name {
			case "duplicate identity":
				receipt.Queries[1] = receipt.Queries[0]
			case "negative stream":
				receipt.Queries[0].Stream = -1
			case "unexpected query":
				receipt.Queries[0].Query = 3
			case "unexpected repetition":
				receipt.Queries[0].Repetition = 2
			case "changed clients":
				request.Clients = 1
				receipt.Queries = receipt.Queries[:4]
			case "changed repetitions":
				request.Repetitions = 1
				receipt.Queries = receipt.Queries[:4]
			case "missing frozen wave":
				frozen.WaveRepetitions = nil
			}
			if err := verifyTrialQueries(trial, frozen, request, receipt); (err != nil) != (name != "complete") {
				t.Fatal(err)
			}
		})
	}
}

func TestControllerProgressDeadline(t *testing.T) {
	now := time.Unix(100, 0)
	for _, tc := range []struct {
		name   string
		active map[int]activeQuery
		want   time.Duration
	}{
		{"idle", nil, 10 * time.Minute},
		{"long query", map[int]activeQuery{0: {1, now.Add(20*time.Minute + 30*time.Second)}}, 20*time.Minute + 30*time.Second},
		{"short query", map[int]activeQuery{0: {1, now.Add(90 * time.Second)}}, 90 * time.Second},
		{"earliest concurrent deadline", map[int]activeQuery{0: {1, now.Add(20 * time.Minute)}, 1: {2, now.Add(12 * time.Minute)}}, 12 * time.Minute},
		{"expired query", map[int]activeQuery{0: {1, now.Add(-time.Second)}}, -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextProgressDeadline(tc.active, now); !got.Equal(now.Add(tc.want)) {
				t.Fatal(got, "want", now.Add(tc.want))
			}
		})
	}
	active := map[int]activeQuery{}
	if err := advanceProgress(active, "start 0 1 1200000000000", now); err != nil {
		t.Fatal(err)
	}
	if got := nextProgressDeadline(active, now.Add(11*time.Minute)); !got.Equal(now.Add(20*time.Minute + 30*time.Second)) {
		t.Fatal("long-running query lost its frozen deadline", got)
	}
	if err := advanceProgress(active, "end 0 1 0", now.Add(11*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := nextProgressDeadline(active, now.Add(11*time.Minute)); !got.Equal(now.Add(21 * time.Minute)) {
		t.Fatal("idle deadline was not restored", got)
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
	queries, wave, err := runQueryStreams(ctx, db, first, r, d, nil)
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
