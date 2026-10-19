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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPerfCounterCoverage(t *testing.T) {
	values, err := parsePerfStat(strings.NewReader("1000,,cycles:u,1000000,99.50,,\n<not supported>,,cache-misses:u,0,0.00,,\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0].Quality != "measured" || values[0].RunningRatio != 0.995 || values[1].Quality != "unavailable" {
		t.Fatal(values)
	}
	if _, err := parsePerfStat(strings.NewReader("")); err == nil {
		t.Fatal("empty profiler output accepted")
	}
}

func TestWeightedNativeStacksPreserveProcessRoles(t *testing.T) {
	raw := "123 124 12.000000: 10000000 cpu-clock:u:\n    1234 duckdb::HashJoin::Probe+0x20 (worker)\n    5678 main.main (worker)\n\n234 235 12.100000: 10000000 cpu-clock:u:\n    1234 [unknown] (unknown)\n\n"
	r, err := parsePerfScript(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if r.Samples != 2 || r.WeightNS != 20_000_000 || r.KnownRatio != 0.5 || r.Quality != "partial" {
		t.Fatal(r)
	}
	if r.Stacks["pid=123;main.main;duckdb::HashJoin::Probe"] != 10_000_000 {
		t.Fatal(r.Stacks)
	}
	if r.Hot["pid=234 [unknown]"] != 10_000_000 {
		t.Fatal(r.Hot)
	}
}

func TestNativePlanSemanticsRemainExact(t *testing.T) {
	v, err := decodeNativeJSON([]byte(`{"matrixone":{"nodes":[{"operator":"Table Scan","table_name":"lineitem","statistics":{"rows":9007199254740993}}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	var work []NativeWork
	nativeWork(v, "plan", &work)
	if len(work) != 1 || work[0].Family != "scan/decode" || work[0].Facts["table_name"] != "lineitem" {
		t.Fatal(work)
	}
	stats := work[0].Facts["statistics"].(map[string]any)
	if stats["rows"].(json.Number).String() != "9007199254740993" {
		t.Fatal(stats)
	}
	if metricMeanings()["mo.DiskIO"].Quality == "" || !strings.Contains(metricMeanings()["duckdb.CPU_TIME"].Aggregation, "operator") {
		t.Fatal("metric semantics lost")
	}
}

func TestActualDuckDBOperatorProfile(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 15*time.Second, errors.New("operator fixture deadline"))
	defer cancel()
	e := Engine{Name: "duckdb", Database: filepath.Join(t.TempDir(), "fixture.db"), Threads: 1, MemoryBytes: 256 << 20}
	db, err := openEngine(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := configureConn(ctx, db, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	root := t.TempDir()
	output := filepath.Join(root, "output")
	if err := os.Mkdir(output, 0750); err != nil {
		t.Fatal(err)
	}
	r := WorkerRequest{Engine: e, Profile: &ProfileSpec{Kind: "operator"}, OutputRoot: output}
	if err := configureNativeProfile(ctx, conn, r); err != nil {
		t.Fatal(err)
	}
	q := Query{Number: 1, Columns: []Column{{Kind: "int"}}, Statements: []string{"SELECT SUM(i) FROM (VALUES (1),(2)) t(i)"}}
	native := &nativeProfile{engine: "duckdb", root: output, prefix: "native-q01-s0-r0"}
	cap, err := executeUnitClock(ctx, conn, q, filepath.Join(output, "q.rows"), captureDisk, nil, native)
	if err != nil {
		t.Fatal(err)
	}
	if cap.Rows != 1 {
		t.Fatal(cap)
	}
	if err := normalizeNativeCapture(root, "duckdb"); err != nil {
		t.Fatal(err)
	}
}

func TestPerfCollectorIntegration(t *testing.T) {
	if os.Getenv("TPCH_COMPARE_PERF_INTEGRATION") != "1" {
		t.Skip("explicit perf-capable-host qualification")
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), 20*time.Second, errors.New("perf integration guard"))
	defer cancel()
	root := t.TempDir()
	profile, err := startParentProfile(ctx, ProfileSpec{Kind: "cpu"}, root, []int{os.Getpid()}, deployment{Engine: Engine{Name: "duckdb"}})
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = profile.stop()
		}
	})
	e := Engine{Name: "duckdb", Database: filepath.Join(t.TempDir(), "cpu.db"), Threads: 1, MemoryBytes: 256 << 20}
	db, err := openEngine(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	start := time.Now()
	for time.Since(start) < time.Second {
		var value int64
		if err := db.QueryRowContext(ctx, "SELECT SUM(i*i) FROM range(100000) t(i)").Scan(&value); err != nil {
			t.Fatal(err)
		}
	}
	record := profile.stop()
	stopped = true
	if record.Status != "complete" {
		t.Fatal(record)
	}
	if err := analyzePerf(ctx, root); err != nil {
		t.Fatal(err)
	}
	stacks, err := readJSON[StackEvidence](filepath.Join(root, "stacks.json"))
	if err != nil {
		t.Fatal(err)
	}
	if stacks.Samples < 1 {
		t.Fatal("no native CPU samples")
	}
}
