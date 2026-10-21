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
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
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
	type reportCase struct {
		name         string
		mode         string
		mutate       func(*WorkerRequest, *WorkerReceipt)
		mutateArm    func(*ArmRecord)
		mutateLoad   func(*WorkerRequest, *WorkerReceipt, *ArmRecord)
		mutateFrozen func(*Frozen)
		breakLoad    string
		armEngine    string
		cpus         []int
		changeConfig bool
		wantError    string
	}
	cases := make([]reportCase, 0, 85)
	cases = append(cases, []reportCase{
		{name: "different results are not verified"},
		{name: "CPU membership ignores campaign order", cpus: []int{30, 16, 28, 18, 26, 20, 24, 22}},
		{name: "equivalent CPU ranges are accepted", mutateArm: func(arm *ArmRecord) {
			arm.After.Raw["cpuset.cpus.effective"] = "0-3,4-7\n"
		}},
		{name: "archived captures need not be present", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.OutputRoot = filepath.Join(request.OutputRoot, "archived")
		}},
		{name: "worker receipt version differs", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Version = 0
		}, wantError: "worker receipt version"},
		{name: "worker failed after collecting timings", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Error = "worker failed"
		}, wantError: "failed/incomplete worker"},
		{name: "worker cleanup failed", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Cleanup = "failed"
		}, wantError: "failed/incomplete worker"},
		{name: "worker cleanup missing", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Cleanup = ""
		}, wantError: "failed/incomplete worker"},
		{name: "worker engine differs", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Engine = "duckdb"
		}, wantError: "worker engine"},
		{name: "request and receipt cannot substitute engine", mutate: func(request *WorkerRequest, r *WorkerReceipt) {
			request.Engine.Name, r.Engine = "duckdb", "duckdb"
		}, wantError: "worker request differs"},
		{name: "worker dataset hash differs", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Dataset.SHA256 = "other dataset"
		}, wantError: "worker dataset"},
		{name: "worker dataset size differs", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Dataset.Bytes++
		}, wantError: "worker dataset"},
		{name: "worker dataset missing", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Dataset = FileID{}
		}, wantError: "worker dataset"},
		{name: "worker variant differs", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Variant = "unlimited_diagnostic"
		}, wantError: "worker variant"},
		{name: "unlimited request cannot supply scored timing", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.Unlimited = true
		}, wantError: "worker request differs"},
		{name: "consistent unlimited workload cannot supply scored timing", mutate: func(request *WorkerRequest, r *WorkerReceipt) {
			request.Unlimited, r.Variant = true, "unlimited_diagnostic"
		}, wantError: "worker request differs"},
		{name: "worker client count differs", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Clients++
		}, wantError: "worker client count"},
		{name: "worker binary size differs", mutate: func(_ *WorkerRequest, r *WorkerReceipt) {
			r.Binary.Bytes++
		}, wantError: "worker executable"},
		{name: "request dataset differs", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.DatasetRoot = filepath.Join(request.DatasetRoot, "other")
		}, wantError: "worker request differs"},
		{name: "request database differs", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.Engine.Database = "tpch_compare_other"
		}, wantError: "worker request differs"},
		{name: "request threads differ", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.Engine.Threads++
		}, wantError: "worker request differs"},
		{name: "request memory differs", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.Engine.MemoryBytes++
		}, wantError: "worker request differs"},
		{name: "request cannot remove warmup", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.Warmup = false
		}, wantError: "worker request differs"},
		{name: "request cannot change seed", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.Seed++
		}, wantError: "worker request differs"},
		{name: "request cannot change query deadlines", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.DeadlinesNS = map[int]int64{1: 1}
		}, wantError: "worker request differs"},
		{name: "request cannot enable reference verification", mutate: func(request *WorkerRequest, _ *WorkerReceipt) {
			request.Reference = true
		}, wantError: "worker request differs"},
		{name: "arm engine differs", mutateArm: func(arm *ArmRecord) {
			arm.Engine = "duckdb"
		}, wantError: "paired arm differs"},
		{name: "arm trial differs", mutateArm: func(arm *ArmRecord) {
			arm.Trial.ID = "other trial"
		}, wantError: "paired arm differs"},
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
	}...)
	for _, engine := range []string{"mo", "duckdb"} {
		for _, load := range []struct {
			name, wantError string
			mutate          func(*WorkerRequest, *WorkerReceipt, *ArmRecord)
		}{
			{"worker error", "failed/incomplete worker", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Error = "load failed" }},
			{"worker cleanup failed", "failed/incomplete worker", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Cleanup = "failed" }},
			{"worker cleanup missing", "failed/incomplete worker", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Cleanup = "" }},
			{"worker version", "worker receipt version", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Version = 0 }},
			{"worker engine", "worker engine", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Engine = "other" }},
			{"worker dataset hash", "worker dataset", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Dataset.SHA256 = "other" }},
			{"worker dataset size", "worker dataset", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Dataset.Bytes++ }},
			{"worker binary hash", "worker executable", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Binary.SHA256 = "other" }},
			{"worker binary size", "worker executable", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Binary.Bytes++ }},
			{"missing timing", "load timing", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Load = nil }},
			{"negative timing", "load timing", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Load.StatisticsNS = -1 }},
			{"unfinished phase", "load timing", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Load.CheckpointNS = 0 }},
			{"query receipt substitution", "load worker", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Queries = []QueryReceipt{{Query: 1}} }},
			{"worker variant", "load worker", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Variant = "unlimited_diagnostic" }},
			{"worker clients", "load worker", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Clients = 2 }},
			{"worker query gate", "load worker", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.Controlled = true }},
			{"worker query window", "load worker", func(_ *WorkerRequest, r *WorkerReceipt, _ *ArmRecord) { r.WaveNS = 1 }},
			{"arm version", "load arm differs", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.Version = 0 }},
			{"arm status", "arm failed", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.Status = "failed" }},
			{"arm engine", "load arm differs", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.Engine = "other" }},
			{"arm scale", "load arm differs", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.Trial.Scale = "10" }},
			{"arm replicate", "load arm differs", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.Trial.Block++ }},
			{"arm error", "arm failed", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.Error = "controller failed" }},
			{"arm cleanup", "arm failed", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.Cleanup = "failed" }},
			{"request operation", "load request differs", func(r *WorkerRequest, _ *WorkerReceipt, _ *ArmRecord) { r.Operation = "verify" }},
			{"request version", "load request differs", func(r *WorkerRequest, _ *WorkerReceipt, _ *ArmRecord) { r.Version = 0 }},
			{"request dataset", "load request differs", func(r *WorkerRequest, _ *WorkerReceipt, _ *ArmRecord) { r.DatasetRoot += "-other" }},
			{"request engine", "load request differs", func(r *WorkerRequest, _ *WorkerReceipt, _ *ArmRecord) { r.Engine.Name = "other" }},
			{"request database", "load request differs", func(r *WorkerRequest, _ *WorkerReceipt, _ *ArmRecord) { r.Engine.Database += "-other" }},
			{"request memory", "load request differs", func(r *WorkerRequest, _ *WorkerReceipt, _ *ArmRecord) { r.Engine.MemoryBytes++ }},
			{"request threads", "load request differs", func(r *WorkerRequest, _ *WorkerReceipt, _ *ArmRecord) { r.Engine.Threads++ }},
			{"request query selection", "load request differs", func(r *WorkerRequest, _ *WorkerReceipt, _ *ArmRecord) { r.Queries = []int{1} }},
			{"request correctness scan", "load request differs", func(r *WorkerRequest, _ *WorkerReceipt, _ *ArmRecord) { r.Reference = true }},
			{"request deadline", "load request differs", func(r *WorkerRequest, _ *WorkerReceipt, _ *ArmRecord) { r.QueryDeadlineNS++ }},
			{"negative disk footprint", "load disk footprint", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.DiskBytes = -1 }},
			{"excess disk footprint", "load disk footprint", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.DiskBytes = 2 << 30 }},
			{"CPU membership", "CPU cap receipt", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) {
				a.Trailing.Raw["cpuset.cpus.effective"] = "8-15"
			}},
			{"memory limit", "resource cap receipt", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.Trailing.Raw["memory.max"] = "max" }},
			{"swap limit", "resource cap receipt", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.Trailing.Raw["memory.swap.max"] = "4096" }},
			{"OOM kill", "OOM kill", func(_ *WorkerRequest, _ *WorkerReceipt, a *ArmRecord) { a.Trailing.OOMKills = 1 }},
			{"archived load paths", "", func(r *WorkerRequest, receipt *WorkerReceipt, _ *ArmRecord) {
				r.OutputRoot += "-archived"
				receipt.Dataset.Path = "archived/dataset.json"
				receipt.Binary.Path = "archived/worker"
			}},
		} {
			cases = append(cases, reportCase{name: "load " + engine + " " + load.name, armEngine: engine, mutateLoad: load.mutate, wantError: load.wantError})
		}
	}
	for _, broken := range []string{"missing arm", "changed arm", "missing request", "changed request", "missing worker", "changed worker"} {
		want := "artifact changed"
		if strings.HasPrefix(broken, "missing") {
			want = "no such file"
		}
		cases = append(cases, reportCase{name: "load " + broken, breakLoad: broken, wantError: want})
	}
	for _, prepared := range []struct {
		name, wantError string
		mutate          func(*Frozen)
	}{
		{"missing scale", "load preparation", func(f *Frozen) { delete(f.Prepared, "1") }},
		{"unsupported frozen version", "unsupported frozen", func(f *Frozen) { f.Version = 0 }},
		{"missing engine", "load preparation", func(f *Frozen) { delete(f.Prepared["1"], "duckdb") }},
		{"missing replica", "load replicate", func(f *Frozen) { p := f.Prepared["1"]["duckdb"]; p.Loads = nil; f.Prepared["1"]["duckdb"] = p }},
		{"extra replica", "load replicate", func(f *Frozen) {
			p := f.Prepared["1"]["duckdb"]
			p.Loads = append(p.Loads, p.Loads[0])
			f.Prepared["1"]["duckdb"] = p
		}},
		{"cross-engine substitution", "load arm differs", func(f *Frozen) {
			p := f.Prepared["1"]["duckdb"]
			p.Loads = f.Prepared["1"]["mo"].Loads
			f.Prepared["1"]["duckdb"] = p
		}},
		{"kept deployment differs", "load deployment", func(f *Frozen) {
			p := f.Prepared["1"]["duckdb"]
			p.Deployment.Engine.Database += "-other"
			f.Prepared["1"]["duckdb"] = p
		}},
	} {
		cases = append(cases, reportCase{name: "load " + prepared.name, mutateFrozen: prepared.mutate, wantError: prepared.wantError})
	}
	cases = append(cases, reportCase{name: "load full campaign archives", mode: "full"}, reportCase{name: "load warm campaign archives", mode: "warm"})
	for _, preparation := range []struct {
		name, wantError string
		mutate          func(*Frozen)
	}{
		{"missing last replica", "load replicate", func(f *Frozen) {
			p := f.Prepared["100"]["duckdb"]
			p.Loads = p.Loads[:2]
			f.Prepared["100"]["duckdb"] = p
		}},
		{"duplicate replica", "load arm differs", func(f *Frozen) {
			p := f.Prepared["100"]["duckdb"]
			p.Loads[2] = p.Loads[0]
		}},
		{"swapped replicas", "load arm differs", func(f *Frozen) {
			p := f.Prepared["100"]["duckdb"]
			p.Loads[0], p.Loads[2] = p.Loads[2], p.Loads[0]
		}},
	} {
		cases = append(cases, reportCase{name: "load full " + preparation.name, mode: "full", mutateFrozen: preparation.mutate, wantError: preparation.wantError})
	}
	for _, engine := range []string{"mo", "duckdb"} {
		for _, boundary := range []string{"before", "after", "after cleanup"} {
			for _, resource := range []struct {
				name, key, value, wantError string
			}{
				{"different CPU membership", "cpuset.cpus.effective", "8-15", "CPU cap receipt"},
				{"duplicate CPU cannot replace missing CPU", "cpuset.cpus.effective", "0-6,6", "CPU cap receipt"},
				{"missing CPU set", "cpuset.cpus.effective", "", "CPU cap receipt"},
				{"unlimited memory", "memory.max", "max", "resource cap receipt"},
				{"different memory limit", "memory.max", "8589934592", "resource cap receipt"},
				{"enabled swap", "memory.swap.max", "4096", "resource cap receipt"},
			} {
				cases = append(cases, reportCase{
					name: fmt.Sprintf("%s %s %s", engine, boundary, resource.name), armEngine: engine, wantError: resource.wantError,
					mutateArm: func(arm *ArmRecord) {
						usage := &arm.Before
						if boundary == "after" {
							usage = &arm.After
						} else if boundary == "after cleanup" {
							usage = &arm.Trailing
						}
						usage.Raw[resource.key] = resource.value
					},
				})
			}
		}
		for _, counter := range []struct {
			name, wantError string
			mutate          func(*ArmRecord)
		}{
			{"OOM during measurement", "OOM kill", func(a *ArmRecord) { a.After.OOMKills++; a.Trailing.OOMKills++ }},
			{"OOM during cleanup", "OOM kill", func(a *ArmRecord) { a.Trailing.OOMKills++ }},
			{"OOM counter moved backwards", "counter moved backwards", func(a *ArmRecord) { a.After.OOMKills-- }},
			{"CPU counter moved backwards", "counter moved backwards", func(a *ArmRecord) { a.After.CPUUsec = a.Before.CPUUsec - 1 }},
			{"cleanup counter moved backwards", "counter moved backwards", func(a *ArmRecord) { a.Trailing.CPUUsec = a.After.CPUUsec - 1 }},
			{"read counter moved backwards", "counter moved backwards", func(a *ArmRecord) { a.After.ReadBytes = a.Before.ReadBytes - 1 }},
			{"write counter moved backwards", "counter moved backwards", func(a *ArmRecord) { a.After.WriteBytes = a.Before.WriteBytes - 1 }},
		} {
			cases = append(cases, reportCase{name: engine + " " + counter.name, armEngine: engine, mutateArm: counter.mutate, wantError: counter.wantError})
		}
	}
	for _, tc := range cases {
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
			if tc.mode != "" {
				c.Mode = tc.mode
				c.Datasets = map[string]string{"1": root, "10": filepath.Join(root, "sf10"), "100": filepath.Join(root, "sf100")}
			}
			if tc.cpus != nil {
				c.CPUs = tc.cpus
			}
			config := write("campaign.json", c)
			configPath := filepath.Join(root, config.Path)
			config.Path = configPath
			p := Preflight{Version: formatVersion, Config: config, Worker: FileID{SHA256: "worker", Bytes: 7}, DatasetIDs: map[string]FileID{}}
			for sf := range c.Datasets {
				p.DatasetIDs[sf] = FileID{SHA256: "dataset-" + sf, Bytes: 42}
			}
			dataset := p.DatasetIDs["1"]
			f := Frozen{Version: formatVersion, Preflight: write("preflight.json", p), Trials: schedule(c), Prepared: map[string]map[string]PreparedEngine{}}
			for sf := range c.Datasets {
				f.Prepared[sf] = map[string]PreparedEngine{}
				for index, engine := range []string{"mo", "duckdb"} {
					prepared := PreparedEngine{Deployment: loadDeployment(c, sf, engine, 1)}
					for rep := 1; rep <= c.loadReplicates(); rep++ {
						deployment := loadDeployment(c, sf, engine, rep)
						trial := Trial{ID: fmt.Sprintf("load-sf%s-%s-%d", sf, engine, rep), Experiment: "load", Scale: sf, Cores: 8, MemoryBytes: c.MemoryBytes, Clients: 1, Block: rep - 1}
						prefix := filepath.Join("preparation", trial.ID)
						request := WorkerRequest{Version: formatVersion, Operation: "load", Engine: deployment.Engine, DatasetRoot: c.Datasets[sf], OutputRoot: filepath.Join(root, prefix, "output"), QueryDeadlineNS: int64(30 * time.Minute)}
						receipt := WorkerReceipt{Version: formatVersion, Engine: engine, Dataset: p.DatasetIDs[sf], Binary: p.Worker, Variant: "qualification", Cleanup: "complete", Clients: 1,
							Load: &LoadReceipt{SchemaNS: int64(time.Millisecond), LoadNS: int64((2 + index) * int(time.Millisecond)), StatisticsNS: int64(time.Millisecond), CheckpointNS: int64(time.Millisecond)}}
						arm := ArmRecord{Version: formatVersion, Trial: trial, Engine: engine, Status: "pass", Cleanup: "complete", DiskBytes: int64(1024 + index)}
						arm.Trailing.Raw = map[string]string{"memory.max": strconv.FormatInt(c.MemoryBytes, 10), "memory.swap.max": "0", "cpuset.cpus.effective": cpuSet(c.CPUs)}
						if tc.mutateLoad != nil && tc.armEngine == engine {
							tc.mutateLoad(&request, &receipt, &arm)
						}
						arm.Request = write(filepath.Join(prefix, "request.json"), request)
						arm.Request.Path = "request.json"
						arm.Worker = write(filepath.Join(prefix, "output", "receipt.json"), receipt)
						arm.Worker.Path = filepath.Join("output", "receipt.json")
						pin := write(filepath.Join(prefix, "arm.json"), arm)
						prepared.Loads = append(prepared.Loads, pin)
						if tc.breakLoad != "" && engine == "duckdb" && sf == "1" && rep == 1 {
							path := filepath.Join(root, pin.Path)
							if strings.HasSuffix(tc.breakLoad, "request") {
								path = filepath.Join(root, prefix, arm.Request.Path)
							} else if strings.HasSuffix(tc.breakLoad, "worker") {
								path = filepath.Join(root, prefix, arm.Worker.Path)
							}
							if err := os.Remove(path); err != nil {
								t.Fatal(err)
							}
							if strings.HasPrefix(tc.breakLoad, "changed") {
								if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
									t.Fatal(err)
								}
							}
						}
					}
					f.Prepared[sf][engine] = prepared
				}
			}
			pair := PairRecord{Version: formatVersion, Trial: f.Trials[0], Status: "pass", Arms: map[string]ArmRecord{}}
			for index, engine := range []string{"mo", "duckdb"} {
				prefix := filepath.Join("runs", "baseline", pair.Trial.ID, engine)
				r := WorkerReceipt{Version: formatVersion, Engine: engine, Dataset: dataset, Binary: p.Worker, Variant: "qualification", Cleanup: "complete", Controlled: true, Clients: pair.Trial.Clients, WaveNS: int64(time.Second)}
				for _, q := range pair.Trial.Queries {
					// Captures deliberately do not exist, and the engines return
					// different row counts. Reporting reads timing receipts only.
					r.Queries = append(r.Queries, QueryReceipt{Query: q, Capture: Capture{Rows: int64(index), ElapsedNS: int64(time.Millisecond)}})
				}
				request := runRequest(pair.Trial, true)
				request.Engine = f.Prepared["1"][engine].Deployment.Engine
				request.Engine.Threads, request.Engine.MemoryBytes = pair.Trial.Cores, pair.Trial.MemoryBytes
				request.DatasetRoot, request.OutputRoot = c.Datasets["1"], filepath.Join(root, prefix, "output")
				if tc.mutate != nil {
					tc.mutate(&request, &r)
				}
				usage := func(cpu uint64) Usage {
					return Usage{CPUUsec: cpu, ReadBytes: cpu, WriteBytes: cpu, OOMKills: 2, Raw: map[string]string{"memory.max": strconv.FormatInt(pair.Trial.MemoryBytes, 10), "memory.swap.max": "0", "cpuset.cpus.effective": cpuSet(c.CPUs[:pair.Trial.Cores])}}
				}
				pair.Arms[engine] = ArmRecord{Version: formatVersion, Trial: pair.Trial, Engine: engine, Status: "pass", Cleanup: "complete", Request: write(filepath.Join(prefix, "request.json"), request), Worker: write(filepath.Join(prefix, "receipt.json"), r), Before: usage(500), After: usage(1000), Trailing: usage(1100)}
				arm := pair.Arms[engine]
				arm.Request.Path = "request.json"
				arm.Worker.Path = "receipt.json"
				if tc.mutateArm != nil && (tc.armEngine == "" || tc.armEngine == engine) {
					tc.mutateArm(&arm)
				}
				pair.Arms[engine] = arm
			}
			pin := write(filepath.Join("runs", "baseline", pair.Trial.ID, "pair.json"), pair)
			pin.Path = filepath.Join(pair.Trial.ID, "pair.json")
			if tc.mutateFrozen != nil {
				tc.mutateFrozen(&f)
			}
			write("runs/baseline/run.json", RunManifest{Version: formatVersion, Frozen: write("frozen.json", f), Pairs: []FileID{pin}})
			if tc.changeConfig {
				c.MORevision = "NEVER-MEASURED-REVISION"
				configPath = filepath.Join(root, write("different-campaign.json", c).Path)
			}
			output := filepath.Join(root, "report")
			err := cli(t.Context(), []string{"report", "--config", configPath, "--run-dir", filepath.Join(root, "runs", "baseline"), "--output", output}, io.Discard, io.Discard)
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
			status := "complete"
			if tc.mode != "" {
				// Mode controls exercise the full preparation matrix while
				// retaining only one measured pair; scores must stay incomplete.
				status = "incomplete"
			}
			if err != nil || !strings.Contains(string(body), "Run status: **"+status+"**") || !strings.Contains(string(body), "does not perform correctness verification") {
				t.Fatal(string(body), err)
			}
			for _, row := range []string{"| 1 | mo | 1 | 0.001000 | 0.002000 | 0.001000 | 0.001000 | 1024 |", "| 1 | duckdb | 1 | 0.001000 | 0.003000 | 0.001000 | 0.001000 | 1025 |"} {
				if !strings.Contains(string(body), row) {
					t.Fatal("admitted load timing missing from report", row)
				}
			}
			if tc.mode == "full" && !strings.Contains(string(body), "| 100 | duckdb | 3 |") {
				t.Fatal("discarded replica timing is missing from the archived full-campaign report")
			}
		})
	}
}

func TestScoredResourcesUseTrialSubsetAndMemory(t *testing.T) {
	c := campaignFixture(t.TempDir())
	c.CPUs = []int{7, 5, 3, 1, 6, 4, 2, 0}
	for _, tc := range []struct {
		name, cpus, memory, wantError string
	}{
		{"trial budget", "1,3,5,7", "8589934592", ""},
		{"different four CPUs", "0-3", "8589934592", "CPU cap receipt"},
		{"campaign memory instead of trial", "1,3,5,7", "34359738368", "resource cap receipt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := Usage{Raw: map[string]string{"cpuset.cpus.effective": tc.cpus, "memory.max": tc.memory, "memory.swap.max": "0"}}
			a := ArmRecord{Trial: Trial{Cores: 4, MemoryBytes: 8 << 30}, Before: u, After: u, Trailing: u}
			err := verifyScoredResources(c, a)
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c.CPUs, []int{7, 5, 3, 1, 6, 4, 2, 0}) {
				t.Fatal("resource admission changed the campaign CPU order", c.CPUs)
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
