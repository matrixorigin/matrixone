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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCampaignCleanupAdmission(t *testing.T) {
	for _, prime := range []bool{false, true} {
		for _, engine := range []string{"mo", "duckdb"} {
			for _, tc := range []struct {
				name, cleanup string
				workerError   bool
				stop          bool
				trial         int
				blockPair     bool
			}{
				{name: "success", cleanup: "complete"},
				{name: "ordinary failure after cleanup", cleanup: "complete", workerError: true},
				{name: "incomplete cleanup", cleanup: "failed", workerError: true, stop: true},
				{name: "incomplete cleanup without error", cleanup: "failed", stop: true},
				{name: "missing cleanup proof", stop: true},
				{name: "unknown cleanup state", cleanup: "unknown", stop: true},
				{name: "later group cleanup", cleanup: "failed", workerError: true, stop: true, trial: 1},
				{name: "pair publication failure", cleanup: "failed", workerError: true, stop: true, blockPair: true},
			} {
				phase := "warm"
				if prime {
					phase = "warm-prime"
				}
				t.Run(phase+"/"+engine+"/"+tc.name, func(t *testing.T) {
					root := t.TempDir()
					c := campaignFixture(root)
					// Only the scheduler/cleanup boundary is exercised. No
					// engines, cgroups, source data or query captures are needed.
					first := "warm"
					want := []string{"warm/mo", "warm/duckdb", "cpu/mo", "cpu/duckdb", "memory/mo", "memory/duckdb"}
					if prime {
						first = "process-warm-os"
						want = []string{"warm-prime/mo", "process-warm-os/mo", "warm-prime/duckdb", "process-warm-os/duckdb", "cpu/mo", "cpu/duckdb", "memory/mo", "memory/duckdb"}
					}
					frozen := Frozen{Version: formatVersion, Prepared: map[string]map[string]PreparedEngine{}, Trials: []Trial{
						{ID: "first", Experiment: first, Scale: "1", Cores: 8, MemoryBytes: c.MemoryBytes, Clients: 1, Queries: []int{1}, Arms: []string{"mo", "duckdb"}},
						{ID: "next", Experiment: "cpu", Scale: "10", Cores: 4, MemoryBytes: c.MemoryBytes, Clients: 1, Queries: []int{1}, Arms: []string{"mo", "duckdb"}},
						{ID: "last", Experiment: "memory", Scale: "100", Cores: 8, MemoryBytes: 8 << 30, Clients: 1, Queries: []int{1}, Arms: []string{"mo", "duckdb"}},
					}}
					for _, sf := range []string{"1", "10", "100"} {
						frozen.Prepared[sf] = map[string]PreparedEngine{}
						for _, name := range []string{"mo", "duckdb"} {
							frozen.Prepared[sf][name] = PreparedEngine{Deployment: deployment{Engine: Engine{Name: name}}}
						}
					}
					if err := publishJSON(filepath.Join(root, "frozen.json"), frozen); err != nil {
						t.Fatal(err)
					}
					targetPhase := phase
					if tc.trial == 1 {
						targetPhase = "cpu"
					}
					cause := errors.New("injected arm failure")
					var calls []string
					targetIndex := -1
					failedArm := ""
					run := func(_ context.Context, _ Campaign, _ Preflight, _ *cgroupManager, d deployment, trial Trial, path string, _ WorkerRequest) (ArmRecord, error) {
						calls = append(calls, trial.Experiment+"/"+d.Engine.Name)
						record := ArmRecord{Version: formatVersion, Trial: trial, Engine: d.Engine.Name, Status: "pass", Cleanup: "complete"}
						var err error
						if trial.ID == frozen.Trials[tc.trial].ID && trial.Experiment == targetPhase && d.Engine.Name == engine {
							targetIndex = len(calls) - 1
							record.Cleanup = tc.cleanup
							if tc.workerError {
								err = cause
								record.Status, record.Error = "failed", cause.Error()
							}
							failedArm = filepath.Join(path, "arm.json")
							if tc.blockPair {
								if e := os.Mkdir(filepath.Join(filepath.Dir(path), "pair.json"), 0750); e != nil {
									t.Fatal(e)
								}
							}
						}
						if e := os.MkdirAll(path, 0750); e != nil {
							t.Fatal(e)
						}
						if e := publishJSON(filepath.Join(path, "arm.json"), record); e != nil {
							t.Fatal(e)
						}
						return record, err
					}
					manifest, err := executeCampaign(t.Context(), c, Preflight{}, frozen, nil, "attempt", run)
					if targetIndex < 0 {
						t.Fatal("failure injection was not reached")
					}
					if tc.stop {
						want = want[:targetIndex+1]
						if err == nil || !strings.Contains(err.Error(), "cleanup") {
							t.Errorf("unsafe cleanup did not fail the campaign: %v", err)
						}
						if tc.workerError && !errors.Is(err, cause) {
							t.Errorf("lost original arm failure: %v", err)
						}
					} else if err != nil {
						t.Errorf("quiescent attempt stopped the campaign: %v", err)
					}
					if !reflect.DeepEqual(calls, want) {
						t.Errorf("admitted phases %v; want %v", calls, want)
					}
					if tc.blockPair && !errors.Is(err, os.ErrExist) {
						t.Errorf("lost pair-publication failure: %v", err)
					}
					stored, e := readJSON[RunManifest](filepath.Join(root, "runs", "attempt", "run.json"))
					if e != nil {
						t.Fatal(e)
					}
					if !reflect.DeepEqual(stored, manifest) || stored.Finished.IsZero() || (stored.Error != "") != tc.stop {
						t.Errorf("inconsistent terminal run manifest: %+v / %+v", stored, manifest)
					}
					wantPairs := len(frozen.Trials)
					if tc.stop {
						wantPairs = tc.trial + 1
					}
					if tc.blockPair {
						wantPairs--
					}
					if len(stored.Pairs) != wantPairs {
						t.Errorf("retained %d pairs, want %d", len(stored.Pairs), wantPairs)
					}
					for i, pin := range stored.Pairs {
						base := filepath.Join(root, "runs", "attempt")
						if e := pin.verify(base); e != nil {
							t.Fatal(e)
						}
						pair, e := readJSON[PairRecord](filepath.Join(base, pin.Path))
						if e != nil {
							t.Fatal(e)
						}
						failed := i == tc.trial && (tc.stop || tc.workerError)
						if pair.Trial.ID != frozen.Trials[i].ID || (pair.Status == "failed") != failed || (pair.Error != "") != failed {
							t.Errorf("incorrect attempt status: %+v", pair)
						}
						if failed && tc.stop && !strings.Contains(pair.Error, "cleanup") {
							t.Errorf("failed pair lost cleanup outcome: %s", pair.Error)
						}
					}
					arm, e := readJSON[ArmRecord](failedArm)
					if e != nil || arm.Cleanup != tc.cleanup {
						t.Errorf("failed attempt evidence was not preserved: %+v, %v", arm, e)
					}
				})
			}
		}
	}
}

func TestApplicationQuiescenceRequiresJoinedProcesses(t *testing.T) {
	for _, tc := range []struct {
		name, server, worker, procs, wantError string
		missing                                bool
		lateJoin                               bool
	}{
		{name: "no processes acquired"},
		{name: "partial startup joined", server: "joined"},
		{name: "all joined", server: "joined", worker: "joined", procs: "\n"},
		{name: "worker join pending", server: "joined", worker: "running", wantError: "not joined"},
		{name: "server join pending", server: "running", worker: "joined", wantError: "not joined"},
		{name: "join proof absent", worker: "unknown", wantError: "not joined"},
		{name: "unmanaged descendant remains", server: "joined", worker: "joined", procs: "12345\n", wantError: "still owns processes"},
		{name: "membership unavailable", server: "joined", worker: "joined", missing: true, wantError: "no such file"},
		{name: "late worker join", server: "joined", worker: "running", wantError: "not joined", lateJoin: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			group := &appGroup{path: t.TempDir()}
			if !tc.missing {
				if err := os.WriteFile(filepath.Join(group.path, "cgroup.procs"), []byte(tc.procs), 0600); err != nil {
					t.Fatal(err)
				}
			}
			process := func(state string) *managedProcess {
				if state == "" {
					return nil
				}
				p := &managedProcess{}
				if state != "unknown" {
					p.done = make(chan struct{})
				}
				if state == "joined" {
					// A worker can fail normally yet join completely. Its exit
					// status is distinct from process/log ownership release.
					p.waitErr = errors.New("ordinary execution error")
					close(p.done)
				}
				return p
			}
			server, worker := process(tc.server), process(tc.worker)
			err := group.quiescent(server, worker)
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("unproven quiescence accepted: %v", err)
			}
			if tc.missing && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("membership error was lost: %v", err)
			}
			if tc.lateJoin {
				close(worker.done)
				if err := group.quiescent(server, worker); err != nil {
					t.Fatalf("finished cleanup still rejected: %v", err)
				}
			}
		})
	}
}
