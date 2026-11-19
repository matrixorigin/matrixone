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
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func writeProfileMetadata(t *testing.T, root, path string, value any) FileID {
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

// Only metadata is needed to test scheduling admission; no engines, cgroups,
// datasets or query result captures are created.
func profileCampaignFixture(t *testing.T) (Campaign, Preflight, Frozen) {
	t.Helper()
	c := campaignFixture(t.TempDir())
	p := Preflight{Version: formatVersion, Worker: FileID{SHA256: "worker", Bytes: 7}, DatasetIDs: map[string]FileID{"1": {SHA256: "dataset", Bytes: 8}}}
	f := Frozen{Version: formatVersion, Prepared: map[string]map[string]PreparedEngine{"1": {}}, DeadlinesNS: map[string]map[int]int64{"1": {}}}
	for _, engine := range []string{"mo", "duckdb"} {
		d := loadDeployment(c, "1", engine, 1)
		trial := pilotTrial(c, "1", engine)
		prefix := filepath.Join("preparation", trial.ID)
		request := runRequest(trial, true)
		request.Engine, request.DatasetRoot = d.Engine, c.Datasets["1"]
		request.OutputRoot = filepath.Join(c.Root, prefix, "output")
		receipt := WorkerReceipt{Version: formatVersion, Engine: engine, Binary: p.Worker, Dataset: p.DatasetIDs["1"],
			Variant: "qualification", Clients: 1, Controlled: true, WaveNS: int64(time.Second), Cleanup: "complete"}
		for _, q := range allQueries() {
			receipt.Queries = append(receipt.Queries, QueryReceipt{Query: q, Capture: Capture{Version: formatVersion, ElapsedNS: int64(q) * int64(time.Millisecond)}})
		}
		record := ArmRecord{Version: formatVersion, Trial: trial, Engine: engine, Status: "pass", Cleanup: "complete"}
		for _, u := range []*Usage{&record.Before, &record.After, &record.Trailing} {
			u.Raw = map[string]string{"memory.max": strconv.FormatInt(c.MemoryBytes, 10), "memory.swap.max": "0", "cpuset.cpus.effective": cpuSet(c.CPUs)}
		}
		record.Request = writeProfileMetadata(t, c.Root, filepath.Join(prefix, "request.json"), request)
		record.Request.Path = "request.json"
		record.Worker = writeProfileMetadata(t, c.Root, filepath.Join(prefix, "output", "receipt.json"), receipt)
		record.Worker.Path = filepath.Join("output", "receipt.json")
		pin := writeProfileMetadata(t, c.Root, filepath.Join(prefix, "arm.json"), record)
		f.Prepared["1"][engine] = PreparedEngine{Deployment: d, Pilot: pin}
	}
	writeProfileMetadata(t, c.Root, "frozen.json", f)
	return c, p, f
}

func TestProfileCleanupAdmission(t *testing.T) {
	for _, pilot := range []bool{false, true} {
		for _, engine := range []string{"mo", "duckdb"} {
			for _, tc := range []struct {
				name, cleanup     string
				armError, loseArm bool
			}{
				{name: "quiescent failure allows next arm", cleanup: "complete", armError: true},
				{name: "failed cleanup", cleanup: "failed", armError: true},
				{name: "failed cleanup without error", cleanup: "failed"},
				{name: "missing cleanup proof"},
				{name: "unknown cleanup state", cleanup: "unknown"},
				{name: "artifact error preserves cleanup cause", cleanup: "failed", armError: true, loseArm: true},
			} {
				t.Run(fmt.Sprintf("pilot=%t/%s/%s", pilot, engine, tc.name), func(t *testing.T) {
					c, p, f := profileCampaignFixture(t)
					ctx, cancel := context.WithCancelCause(t.Context())
					defer cancel(nil)
					cause := errors.New("injected diagnostic failure")
					stopped := errors.New("control admitted its next arm")
					calls, target := 0, 0
					runner := func(_ context.Context, _ Campaign, _ Preflight, _ *cgroupManager, d deployment, trial Trial, root string, request WorkerRequest) (ArmRecord, error) {
						calls++
						if !request.Warmup || !request.WarmupSelected || request.Reference || request.Unlimited {
							t.Fatal("diagnostic/control does not discard its cold run", request)
						}
						record := ArmRecord{Version: formatVersion, Trial: trial, Engine: d.Engine.Name, Status: "failed", Cleanup: "complete", Error: cause.Error()}
						var err error = cause
						if target != 0 {
							cancel(stopped)
						} else if d.Engine.Name == engine {
							target = calls
							record.Cleanup = tc.cleanup
							if !tc.armError {
								err = nil
							}
						}
						if target != calls || !tc.loseArm {
							writeProfileMetadata(t, root, "arm.json", record)
						}
						return record, err
					}
					err := executeProfileSchedule(ctx, c, p, f, nil, "attempt", pilot, nil, runner)
					wantCalls := target
					if tc.cleanup == "complete" {
						wantCalls++
						if !errors.Is(err, stopped) {
							t.Fatal("quiescent failure did not allow the next arm", err)
						}
					} else if err == nil || !strings.Contains(err.Error(), "cleanup") {
						t.Fatal("incomplete cleanup did not stop profiling", err)
					}
					if target == 0 || calls != wantCalls {
						t.Fatalf("admitted %d arms, want %d", calls, wantCalls)
					}
					if tc.armError && !errors.Is(err, cause) {
						t.Fatal("original arm failure lost", err)
					}
					if tc.loseArm && !errors.Is(err, os.ErrNotExist) {
						t.Fatal("artifact failure lost", err)
					}
					root := filepath.Join(c.Root, "profiles", "attempt")
					manifest, err := readJSON[ProfileManifest](filepath.Join(root, "profiles.json"))
					if err != nil || manifest.Error == "" || len(manifest.Attempts) != calls {
						t.Fatal("failure evidence missing", err, manifest)
					}
					failed := manifest.Attempts[target-1]
					if failed.Engine != engine || failed.Status != "failed" || failed.Error == "" {
						t.Fatal("failed arm identity/status lost", failed)
					}
					if !tc.loseArm {
						if err := failed.Arm.verify(root); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func TestProfilePilotUsesRecordedQueryIdentity(t *testing.T) {
	c, p, f := profileCampaignFixture(t)
	prepared := f.Prepared["1"]["duckdb"]
	root := filepath.Dir(filepath.Join(c.Root, prepared.Pilot.Path))
	record, err := readJSON[ArmRecord](filepath.Join(root, "arm.json"))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := readJSON[WorkerReceipt](filepath.Join(root, record.Worker.Path))
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(receipt.Queries)
	if err := os.Remove(filepath.Join(root, record.Worker.Path)); err != nil {
		t.Fatal(err)
	}
	record.Worker = writeProfileMetadata(t, root, filepath.Join("output", "receipt.json"), receipt)
	if err := os.Remove(filepath.Join(root, "arm.json")); err != nil {
		t.Fatal(err)
	}
	prepared.Pilot = writeProfileMetadata(t, c.Root, filepath.Join("preparation", record.Trial.ID, "arm.json"), record)
	f.Prepared["1"]["duckdb"] = prepared
	if _, elapsed, err := profilePilotTiming(c, p, f, "1", "duckdb", 6); err != nil || elapsed != int64(6*time.Millisecond) {
		t.Fatalf("query identity was replaced by its slice position: %d %v", elapsed, err)
	}
	p.Worker.Bytes++
	if _, _, err := profilePilotTiming(c, p, f, "1", "duckdb", 6); err == nil || !strings.Contains(err.Error(), "worker executable") {
		t.Fatal("foreign worker accepted", err)
	}
}

func TestDeepProfilesRequireCompleteWarmCells(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(map[string]*reportCell)
	}{
		{name: "complete"},
		{name: "missing scale", mutate: func(c map[string]*reportCell) { delete(c, "100") }},
		{name: "failed cell", mutate: func(c map[string]*reportCell) { c["10"].Failures = []string{"failed arm"} }},
		{name: "missing query", mutate: func(c map[string]*reportCell) { delete(c["10"].Queries, 22) }},
		{name: "missing block", mutate: func(c map[string]*reportCell) { c["10"].Queries[1] = c["10"].Queries[1][:9] }},
		{name: "wrong stream count", mutate: func(c map[string]*reportCell) {
			c["10"].Queries[1][0] = append(c["10"].Queries[1][0], pairedTimes{1, 1})
		}},
		{name: "invalid timing", mutate: func(c map[string]*reportCell) { c["10"].Queries[1][0][0].MO = math.NaN() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cells := map[string]*reportCell{}
			for _, sf := range []string{"10", "100"} {
				cell := &reportCell{Trial: Trial{Experiment: "warm", Scale: sf}, Expected: 10, Suites: make([]pairedTimes, 10), Queries: map[int][][]pairedTimes{}}
				for _, q := range allQueries() {
					for range 10 {
						cell.Queries[q] = append(cell.Queries[q], []pairedTimes{{MO: 1, DuckDB: float64(q)}})
					}
				}
				cells[sf] = cell
			}
			if tc.mutate != nil {
				tc.mutate(cells)
			}
			selected, err := selectDeepProfiles(cells)
			if tc.mutate != nil {
				if err == nil {
					t.Fatal("incomplete/invalid evidence selected diagnostic targets")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, sf := range []string{"10", "100"} {
				for _, q := range []int{1, 2, 3, 4, 5, 6, 9, 18, 19, 20, 21, 22} {
					if !selected[sf][q] {
						t.Fatalf("missing extreme or semantic control SF%s Q%d", sf, q)
					}
				}
				if selected[sf][10] {
					t.Fatal("middle gap unexpectedly selected as an extreme")
				}
			}
		})
	}
}
