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
	"reflect"
	"slices"
	"time"
)

type ProfileAttempt struct {
	Scale  string `json:"scale"`
	Query  int    `json:"query"`
	Engine string `json:"engine"`
	Kind   string `json:"kind"`
	Cores  int    `json:"cores"`
	Arm    FileID `json:"arm"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}
type ProfileManifest struct {
	Version   int              `json:"version"`
	Pilot     bool             `json:"pilot"`
	Frozen    FileID           `json:"frozen"`
	Attempts  []ProfileAttempt `json:"attempts"`
	Artifacts []FileID         `json:"artifacts"`
	Error     string           `json:"error,omitempty"`
}

func profileCampaign(ctx context.Context, config, id, baseline string, pilot bool) (retErr error) {
	c, err := readJSON[Campaign](config)
	if err != nil {
		return err
	}
	if err := c.validate(); err != nil {
		return err
	}
	p, err := readJSON[Preflight](filepath.Join(c.Root, "preflight.json"))
	if err != nil {
		return err
	}
	if err := campaignPin(c, p); err != nil {
		return err
	}
	lease, err := campaignLease(c.Root)
	if err != nil {
		return err
	}
	defer lease.Close()
	f, err := readJSON[Frozen](filepath.Join(c.Root, "frozen.json"))
	if err != nil {
		return err
	}
	if !identifier.MatchString(id) {
		return errors.New("profile ID must be a lower-case identifier")
	}
	if f.Version != formatVersion {
		return errors.New("unsupported frozen format")
	}
	if err := f.Preflight.verify(c.Root); err != nil {
		return err
	}
	preflight, err := readJSON[Preflight](filepath.Join(c.Root, f.Preflight.Path))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(preflight, p) {
		return errors.New("profiling preflight differs from frozen campaign")
	}
	deep := map[string]map[int]bool{}
	if !pilot {
		deep, err = deepProfileSelection(c, f, baseline)
		if err != nil {
			return err
		}
	}
	manager, err := setupCgroups(c)
	if err != nil {
		return err
	}
	return executeProfileSchedule(ctx, c, p, f, manager, id, pilot, deep, runArm)
}

func executeProfileSchedule(ctx context.Context, c Campaign, p Preflight, f Frozen, manager *cgroupManager, id string, pilot bool, deep map[string]map[int]bool, arm armRunner) (retErr error) {
	root := filepath.Join(c.Root, "profiles", id)
	if err := os.MkdirAll(filepath.Dir(root), 0750); err != nil {
		return err
	}
	if err := os.Mkdir(root, 0750); err != nil {
		return err
	}
	manifest := ProfileManifest{Version: formatVersion, Pilot: pilot}
	var err error
	manifest.Frozen, err = artifact(c.Root, filepath.Join(c.Root, "frozen.json"))
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			manifest.Error = retErr.Error()
		}
		walkErr := filepath.WalkDir(root, func(path string, e os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !e.Type().IsRegular() {
				return nil
			}
			pin, err := artifact(root, path)
			if err != nil {
				return err
			}
			manifest.Artifacts = append(manifest.Artifacts, pin)
			return nil
		})
		retErr = errors.Join(retErr, walkErr, publishJSON(filepath.Join(root, "profiles.json"), manifest))
	}()
	runAt := func(sf string, q int, engine, kind string, rep, cores int) error {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		used, err := diskUsage(root)
		if err != nil {
			return err
		}
		if used >= c.ProfileBytes {
			return errors.New("profile campaign quota exhausted")
		}
		prepared, timeNS, err := profilePilotTiming(c, p, f, sf, engine, q)
		if err != nil {
			return err
		}
		if cores == 1 {
			timeNS *= 8
		}
		target := int64(20 * time.Second)
		maxReps := 2000
		if pilot {
			target = int64(time.Second)
			maxReps = 100
		}
		reps := min(max(1, int(math.Ceil(float64(target)/float64(max(timeNS, 1))))), maxReps)
		if kind == "operator" {
			reps = 1
		}
		t := Trial{ID: fmt.Sprintf("%s-sf%s-q%02d-%s-c%d-%d", kind, sf, q, engine, cores, rep), Experiment: "profile", Scale: sf, Cores: cores, MemoryBytes: c.MemoryBytes, Clients: 1, Queries: []int{q}}
		request := runRequest(t, true)
		// Discard a target warm run before collectors start, including controls.
		request.WarmupSelected = true
		request.Repetitions = reps
		if kind != "operator" {
			request.MaxWindowNS = int64(60 * time.Second)
		}
		request.DeadlinesNS = f.DeadlinesNS[sf]
		if kind != "control" {
			request.Profile = &ProfileSpec{Kind: kind}
		}
		path := filepath.Join(root, t.ID)
		record, runErr := arm(ctx, c, p, manager, prepared.Deployment, t, path, request)
		unsafeCleanup := record.Cleanup != "complete"
		if unsafeCleanup {
			runErr = errors.Join(runErr, fmt.Errorf("cleanup %q did not establish application quiescence", record.Cleanup))
		}
		attempt := ProfileAttempt{Scale: sf, Query: q, Engine: engine, Kind: kind, Cores: cores, Status: "complete"}
		attempt.Arm, err = artifact(root, filepath.Join(path, "arm.json"))
		if err != nil {
			err = errors.Join(runErr, err)
			attempt.Status, attempt.Error = "failed", err.Error()
			manifest.Attempts = append(manifest.Attempts, attempt)
			return err
		}
		if runErr != nil {
			attempt.Status = "failed"
			attempt.Error = runErr.Error()
		} else {
			if record.Profile != nil && record.Profile.Status != "complete" {
				attempt.Status = "partial"
				attempt.Error = fmt.Sprint(record.Profile.Errors)
			}
			if kind == "operator" {
				if err := normalizeNativeCapture(path, engine); err != nil {
					attempt.Status = "partial"
					attempt.Error = err.Error()
				}
			}
			if kind == "cpu" || kind == "wait" || kind == "branches" || kind == "cache" {
				if err := analyzePerf(ctx, path); err != nil {
					attempt.Status = "partial"
					attempt.Error = errors.Join(runErr, err).Error()
				}
			}
		}
		manifest.Attempts = append(manifest.Attempts, attempt)
		_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", t.ID, attempt.Status)
		if unsafeCleanup || ctx.Err() != nil {
			return errors.Join(runErr, context.Cause(ctx))
		}
		return nil
	}
	run := func(sf string, q int, engine, kind string, rep int) error { return runAt(sf, q, engine, kind, rep, 8) }
	scales := []string{"1", "10", "100"}
	queries := allQueries()
	if pilot {
		scales = []string{"1"}
		queries = []int{1, 6}
	}
	for _, sf := range scales {
		for _, q := range queries {
			for _, engine := range []string{"mo", "duckdb"} {
				if sf == "10" && deep[sf][q] {
					for i, kind := range []string{"control", "cpu", "control"} {
						if err := runAt(sf, q, engine, kind, i, 1); err != nil {
							return err
						}
					}
				}
				replays := 3
				if pilot {
					replays = 1
				}
				for rep := 0; rep < replays; rep++ {
					if err := run(sf, q, engine, "operator", rep); err != nil {
						return err
					}
				}
			}
		}
	}
	scales = []string{"10", "100"}
	if pilot {
		scales = []string{"1"}
	}
	for _, sf := range scales {
		for _, q := range queries {
			for _, engine := range []string{"mo", "duckdb"} {
				for i, kind := range []string{"control", "cpu", "control"} {
					if err := run(sf, q, engine, kind, i); err != nil {
						return err
					}
				}
				if pilot || deep[sf][q] {
					for i, kind := range []string{"alloc", "control", "wait", "control", "branches", "control", "cache", "control"} {
						if err := run(sf, q, engine, kind, i+3); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return writeProfileSummary(c, root, manifest)
}

func deepProfileSelection(c Campaign, f Frozen, root string) (map[string]map[int]bool, error) {
	if root == "" {
		return nil, errors.New("full profiling requires the measured baseline run directory")
	}
	evidence, err := readCampaignEvidence(c, root)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(evidence.frozen, f) {
		return nil, errors.New("profiling baseline differs from frozen campaign")
	}
	return selectDeepProfiles(evidence.cells)
}

func selectDeepProfiles(cells map[string]*reportCell) (map[string]map[int]bool, error) {
	values := map[string]map[int][][]pairedTimes{}
	for _, cell := range cells {
		if cell.Trial.Experiment != "warm" {
			continue
		}
		sf := cell.Trial.Scale
		if sf != "10" && sf != "100" {
			continue
		}
		if values[sf] != nil || len(cell.Failures) != 0 || cell.Expected != 10 || len(cell.Suites) != 10 {
			return nil, errors.New("baseline warm cell is incomplete or ambiguous")
		}
		values[sf] = cell.Queries
	}
	out := map[string]map[int]bool{}
	for _, sf := range []string{"10", "100"} {
		if len(values[sf]) != 22 {
			return nil, errors.New("baseline lacks complete SF10/SF100 query coverage")
		}
		out[sf] = map[int]bool{1: true, 6: true, 9: true, 18: true}
		type gap struct {
			q     int
			ratio float64
		}
		var gaps []gap
		for q, v := range values[sf] {
			if len(v) != 10 {
				return nil, errors.New("baseline warm cell is incomplete")
			}
			mo, duck := make([]float64, len(v)), make([]float64, len(v))
			for i, block := range v {
				if len(block) != 1 || block[0].MO <= 0 || block[0].DuckDB <= 0 ||
					math.IsNaN(block[0].MO) || math.IsNaN(block[0].DuckDB) || math.IsInf(block[0].MO, 0) || math.IsInf(block[0].DuckDB, 0) {
					return nil, errors.New("invalid baseline paired query timing")
				}
				mo[i], duck[i] = block[0].MO, block[0].DuckDB
			}
			gaps = append(gaps, gap{q, median(duck) / median(mo)})
		}
		slices.SortFunc(gaps, func(a, b gap) int {
			if a.ratio < b.ratio {
				return -1
			}
			if a.ratio > b.ratio {
				return 1
			}
			return a.q - b.q
		})
		for _, g := range gaps[:5] {
			out[sf][g.q] = true
		}
		for _, g := range gaps[len(gaps)-5:] {
			out[sf][g.q] = true
		}
	}
	return out, nil
}

func profilePilotTiming(c Campaign, p Preflight, f Frozen, scale, engine string, query int) (PreparedEngine, int64, error) {
	prepared, ok := f.Prepared[scale][engine]
	if !ok || !reflect.DeepEqual(prepared.Deployment, loadDeployment(c, scale, engine, 1)) {
		return PreparedEngine{}, 0, errors.New("profiling deployment differs from frozen campaign")
	}
	if err := prepared.Pilot.verify(c.Root); err != nil {
		return PreparedEngine{}, 0, err
	}
	path := filepath.Join(c.Root, prepared.Pilot.Path)
	record, err := readJSON[ArmRecord](path)
	if err != nil {
		return PreparedEngine{}, 0, err
	}
	trial := pilotTrial(c, scale, engine)
	if record.Version != formatVersion || record.Engine != engine || !reflect.DeepEqual(record.Trial, trial) {
		return PreparedEngine{}, 0, errors.New("profiling pilot differs from frozen campaign")
	}
	root := filepath.Dir(path)
	receipt, _, err := readArm(root, record)
	if err != nil {
		return PreparedEngine{}, 0, err
	}
	if err := record.Request.verify(root); err != nil {
		return PreparedEngine{}, 0, err
	}
	request, err := readJSON[WorkerRequest](filepath.Join(root, record.Request.Path))
	if err != nil {
		return PreparedEngine{}, 0, err
	}
	expected := runRequest(trial, true)
	expected.Engine = prepared.Deployment.Engine
	expected.DatasetRoot, expected.OutputRoot = c.Datasets[scale], request.OutputRoot
	if !reflect.DeepEqual(request, expected) {
		return PreparedEngine{}, 0, errors.New("profiling pilot request differs from frozen campaign")
	}
	if err := verifyScoredWorker(trial, engine, p, receipt); err != nil {
		return PreparedEngine{}, 0, err
	}
	if err := verifyTrialQueries(trial, f, request, receipt); err != nil {
		return PreparedEngine{}, 0, err
	}
	if err := verifyScoredResources(c, record); err != nil {
		return PreparedEngine{}, 0, err
	}
	for _, q := range receipt.Queries {
		if q.Query == query && q.Error == "" && q.Capture.ElapsedNS > 0 {
			return prepared, q.Capture.ElapsedNS, nil
		}
	}
	return PreparedEngine{}, 0, errors.New("profiling pilot query timing missing or invalid")
}
