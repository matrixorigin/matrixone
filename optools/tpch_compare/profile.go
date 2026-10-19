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
	root := filepath.Join(c.Root, "profiles", id)
	if err := os.MkdirAll(filepath.Dir(root), 0750); err != nil {
		return err
	}
	if err := os.Mkdir(root, 0750); err != nil {
		return err
	}
	manager, err := setupCgroups(c)
	if err != nil {
		return err
	}
	manifest := ProfileManifest{Version: formatVersion, Pilot: pilot}
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
	deep := map[string]map[int]bool{}
	if !pilot {
		deep, err = deepProfileSelection(c, f, baseline)
		if err != nil {
			return err
		}
	}
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
		prepared := f.Prepared[sf][engine]
		armPath := filepath.Join(c.Root, prepared.Pilot.Path)
		qual, err := readJSON[ArmRecord](armPath)
		if err != nil {
			return err
		}
		receipt, _, err := readArm(filepath.Dir(armPath), qual)
		if err != nil {
			return err
		}
		timeNS := receipt.Queries[q-1].Capture.ElapsedNS
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
		request.WarmupSelected = pilot
		request.Repetitions = reps
		if kind != "operator" {
			request.MaxWindowNS = int64(60 * time.Second)
		}
		request.DeadlinesNS = f.DeadlinesNS[sf]
		if kind != "control" {
			request.Profile = &ProfileSpec{Kind: kind}
		}
		path := filepath.Join(root, t.ID)
		record, runErr := runArm(ctx, c, p, manager, prepared.Deployment, t, path, request)
		attempt := ProfileAttempt{Scale: sf, Query: q, Engine: engine, Kind: kind, Cores: cores, Status: "complete"}
		attempt.Arm, err = artifact(root, filepath.Join(path, "arm.json"))
		if err != nil {
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
	run, err := readJSON[RunManifest](filepath.Join(root, "run.json"))
	if err != nil {
		return nil, err
	}
	if err := run.Frozen.verify(c.Root); err != nil {
		return nil, err
	}
	values := map[string]map[int][]pairedTimes{}
	for _, pin := range run.Pairs {
		if err := pin.verify(root); err != nil {
			return nil, err
		}
		path := filepath.Join(root, pin.Path)
		pair, err := readJSON[PairRecord](path)
		if err != nil {
			return nil, err
		}
		if pair.Trial.Experiment != "warm" || pair.Status != "pass" {
			continue
		}
		sf := pair.Trial.Scale
		if sf != "10" && sf != "100" {
			continue
		}
		if values[sf] == nil {
			values[sf] = map[int][]pairedTimes{}
		}
		a, _, err := readArm(filepath.Join(filepath.Dir(path), "mo"), pair.Arms["mo"])
		if err != nil {
			return nil, err
		}
		b, _, err := readArm(filepath.Join(filepath.Dir(path), "duckdb"), pair.Arms["duckdb"])
		if err != nil {
			return nil, err
		}
		for i, q := range a.Queries {
			values[sf][q.Query] = append(values[sf][q.Query], pairedTimes{float64(q.Capture.ElapsedNS), float64(b.Queries[i].Capture.ElapsedNS)})
		}
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
			for i, p := range v {
				mo[i], duck[i] = p.MO, p.DuckDB
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
	_ = f
	return out, nil
}
