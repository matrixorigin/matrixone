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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"
)

type PreparedEngine struct {
	Deployment    deployment `json:"deployment"`
	Loads         []FileID   `json:"loads"`
	Verification  FileID     `json:"verification"`
	Qualification FileID     `json:"qualification"`
}
type Frozen struct {
	Version         int                                  `json:"version"`
	Preflight       FileID                               `json:"preflight"`
	Prepared        map[string]map[string]PreparedEngine `json:"prepared"`
	DeadlinesNS     map[string]map[int]int64             `json:"deadlines_ns"`
	WaveRepetitions map[int]int                          `json:"wave_repetitions"`
	Trials          []Trial                              `json:"trials"`
	WorstCaseNS     int64                                `json:"worst_case_ns"`
}

func runRequest(t Trial, warm bool) WorkerRequest {
	return WorkerRequest{Version: formatVersion, Operation: "queries", Queries: t.Queries, Repetitions: 1, Clients: t.Clients, Warmup: warm, QueryDeadlineNS: int64(30 * time.Minute)}
}

func artifact(root, path string) (FileID, error) {
	id, err := identify(path)
	if err != nil {
		return id, err
	}
	id.Path, err = filepath.Rel(root, path)
	if err != nil || !filepath.IsLocal(id.Path) {
		return FileID{}, errors.New("artifact outside campaign ownership")
	}
	return id, nil
}

func readArm(root string, record ArmRecord) (WorkerReceipt, string, error) {
	if record.Status != "pass" || record.Error != "" || record.Cleanup != "complete" {
		return WorkerReceipt{}, "", errors.New("arm failed or did not clean up")
	}
	if err := record.Worker.verify(root); err != nil {
		return WorkerReceipt{}, "", err
	}
	path := filepath.Join(root, record.Worker.Path)
	r, err := readJSON[WorkerReceipt](path)
	return r, filepath.Dir(path), err
}

func comparePair(ctx context.Context, c Campaign, p Preflight, manager *cgroupManager, prepared map[string]PreparedEngine, t Trial, root string, records map[string]ArmRecord) error {
	a, ra, err := readArm(filepath.Join(root, "mo"), records["mo"])
	if err != nil {
		return err
	}
	b, rb, err := readArm(filepath.Join(root, "duckdb"), records["duckdb"])
	if err != nil {
		return err
	}
	if len(a.Queries) == 0 || len(a.Queries) != len(b.Queries) {
		return errors.New("paired result selection is incomplete")
	}
	d, err := loadDataset(c.Datasets[t.Scale], false)
	if err != nil {
		return err
	}
	for i, x := range a.Queries {
		y := b.Queries[i]
		if x.Query != y.Query || x.Repetition != y.Repetition || x.Stream != y.Stream || x.Error != "" || y.Error != "" {
			return errors.New("paired query schedules differ or failed")
		}
		q := d.Queries[x.Query-1]
		if err := compareCaptures(ra, rb, x.Capture, y.Capture, q); err != nil {
			if q.Limit == 0 {
				return err
			}
			// A differing LIMIT boundary is accepted only after two independent
			// unlimited replays agree and prove both selected multisets legal.
			full := map[string]Capture{}
			fullRoots := map[string]string{}
			for _, engine := range []string{"mo", "duckdb"} {
				diag := t
				diag.Experiment = "limit-membership"
				diag.Clients = 1
				diag.Queries = []int{x.Query}
				request := runRequest(diag, true)
				request.Unlimited = true
				path := filepath.Join(root, fmt.Sprintf("boundary-q%02d-s%d-r%d-%s", x.Query, x.Stream, x.Repetition, engine))
				diagCtx, cancel := context.WithTimeoutCause(ctx, time.Hour, errors.New("LIMIT reference deadline"))
				record, runErr := runArm(diagCtx, c, p, manager, prepared[engine].Deployment, diag, path, request)
				cancel()
				if runErr != nil {
					return errors.Join(err, runErr)
				}
				receipt, dir, readErr := readArm(path, record)
				if readErr != nil || len(receipt.Queries) != 1 {
					return errors.Join(err, readErr, errors.New("incomplete unlimited replay"))
				}
				full[engine] = receipt.Queries[0].Capture
				fullRoots[engine] = dir
			}
			unlimited := q
			unlimited.Limit = 0
			if e := compareCaptures(fullRoots["mo"], fullRoots["duckdb"], full["mo"], full["duckdb"], unlimited); e != nil {
				return errors.Join(err, e)
			}
			if e := validateLimit(ra, fullRoots["mo"], x.Capture, full["mo"], q); e != nil {
				return errors.Join(err, e)
			}
			if e := validateLimit(rb, fullRoots["duckdb"], y.Capture, full["duckdb"], q); e != nil {
				return errors.Join(err, e)
			}
		}
	}
	return nil
}

func prepareCampaign(ctx context.Context, c Campaign, p Preflight, manager *cgroupManager) (f Frozen, retErr error) {
	f = Frozen{Version: formatVersion, Prepared: map[string]map[string]PreparedEngine{}, DeadlinesNS: map[string]map[int]int64{}, WaveRepetitions: map[int]int{}}
	var err error
	f.Preflight, err = artifact(c.Root, filepath.Join(c.Root, "preflight.json"))
	if err != nil {
		return f, err
	}
	loadReps := 3
	if c.Mode != "full" {
		loadReps = 1
	}
	scales := []string{"1"}
	if c.Mode != "sf1-smoke" {
		scales = []string{"1", "10", "100"}
	}
	for _, sf := range scales {
		f.Prepared[sf] = map[string]PreparedEngine{}
		f.DeadlinesNS[sf] = map[int]int64{}
		for _, engine := range []string{"mo", "duckdb"} {
			var kept PreparedEngine
			for rep := 1; rep <= loadReps; rep++ {
				d, err := prepareDeployment(c, sf, engine, rep)
				if err != nil {
					return f, err
				}
				t := Trial{ID: fmt.Sprintf("load-sf%s-%s-%d", sf, engine, rep), Experiment: "load", Scale: sf, Cores: 8, MemoryBytes: c.MemoryBytes, Clients: 1, Block: rep - 1}
				root := filepath.Join(c.Root, "preparation", t.ID)
				request := WorkerRequest{Operation: "load", QueryDeadlineNS: int64(30 * time.Minute)}
				record, err := runArm(ctx, c, p, manager, d, t, root, request)
				if err != nil {
					return f, fmt.Errorf("%s: %w", t.ID, err)
				}
				id, err := artifact(c.Root, filepath.Join(root, "arm.json"))
				if err != nil {
					return f, err
				}
				kept.Loads = append(kept.Loads, id)
				if rep == 1 {
					kept.Deployment = d
				} else {
					if err := discardDeployment(c, d); err != nil {
						return f, err
					}
				}
				_ = record
			}
			// The independent verification uses a newly reopened deployment.
			t := Trial{ID: "reopen-sf" + sf + "-" + engine, Experiment: "verify", Scale: sf, Cores: 8, MemoryBytes: c.MemoryBytes, Clients: 1}
			root := filepath.Join(c.Root, "preparation", t.ID)
			record, err := runArm(ctx, c, p, manager, kept.Deployment, t, root, WorkerRequest{Operation: "verify", QueryDeadlineNS: int64(30 * time.Minute)})
			if err != nil {
				return f, err
			}
			kept.Verification, err = artifact(c.Root, filepath.Join(root, "arm.json"))
			if err != nil {
				return f, err
			}
			_ = record
			fixture := t
			fixture.ID = "fixtures-sf" + sf + "-" + engine
			fixture.Experiment = "fixtures"
			if _, err := runArm(ctx, c, p, manager, kept.Deployment, fixture, filepath.Join(c.Root, "preparation", fixture.ID), WorkerRequest{Operation: "fixtures", QueryDeadlineNS: int64(30 * time.Minute)}); err != nil {
				return f, err
			}
			t.ID = "qualify-sf" + sf + "-" + engine
			t.Experiment = "qualification"
			t.Queries = allQueries()
			root = filepath.Join(c.Root, "preparation", t.ID)
			request := runRequest(t, true)
			request.Reference = sf == "1"
			record, err = runArm(ctx, c, p, manager, kept.Deployment, t, root, request)
			if err != nil {
				return f, err
			}
			kept.Qualification, err = artifact(c.Root, filepath.Join(root, "arm.json"))
			if err != nil {
				return f, err
			}
			receipt, _, err := readArm(root, record)
			if err != nil {
				return f, err
			}
			for _, q := range receipt.Queries {
				deadline := min(max(q.Capture.ElapsedNS*10, int64(30*time.Second)), int64(30*time.Minute))
				f.DeadlinesNS[sf][q.Query] = max(f.DeadlinesNS[sf][q.Query], deadline)
			}
			f.Prepared[sf][engine] = kept
		}
		// Qualify both complete outputs before any scored schedule is frozen.
		d, err := loadDataset(c.Datasets[sf], false)
		if err != nil {
			return f, err
		}
		roots := map[string]string{}
		receipts := map[string]WorkerReceipt{}
		for _, engine := range []string{"mo", "duckdb"} {
			armPath := filepath.Join(c.Root, f.Prepared[sf][engine].Qualification.Path)
			r, err := readJSON[ArmRecord](armPath)
			if err != nil {
				return f, err
			}
			receipt, dir, err := readArm(filepath.Dir(armPath), r)
			if err != nil {
				return f, err
			}
			roots[engine] = dir
			receipts[engine] = receipt
		}
		for i, q := range d.Queries {
			if err := compareCaptures(roots["mo"], roots["duckdb"], receipts["mo"].Queries[i].Capture, receipts["duckdb"].Queries[i].Capture, q); err != nil {
				return f, fmt.Errorf("SF%s qualification: %w", sf, err)
			}
		}
	}
	if c.Mode == "full" {
		for _, clients := range []int{1, 2, 4, 8} {
			fast := int64(0)
			for _, engine := range []string{"mo", "duckdb"} {
				t := Trial{ID: fmt.Sprintf("wave-pilot-n%d-%s", clients, engine), Experiment: "wave-pilot", Scale: "10", Cores: 8, MemoryBytes: c.MemoryBytes, Clients: clients, Queries: allQueries()}
				request := runRequest(t, true)
				request.Seed = c.Seed
				root := filepath.Join(c.Root, "preparation", t.ID)
				record, err := runArm(ctx, c, p, manager, f.Prepared["10"][engine].Deployment, t, root, request)
				if err != nil {
					return f, err
				}
				receipt, _, err := readArm(root, record)
				if err != nil {
					return f, err
				}
				if fast == 0 || receipt.WaveNS < fast {
					fast = receipt.WaveNS
				}
			}
			if fast <= 0 {
				return f, errors.New("pilot produced no complete throughput window")
			}
			f.WaveRepetitions[clients] = min(max(1, int((int64(30*time.Second)+fast-1)/fast)), 4000/(clients*22))
		}
	}
	f.Trials = schedule(c)
	for _, t := range f.Trials {
		factor := int64(1)
		if t.Experiment == "concurrency" {
			factor = int64(f.WaveRepetitions[t.Clients])
		}
		for _, q := range t.Queries {
			f.WorstCaseNS += 2 * factor * f.DeadlinesNS[t.Scale][q]
		}
		f.WorstCaseNS += int64(20 * time.Minute)
	}
	return f, publishJSON(filepath.Join(c.Root, "frozen.json"), f)
}

func discardDeployment(c Campaign, d deployment) error {
	rel, err := filepath.Rel(c.Root, d.Root)
	if err != nil || !filepath.IsLocal(rel) || !slices.Contains([]string{"load-2", "load-3"}, filepath.Base(d.Root)) {
		return errors.New("refusing to remove unowned deployment")
	}
	owner, err := readJSON[map[string]any](filepath.Join(d.Root, "owner.json"))
	if err != nil || owner["purpose"] != "tpch-compare deployment" {
		return errors.New("deployment ownership receipt is missing")
	}
	return os.RemoveAll(d.Root)
}

type PairRecord struct {
	Version     int                  `json:"version"`
	Trial       Trial                `json:"trial"`
	Arms        map[string]ArmRecord `json:"arms"`
	Correctness string               `json:"correctness"`
	Error       string               `json:"error,omitempty"`
}
type RunManifest struct {
	Version  int       `json:"version"`
	Mode     string    `json:"mode"`
	Frozen   FileID    `json:"frozen"`
	Pairs    []FileID  `json:"pairs"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Error    string    `json:"error,omitempty"`
}

func executeCampaign(ctx context.Context, c Campaign, p Preflight, f Frozen, manager *cgroupManager, id string) (manifest RunManifest, retErr error) {
	if !identifier.MatchString(id) {
		return manifest, errors.New("run ID must be a lower-case identifier")
	}
	root := filepath.Join(c.Root, "runs", id)
	if err := os.MkdirAll(filepath.Dir(root), 0750); err != nil {
		return manifest, err
	}
	if err := os.Mkdir(root, 0750); err != nil {
		return manifest, err
	}
	manifest = RunManifest{Version: formatVersion, Mode: c.Mode, Started: time.Now().UTC()}
	var err error
	manifest.Frozen, err = artifact(c.Root, filepath.Join(c.Root, "frozen.json"))
	if err != nil {
		return manifest, err
	}
	defer func() {
		manifest.Finished = time.Now().UTC()
		if retErr != nil {
			manifest.Error = retErr.Error()
		}
		retErr = errors.Join(retErr, publishJSON(filepath.Join(root, "run.json"), manifest))
	}()
	for _, t := range f.Trials {
		if err := ctx.Err(); err != nil {
			return manifest, context.Cause(ctx)
		}
		pair := PairRecord{Version: formatVersion, Trial: t, Arms: map[string]ArmRecord{}, Correctness: "failed"}
		pairRoot := filepath.Join(root, t.ID)
		if err := os.Mkdir(pairRoot, 0750); err != nil {
			return manifest, err
		}
		var pairErr error
		for _, engine := range t.Arms {
			d := f.Prepared[t.Scale][engine].Deployment
			if t.Experiment == "process-warm-os" {
				prime := t
				prime.Experiment = "warm-prime"
				prime.Queries = allQueries()
				request := runRequest(prime, false)
				request.DeadlinesNS = f.DeadlinesNS[t.Scale]
				request.Seed = c.Seed
				path := filepath.Join(pairRoot, "prime-"+engine)
				if _, err := runArm(ctx, c, p, manager, d, prime, path, request); err != nil {
					pairErr = errors.Join(pairErr, err)
				}
			}
			request := runRequest(t, t.Experiment != "process-warm-os" && t.Experiment != "process-cold-os")
			request.DeadlinesNS = f.DeadlinesNS[t.Scale]
			request.Seed = c.Seed
			if t.Experiment == "concurrency" {
				request.Repetitions = f.WaveRepetitions[t.Clients]
			}
			record, err := runArm(ctx, c, p, manager, d, t, filepath.Join(pairRoot, engine), request)
			pair.Arms[engine] = record
			pairErr = errors.Join(pairErr, err)
		}
		if pairErr == nil {
			pairErr = comparePair(ctx, c, p, manager, f.Prepared[t.Scale], t, pairRoot, pair.Arms)
		}
		if pairErr != nil {
			pair.Error = pairErr.Error()
		} else {
			pair.Correctness = "pass"
		}
		path := filepath.Join(pairRoot, "pair.json")
		if err := publishJSON(path, pair); err != nil {
			return manifest, err
		}
		pin, err := artifact(root, path)
		if err != nil {
			return manifest, err
		}
		manifest.Pairs = append(manifest.Pairs, pin)
		_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", t.ID, pair.Correctness)
	}
	return manifest, nil
}

func campaignRun(ctx context.Context, config, phase, id string) error {
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
	manager, err := setupCgroups(c)
	if err != nil {
		return err
	}
	if phase == "prepare" {
		_, err := prepareCampaign(ctx, c, p, manager)
		return err
	}
	f, err := readJSON[Frozen](filepath.Join(c.Root, "frozen.json"))
	if err != nil {
		return err
	}
	if err := f.Preflight.verify(c.Root); err != nil {
		return err
	}
	if phase != "measure" {
		return errors.New("run phase must be prepare or measure")
	}
	if id == "" {
		id = "run_" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	_, err = executeCampaign(ctx, c, p, f, manager, id)
	return err
}
