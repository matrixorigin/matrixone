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
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

type pairedTimes struct{ MO, DuckDB float64 }

func median(values []float64) float64 {
	v := slices.Clone(values)
	slices.Sort(v)
	n := len(v)
	if n == 0 {
		return math.NaN()
	}
	if n%2 == 1 {
		return v[n/2]
	}
	return (v[n/2-1] + v[n/2]) / 2
}
func pairedRatio(blocks [][]pairedTimes, seed uint64) (ratio, low, high float64) {
	var values []pairedTimes
	for _, block := range blocks {
		values = append(values, block...)
	}
	mo, duck := make([]float64, len(values)), make([]float64, len(values))
	for i, v := range values {
		mo[i], duck[i] = v.MO, v.DuckDB
	}
	ratio = median(duck) / median(mo)
	if len(blocks) < 2 {
		return ratio, math.NaN(), math.NaN()
	}
	rng := rand.New(rand.NewPCG(seed, seed^0x7f4a7c15))
	samples := make([]float64, 10000)
	for i := range samples {
		mo = mo[:0]
		duck = duck[:0]
		for range blocks {
			block := blocks[rng.IntN(len(blocks))]
			for _, v := range block {
				mo = append(mo, v.MO)
				duck = append(duck, v.DuckDB)
			}
		}
		samples[i] = median(duck) / median(mo)
	}
	slices.Sort(samples)
	return ratio, samples[249], samples[9749]
}

type reportCell struct {
	Trial    Trial
	Queries  map[int][][]pairedTimes
	Suites   []pairedTimes
	Failures []string
	Arms     []ArmRecord
	Expected int
}

func cellKey(t Trial) string {
	key := fmt.Sprintf("%s/sf%s/c%d/m%d/n%d", t.Experiment, t.Scale, t.Cores, t.MemoryBytes>>30, t.Clients)
	if len(t.Queries) == 1 {
		key += fmt.Sprintf("/q%d", t.Queries[0])
	}
	return key
}

func reportCampaign(config, root, output string) (retErr error) {
	c, err := readJSON[Campaign](config)
	if err != nil {
		return err
	}
	if err := c.validate(); err != nil {
		return err
	}
	run, err := readJSON[RunManifest](filepath.Join(root, "run.json"))
	if err != nil {
		return err
	}
	if run.Version != formatVersion {
		return errors.New("unsupported run format")
	}
	if err := run.Frozen.verify(c.Root); err != nil {
		return err
	}
	f, err := readJSON[Frozen](filepath.Join(c.Root, run.Frozen.Path))
	if err != nil {
		return err
	}
	if err := f.Preflight.verify(c.Root); err != nil {
		return err
	}
	p, err := readJSON[Preflight](filepath.Join(c.Root, f.Preflight.Path))
	if err != nil {
		return err
	}
	if err := verifyCampaignConfig(c, p); err != nil {
		return err
	}
	if !reflect.DeepEqual(f.Trials, schedule(c)) {
		return errors.New("frozen schedule differs from the declared campaign")
	}
	if len(run.Pairs) > len(f.Trials) {
		return errors.New("unexpected extra paired blocks")
	}
	seen := map[string]bool{}
	cells := map[string]*reportCell{}
	for _, t := range f.Trials {
		key := cellKey(t)
		if cells[key] == nil {
			cells[key] = &reportCell{Trial: t, Queries: map[int][][]pairedTimes{}}
		}
		cells[key].Expected++
	}
	for _, pin := range run.Pairs {
		if err := pin.verify(root); err != nil {
			return err
		}
		path := filepath.Join(root, pin.Path)
		pair, err := readJSON[PairRecord](path)
		if err != nil {
			return err
		}
		if seen[pair.Trial.ID] {
			return errors.New("duplicate paired block")
		}
		seen[pair.Trial.ID] = true
		index := slices.IndexFunc(f.Trials, func(t Trial) bool { return t.ID == pair.Trial.ID })
		if index < 0 || !reflect.DeepEqual(pair.Trial, f.Trials[index]) {
			return errors.New("paired block does not match frozen schedule")
		}
		key := cellKey(pair.Trial)
		cell := cells[key]
		if pair.Status != "pass" || pair.Error != "" {
			cell.Failures = append(cell.Failures, pair.Trial.ID+": "+pair.Error)
			continue
		}
		receipts := map[string]WorkerReceipt{}
		for _, engine := range []string{"mo", "duckdb"} {
			record, ok := pair.Arms[engine]
			if !ok {
				return errors.New("paired arm missing")
			}
			if record.Version != formatVersion || record.Engine != engine || !reflect.DeepEqual(record.Trial, pair.Trial) {
				return errors.New("paired arm differs from frozen trial")
			}
			armRoot := filepath.Join(filepath.Dir(path), engine)
			receipt, _, err := readArm(armRoot, record)
			if err != nil {
				return err
			}
			if err := record.Request.verify(armRoot); err != nil {
				return err
			}
			request, err := readJSON[WorkerRequest](filepath.Join(armRoot, record.Request.Path))
			if err != nil {
				return err
			}
			prepared, ok := f.Prepared[pair.Trial.Scale][engine]
			if !ok || prepared.Deployment.Engine.Name != engine {
				return errors.New("frozen deployment missing or differs from paired engine")
			}
			expected := scoredRequest(c, f, pair.Trial)
			expected.Engine = prepared.Deployment.Engine
			expected.Engine.Threads, expected.Engine.MemoryBytes = pair.Trial.Cores, pair.Trial.MemoryBytes
			expected.DatasetRoot = c.Datasets[pair.Trial.Scale]
			// Capture locations are artifact bookkeeping, not frozen workload.
			// Reporting archived receipts does not require result captures.
			expected.OutputRoot = request.OutputRoot
			if !reflect.DeepEqual(request, expected) {
				return errors.New("worker request differs from frozen trial")
			}
			if err := verifyScoredWorker(pair.Trial, engine, p, receipt); err != nil {
				return err
			}
			if err := verifyTrialQueries(pair.Trial, f, request, receipt); err != nil {
				return err
			}
			receipts[engine] = receipt
			cell.Arms = append(cell.Arms, record)
		}
		a, b := receipts["mo"], receipts["duckdb"]
		if len(a.Queries) != len(b.Queries) {
			return errors.New("paired stream sizes differ")
		}
		blockTimes := map[int][]pairedTimes{}
		for i, x := range a.Queries {
			y := b.Queries[i]
			if x.Query < 1 || x.Query > 22 || x.Query != y.Query || x.Stream != y.Stream || x.Repetition != y.Repetition || x.Error != "" || y.Error != "" || x.Capture.ElapsedNS <= 0 || y.Capture.ElapsedNS <= 0 {
				return errors.New("invalid paired query timing")
			}
			blockTimes[x.Query] = append(blockTimes[x.Query], pairedTimes{float64(x.Capture.ElapsedNS) / 1e9, float64(y.Capture.ElapsedNS) / 1e9})
		}
		for q, values := range blockTimes {
			cell.Queries[q] = append(cell.Queries[q], values)
		}
		cell.Suites = append(cell.Suites, pairedTimes{float64(a.WaveNS) / 1e9, float64(b.WaveNS) / 1e9})
	}
	if err := os.Mkdir(output, 0750); err != nil {
		return err
	}
	mdFile, err := os.OpenFile(filepath.Join(output, "report.md"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, mdFile.Close()) }()
	md := bufio.NewWriter(mdFile)
	table, err := os.OpenFile(filepath.Join(output, "queries.csv"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, table.Close()) }()
	writer := csv.NewWriter(table)
	defer writer.Flush()
	if err := writer.Write([]string{"cell", "query", "samples", "mo_median_s", "duckdb_median_s", "duckdb_over_mo", "ratio_ci95_low", "ratio_ci95_high", "cell_status"}); err != nil {
		return err
	}
	status := "complete"
	if len(run.Pairs) != len(f.Trials) || run.Error != "" {
		status = "incomplete"
	}
	for _, cell := range cells {
		if len(cell.Failures) > 0 || len(cell.Suites) != cell.Expected {
			status = "incomplete"
		}
	}
	_, _ = fmt.Fprintf(md, "# MatrixOne versus DuckDB TPC-H\n\nMode: %s. Run status: **%s**. Completed paired blocks: %d/%d.\n\nSpeedup is DuckDB / MO; values above 1 favor MO. Scales, cache conditions and budgets are separate.\n", c.Mode, status, len(run.Pairs), len(f.Trials))
	_, _ = fmt.Fprintln(md, "\nThis benchmark does not perform correctness verification. Completion describes execution and collection of timing samples.")
	_, _ = fmt.Fprintf(md, "\nMO source: `%s`. MO binary SHA-256: `%s`. Worker SHA-256: `%s`.\n\nApplication CPUs: `%s`; controller CPUs: `%s`; primary memory: %d bytes; disk allowance: %d bytes/engine.\n\nMO build: `%s`.\n", c.MORevision, p.MO.SHA256, p.Worker.SHA256, cpuSet(c.CPUs), cpuSet(c.ControllerCPUs), c.MemoryBytes, c.DiskBytes, c.MOBuild)
	keys := make([]string, 0, len(cells))
	for key := range cells {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		cell := cells[key]
		cellStatus := "complete"
		if len(cell.Failures) > 0 || len(cell.Suites) != cell.Expected {
			cellStatus = "incomplete"
		}
		_, _ = fmt.Fprintf(md, "\n## %s\n\nStatus: %s. Valid paired blocks: %d.\n\n| Query | MO median s | DuckDB median s | DuckDB / MO | 95%% paired CI | Samples |\n|---|---:|---:|---:|---:|---:|\n", key, cellStatus, len(cell.Suites))
		var moMedians, duckMedians []float64
		var logs []float64
		for _, q := range cell.Trial.Queries {
			blocks := cell.Queries[q]
			var values []pairedTimes
			for _, block := range blocks {
				values = append(values, block...)
			}
			if len(values) == 0 || cellStatus != "complete" {
				_, _ = fmt.Fprintf(md, "| Q%d | N/A | N/A | N/A | N/A | 0 |\n", q)
				if err := writer.Write([]string{key, strconv.Itoa(q), strconv.Itoa(len(values)), "N/A", "N/A", "N/A", "N/A", "N/A", cellStatus}); err != nil {
					return err
				}
				continue
			}
			mo, duck := make([]float64, len(values)), make([]float64, len(values))
			for i, v := range values {
				mo[i], duck[i] = v.MO, v.DuckDB
			}
			mm, dm := median(mo), median(duck)
			ratio, low, high := pairedRatio(blocks, c.Seed)
			moMedians = append(moMedians, mm)
			duckMedians = append(duckMedians, dm)
			logs = append(logs, math.Log(ratio))
			_, _ = fmt.Fprintf(md, "| Q%d | %.6f | %.6f | %.3f | %s–%s | %d |\n", q, mm, dm, ratio, fmtNumber(low), fmtNumber(high), len(values))
			if err := writer.Write([]string{key, strconv.Itoa(q), strconv.Itoa(len(values)), fmtNumber(mm), fmtNumber(dm), fmtNumber(ratio), fmtNumber(low), fmtNumber(high), cellStatus}); err != nil {
				return err
			}
		}
		if cellStatus == "complete" && len(cell.Trial.Queries) == 22 && len(cell.Suites) > 0 {
			mo, duck := make([]float64, len(cell.Suites)), make([]float64, len(cell.Suites))
			for i, v := range cell.Suites {
				mo[i], duck[i] = v.MO, v.DuckDB
			}
			sumMO, sumDuck, logSum := 0.0, 0.0, 0.0
			for _, v := range moMedians {
				sumMO += v
			}
			for _, v := range duckMedians {
				sumDuck += v
			}
			for _, v := range logs {
				logSum += v
			}
			_, _ = fmt.Fprintf(md, "\nMedian complete suite/wave: MO %.6f s; DuckDB %.6f s. Sum of query medians: MO %.6f s; DuckDB %.6f s. Query speedup geometric mean: %.3f.\n", median(mo), median(duck), sumMO, sumDuck, math.Exp(logSum/float64(len(logs))))
		}
		for _, failure := range cell.Failures {
			_, _ = fmt.Fprintf(md, "\n- Failed block: %s\n", strings.ReplaceAll(failure, "\n", " "))
		}
	}
	for _, t := range f.Trials {
		if !seen[t.ID] {
			_, _ = fmt.Fprintf(md, "\nMissing paired block: `%s`.\n", t.ID)
		}
	}
	_, _ = fmt.Fprintln(md, "\nCPU and peak memory belong to whole measured windows. Per-query resource attribution requires the dedicated profiling replays; operator timings are not OS CPU time. Tail percentiles require larger per-query samples.")
	writer.Flush()
	return errors.Join(writer.Error(), md.Flush(), writeWindows(output, cells), writeLoads(md, c, f))
}

func writeWindows(output string, cells map[string]*reportCell) (retErr error) {
	f, err := os.OpenFile(filepath.Join(output, "windows.csv"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"trial", "engine", "cpu_seconds", "memory_peak_bytes", "peak_scope", "read_bytes", "write_bytes", "disk_after_cleanup_bytes", "startup_seconds", "startup_to_result_seconds"}); err != nil {
		return err
	}
	keys := make([]string, 0, len(cells))
	for key := range cells {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		for _, a := range cells[key].Arms {
			if a.After.CPUUsec < a.Before.CPUUsec || a.After.ReadBytes < a.Before.ReadBytes || a.After.WriteBytes < a.Before.WriteBytes {
				return errors.New("resource counter moved backwards")
			}
			if strings.TrimSpace(a.After.Raw["memory.max"]) != strconv.FormatInt(a.Trial.MemoryBytes, 10) || strings.TrimSpace(a.After.Raw["memory.swap.max"]) != "0" {
				return errors.New("resource cap receipt is missing or differs")
			}
			cpus, err := parseCPUSet(a.After.Raw["cpuset.cpus.effective"])
			if err != nil || len(cpus) != a.Trial.Cores {
				return errors.New("CPU cap receipt is missing or differs")
			}
			peak := "N/A"
			if a.After.PeakQuality == "window" {
				peak = strconv.FormatUint(a.After.PeakBytes, 10)
			}
			if err := w.Write([]string{a.Trial.ID, a.Engine, fmtNumber(float64(a.After.CPUUsec-a.Before.CPUUsec) / 1e6), peak, a.After.PeakQuality, strconv.FormatUint(a.After.ReadBytes-a.Before.ReadBytes, 10), strconv.FormatUint(a.After.WriteBytes-a.Before.WriteBytes, 10), strconv.FormatInt(a.DiskBytes, 10), fmtNumber(float64(a.StartupNS) / 1e9), fmtNumber(float64(a.StartupToResultNS) / 1e9)}); err != nil {
				return err
			}
		}
	}
	w.Flush()
	return w.Error()
}

func writeLoads(w *bufio.Writer, c Campaign, f Frozen) error {
	_, _ = fmt.Fprintln(w, "\n## Data preparation\n\n| Scale | Engine | Load replicate | Schema s | Load s | Statistics s | Checkpoint s | Allocated bytes after cleanup |\n|---|---|---:|---:|---:|---:|---:|---:|")
	for _, sf := range []string{"1", "10", "100"} {
		for _, engine := range []string{"mo", "duckdb"} {
			prepared, ok := f.Prepared[sf][engine]
			if !ok {
				continue
			}
			for i, pin := range prepared.Loads {
				if err := pin.verify(c.Root); err != nil {
					return err
				}
				path := filepath.Join(c.Root, pin.Path)
				a, err := readJSON[ArmRecord](path)
				if err != nil {
					return err
				}
				receipt, _, err := readArm(filepath.Dir(path), a)
				if err != nil {
					return err
				}
				if receipt.Load == nil {
					return errors.New("load timing receipt missing")
				}
				v := receipt.Load
				_, _ = fmt.Fprintf(w, "| %s | %s | %d | %.6f | %.6f | %.6f | %.6f | %d |\n", sf, engine, i+1, float64(v.SchemaNS)/1e9, float64(v.LoadNS)/1e9, float64(v.StatisticsNS)/1e9, float64(v.CheckpointNS)/1e9, a.DiskBytes)
			}
		}
	}
	return w.Flush()
}

func verifyTrialQueries(t Trial, f Frozen, request WorkerRequest, receipt WorkerReceipt) error {
	repetitions := 1
	if t.Experiment == "concurrency" {
		repetitions = f.WaveRepetitions[t.Clients]
	}
	if t.Clients < 1 || t.Clients > 8 || len(t.Queries) < 1 || len(t.Queries) > 22 ||
		repetitions < 1 || repetitions > 2000 || repetitions > 4000/t.Clients/len(t.Queries) {
		return errors.New("invalid frozen query workload")
	}
	if request.Version != formatVersion || request.Operation != "queries" ||
		request.Clients != t.Clients || request.Repetitions != repetitions ||
		!slices.Equal(request.Queries, t.Queries) {
		return errors.New("worker request differs from frozen trial")
	}
	if len(receipt.Queries) != len(t.Queries)*t.Clients*repetitions {
		return errors.New("partial query stream cannot supply a suite score")
	}
	type identity struct{ query, stream, repetition int }
	seen := make(map[identity]bool, len(receipt.Queries))
	for _, q := range receipt.Queries {
		id := identity{q.Query, q.Stream, q.Repetition}
		if !slices.Contains(t.Queries, q.Query) || q.Stream < 0 || q.Stream >= t.Clients ||
			q.Repetition < 0 || q.Repetition >= repetitions || seen[id] {
			return errors.New("query identities do not cover frozen trial")
		}
		seen[id] = true
	}
	return nil
}

func verifyScoredWorker(t Trial, engine string, p Preflight, receipt WorkerReceipt) error {
	if receipt.Version != formatVersion {
		return errors.New("worker receipt version differs")
	}
	if receipt.Error != "" || receipt.Cleanup != "complete" {
		return errors.New("failed/incomplete worker receipt cannot supply scored timing")
	}
	if receipt.Engine != engine {
		return errors.New("worker engine differs from paired arm")
	}
	dataset, ok := p.DatasetIDs[t.Scale]
	if !ok || dataset.SHA256 == "" || dataset.Bytes <= 0 || receipt.Dataset.SHA256 != dataset.SHA256 || receipt.Dataset.Bytes != dataset.Bytes {
		return errors.New("worker dataset differs from frozen trial")
	}
	if receipt.Variant != "qualification" {
		return errors.New("worker variant cannot supply scored timing")
	}
	if receipt.Clients != t.Clients {
		return errors.New("worker client count differs from frozen trial")
	}
	if !receipt.Controlled || receipt.WaveNS <= 0 {
		return errors.New("standalone/incomplete worker cannot supply scored timing")
	}
	if p.Worker.SHA256 == "" || p.Worker.Bytes <= 0 || receipt.Binary.SHA256 != p.Worker.SHA256 || receipt.Binary.Bytes != p.Worker.Bytes {
		return errors.New("worker executable changed")
	}
	return nil
}
func fmtNumber(n float64) string {
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return "N/A"
	}
	return strconv.FormatFloat(n, 'f', 6, 64)
}
