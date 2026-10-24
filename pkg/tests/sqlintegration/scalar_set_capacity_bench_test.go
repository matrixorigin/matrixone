// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/stretchr/testify/require"
)

type scalarCapacityProfile struct {
	Name                                   string
	Outer, InnerPerArm, Width, Concurrency int
	Sparse, Distinct, ForceSpill           bool
	ValueKinds                             int
	ResourceError                          bool
	Skew                                   bool
	MixedDependent                         bool
}
type scalarCapacitySample struct {
	Phase                    string
	Round, Worker            int
	Started, Finished        time.Time
	FirstRowNS, CompletionNS int64
	Rows, ErrorCode          int
}
type scalarCapacityResult struct {
	Physical                         string
	Internal                         []resource.StatementResourceSummary
	Settings                         map[string]int64
	Profile                          scalarCapacityProfile
	Samples                          []scalarCapacitySample
	Analyze                          string
	AnalyzeErrorCode                 int
	ProcessLifetimeRSSHighWaterBytes int64
}

// This opt-in experiment owns its fixed protocol: first invocation separately,
// one excluded warm-up, then three measured rounds. Use -benchtime=1x -count=1.
// The first invocation is NOT a promise of cold OS or storage caches. Ordinary
// SELECT timing and EXPLAIN ANALYZE are separate executions. RSS is whole-process
// lifetime high-water, not query peak or an incremental allocation measurement.
func BenchmarkScalarSetFallbackCapacity(b *testing.B) {
	report := os.Getenv("MO_SCALAR_SET_CAPACITY_REPORT")
	if report == "" {
		b.Skip("set MO_SCALAR_SET_CAPACITY_REPORT for this manual capacity experiment")
	}
	require.Equal(b, 1, b.N, "use -benchtime=1x")
	b.StopTimer()
	profiles := []scalarCapacityProfile{
		{Name: "baseline", Outer: 8, InnerPerArm: 8, Concurrency: 1},
		{Name: "outer", Outer: 64, InnerPerArm: 8, Concurrency: 1},
		{Name: "candidates", Outer: 8, InnerPerArm: 128, Concurrency: 1},
		{Name: "sparse", Outer: 8, InnerPerArm: 8, Concurrency: 1, Sparse: true},
		{Name: "sparse-boundary", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 1, Sparse: true},
		{Name: "sparse-boundary-concurrent", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 2, Sparse: true},
		{Name: "distinct-cardinality-error", Outer: 8, InnerPerArm: 8, Concurrency: 1, Distinct: true},
		{Name: "width", Outer: 8, InnerPerArm: 8, Width: 256, Concurrency: 1},
		{Name: "concurrency", Outer: 8, InnerPerArm: 8, Concurrency: 2},
		{Name: "combined", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 2},
		{Name: "high-distinctness-boundary", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 2, Distinct: true},
		{Name: "balanced-two-values", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 1, Distinct: true, ValueKinds: 2},
		{Name: "skewed-two-values", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 1, Distinct: true, ValueKinds: 2, Skew: true},
		{Name: "forced-spill", Outer: 64, InnerPerArm: 128, Concurrency: 1, ForceSpill: true},
		{Name: "forced-spill-boundary-concurrent", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 2, ForceSpill: true},
		{Name: "streamed-outer", Outer: 16384, InnerPerArm: 8, Concurrency: 1, Sparse: true},
		{Name: "larger-candidates", Outer: 128, InnerPerArm: 4096, Concurrency: 1, Sparse: true},
		{Name: "wide-concurrent", Outer: 128, InnerPerArm: 256, Width: 4096, Concurrency: 2},
		{Name: "larger-dense", Outer: 512, InnerPerArm: 1024, Concurrency: 1},
		{Name: "work-boundary-sparse", Outer: 4096, InnerPerArm: 2048, Concurrency: 1, Sparse: true},
		{Name: "work-over-limit", Outer: 4097, InnerPerArm: 2048, Concurrency: 1, Sparse: true, ResourceError: true},
		{Name: "mixed-baseline", Outer: 8, InnerPerArm: 8, Concurrency: 1, MixedDependent: true},
		{Name: "mixed-concurrent", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 2, MixedDependent: true},
		{Name: "mixed-streamed-outer", Outer: 16384, InnerPerArm: 8, Concurrency: 1, Sparse: true, MixedDependent: true},
		{Name: "mixed-work-boundary", Outer: 4096, InnerPerArm: 2048, Concurrency: 1, Sparse: true, MixedDependent: true},
		{Name: "mixed-work-over-limit", Outer: 4097, InnerPerArm: 2048, Concurrency: 1, Sparse: true, ResourceError: true, MixedDependent: true},
	}
	selection, selected := os.LookupEnv("MO_SCALAR_SET_CAPACITY_PROFILES")
	profiles, err := selectScalarCapacityProfiles(profiles, selection, selected)
	require.NoError(b, err)
	selectedNames := make([]string, 0, len(profiles))
	for _, p := range profiles {
		selectedNames = append(selectedNames, p.Name)
	}
	var results []scalarCapacityResult
	embed.RunSingleCNBaseClusterTests(b, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(b, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(b, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		_, err = db.ExecContext(ctx, "create database scalar_set_capacity")
		require.NoError(b, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, cleanupErr := db.ExecContext(cleanupCtx, "drop database scalar_set_capacity")
			require.NoError(b, cleanupErr)
		}()
		for _, p := range profiles {
			result := runScalarCapacityProfile(b, ctx, db, cn.RawService().(cnservice.Service).GetSQLExecutor(), p, selected)
			results = append(results, result)
		}
	})
	data, err := json.MarshalIndent(struct {
		Protocol, Platform, Toolchain, MemorySemantics string
		Results                                        []scalarCapacityResult
		SelectedProfiles                               []string
		CompleteQualification                          bool
	}{"first-invocation; excluded warm-up; three measured rounds; separate EXPLAIN ANALYZE", runtime.GOOS + "/" + runtime.GOARCH, runtime.Version(), "RSS is process lifetime high-water; MemorySize is operator-reported, neither is query-accounted peak", results, selectedNames, !selected}, "", "  ")
	require.NoError(b, err)
	require.NoError(b, os.WriteFile(report, append(data, '\n'), 0600))
}

func runScalarCapacityProfile(b *testing.B, ctx context.Context, db *sql.DB, internal executor.SQLExecutor, p scalarCapacityProfile, diagnostic bool) scalarCapacityResult {
	b.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(b, err)
	defer conn.Close()
	exec := func(q string) { _, e := conn.ExecContext(ctx, q); require.NoError(b, e, q) }
	exec("use scalar_set_capacity")
	exec("drop table if exists outer_rows")
	exec("drop table if exists inner_rows")
	exec("create table outer_rows(id int primary key,pad varchar(8192))")
	exec("create table inner_rows(id int primary key,v double)")
	var values []string
	for i := 0; i < p.Outer; i++ {
		id := -i - 1
		if p.Sparse {
			id = p.InnerPerArm - 1 + i*(p.InnerPerArm+1)
		}
		values = append(values, fmt.Sprintf("(%d,'%s')", id, strings.Repeat("x", p.Width)))
	}
	exec("insert into outer_rows values " + strings.Join(values, ","))
	values = nil
	for i := 1; i <= p.InnerPerArm; i++ {
		v := "-0e0"
		if p.Distinct {
			v = fmt.Sprint(i)
			if p.ValueKinds > 0 {
				v = fmt.Sprint(1 + i%p.ValueKinds)
				if p.Skew {
					v = "1"
					if i == p.InnerPerArm {
						v = "2"
					}
				}
			}
		}
		values = append(values, fmt.Sprintf("(%d,%s)", i, v))
	}
	exec("insert into inner_rows values " + strings.Join(values, ","))
	// Hidden integer identity prevents identical signed-zero candidates from
	// collapsing before range matching. I is twice InnerPerArm across both arms.
	query := `select o.id,o.pad,cast((select i.v from inner_rows i where i.id>o.id union select i.v from inner_rows i where i.id>o.id) as varchar) from outer_rows o order by o.id`
	if p.MixedDependent {
		query = strings.Replace(query, " from outer_rows o", ",(select (select o.id+1)) from outer_rows o", 1)
	}
	result := scalarCapacityResult{Profile: p}
	conns := make([]*sql.Conn, p.Concurrency)
	for i := range conns {
		conns[i], err = db.Conn(ctx)
		require.NoError(b, err)
		defer conns[i].Close()
		_, err = conns[i].ExecContext(ctx, "use scalar_set_capacity")
		require.NoError(b, err)
		_, err = conns[i].ExecContext(ctx, "set max_dop=1")
		require.NoError(b, err)
		_, err = conns[i].ExecContext(ctx, "set agg_spill_mem=0")
		require.NoError(b, err)
		if p.ForceSpill {
			_, err = conns[i].ExecContext(ctx, "set agg_spill_mem=1")
			require.NoError(b, err)
		}
	}
	result.Settings = make(map[string]int64)
	for _, name := range []string{"max_dop", "agg_spill_mem", "join_spill_mem", "sort_spill_mem"} {
		var setting int64
		require.NoError(b, conns[0].QueryRowContext(ctx, "select @@"+name).Scan(&setting))
		result.Settings[name] = setting
	}
	for round := -2; round < 3; round++ {
		phase := "measured"
		if round == -2 {
			phase = "first-invocation"
		}
		if round == -1 {
			phase = "excluded-warmup"
		}
		samples := make([]scalarCapacitySample, p.Concurrency)
		errs := make([]error, p.Concurrency)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for worker := range conns {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				<-start
				samples[worker], errs[worker] = measureScalarCapacity(ctx, conns[worker], query, p)
				samples[worker].Phase = phase
				samples[worker].Round = round
				samples[worker].Worker = worker
			}(worker)
		}
		var before scalarCapacityProcessSnapshot
		if diagnostic {
			before = snapshotScalarCapacityProcess(b)
		}
		close(start)
		wg.Wait()
		// Diagnostics are outside each query's timed interval. They describe the
		// whole process and cannot replace statement-account measurements.
		if diagnostic {
			after := snapshotScalarCapacityProcess(b)
			b.Logf("process-diagnostic profile=%s phase=%s round=%d before=%+v after=%+v", p.Name, phase, round, before, after)
		}
		for i := range samples {
			// Emit before acceptance assertions so a failed qualification retains
			// the actual phase, rows, error code and timing instead of losing it.
			data, marshalErr := json.Marshal(samples[i])
			require.NoError(b, marshalErr)
			b.Logf("sample profile=%s data=%s", p.Name, data)
		}
		for i := range samples {
			require.NoError(b, errs[i], "profile=%s phase=%s worker=%d", p.Name, phase, i)
			// These fixed qualification budgets are benchmark acceptance criteria,
			// never wall-clock assertions in the ordinary correctness suite.
			if phase == "first-invocation" {
				require.LessOrEqual(b, samples[i].CompletionNS, int64(10*time.Second), p.Name)
			} else if phase == "measured" {
				require.LessOrEqual(b, samples[i].CompletionNS, int64(5*time.Second), p.Name)
			}
		}
		if len(samples) == 2 {
			require.True(b, samples[0].Started.Before(samples[1].Finished) && samples[1].Started.Before(samples[0].Finished), "concurrent query intervals must overlap")
		}
		result.Samples = append(result.Samples, samples...)
	}
	rows, err := conns[0].QueryContext(ctx, "explain analyze "+query)
	if err == nil {
		func() {
			defer rows.Close()
			for rows.Next() {
				var line string
				require.NoError(b, rows.Scan(&line))
				result.Analyze += line + "\n"
			}
			err = rows.Err()
		}()
	}
	if err != nil {
		require.NotZero(b, p.expectedCode(), "only expected errors may fail EXPLAIN")
		var e *mysqlDriver.MySQLError
		require.ErrorAs(b, err, &e)
		result.AnalyzeErrorCode = int(e.Number)
		require.Equal(b, p.expectedCode(), result.AnalyzeErrorCode)
	} else {
		require.Zero(b, p.expectedCode(), "error profile must fail EXPLAIN")
		lower := strings.ToLower(result.Analyze)
		require.Contains(b, lower, "row_number")
		require.Contains(b, lower, "join type: left")
		require.GreaterOrEqual(b, strings.Count(lower, "aggregate\n"), 2)
		require.Contains(b, lower, "count(")
		require.Contains(b, lower, "any_value(")
		if p.ForceSpill {
			require.GreaterOrEqual(b, strings.Count(lower, "spillrows="), 2, "both aggregation stages must spill")
		}

	}

	if p.expectedCode() == 0 {
		physicalRows, physicalErr := conns[0].QueryContext(ctx, "explain phyplan analyze "+query)
		require.NoError(b, physicalErr)
		func() {
			defer physicalRows.Close()
			for physicalRows.Next() {
				var line string
				require.NoError(b, physicalRows.Scan(&line))
				result.Physical += line + "\n"
			}
			require.NoError(b, physicalRows.Err())
		}()
		require.Contains(b, result.Physical, "Merge")
		if p.ForceSpill {
			require.Contains(b, result.Physical, "GroupSpillReloadRecords:")
			require.Contains(b, result.Physical, "GroupSpillBucketsCreated:")
		}
		// Read exact integer counters from the existing physical text format.
		// EXPLAIN is a separate serial query, not a concurrent spill measurement.
		var spillBytes uint64
		memoryCounters, spillCounters := 0, 0
		for _, line := range strings.Split(result.Physical, "\n") {
			if !strings.Contains(line, "CallNum:") {
				continue
			}
			for _, field := range strings.Fields(line) {
				for _, name := range []string{"MemSize", "SpillSize"} {
					if !strings.HasPrefix(field, name+":") {
						continue
					}
					value, parseErr := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(field, name+":"), "bytes"), 10, 64)
					require.NoError(b, parseErr, field)
					if name == "MemSize" {
						memoryCounters++
						require.LessOrEqual(b, value, uint64(256<<20), p.Name)
					} else {
						spillCounters++
						spillBytes += value
					}
				}
			}
		}
		require.Positive(b, memoryCounters, "physical memory counters must be present")
		require.Positive(b, spillCounters, "physical spill counters must be present")
		require.LessOrEqual(b, spillBytes, uint64(64<<20), p.Name)
	}
	// This separate internal-executor run captures terminal generation accounting,
	// including cardinality failures that cannot return an EXPLAIN payload.
	result.Internal = make([]resource.StatementResourceSummary, p.Concurrency)
	internalErrors := make([]error, p.Concurrency)
	startInternal := make(chan struct{})
	var internalWG sync.WaitGroup
	for worker := range result.Internal {
		internalWG.Add(1)
		go func(worker int) {
			defer internalWG.Done()
			<-startInternal
			root := resource.NewRoot(resource.ConnInternal)
			internalCtx := resource.ContextWithRoot(ctx, root)
			opts := executor.Options{}.WithDatabase("scalar_set_capacity").WithResolveVariableFunc(func(name string, system, global bool) (any, error) {
				if value, ok := result.Settings[name]; ok {
					return value, nil
				}
				return executor.DefaultResolveVariable(name, system, global)
			})
			res, execErr := internal.Exec(internalCtx, query, opts)
			res.Close()
			internalErrors[worker] = execErr
			result.Internal[worker] = root.PreResponseSummary()
		}(worker)
	}
	close(startInternal)
	internalWG.Wait()
	var accountPeakSum uint64
	for worker, summary := range result.Internal {
		if p.expectedCode() != 0 {
			var moError *moerr.Error
			require.ErrorAs(b, internalErrors[worker], &moError)
			require.Equal(b, uint16(p.expectedCode()), moError.MySQLCode())
		} else {
			require.NoError(b, internalErrors[worker])
		}
		require.Positive(b, summary.Allocation.GenerationCount)
		require.Equal(b, summary.Allocation.GenerationCount, summary.Allocation.ValidGenerationCount)
		require.Zero(b, summary.Allocation.FailedGenerationCount)
		require.Positive(b, summary.Allocation.MaxGenerationPeak)
		require.Zero(b, summary.Allocation.LiveBytesAtTerminal)
		require.LessOrEqual(b, summary.Allocation.MaxGenerationPeak, uint64(256<<20))
		accountPeakSum += summary.Allocation.SumGenerationPeak
		require.Zero(b, summary.MissingMemoryDomainCount)
		require.Equal(b, "complete", summary.Quality.String())
		b.Logf("internal-account profile=%s worker=%d generationPeak=%d missingDomains=%d quality=%s", p.Name, worker, summary.Allocation.MaxGenerationPeak, summary.MissingMemoryDomainCount, summary.Quality)
	}
	// A conservative sum bound; internal execution overlap is not measured.
	require.LessOrEqual(b, accountPeakSum, uint64(p.Concurrency)*(256<<20), p.Name)

	var usage syscall.Rusage
	require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
	result.ProcessLifetimeRSSHighWaterBytes = usage.Maxrss
	if runtime.GOOS != "darwin" {
		result.ProcessLifetimeRSSHighWaterBytes *= 1024
	}
	require.LessOrEqual(b, result.ProcessLifetimeRSSHighWaterBytes, int64(4<<30), p.Name)
	b.Logf("profile=%s O=%d I=%d width=%d C=%d lifetimeRSS=%d", p.Name, p.Outer, p.InnerPerArm*2, p.Width, p.Concurrency, result.ProcessLifetimeRSSHighWaterBytes)
	return result
}

func measureScalarCapacity(ctx context.Context, conn *sql.Conn, query string, p scalarCapacityProfile) (sample scalarCapacitySample, err error) {
	sample.Started = time.Now()
	defer func() {
		sample.Finished = time.Now()
		sample.CompletionNS = sample.Finished.Sub(sample.Started).Nanoseconds()
		if err != nil {
			var e *mysqlDriver.MySQLError
			if errors.As(err, &e) && int(e.Number) == p.expectedCode() {
				sample.ErrorCode = p.expectedCode()
				err = nil
			}
		}
	}()
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return sample, err
	}
	defer rows.Close()
	for rows.Next() {
		if sample.Rows == 0 {
			sample.FirstRowNS = time.Since(sample.Started).Nanoseconds()
		}
		var id int
		var pad string
		var value sql.NullString
		var dependent int64
		columns := []any{&id, &pad, &value}
		if p.MixedDependent {
			columns = append(columns, &dependent)
		}
		if err = rows.Scan(columns...); err != nil {
			return sample, err
		}
		if p.MixedDependent && dependent != int64(id)+1 {
			return sample, fmt.Errorf("dependent result at id=%d: %d", id, dependent)
		}
		expectedID := -p.Outer + sample.Rows
		if p.Sparse {
			expectedID = p.InnerPerArm - 1 + sample.Rows*(p.InnerPerArm+1)
		}
		if id != expectedID || pad != strings.Repeat("x", p.Width) {
			return sample, fmt.Errorf("outer result id/width mismatch: %d/%d", id, len(pad))
		}
		matched := !p.Sparse || sample.Rows == 0
		if value.Valid != matched || (matched && value.String != "-0") {
			return sample, fmt.Errorf("membership result at id=%d: %+v", id, value)
		}
		sample.Rows++
	}
	if err = rows.Err(); err != nil {
		return sample, err
	}
	if p.expectedCode() != 0 {
		return sample, fmt.Errorf("expected error %d", p.expectedCode())
	}
	if sample.Rows != p.Outer {
		return sample, fmt.Errorf("got %d rows, want %d", sample.Rows, p.Outer)
	}
	return sample, nil
}

func (p scalarCapacityProfile) expectedCode() int {
	if p.ResourceError {
		return int(moerr.ErrScalarSetResourceBudgetExceeded)
	}
	if p.Distinct {
		return 1242
	}
	return 0
}

// Selection is diagnostic only; an unset selector is the complete qualification.
// Reject misspellings, duplicates and empty selections before starting a cluster.
func selectScalarCapacityProfiles(profiles []scalarCapacityProfile, selection string, selected bool) ([]scalarCapacityProfile, error) {
	if !selected {
		return profiles, nil
	}
	wanted := make(map[string]bool)
	for _, name := range strings.Split(selection, ",") {
		if name == "" || wanted[name] {
			return nil, fmt.Errorf("empty or duplicate capacity profile %q", name)
		}
		wanted[name] = true
	}
	var result []scalarCapacityProfile
	for _, p := range profiles {
		if wanted[p.Name] {
			result = append(result, p)
			delete(wanted, p.Name)
		}
	}
	if len(wanted) != 0 {
		return nil, fmt.Errorf("unknown capacity profiles: %v", wanted)
	}
	return result, nil
}

type scalarCapacityProcessSnapshot struct {
	At                                  time.Time
	Usage                               syscall.Rusage
	NumGC                               uint32
	PauseTotalNS, TotalAlloc, HeapAlloc uint64
}

func snapshotScalarCapacityProcess(b *testing.B) scalarCapacityProcessSnapshot {
	b.Helper()
	var result scalarCapacityProcessSnapshot
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &result.Usage))
	result.At = time.Now()
	result.NumGC, result.PauseTotalNS = memory.NumGC, memory.PauseTotalNs
	result.TotalAlloc, result.HeapAlloc = memory.TotalAlloc, memory.HeapAlloc
	return result
}

func TestScalarCapacityProfileSelection(t *testing.T) {
	profiles := []scalarCapacityProfile{{Name: "ordinary"}, {Name: "mixed"}}
	all, err := selectScalarCapacityProfiles(profiles, "", false)
	require.NoError(t, err)
	require.Equal(t, profiles, all)
	chosen, err := selectScalarCapacityProfiles(profiles, "mixed,ordinary", true)
	require.NoError(t, err)
	require.Equal(t, profiles, chosen, "selection preserves declared execution order")
	chosen, err = selectScalarCapacityProfiles(profiles, "mixed", true)
	require.NoError(t, err)
	require.Equal(t, profiles[1:], chosen)
	for _, invalid := range []string{"", "unknown", "mixed,unknown", "mixed,mixed", "mixed,", " mixed"} {
		_, err = selectScalarCapacityProfiles(profiles, invalid, true)
		require.Error(t, err, invalid)
	}
}
