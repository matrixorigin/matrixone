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
	Skew                                   bool
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
		{Name: "distinct-cardinality-error", Outer: 8, InnerPerArm: 8, Concurrency: 1, Distinct: true},
		{Name: "width", Outer: 8, InnerPerArm: 8, Width: 256, Concurrency: 1},
		{Name: "concurrency", Outer: 8, InnerPerArm: 8, Concurrency: 2},
		{Name: "combined", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 2},
		{Name: "high-distinctness-boundary", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 2, Distinct: true},
		{Name: "balanced-two-values", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 1, Distinct: true, ValueKinds: 2},
		{Name: "skewed-two-values", Outer: 64, InnerPerArm: 128, Width: 256, Concurrency: 1, Distinct: true, ValueKinds: 2, Skew: true},
		{Name: "forced-spill", Outer: 64, InnerPerArm: 128, Concurrency: 1, ForceSpill: true},
	}
	var results []scalarCapacityResult
	embed.RunSingleCNBaseClusterTests(b, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(b, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(b, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
			result := runScalarCapacityProfile(b, ctx, db, cn.RawService().(cnservice.Service).GetSQLExecutor(), p)
			results = append(results, result)
		}
	})
	data, err := json.MarshalIndent(struct {
		Protocol, Platform, Toolchain, MemorySemantics string
		Results                                        []scalarCapacityResult
	}{"first-invocation; excluded warm-up; three measured rounds; separate EXPLAIN ANALYZE", runtime.GOOS + "/" + runtime.GOARCH, runtime.Version(), "RSS is process lifetime high-water; MemorySize is operator-reported, neither is query-accounted peak", results}, "", "  ")
	require.NoError(b, err)
	require.NoError(b, os.WriteFile(report, append(data, '\n'), 0600))
}

func runScalarCapacityProfile(b *testing.B, ctx context.Context, db *sql.DB, internal executor.SQLExecutor, p scalarCapacityProfile) scalarCapacityResult {
	b.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(b, err)
	defer conn.Close()
	exec := func(q string) { _, e := conn.ExecContext(ctx, q); require.NoError(b, e, q) }
	exec("use scalar_set_capacity")
	exec("drop table if exists outer_rows")
	exec("drop table if exists inner_rows")
	exec("create table outer_rows(id int primary key,pad varchar(256))")
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
		close(start)
		wg.Wait()
		for i := range samples {
			require.NoError(b, errs[i], "profile=%s phase=%s worker=%d", p.Name, phase, i)
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
		require.True(b, p.Distinct, "only the distinct profile may fail EXPLAIN")
		var e *mysqlDriver.MySQLError
		require.ErrorAs(b, err, &e)
		result.AnalyzeErrorCode = int(e.Number)
		require.Equal(b, 1242, result.AnalyzeErrorCode)
	} else {
		require.False(b, p.Distinct, "distinct profile must fail EXPLAIN with 1242")
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

	if !p.Distinct {
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
	for worker, summary := range result.Internal {
		if p.Distinct {
			var moError *moerr.Error
			require.ErrorAs(b, internalErrors[worker], &moError)
			require.Equal(b, moerr.ErrSubqueryNo1Row, moError.ErrorCode())
		} else {
			require.NoError(b, internalErrors[worker])
		}
		require.Positive(b, summary.Allocation.GenerationCount)
		require.Equal(b, summary.Allocation.GenerationCount, summary.Allocation.ValidGenerationCount)
		require.Zero(b, summary.Allocation.FailedGenerationCount)
		require.Positive(b, summary.Allocation.MaxGenerationPeak)
		require.Zero(b, summary.Allocation.LiveBytesAtTerminal)
		require.LessOrEqual(b, summary.Allocation.MaxGenerationPeak, uint64(256<<20))
		require.Zero(b, summary.MissingMemoryDomainCount)
		require.Equal(b, "complete", summary.Quality.String())
		b.Logf("internal-account profile=%s worker=%d generationPeak=%d missingDomains=%d quality=%s", p.Name, worker, summary.Allocation.MaxGenerationPeak, summary.MissingMemoryDomainCount, summary.Quality)
	}

	var usage syscall.Rusage
	require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
	result.ProcessLifetimeRSSHighWaterBytes = usage.Maxrss
	if runtime.GOOS != "darwin" {
		result.ProcessLifetimeRSSHighWaterBytes *= 1024
	}
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
			if errors.As(err, &e) && p.Distinct && e.Number == 1242 {
				sample.ErrorCode = 1242
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
		if err = rows.Scan(&id, &pad, &value); err != nil {
			return sample, err
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
	if p.Distinct {
		return sample, fmt.Errorf("expected scalar cardinality error 1242")
	}
	if sample.Rows != p.Outer {
		return sample, fmt.Errorf("got %d rows, want %d", sample.Rows, p.Outer)
	}
	return sample, nil
}
