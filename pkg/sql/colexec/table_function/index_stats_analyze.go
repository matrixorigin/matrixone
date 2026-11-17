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

package table_function

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/diagnostic"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (s *indexStatsState) analyzeIndex(proc *process.Process, sp *sqlexec.SqlProcess) (map[string]any, []indexstats.Suggestion, error) {
	ctx := proc.GetTopContext()
	out := map[string]any{}
	unavailable := func(status string, err error) (map[string]any, []indexstats.Suggestion, error) {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, nil, err
		}
		out["status"] = status
		return out, nil, nil
	}
	if !strings.EqualFold(s.config.OpType, "vector_l2_ops") {
		return unavailable("unsupported_distance", nil)
	}
	if s.untrained || len(s.buckets) == 0 {
		return unavailable("untrained", nil)
	}
	if s.config.Async {
		system := *sp
		system.WithExecutionIdentity(catalog.System_Account, catalog.MO_CATALOG)
		progress, err := readIndexStatsMaintenanceObserved(proc, sp, s.config, func(query string) (executor.Result, error) { return indexStatsRunSQL(&system, query) })
		if err != nil {
			return unavailable("progress_snapshot_unavailable", err)
		}
		if progress["caught_up"] != true {
			out["maintenance"] = progress
			status := "index_not_caught_up"
			if progress["caught_up"] == nil {
				status = "progress_snapshot_unavailable"
			}
			return unavailable(status, nil)
		}
	}
	opts := s.config.Analyze
	limit := opts.SampleCount
	if opts.Retrain {
		limit = max(limit, min(4096, 5*len(s.buckets)))
	}
	// Admit vector payload before SQL materializes the sample. Reserve half of
	// the budget for keys, neighbor heaps and the optional training workspace.
	if len(s.buckets) > 0 && len(s.buckets[0].Center) > 0 {
		limit = min(limit, (diagnostic.MaxAnalysisBytes/2)/(len(s.buckets[0].Center)*8+128))
	}
	if limit < 1 {
		return unavailable("sample_resource_limit", nil)
	}
	source := sqlquote.QualifiedIdent(s.config.Database, s.config.Table)
	projection := indexSourceKey("src", s.config.PrimaryKey) + "," + sqlquote.QualifiedIdent("src", s.config.Column)
	scan := "SELECT " + projection + " FROM " + source + " AS src WHERE " + sqlquote.QualifiedIdent("src", s.config.Column) + " IS NOT NULL"
	// Reuse the bounded reservoir over a streamed source read. SQL never
	// materializes a top-N of potentially large vector/key cells before admission.
	reservoir := diagnostic.NewReservoir(limit)
	var scratch []float64
	err := streamIndexStats(proc, sp, scan, func(bat *batch.Batch) error {
		for row := 0; row < bat.RowCount(); row++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			values, err := indexStatsVectorInto(bat.Vecs[1], row, scratch)
			if err != nil {
				return err
			}
			scratch = values
			reservoir.Observe(bat.Vecs[0].GetStringAt(row), 0, values)
			if reservoir.Status != "" {
				return diagnostic.ErrAnalysisLimit
			}
		}
		return nil
	})
	if err != nil {
		return unavailable("source_sample_unavailable", err)
	}
	samples := reservoir.Samples
	var old map[string]bool
	if s.build != nil && s.build.Historical && s.build.SourceTableID == s.config.TableID {
		old, err = readIndexOldKeys(proc, sp, s.config, s.build, sampleKeys(samples))
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			old = nil
			out["cohort_status"] = "history_unavailable"
		}
	} else {
		out["cohort_status"] = "build_snapshot_unavailable"
	}
	queries := diagnostic.SelectQueries(samples, old, opts.SampleCount)
	exact := diagnostic.NewExactNeighbors(queries)
	err = streamIndexStats(proc, sp, scan, func(bat *batch.Batch) error {
		for row := 0; row < bat.RowCount(); row++ {
			values, err := indexStatsVectorInto(bat.Vecs[1], row, scratch)
			if err != nil {
				return err
			}
			scratch = values
			if err := exact.Observe(ctx, bat.Vecs[0].GetStringAt(row), values); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return unavailable("exact_neighbors_unavailable", err)
	}
	membership := map[string]int64{}
	err = runIndexKeyLookup(sp, exact.Keys(), func(list string, n int) string {
		selected := "(SELECT " + sqlquote.Ident(s.config.PrimaryKey) + " FROM " + source + " AS src WHERE " + indexSourceKey("src", s.config.PrimaryKey) + " IN (" + list + ") LIMIT " + strconv.Itoa(n) + ") AS sampled"
		return "SELECT " + indexSourceKey("sampled", s.config.PrimaryKey) + ",e." + sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Entries_id) +
			" FROM " + sqlquote.QualifiedIdent(s.config.Database, s.config.Entries) + " AS e JOIN " + selected + " ON e." + sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Entries_pk) + "=" + sqlquote.QualifiedIdent("sampled", s.config.PrimaryKey) +
			" WHERE e." + sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Entries_version) + "=" + fmt.Sprint(s.version)
	}, func(bat *batch.Batch) error {
		for row := 0; row < bat.RowCount(); row++ {
			key := bat.Vecs[0].GetStringAt(row)
			cid := vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], row)
			if prev, ok := membership[key]; ok && prev != cid {
				return moerr.NewInvalidInput(proc.Ctx, "ambiguous neighbor membership")
			}
			membership[strings.Clone(key)] = cid
		}
		return nil
	})
	if err != nil {
		return unavailable("index_coverage_unavailable", err)
	}
	probe := 0
	if resolve := proc.GetResolveVariableFunc(); resolve != nil {
		if v, err := resolve("probe_limit", true, false); err == nil {
			probe, _ = strconv.Atoi(fmt.Sprint(v))
		}
	}
	coverage, advice, err := exact.Coverage(ctx, s.buckets, membership, old, opts.RecallTarget, probe)
	if err != nil {
		return unavailable("coverage_unavailable", err)
	}
	out["recall"] = coverage
	if opts.Retrain {
		retrain, gain, err := diagnostic.Retrain(ctx, samples, s.buckets)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			retrain = map[string]any{"status": "retrain_unavailable"}
		}
		out["retrain"] = retrain
		if gain {
			advice = indexstats.AddReindex(advice, "sampled_retraining_gain", nil)
		}
	}
	return out, advice, nil
}
