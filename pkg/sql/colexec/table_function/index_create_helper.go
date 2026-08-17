// Copyright 2022 Matrix Origin
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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// runSqlFunc matches the signature of sqlexec.RunSql so callers can pass their
// own per-algorithm mockable variable (ivfpq_runSql / cagra_runSql / …).
type runSqlFunc func(*sqlexec.SqlProcess, string) (executor.Result, error)

// quoteIdent wraps ident in backticks and doubles any embedded backticks —
// the standard MySQL identifier escape. Without this, a column or table
// name containing a backtick (e.g. "a`b") would break out of the
// quoted-identifier context and let an attacker append arbitrary SQL.
func quoteIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

// fetchSrcTableRowCount returns the number of indexable rows in `db`.`src`.
// When vecCol is non-empty it counts only rows whose indexed vector is non-NULL.
// NULL-vector rows are skipped on the build path (and routed to the CDC tail), so
// counting them would over-state the build size and let a chunk fall below the
// cuVS minimum graph size. Used to auto-populate IndexCapacity and to derive the
// small-tail CDC cutoff; this non-NULL basis MUST match the build cursor, which
// likewise advances only on non-NULL rows.
func fetchSrcTableRowCount(proc *process.Process, runSql runSqlFunc, db, src, vecCol string) (int64, error) {
	sql := fmt.Sprintf("SELECT count(*) FROM %s.%s", quoteIdent(db), quoteIdent(src))
	if vecCol != "" {
		sql += fmt.Sprintf(" WHERE %s IS NOT NULL", quoteIdent(vecCol))
	}
	res, err := runSql(sqlexec.NewSqlProcess(proc), sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()
	if len(res.Batches) == 0 || res.Batches[0].RowCount() != 1 {
		return 0, moerr.NewInternalError(proc.Ctx, "failed to determine source table row count")
	}
	return vector.GetFixedAtWithTypeCheck[int64](res.Batches[0].Vecs[0], 0), nil
}

// capacityPlan is the resolved per-sub-index build size and the small-tail cutoff that
// follows from it.
type capacityPlan struct {
	Capacity  int64 // rows per sub-index
	CdcCutoff int64 // rows before this index go to cuVS; the remainder to the CDC tail
	NumSubIdx int64
	VRAMBound bool // the GPU limit, not the request, decided Capacity
}

// planCapacity resolves how many rows one GPU sub-index may cover.
//
// The VRAM bound applies to EVERY build, not just the default. An explicit
// max_index_capacity is a request, not an override: honouring a request larger than the
// device can hold would reintroduce exactly the out-of-memory this bound exists to
// prevent. rowsFit is what the GPU can take (0 = not measured, e.g. the caller declined
// to bound); srcRowCount always clamps too, because InitEmpty preallocates
// capacity*dim*sizeof(Q) host bytes up front and a 20-row table must not reserve
// gigabytes for rows that do not exist.
//
// threshold is the algorithm's per-sub-index k-means minimum (ivfpq lists, cagra
// intermediate_graph_degree). A capacity below it is rejected rather than tolerated: the
// legacy behaviour routed EVERY row to the brute-force CDC tail, which for a large table
// means the whole dataset as per-row host copies plus an index that cannot answer a
// vector search. Only a genuinely small table (fewer rows than the threshold, one chunk)
// legitimately takes the tail.
//
// sharded rejects a split outright. A sharded sub-index is packed with a "shards" key in
// its manifest and reloaded by a loader that branches on the configured mode, so silently
// building sharded sub-indexes as single-GPU produces indexes that cannot be loaded back.
func planCapacity(
	srcRowCount, explicitCapacity, rowsFit, threshold int64,
	sharded bool, algo, paramName string,
) (capacityPlan, error) {
	if srcRowCount <= 0 {
		return capacityPlan{}, moerr.NewInternalErrorNoCtxf("%s: source row count must be positive", algo)
	}

	capacity := explicitCapacity
	if capacity <= 0 {
		capacity = srcRowCount
	}

	vramBound := false
	if rowsFit > 0 && rowsFit < capacity {
		capacity = rowsFit
		vramBound = true
	}
	// Never reserve for rows that do not exist.
	if capacity > srcRowCount {
		capacity = srcRowCount
		vramBound = false
	}

	if sharded && capacity < srcRowCount {
		return capacityPlan{}, moerr.NewInvalidInputNoCtxf(
			"%s: distribution_mode 'sharded' cannot be combined with a split build "+
				"(capacity %d < %d rows): each sub-index would be packed as a sharded "+
				"index and could not be reloaded; use 'single'/'replicated', or raise %s",
			algo, capacity, srcRowCount, paramName)
	}

	if threshold > 0 && capacity < threshold {
		// One chunk covering a table smaller than the minimum is the legitimate
		// small-table case: those rows go to the CDC tail and are served by brute force.
		if capacity >= srcRowCount {
			return capacityPlan{Capacity: capacity, CdcCutoff: 0, NumSubIdx: 1, VRAMBound: vramBound}, nil
		}
		if vramBound {
			return capacityPlan{}, moerr.NewInvalidInputNoCtxf(
				"%s: GPU memory allows only %d rows per sub-index but the k-means minimum is %d; "+
					"lower it, or use a narrower storage type (QUANTIZATION)", algo, capacity, threshold)
		}
		return capacityPlan{}, moerr.NewInvalidInputNoCtxf(
			"%s: %s (%d) must be >= %d; every sub-index would fall below the k-means "+
				"minimum and the whole table would be written as a brute-force CDC tail",
			algo, paramName, capacity, threshold)
	}

	plan := capacityPlan{Capacity: capacity, CdcCutoff: srcRowCount, VRAMBound: vramBound}
	plan.NumSubIdx = (srcRowCount + capacity - 1) / capacity
	// Only the LAST chunk can be short. If it is below the k-means minimum those rows
	// cannot seed centroids, so they go to the CDC tail instead.
	if tail := srcRowCount % capacity; threshold > 0 && tail > 0 && tail < threshold {
		plan.CdcCutoff = srcRowCount - tail
	}
	return plan, nil
}

// quantizationBytes is the on-device element size of one vector component for a storage
// quantization. It sizes the per-row cost used to bound a build against VRAM.
func quantizationBytes(qt metric.QuantizationType) uint64 {
	switch qt {
	case metric.Quantization_F16:
		return 2
	case metric.Quantization_INT8, metric.Quantization_UINT8:
		return 1
	default: // Quantization_F32
		return 4
	}
}
