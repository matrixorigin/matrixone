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

	"github.com/shirou/gopsutil/v3/mem"

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
	HostBound bool // host memory, not the request, decided Capacity
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
	srcRowCount, explicitCapacity, rowsFit, hostRowsFit, threshold int64,
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
	// Capacity is a HOST allocation before it is anything else: the C++ constructor
	// does flattened_host_dataset.resize(capacity * dim) up front (ivf_pq.hpp:226,258),
	// and resize value-initializes, so it is committed RSS rather than lazy pages.
	//
	// This bound used to come for free. While the per-row cost included the dataset
	// term (dim * sizeof(Q)), bounding against VRAM incidentally bounded the host
	// buffer at roughly the same figure. Sizing ivfpq against the PQ codes instead --
	// correct, since the dataset is now streamed and never resident -- broke that
	// coupling and multiplied the host allocation by dim*sizeof(Q)/(m+8), about 7.7x
	// at dim 768 / f16 / m 192. Without an explicit bound a 20 GB card derives a 63M
	// capacity and asks the host for 97 GB.
	hostBound := false
	if hostRowsFit > 0 && hostRowsFit < capacity {
		capacity = hostRowsFit
		hostBound = true
		vramBound = false
	}
	// Never reserve for rows that do not exist.
	if capacity > srcRowCount {
		capacity = srcRowCount
		vramBound = false
		hostBound = false
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
			return capacityPlan{Capacity: capacity, CdcCutoff: 0, NumSubIdx: 1,
				VRAMBound: vramBound, HostBound: hostBound}, nil
		}
		if hostBound {
			return capacityPlan{}, moerr.NewInvalidInputNoCtxf(
				"%s: host memory allows only %d rows per sub-index but the k-means minimum is %d; "+
					"lower it, or use a narrower storage type (QUANTIZATION), which shrinks the "+
					"host build buffer", algo, capacity, threshold)
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

	plan := capacityPlan{Capacity: capacity, CdcCutoff: srcRowCount,
		VRAMBound: vramBound, HostBound: hostBound}
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

// calculatePqDim mirrors cuVS index<IdxT>::calculate_pq_dim
// (cuvs/cpp/src/neighbors/ivf_pq_index.cu:611), which is what cuVS uses when
// pq_dim (our `m`) is left at 0. Sizing a build against the wrong value is not a
// rounding error: at dim 768 cuVS picks 384, twice the 192 the wiki_all template
// configures, so a default-m build needs twice the device memory a configured one
// does.
func calculatePqDim(dim uint64) uint64 {
	if dim >= 128 {
		dim /= 2
	}
	if r := dim &^ 31; r > 0 { // raft::round_down_safe(dim, 32)
		return r
	}
	r := uint64(1)
	for r<<1 <= dim {
		r <<= 1
	}
	return r
}

// pqCodeBytes is the device footprint of ONE indexed row: its PQ code plus the
// int64 payload cuVS stores alongside it in the IVF list. This is what actually
// scales with row count, and unlike the build dataset it cannot be streamed — a
// search has to reach every list, so the whole index stays resident. Sub-index
// rotation does not help here; splitting an index does not shrink the sum.
//
// Real peak during extend runs above this by ~30-40% for cuVS workspace that is
// not folded in here: at 87.5M / dim 768 / f16 / m=192 the tar is 17.6 GB but the
// device peak is ~24 GB (measured on an L40S). The 60% VRAM rule the planner
// applies on top of this bound absorbs the gap on any workload measured so far,
// but a user tuning a build to exactly the advertised rowsFit ceiling will OOM
// before the check says they should. See ivfpq_train_extend.md for the numbers.
func pqCodeBytes(dim, m, bitsPerCode uint64) uint64 {
	if m == 0 {
		m = calculatePqDim(dim)
	}
	if bitsPerCode == 0 {
		bitsPerCode = 8
	}
	return (m*bitsPerCode+7)/8 + 8
}

// trainsetBytesPerElem is the PEAK device cost of one vector component in the
// k-means trainset.
//
// cuVS materialises the trainset as float32 whatever the storage type, and for a
// non-float T it also allocates a trainset_tmp in T and converts
// (ivf_pq_build.cuh:1288-1307). Both are live at the peak. So a NARROWER storage
// type costs MORE here, not less: f16 is 4+2=6 bytes against f32's 4. Narrow
// types still pay off in host memory and in the final index; this one term runs
// the other way, and sizing it with the storage width under-counts f16 by a third.
func trainsetBytesPerElem(qt metric.QuantizationType) uint64 {
	if qt == metric.Quantization_F32 {
		return 4
	}
	return 4 + quantizationBytes(qt)
}

// kmeansPointsPerCentroid is the conventional floor for k-means to have enough
// evidence per cluster. cuVS does not enforce it — validate_build_params only
// checks rows >= n_lists — so a build can train 16 points per centroid, succeed,
// and quietly return a worse index.
const kmeansPointsPerCentroid = 39

// trainPlan is the resolved k-means training sample for one sub-index.
type trainPlan struct {
	Fraction float64 // effective fraction, after any clamp
	Rows     int64   // effective training rows
	Clamped  bool    // device memory, not the request, decided Rows
	Thin     bool    // fewer than kmeansPointsPerCentroid rows per centroid
}

// planTrainFraction resolves how much of a sub-index cuVS may train on.
//
// The build has two phases whose allocations do NOT overlap: the trainset and its
// k-means scratch are scoped to a block that closes at ivf_pq_build.cuh:1369,
// before detail::extend at :1374 allocates the list data. Peak is therefore
// max(train, index) and the two are budgeted independently — which is why the
// fraction is clamped here rather than folded into the per-row cost that bounds
// capacity. Trading capacity away to afford a larger training sample would be
// backwards: capacity is bounded by the index, which no amount of streaming can
// shrink, while the sample can simply be made smaller.
//
// maxTrainRows is what the device can hold (0 = not measured). cuVS floors the
// sample at n_lists itself (ratio = max(1, N / max(fraction*N, n_lists)),
// :1257-1260), so that floor is mirrored rather than imposed.
func planTrainFraction(capacity, maxTrainRows, nLists int64, requested float64) trainPlan {
	if capacity <= 0 {
		return trainPlan{}
	}
	frac := requested
	if frac <= 0 || frac > 1 {
		frac = 1
	}

	rows := int64(float64(capacity) * frac)
	clamped := false
	if maxTrainRows > 0 && rows > maxTrainRows {
		rows = maxTrainRows
		clamped = true
	}
	// cuVS's own floor, and it wins over the clamp: a sample below n_lists cannot
	// seed the centroids at all, so if the device cannot hold n_lists rows the
	// build is going to fail on its own terms rather than silently under-train.
	if nLists > 0 && rows < nLists {
		rows = nLists
	}
	if rows > capacity {
		rows = capacity
	}

	p := trainPlan{Fraction: float64(rows) / float64(capacity), Rows: rows, Clamped: clamped}
	if nLists > 0 && rows < nLists*kmeansPointsPerCentroid {
		p.Thin = true
	}
	return p
}

// hostBudgetNumerator/Denominator mirror the GPU rule in cap_train_rows_to_gpu_mem:
// take 60% of what is actually available, not all of it. The reasoning carries over
// unchanged — the buffer is a single contiguous allocation, the allocator needs room
// either side of it, and mo-service is still serving queries out of the same pool
// while the build runs. Deliberately the same fraction as the device side: two memory
// heuristics that can disagree is itself a defect.
const hostBudgetNumerator, hostBudgetDenominator = 6, 10

// hostRowsFittingMem returns how many rows of perRowBytes fit the host budget, and the
// available figure it was derived from.
//
// perRowBytes here is the HOST cost of a row, which is not the device cost: what the
// build buffer holds is dim * sizeof(Q) of storage-typed vector, whatever the index
// ends up costing on the GPU.
//
// Unlike the GPU query, a failure here is NOT fatal. cudaMemGetInfo failing means the
// device is unusable and guessing would reintroduce the OOM being prevented; a
// gopsutil failure just means this one extra bound cannot be applied on this platform,
// and the device bound plus the srcRowCount clamp still hold. Returning 0 disables the
// bound rather than failing a build that would have worked.
func hostRowsFittingMem(perRowBytes uint64) (rows int64, availBytes uint64, err error) {
	if perRowBytes == 0 {
		return 0, 0, nil
	}
	vm, err := mem.VirtualMemory()
	if err != nil || vm == nil {
		return 0, 0, err
	}
	budget := vm.Available / hostBudgetDenominator * hostBudgetNumerator
	return int64(budget / perRowBytes), vm.Available, nil
}
