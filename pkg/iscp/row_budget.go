// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iscp

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const DefaultTargetChangeRows = 100_000

func indexTargetRows(spec *TriggerSpec) int {
	if spec == nil || spec.TargetRows == 0 {
		return DefaultTargetChangeRows
	}
	return spec.TargetRows
}

func rowBudgetForJobs(specs []*JobSpec) (int, error) {
	target := 0
	targetSet := false
	for _, spec := range specs {
		if spec == nil {
			return 0, moerr.NewInternalErrorNoCtx("nil ISCP job spec")
		}
		if spec.TargetRows < 0 {
			return 0, moerr.NewInvalidInputNoCtx("negative ISCP target rows")
		}
		current := spec.TargetRows
		if spec.ConsumerInfo.ConsumerType == int8(ConsumerType_IndexSync) {
			current = indexTargetRows(&spec.TriggerSpec)
		}
		if current > 0 && spec.ConsumerInfo.ConsumerType != int8(ConsumerType_IndexSync) && spec.JobType != TriggerType_AlwaysUpdate {
			return 0, moerr.NewInvalidInputNoCtx("ISCP target rows requires an independent AlwaysUpdate job")
		}
		if targetSet && target != current {
			return 0, moerr.NewInternalErrorNoCtx("mixed ISCP row budgets in one iteration")
		}
		target = current
		targetSet = true
	}
	return target, nil
}

// selectRowBudgetTo probes complete timestamp prefixes under one source
// snapshot. Probe rows are historical operations, before PK net-effect
// coalescing, so the count cannot decrease when the prefix grows.
func selectRowBudgetTo(
	ctx context.Context,
	from, to types.TS,
	target int,
	probe func(context.Context, types.TS, types.TS, int) (int, error),
) (types.TS, int, error) {
	if target <= 0 || from.GT(&to) {
		return types.TS{}, 0, moerr.NewInvalidInputNoCtx("invalid ISCP row budget range")
	}
	rows, err := probe(ctx, from, to, target)
	if err != nil || rows <= target {
		return to, rows, err
	}

	// Counts of uncoalesced operations are monotonic over timestamp prefixes.
	// Find the largest prefix that fits, so the next commit is deferred only
	// when including it would exceed the target.
	lo, hi := from, to.Prev()
	bestTo, bestRows := from.Prev(), 0
	for lo.LE(&hi) {
		if err = ctx.Err(); err != nil {
			return types.TS{}, 0, err
		}
		mid := midpointTS(lo, hi)
		rows, err = probe(ctx, from, mid, target)
		if err != nil {
			return types.TS{}, 0, err
		}
		if rows <= target {
			if rows == target {
				return mid, rows, nil
			}
			bestTo, bestRows = mid, rows
			lo = mid.Next()
		} else {
			hi = mid.Prev()
		}
	}
	if bestRows > 0 {
		return bestTo, bestRows, nil
	}
	// No nonempty prefix fits. The first complete commit is larger than the
	// target; admit that commit so the job always makes progress.
	rows, err = probe(ctx, from, lo, target)
	if err != nil {
		return types.TS{}, 0, err
	}
	return lo, rows, nil
}

func midpointTS(lo, hi types.TS) types.TS {
	if lo.Physical() != hi.Physical() {
		// Physical timestamps in an ISCP range are nonnegative Unix nanos.
		p := lo.Physical() + (hi.Physical()-lo.Physical())/2
		return types.BuildTS(p, math.MaxUint32)
	}
	return types.BuildTS(lo.Physical(), lo.Logical()+(hi.Logical()-lo.Logical())/2)
}

func probeChangeRows(
	ctx context.Context,
	from, to types.TS,
	target int,
	mp *mpool.MPool,
	collect func(context.Context, types.TS, types.TS) (engine.ChangesHandle, error),
) (rows int, err error) {
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	probeCtx := engine.WithCollectChangesDisableQuick(
		engine.WithCollectChangesPreserveAllVersions(ctx))
	h, err := collect(probeCtx, from, to)
	if err != nil {
		if h != nil {
			_ = h.Close()
		}
		return 0, err
	}
	if h == nil {
		return 0, moerr.NewInternalErrorNoCtx("nil ISCP tail row probe handle")
	}
	defer func() {
		if closeErr := h.Close(); err == nil {
			err = closeErr
		}
	}()
	for {
		if err = ctx.Err(); err != nil {
			return rows, err
		}
		var data, tombstone *batch.Batch
		var hint engine.ChangesHandle_Hint
		data, tombstone, hint, err = h.Next(ctx, mp)
		if data != nil {
			rows += data.RowCount()
			data.Clean(mp)
		}
		if tombstone != nil {
			rows += tombstone.RowCount()
			tombstone.Clean(mp)
		}
		if err != nil {
			return rows, err
		}
		if hint == engine.ChangesHandle_Snapshot {
			return rows, moerr.NewInternalErrorNoCtx("snapshot in ISCP tail row probe")
		}
		if rows > target {
			return rows, nil
		}
		if data == nil && tombstone == nil {
			if hint == engine.ChangesHandle_Tail_wip {
				return rows, moerr.NewInternalErrorNoCtx("incomplete ISCP tail row probe")
			}
			return rows, nil
		}
	}
}
