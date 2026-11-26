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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	equalityBoundObjects = 128
	equalityBoundBlocks  = 512
)

// BoundEqualityRows counts physical rows and those potentially matching in
// one already subscribed snapshot. Deletions are not deducted. Zero totals
// mean the observation was unavailable.
func (e *Engine) BoundEqualityRows(
	ctx context.Context, op client.TxnOperator, proc *process.Process, table *plan.TableDef, column *plan.ColDef, filter *plan.Expr,
) (candidates, total uint64) {
	if context.Cause(ctx) != nil || op == nil || proc == nil || table.DbId == 0 || table.TblId == 0 ||
		column.Primary || column.Seqnum >= objectio.SEQNUM_UPPER || op.IsSnapOp() {
		return 0, 0
	}
	snapshot := op.SnapshotTS()
	txn, ok := op.GetWorkspace().(*Transaction)
	if !ok || txn.engine != e || !txn.ReadOnly() || txn.haveDDL.Load() || snapshot.IsEmpty() {
		return 0, 0
	}
	push := e.PushClient()
	if push == nil || push.subscriber == nil {
		return 0, 0
	}
	generation := push.replayVersion.Load()
	state, sub, ok := push.pointSamplePartition(table.DbId, table.TblId, snapshot)
	if !ok || generation == 0 || state.ApproxDataObjectsNum() > equalityBoundObjects {
		return 0, 0
	}
	matcher, ok := equalityBlockMatcher(proc, table, column, filter)
	if !ok {
		return 0, 0
	}
	ts := types.TimestampToTS(snapshot)
	iter, err := state.NewObjectsIter(ts, false, false)
	if err != nil {
		return 0, 0
	}
	defer iter.Close()

	// This count includes old versions and can overcount persisted rows.
	bound := uint64(state.ApproxInMemRows())
	snapshotRows := bound
	objects, blocks := 0, 0
	for iter.Next() {
		objects++
		if objects > equalityBoundObjects || context.Cause(ctx) != nil {
			return 0, 0
		}
		obj := iter.Entry()
		if !obj.Visible(ts) {
			continue
		}
		rows := uint64(obj.Rows())
		if rows == 0 {
			return 0, 0
		}
		snapshotRows += rows
		count := int(obj.BlkCnt())
		if !obj.GetAppendable() && count <= equalityBoundBlocks-blocks {
			blocks += count
			if candidate, observed := objectio.CachedMatchingBlockRows(
				ctx, obj.Location(), uint16(column.Seqnum), types.T(column.Typ.Id), count, obj.Rows(), matcher,
			); observed {
				rows = candidate
			}
		}
		bound += rows
	}
	latest, latestSub, valid := push.pointSamplePartition(table.DbId, table.TblId, snapshot)
	if !valid || latest != state || latestSub != sub || generation != push.replayVersion.Load() ||
		!op.SnapshotTS().Equal(snapshot) || op.GetWorkspace() != txn || !txn.ReadOnly() || txn.haveDDL.Load() || context.Cause(ctx) != nil {
		return 0, 0
	}
	return bound, snapshotRows
}

func equalityBlockMatcher(proc *process.Process, table *plan.TableDef, column *plan.ColDef, filter *plan.Expr) (func(int, objectio.BlockObject) (bool, bool, error), bool) {
	pos, exists := table.Name2ColIndex[column.Name]
	if !exists || pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] != column {
		return nil, false
	}
	expr := plan2.DeepCopyExpr(filter)
	var executors []colexec.ExpressionExecutor
	defer func() {
		for _, executor := range executors {
			executor.Free()
		}
	}()
	if _, err := plan2.ReplaceFoldExpr(proc, expr, &executors); err != nil {
		return nil, false
	}
	if err := plan2.EvalFoldExpr(proc, expr, &executors); err != nil {
		return nil, false
	}
	_, _, _, match, _, ok, _ := readutil.CompileFilterExpr(expr, table, nil)
	if !ok || match == nil {
		return nil, false
	}
	return func(i int, b objectio.BlockObject) (bool, bool, error) {
		return match(i, b, nil)
	}, true
}
