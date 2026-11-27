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
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

const (
	equalityBoundObjects = 128
	equalityBoundBlocks  = 512
)

// BoundEqualityRows counts physical rows and those potentially matching in
// one already subscribed snapshot. Deletions are not deducted. Zero totals
// mean the observation was unavailable.
func (e *Engine) BoundEqualityRows(
	ctx context.Context, op client.TxnOperator, table *plan.TableDef, column *plan.ColDef, value []byte, valueType types.Type,
) (candidates, total uint64) {
	if context.Cause(ctx) != nil || op == nil || table.DbId == 0 || table.TblId == 0 ||
		column.Primary || column.Seqnum >= objectio.SEQNUM_UPPER || op.IsSnapOp() {
		return 0, 0
	}
	read, valid := client.TryCatalogReadStamp(op)
	if !valid || read.Snapshot.IsEmpty() {
		return 0, 0
	}
	txn, ok := read.Workspace.(*Transaction)
	if !ok || txn.engine != e || !txn.ReadOnly() || txn.haveDDL.Load() {
		return 0, 0
	}
	push := e.PushClient()
	if push == nil || push.subscriber == nil {
		return 0, 0
	}
	source := push.subscriber.tryClient()
	if source == nil {
		return 0, 0
	}
	state, sub, ok := push.planningInputSnapshot(table.DbId, table.TblId, read)
	if !ok || state.ApproxDataObjectsNum() > equalityBoundObjects {
		return 0, 0
	}
	columnType := plan2.MakeTypeByPlan2Type(column.Typ)
	if columnType.Oid != valueType.Oid {
		return 0, 0
	}
	match, _, ok := index.MakeScalarZoneMapMatcher(columnType, value, valueType, 0, nil, types.ComparisonEqual)
	if !ok {
		return 0, 0
	}
	ts := types.TimestampToTS(read.Snapshot)
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
			rows = cachedEqualityObjectRows(ctx, obj.Location(), obj.BlkCnt(), obj.Rows(), uint16(column.Seqnum), columnType, match)
		}
		bound += rows
	}
	latest, latestSub, valid := push.planningInputSnapshot(table.DbId, table.TblId, read)
	current, stable := client.TryCatalogReadStamp(op)
	if !valid || latest != state || latestSub != sub || source != push.subscriber.tryClient() ||
		!stable || !read.MatchesRead(current, client.CatalogSnapshotSemantic) || !txn.ReadOnly() ||
		txn.haveDDL.Load() || context.Cause(ctx) != nil {
		return 0, 0
	}
	return bound, snapshotRows
}

func cachedEqualityObjectRows(
	ctx context.Context, location objectio.Location, blocks, rows uint32, seq uint16, columnType types.Type, match func(index.ZM) index.ZoneMapMatch,
) uint64 {
	fallback := uint64(rows)
	view, ok := objectio.TryCachedColumnMeta(ctx, location, seq, int(blocks))
	if !ok || view.BlockCount() != blocks {
		return fallback
	}
	var total, candidates uint64
	var block objectio.ColumnBlockMeta
	for i := uint32(0); i < blocks; i++ {
		block, ok = view.Block(i)
		if !ok || context.Cause(ctx) != nil {
			return fallback
		}
		blockRows := uint64(block.Rows)
		total += blockRows
		zm := objectio.ZoneMap(block.ZoneMap[:])
		if !zm.HasType(columnType) || match(zm).MayMatch() {
			candidates += blockRows
		}
	}
	if total != fallback {
		return fallback
	}
	return candidates
}
