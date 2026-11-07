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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// planningInputSnapshot only borrows existing state. Unlike ordinary partition
// admission it cannot create a partition, subscribe, wait, or read checkpoints.
func (c *PushClient) planningInputSnapshot(db, table uint64, read client.CatalogReadStamp) (*logtailreplay.PartitionState, *subEntry, bool) {
	if c == nil || c.eng == nil || !c.receivedLogTailTime.ready.Load() {
		return nil, nil, false
	}
	s := &c.subscribed
	if !s.rw.TryRLock() {
		return nil, nil, false
	}
	defer s.rw.RUnlock()
	entry := s.m[table]
	if entry == nil || entry.dbID != db || entry.state != Subscribed || entry.pendingTo.Load() != nil {
		return nil, nil, false
	}
	if !c.eng.TryRLock() {
		return nil, nil, false
	}
	part := c.eng.partitions[[2]uint64{db, table}]
	c.eng.RUnlock()
	if part == nil {
		return nil, nil, false
	}
	state := part.Snapshot()
	waterline := c.receivedLogTailTime.getTimestamp()
	if !c.receivedLogTailTime.ready.Load() || !read.Snapshot.Prev().LessEq(waterline) || !state.CanServe(types.TimestampToTS(read.Snapshot)) {
		return nil, nil, false
	}
	return state, entry, true
}

func spendIndexInputWork(ctx context.Context, budget *int, work int) bool {
	if ctx == nil || context.Cause(ctx) != nil || budget == nil || work < 0 || *budget < work {
		return false
	}
	*budget -= work
	return true
}

// ObserveIndexBlockInput returns no partial estimate. Every contributing
// object belongs to the same complete immutable snapshot and cached metadata.
// The result is a cost observation; it authorizes neither rows nor a new plan.
func (tbl *txnTableDelegate) ObserveIndexBlockInput(ctx context.Context, read client.CatalogReadStamp, req engine.IndexBlockInputRequest, budget *int) (result engine.IndexBlockInput, complete bool, err error) {
	defer func() {
		if !complete {
			result = engine.IndexBlockInput{}
		}
		if cause := planningContextCause(ctx); cause != nil {
			result, complete, err = engine.IndexBlockInput{}, false, cause
		}
	}()
	if !spendIndexInputWork(ctx, budget, 1) || req.Stats.Rows <= 0 || req.Stats.Rows > 1<<53 || math.Trunc(req.Stats.Rows) != req.Stats.Rows || req.Stats.Blocks <= 0 || req.Op > types.ComparisonGreaterEqual || len(req.Bound) > 4096 {
		return result, false, nil
	}
	t, txn, ok := tbl.planningRead(ctx, read)
	if !ok {
		return result, false, nil
	}
	current, ok, err := tbl.ReadIndexCostStats(ctx, read, req.Observed, req.Column)
	if err != nil || !ok || current != req.Stats {
		return result, false, err
	}
	pos, ok := t.tableDef.Name2ColIndex[req.Column]
	if !ok || pos < 0 || int(pos) >= len(t.tableDef.Cols) || t.tableDef.Cols[pos] == nil || t.tableDef.Cols[pos].Seqnum > math.MaxUint16 {
		return result, false, nil
	}
	column := t.tableDef.Cols[pos]
	columnType := types.New(types.T(column.Typ.Id), column.Typ.Width, column.Typ.Scale)
	columnType.Charset = uint8(column.Typ.Charset)
	match, _, ok := index.MakeScalarZoneMapMatcher(columnType, req.Bound, req.BoundType, req.ComparisonScale, req.Zone, req.Op)
	if !ok {
		return result, false, nil
	}
	c := txn.engine.PushClient()
	if c == nil {
		return result, false, nil
	}
	subscriber := c.subscriber
	if subscriber == nil {
		return result, false, nil
	}
	// Each production reconnect publishes a newly constructed client. Holding
	// this pointer until validation prevents address reuse across a swap.
	source := subscriber.tryClient()
	if source == nil {
		return result, false, nil
	}
	state, entry, ok := c.planningInputSnapshot(t.db.databaseId, t.tableId, read)
	if !ok || state.ApproxInMemRows() != 0 || state.ApproxInMemTombstones() != 0 || state.ApproxTombstoneObjectsNum() != 0 ||
		!spendIndexInputWork(ctx, budget, state.ApproxDataObjectsNum()) {
		return result, false, nil
	}
	iter, err := state.NewObjectsIter(types.TimestampToTS(read.Snapshot), true, false)
	if err != nil {
		return result, false, nil
	}
	defer iter.Close()
	var total engine.IndexBlockInput
	// The matcher is synchronous and retains no ZM. Reuse copied value buffers
	// rather than making a potentially escaping array for every visited block.
	var objectMeta, block objectio.ColumnBlockMeta
	for iter.Next() {
		if !spendIndexInputWork(ctx, budget, 2) {
			return result, false, nil
		}
		object := iter.Entry()
		if object.GetAppendable() {
			return result, false, nil
		}
		meta, ok := objectio.TryCachedColumnMeta(ctx, object.Location(), uint16(column.Seqnum), *budget)
		if !ok || meta.BlockCount() != object.BlkCnt() || !spendIndexInputWork(ctx, budget, 2*int(meta.BlockCount())+1) {
			return result, false, nil
		}
		objectMeta, ok = meta.Object()
		if !ok || objectMeta.Rows != object.Rows() {
			return result, false, nil
		}
		objectZM := objectio.ZoneMap(objectMeta.ZoneMap[:])
		if !objectZM.HasType(columnType) {
			return result, false, nil
		}
		objectMatch := match(objectZM)
		if !objectMatch.Known {
			return result, false, nil
		}
		var rows uint64
		for i := uint32(0); i < meta.BlockCount(); i++ {
			if context.Cause(ctx) != nil {
				return result, false, nil
			}
			block, ok = meta.Block(i)
			if !ok {
				return result, false, nil
			}
			rows += uint64(block.Rows)
			zm := objectio.ZoneMap(block.ZoneMap[:])
			if !zm.HasType(columnType) {
				return result, false, nil
			}
			blockMatch := match(zm)
			if !blockMatch.Known {
				return result, false, nil
			}
			if objectMatch.Match && blockMatch.Match {
				result.Rows += uint64(block.Rows)
				result.Blocks++
			}
		}
		if rows != uint64(object.Rows()) {
			return result, false, nil
		}
		total.Rows += rows
		total.Blocks += uint64(meta.BlockCount())
		if total.Rows > uint64(req.Stats.Rows) || total.Blocks > uint64(req.Stats.Blocks) {
			return result, false, nil
		}
	}
	if total.Rows != uint64(req.Stats.Rows) || total.Blocks != uint64(req.Stats.Blocks) || !spendIndexInputWork(ctx, budget, 1) {
		return result, false, nil
	}
	latest, latestEntry, valid := c.planningInputSnapshot(t.db.databaseId, t.tableId, read)
	transport := subscriber.tryClient()
	current, ok, err = tbl.ReadIndexCostStats(ctx, read, req.Observed, req.Column)
	complete = err == nil && ok && current == req.Stats && valid && latest == state && latestEntry == entry && source == transport && planningReadStillCurrent(tbl, ctx, read, t, txn)
	return result, complete, err
}
