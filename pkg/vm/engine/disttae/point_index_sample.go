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
	"math/rand/v2"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

const (
	maxPointSampleObjects = 4
	maxPointSampleBlocks  = 32
	maxPointSampleExtents = 64
	maxPointSampleBytes   = 2 << 20
)

// SamplePointIndexMatches borrows an already subscribed base-table snapshot
// and already cached INT columns for one statement. The counts only inform a
// comparison of competing access paths.
func (e *Engine) SamplePointIndexMatches(
	ctx context.Context, op client.TxnOperator, db, table uint64, rows int,
	candidates [][]engine.IndexPointKey, mp *mpool.MPool,
) (engine.IndexPointSample, bool) {
	var empty engine.IndexPointSample
	if ctx == nil || context.Cause(ctx) != nil || op == nil || db == 0 || table == 0 || mp == nil ||
		rows <= objectio.BlockMaxRows || rows > maxPointSampleBlocks*objectio.BlockMaxRows ||
		len(candidates) < 2 || len(candidates) > engine.MaxPointSampleCandidates {
		return empty, false
	}
	snapshot := op.SnapshotTS()
	if op.IsSnapOp() || snapshot.IsEmpty() {
		return empty, false
	}
	txn, ok := op.GetWorkspace().(*Transaction)
	if !ok || txn.engine != e || !txn.ReadOnly() || txn.haveDDL.Load() {
		return empty, false
	}
	push := e.PushClient()
	if push == nil || push.subscriber == nil {
		return empty, false
	}
	generation := push.replayVersion.Load()
	if generation == 0 {
		return empty, false
	}
	state, sub, ok := push.pointSamplePartition(db, table, snapshot)
	if !ok || state.ApproxInMemRows() != 0 || state.ApproxInMemTombstones() != 0 ||
		state.ApproxTombstoneObjectsNum() != 0 || state.ApproxDataObjectsNum() > maxPointSampleObjects {
		return empty, false
	}
	var columnsBuf [engine.MaxPointSampleColumns]uint16
	columns := columnsBuf[:0]
	var positions [engine.MaxPointSampleCandidates][engine.MaxPointSampleColumns]int
	for i, keys := range candidates {
		if len(keys) == 0 || len(keys) > engine.MaxPointSampleColumns {
			return empty, false
		}
		for j, key := range keys {
			pos := slices.Index(columns, key.Seqnum)
			if pos < 0 {
				pos = len(columns)
				if pos == len(columnsBuf) {
					return empty, false
				}
				columns = append(columns, key.Seqnum)
			}
			positions[i][j] = pos
		}
	}
	iter, err := state.NewObjectsIter(types.TimestampToTS(snapshot), true, false)
	if err != nil {
		return empty, false
	}
	defer iter.Close()

	var layout [maxPointSampleBlocks]objectio.CachedInt32Block
	var names [maxPointSampleBlocks]string
	numBlocks, totalRows := 0, 0
	for iter.Next() {
		obj := iter.Entry()
		end := numBlocks + int(obj.BlkCnt())
		if context.Cause(ctx) != nil || obj.GetAppendable() || end > len(layout) {
			return empty, false
		}
		location := obj.Location()
		if !objectio.CachedInt32Blocks(ctx, location, columns, layout[numBlocks:end]) {
			return empty, false
		}
		name := location.Name().String()
		objectRows := 0
		for i := numBlocks; i < end; i++ {
			objectRows += int(layout[i].Rows)
			names[i] = name
		}
		if objectRows != int(obj.Rows()) {
			return empty, false
		}
		totalRows += objectRows
		numBlocks = end
	}
	if totalRows != rows {
		return empty, false
	}

	// Select the entire sample before looking at column cache hits. Layout
	// order and row count are independent of predicates and candidate order.
	rng := rand.New(rand.NewPCG(0, uint64(totalRows)))
	var picks [maxPointSampleBlocks][]int64
	for range engine.PointSampleRows {
		row := rng.IntN(totalRows)
		for i := range layout[:numBlocks] {
			if row < int(layout[i].Rows) {
				picks[i] = append(picks[i], int64(row))
				break
			}
			row -= int(layout[i].Rows)
		}
	}
	encoded, decoded, extents := uint64(0), uint64(0), 0
	for i, block := range layout[:numBlocks] {
		if len(picks[i]) == 0 {
			continue
		}
		for j := range columns {
			extent := objectio.Extent(block.Extents[j][:])
			encoded += uint64(extent.Length())
			decoded += uint64(extent.OriginSize())
			extents++
		}
	}
	if extents > maxPointSampleExtents || encoded > maxPointSampleBytes || decoded > maxPointSampleBytes {
		return empty, false
	}
	fs, err := fileservice.Get[fileservice.FileService](e.fs, defines.SharedFileServiceName)
	if err != nil {
		return empty, false
	}
	var sample engine.IndexPointSample
	var selected [engine.PointSampleRows]objectio.CachedInt32Row
	for i, block := range layout[:numBlocks] {
		if len(picks[i]) == 0 {
			continue
		}
		batch := selected[:len(picks[i])]
		if !objectio.ReadCachedInt32Rows(ctx, fs, names[i], block, len(columns), picks[i], batch, mp) {
			// Every row in this block remains unknown for every candidate.
			continue
		}
		countPointSampleMatches(&sample.Matches, batch, candidates, positions)
		sample.Observed += len(batch)
	}

	latest, latestSub, valid := push.pointSamplePartition(db, table, snapshot)
	if sample.Observed == 0 || !valid || latest != state || latestSub != sub || generation != push.replayVersion.Load() ||
		!op.SnapshotTS().Equal(snapshot) || op.GetWorkspace() != txn || !txn.ReadOnly() ||
		txn.haveDDL.Load() || context.Cause(ctx) != nil {
		return empty, false
	}
	return sample, true
}

func countPointSampleMatches(
	matches *[engine.MaxPointSampleCandidates]int,
	rows []objectio.CachedInt32Row, candidates [][]engine.IndexPointKey,
	positions [engine.MaxPointSampleCandidates][engine.MaxPointSampleColumns]int,
) {
	for _, row := range rows {
		for i, keys := range candidates {
			match := true
			for j, key := range keys {
				col := positions[i][j]
				if row.Present&(1<<col) == 0 || row.Values[col] != key.Value {
					match = false
					break
				}
			}
			if match {
				matches[i]++
			}
		}
	}
}

// pointSamplePartition borrows the current subscription and its already
// installed partition without entering the normal subscription slow path.
func (c *PushClient) pointSamplePartition(db, table uint64, snapshot timestamp.Timestamp) (*logtailreplay.PartitionState, *subEntry, bool) {
	if !c.receivedLogTailTime.ready.Load() || !c.subscribed.rw.TryRLock() {
		return nil, nil, false
	}
	defer c.subscribed.rw.RUnlock()
	sub := c.subscribed.m[table]
	if sub == nil || sub.dbID != db || sub.state != Subscribed || sub.partition == nil || sub.pendingTo.Load() != nil || !c.eng.TryRLock() {
		return nil, nil, false
	}
	part := c.eng.partitions[[2]uint64{db, table}]
	c.eng.RUnlock()
	if part != sub.partition {
		return nil, nil, false
	}
	state := part.Snapshot()
	waterline := c.receivedLogTailTime.getTimestamp()
	if !c.receivedLogTailTime.ready.Load() || !snapshot.Prev().LessEq(waterline) || !state.CanServe(types.TimestampToTS(snapshot)) {
		return nil, nil, false
	}
	return state, sub, true
}
