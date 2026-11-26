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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

func (tbl *txnTableDelegate) PrepareCountTombstoneSample(ctx context.Context) (func(context.Context, int) ([][]byte, error), error) {
	if tbl.combined.is || tbl.shard.is {
		return nil, nil
	}
	return tbl.origin.PrepareCountTombstoneSample(ctx)
}

// PrepareCountTombstoneSample admits the local snapshot needed by the scan.
// The returned sampler has its own optional deadline and reads only this
// immutable partition state.
func (tbl *txnTable) PrepareCountTombstoneSample(ctx context.Context) (func(context.Context, int) ([][]byte, error), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tbl.db.op.IsSnapOp() {
		return nil, nil
	}
	txn := tbl.getTxn()
	if !txn.ReadOnly() {
		return nil, nil
	}

	// Reuse a ready partition on the hot path. On first access the scan
	// requires ordinary snapshot admission even if every CN is cold.
	snapshot := tbl.db.op.SnapshotTS()
	part, _, available := txn.engine.PushClient().planningInputSnapshot(
		tbl.db.databaseId, tbl.tableId, client.CatalogReadStamp{Snapshot: snapshot},
	)
	if !available {
		var err error
		part, err = tbl.getPartitionState(ctx)
		if err != nil {
			return nil, err
		}
	}
	if part.ApproxTombstoneObjectsNum() == 0 {
		return nil, nil
	}

	return func(ctx context.Context, minRows int) ([][]byte, error) {
		// Reservoir sampling gives each object the same chance of being
		// selected, stably for this table and the visible object order.
		rng := rand.New(rand.NewPCG(tbl.tableId, 0))
		var (
			sampled [objectio.MaxObjectCacheProbeIDs]objectio.ObjectId
			objects int
			rows    uint64
		)
		err := part.VisitVisibleTombstoneObjects(ctx, types.TimestampToTS(snapshot), func(obj objectio.ObjectEntry) {
			rows += uint64(obj.Rows())
			pos := objects
			objects++
			if pos >= len(sampled) {
				pos = rng.IntN(objects)
			}
			if pos < len(sampled) {
				sampled[pos] = *obj.ObjectName().ObjectId()
			}
		})
		if err != nil || rows <= uint64(minRows) {
			return nil, err
		}
		result := make([][]byte, min(objects, len(sampled)))
		for i := range result {
			result[i] = sampled[i][:]
		}
		return result, nil
	}, nil
}
