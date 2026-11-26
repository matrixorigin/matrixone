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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// SampleCountTombstoneObjects returns a bounded sample from the same committed
// snapshot used by a single-CN COUNT scan, once it crosses the per-block threshold.
func SampleCountTombstoneObjects(ctx context.Context, rel engine.Relation, minRows int) ([][]byte, error) {
	var tbl *txnTable
	switch r := rel.(type) {
	case *txnTable:
		tbl = r
	case *txnTableDelegate:
		if r.combined.is || r.shard.is {
			return nil, nil
		}
		tbl = r.origin
	default:
		return nil, nil
	}
	if tbl.db.op.IsSnapOp() || !tbl.getTxn().ReadOnly() {
		return nil, nil
	}

	part, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	if part.ApproxTombstoneObjectsNum() == 0 {
		return nil, nil
	}
	iter, err := part.NewObjectsIter(types.TimestampToTS(tbl.db.op.SnapshotTS()), true, true)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// Reservoir sampling gives each object the same chance of being selected.
	// The same table and visible object order yield a stable sample.
	rng := rand.New(rand.NewPCG(tbl.tableId, 0))
	var (
		sampled [objectio.MaxObjectCacheProbeIDs]objectio.ObjectId
		objects int
		rows    uint64
	)
	for iter.Next() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		obj := iter.Entry()
		rows += uint64(obj.Rows())
		pos := objects
		objects++
		if pos >= len(sampled) {
			pos = rng.IntN(objects)
		}
		if pos < len(sampled) {
			sampled[pos] = *obj.ObjectName().ObjectId()
		}
	}
	if rows <= uint64(minRows) {
		return nil, nil
	}
	result := make([][]byte, min(objects, len(sampled)))
	for i := range result {
		result[i] = sampled[i][:]
	}
	return result, nil
}
