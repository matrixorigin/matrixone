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

package logtailreplay

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

func BenchmarkPartitionStateHandleRowsInsert(b *testing.B) {
	pool := mpool.MustNewZero()
	input, clean := newReplayInsertBatch(b, pool)
	b.Cleanup(clean)

	state := NewPartitionState("", false, 42, false)
	packer := types.NewPacker()
	b.Cleanup(packer.Close)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		state.HandleRowsInsert(ctx, input, 0, packer, pool)
	}
}

func BenchmarkPartitionStateHandleRowsDelete(b *testing.B) {
	pool := mpool.MustNewZero()
	input, clean := newReplayDeleteBatch(b, pool)
	b.Cleanup(clean)

	state := NewPartitionState("", false, 42, false)
	packer := types.NewPacker()
	b.Cleanup(packer.Close)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		state.HandleRowsDelete(ctx, input, packer, pool)
	}
}

func newReplayInsertBatch(b *testing.B, pool *mpool.MPool) (*api.Batch, func()) {
	b.Helper()

	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	pkVec := vector.NewVec(types.T_int64.ToType())

	segmentID := objectio.NewSegmentid()
	blockID := objectio.NewBlockid(segmentID, 0, 0)
	vector.AppendFixed(rowIDVec, objectio.NewRowid(blockID, 0), false, pool)
	vector.AppendFixed(tsVec, types.BuildTS(1, 0), false, pool)
	vector.AppendFixed(pkVec, int64(1), false, pool)

	input := &api.Batch{
		Attrs: []string{"rowid", "time", "pk"},
		Vecs: []api.Vector{
			mustVectorToProto(rowIDVec),
			mustVectorToProto(tsVec),
			mustVectorToProto(pkVec),
		},
	}
	return input, func() {
		rowIDVec.Free(pool)
		tsVec.Free(pool)
		pkVec.Free(pool)
	}
}

func newReplayDeleteBatch(b *testing.B, pool *mpool.MPool) (*api.Batch, func()) {
	b.Helper()

	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	pkVec := vector.NewVec(types.T_int64.ToType())
	tombstoneRowIDVec := vector.NewVec(types.T_Rowid.ToType())

	segmentID := objectio.NewSegmentid()
	blockID := objectio.NewBlockid(segmentID, 0, 0)
	vector.AppendFixed(rowIDVec, objectio.NewRowid(blockID, 0), false, pool)
	vector.AppendFixed(tsVec, types.BuildTS(2, 0), false, pool)
	vector.AppendFixed(pkVec, int64(1), false, pool)
	vector.AppendFixed(tombstoneRowIDVec, types.RandomRowid(), false, pool)

	input := &api.Batch{
		Attrs: []string{"rowid", "time", "pk", "tombstone_rowid"},
		Vecs: []api.Vector{
			mustVectorToProto(rowIDVec),
			mustVectorToProto(tsVec),
			mustVectorToProto(pkVec),
			mustVectorToProto(tombstoneRowIDVec),
		},
	}
	return input, func() {
		rowIDVec.Free(pool)
		tsVec.Free(pool)
		pkVec.Free(pool)
		tombstoneRowIDVec.Free(pool)
	}
}
