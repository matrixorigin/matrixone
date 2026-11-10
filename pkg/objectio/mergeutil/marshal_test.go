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

package mergeutil

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestMergeSortBatchesPreservesOwnedVarlena(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	newBatch := func() *batch.Batch {
		return batch.NewWithSchema(false, []string{"id", "payload"},
			[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
	}
	values := []string{"first value stored in the area", "inline", "third value stored in the area",
		"fourth value stored in the area", "", "sixth value stored in the area"}
	batches := []*batch.Batch{newBatch(), newBatch()}
	for _, input := range batches {
		t.Cleanup(func() { input.Clean(mp) })
	}
	// Interleaved keys force the per-row merge instead of disjoint concatenation.
	for i, value := range values {
		input := batches[i%2]
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(i+1), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(value), false, mp))
	}
	for _, input := range batches {
		input.SetRowCount(len(values) / 2)
	}
	buffer := newBatch()
	t.Cleanup(func() { buffer.Clean(mp) })
	for range 2 {
		sinkCalls := 0
		var err error
		buffer, err = MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) (*batch.Batch, error) {
			sinkCalls++
			require.Equal(t, []int64{1, 2, 3, 4, 5, 6}, vector.MustFixedColNoTypeCheck[int64](out.Vecs[0]))
			require.Equal(t, len(values), out.RowCount())
			require.True(t, out.Vecs[1].VarlenaAreaIsDisjoint(),
				"merge copies each value into owned storage; serialization must retain that proof")
			encoded, err := out.Vecs[1].MarshalBinary()
			require.NoError(t, err)
			decoded := vector.NewVecFromReuse()
			defer decoded.Free(nil)
			require.NoError(t, decoded.UnmarshalBinary(encoded))
			for row, value := range values {
				require.Equal(t, value, out.Vecs[1].GetStringAt(row))
				require.Equal(t, value, decoded.GetStringAt(row))
			}
			out.CleanOnlyData()
			return out, nil
		}, mp, nil)
		require.NoError(t, err)
		require.Equal(t, 1, sinkCalls)
	}
}

// Include serialization in the sink: disjoint ownership after the per-row
// merge controls whether long values can be written in bulk to object storage.
func BenchmarkMergeSortBatchesOverlappingVarlenaMarshal(b *testing.B) {
	mp := mpool.MustNewZero()
	newBatch := func() *batch.Batch {
		return batch.NewWithSchema(false, []string{"id", "payload"},
			[]types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
	}
	batches := []*batch.Batch{newBatch(), newBatch()}
	for _, input := range batches {
		b.Cleanup(func() { input.Clean(mp) })
	}
	value := bytes.Repeat([]byte{'v'}, 49)
	for i, input := range batches {
		for row := range objectio.BlockMaxRows {
			require.NoError(b, vector.AppendFixed(input.Vecs[0], int64(row*len(batches)+i), false, mp))
			require.NoError(b, vector.AppendBytes(input.Vecs[1], value, false, mp))
		}
		input.SetRowCount(objectio.BlockMaxRows)
	}
	buffer := newBatch()
	b.Cleanup(func() { buffer.Clean(mp) })
	var wire bytes.Buffer
	wire.Grow(objectio.BlockMaxRows * 80)
	sink := func(out *batch.Batch) (*batch.Batch, error) {
		for _, vec := range out.Vecs {
			wire.Reset()
			plan, err := vec.PrepareMarshalBinary()
			if err != nil {
				return out, err
			}
			if err := plan.MarshalTo(&wire); err != nil {
				return out, err
			}
		}
		out.CleanOnlyData()
		return out, nil
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(batches) * objectio.BlockMaxRows * (8 + len(value))))
	b.ResetTimer()
	for range b.N {
		var err error
		buffer, err = MergeSortBatches(batches, 0, buffer, sink, mp, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}
