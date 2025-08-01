// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestOneSchemaBatchBuffer(t *testing.T) {
	mp := mpool.MustNewZero()

	attr := []string{"a", "b", "c"}
	typs := []types.Type{types.T_int16.ToType(), types.T_int32.ToType(), types.T_int64.ToType()}

	buffer := NewOneSchemaBatchBuffer(mpool.GB, attr, typs, false)
	require.NotNil(t, buffer)

	var bats []*batch.Batch
	for i := 0; i < 10; i++ {
		bat := buffer.FetchWithSchema(attr, typs)
		bats = append(bats, bat)
		require.NotNil(t, bat)
	}

	for i := range bats {
		buffer.Putback(bats[i], mp)
	}

	bats = bats[:0]
	for i := 0; i < 10; i += 2 {
		bat := buffer.FetchWithSchema(attr, typs)
		require.NotNil(t, bat)
		bats = append(bats, bat)

		bat = buffer.Fetch()
		require.NotNil(t, bat)
		bats = append(bats, bat)
	}

	for i := range bats {
		buffer.Putback(bats[i], mp)
	}

}

func TestGeneralBatchBuffer1(t *testing.T) {
	mp := mpool.MustNewZero()
	buffer := NewGeneralBatchBuffer(mpool.KB*2, false, 1024, 4096)

	attrs1 := []string{"a", "b", "c"}
	typs1 := []types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_char.ToType()}

	attrs2 := []string{"a", "b", "c"}
	typs2 := []types.Type{types.T_int64.ToType(), types.T_char.ToType(), types.T_int64.ToType()}

	bat1 := buffer.FetchWithSchema(attrs1, typs1)
	require.NotNil(t, bat1)
	curr, high, _, _ := buffer.Usage()
	require.Equal(t, 0, curr)
	require.Equal(t, 0, high)

	// one row size attrs1: 8+8+32=48 bytes
	varlenVal := []byte("11111111111111111111111111111111") // 32 bytes
	for i := 0; i < 100; i++ {
		err := vector.AppendFixed(bat1.Vecs[0], int64(i), false, mp)
		require.NoError(t, err)
		err = vector.AppendFixed(bat1.Vecs[1], int64(i), false, mp)
		require.NoError(t, err)
		err = vector.AppendBytes(bat1.Vecs[2], varlenVal, false, mp)
		require.NoError(t, err)
	}
	buffer.Putback(bat1, mp)

	curr, high, _, _ = buffer.Usage()
	require.Equal(t, 2048, curr)
	require.Equal(t, 2048, high)

	bat2 := buffer.FetchWithSchema(attrs2, typs2)

	require.Equal(t, 1024, bat2.Vecs[0].Allocated())
	require.Equal(t, 1024, bat2.Vecs[2].Allocated())

	buffer.Putback(bat2, mp)
	require.Equal(t, 2048, curr)
	require.Equal(t, 2048, high)

	buffer.Close(mp) // no panic
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestVectorsCopyToBatch(t *testing.T) {
	var vecs Vectors
	require.NoError(t, VectorsCopyToBatch(vecs, nil, nil))

	vecs = NewVectors(2)
	mp1 := mpool.MustNewZero()

	for i := 0; i < 2; i++ {
		typ := types.T_int64.ToType()
		vecs[i].ResetWithNewType(&typ)
		for j := 0; j < 100; j++ {
			vector.AppendFixed(&vecs[i], int64(j), false, mp1)
		}
	}

	bat := batch.NewWithSize(2)
	defer bat.Clean(mp1)
	require.NoError(t, VectorsCopyToBatch(vecs, bat, mp1))
	require.Equal(t, vecs.Rows(), bat.RowCount())

	bat.CleanOnlyData()
	require.NoError(t, VectorsCopyToBatch(vecs, bat, mp1))
	require.Equal(t, vecs.Rows(), bat.RowCount())

	vecs[0].ResetWithSameType()
	vecs[1].ResetWithSameType()
	for i := 0; i < 70000; i++ {
		vector.AppendFixed(&vecs[0], int64(i), false, mp1)
		vector.AppendFixed(&vecs[1], int64(i), false, mp1)
	}
	defer vecs.Free(mp1)

	mp2, err := mpool.NewMPool(t.Name(), mpool.MB, mpool.NoFixed)
	require.NoError(t, err)
	bat2 := batch.NewWithSize(2)
	defer bat2.Clean(mp1)
	require.Error(t, VectorsCopyToBatch(vecs, bat2, mp2))
}
