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

	buffer := NewOneSchemaBatchBuffer(mpool.GB, attr, typs)
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
