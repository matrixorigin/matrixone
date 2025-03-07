// Copyright 2021 Matrix Origin
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

package batch

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCompactBatchsPush(t *testing.T) {
	var err error
	var bat1, bat2 *Batch
	mp := mpool.MustNewZero()
	bats := NewCompactBatchs()

	//empty input
	bat1 = NewWithSize(1)
	err = bats.Push(mp, bat1)
	require.NoError(t, err)
	require.Nil(t, bats.Get(0))
	bats.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	//simple test
	bat1 = makeTestBatch(10, mp)
	err = bats.Push(mp, bat1)
	require.NoError(t, err)
	require.Equal(t, 1, bats.Length())
	require.Equal(t, 10, bats.RowCount())
	require.Equal(t, 10, bats.Get(0).rowCount)
	bats.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount < DefaultBatchMaxRow
	bat1 = makeTestBatch(10, mp)
	bat2 = makeTestBatch(10, mp)
	_ = bats.Push(mp, bat1)
	err = bats.Push(mp, bat2)
	require.NoError(t, err)
	require.Equal(t, 1, bats.Length())
	require.Equal(t, 20, bats.RowCount())
	require.Equal(t, 20, bats.Get(0).rowCount)
	bats.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount > DefaultBatchMaxRow
	// but bat1.rowCount + bat2.rowCount - DefaultBatchMaxRow < DefaultBatchMaxRow
	bat1 = makeTestBatch(3, mp)
	bat2 = makeTestBatch(8192, mp)
	_ = bats.Push(mp, bat1)
	err = bats.Push(mp, bat2)
	require.NoError(t, err)
	require.Equal(t, 2, bats.Length())
	require.Equal(t, 8195, bats.RowCount())
	require.Equal(t, 8192, bats.Get(0).rowCount)
	require.Equal(t, 3, bats.Get(1).rowCount)
	bats.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount > DefaultBatchMaxRow
	// but bat1.rowCount + bat2.rowCount - DefaultBatchMaxRow > DefaultBatchMaxRow
	bat1 = makeTestBatch(3, mp)
	bat2 = makeTestBatch(8192*2+1, mp)
	_ = bats.Push(mp, bat1)
	err = bats.Push(mp, bat2)
	require.NoError(t, err)
	require.Equal(t, 3, bats.Length())
	require.Equal(t, 8192*2+1+3, bats.RowCount())
	require.Equal(t, 8192, bats.Get(0).rowCount)
	require.Equal(t, 8192, bats.Get(1).rowCount)
	require.Equal(t, 4, bats.Get(2).rowCount)
	bats.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestCompactBatchsExtend(t *testing.T) {
	var err error
	var bat1, bat2 *Batch
	mp := mpool.MustNewZero()
	bats := NewCompactBatchs()

	//empty input
	bat1 = NewWithSize(1)
	err = bats.Extend(mp, bat1)
	bat1.Clean(mp)
	require.NoError(t, err)
	require.Nil(t, bats.Get(0))
	bats.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	//simple test
	bat1 = makeTestBatch(10, mp)
	err = bats.Extend(mp, bat1)
	bat1.Clean(mp)
	require.NoError(t, err)
	require.Equal(t, 1, bats.Length())
	require.Equal(t, 10, bats.RowCount())
	require.Equal(t, 10, bats.Get(0).rowCount)
	bats.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount < DefaultBatchMaxRow
	bat1 = makeTestBatch(10, mp)
	bat2 = makeTestBatch(10, mp)
	_ = bats.Extend(mp, bat1)
	err = bats.Extend(mp, bat2)
	require.NoError(t, err)
	bat1.Clean(mp)
	bat2.Clean(mp)
	require.Equal(t, 1, bats.Length())
	require.Equal(t, 20, bats.RowCount())
	require.Equal(t, 20, bats.Get(0).rowCount)
	bats.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount > DefaultBatchMaxRow
	// but bat1.rowCount + bat2.rowCount - DefaultBatchMaxRow < DefaultBatchMaxRow
	bat1 = makeTestBatch(3, mp)
	bat2 = makeTestBatch(8192, mp)
	_ = bats.Extend(mp, bat1)
	err = bats.Extend(mp, bat2)
	require.NoError(t, err)
	bat1.Clean(mp)
	bat2.Clean(mp)
	require.Equal(t, 2, bats.Length())
	require.Equal(t, 8195, bats.RowCount())
	require.Equal(t, 8192, bats.Get(0).rowCount)
	require.Equal(t, 3, bats.Get(1).rowCount)
	bats.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	// bat1.rowCount + bat2.rowCount > DefaultBatchMaxRow
	// but bat1.rowCount + bat2.rowCount - DefaultBatchMaxRow > DefaultBatchMaxRow
	bat1 = makeTestBatch(3, mp)
	bat2 = makeTestBatch(8192*2+1, mp)
	_ = bats.Extend(mp, bat1)
	err = bats.Extend(mp, bat2)
	require.NoError(t, err)
	bat1.Clean(mp)
	bat2.Clean(mp)
	require.Equal(t, 3, bats.Length())
	require.Equal(t, 8192*2+1+3, bats.RowCount())
	require.Equal(t, 8192, bats.Get(0).rowCount)
	require.Equal(t, 8192, bats.Get(1).rowCount)
	require.Equal(t, 4, bats.Get(2).rowCount)

	bat := bats.Pop()
	require.Equal(t, 4, bat.rowCount)
	bat.Clean(mp)

	bat = bats.PopFront()
	require.Equal(t, 8192, bat.rowCount)
	bat.Clean(mp)

	bats.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestCompactBatchsUnion(t *testing.T) {
	mp := mpool.MustNewZero()
	bats := NewCompactBatchs()

	bat1 := makeTestBatch(50, mp)
	bat2 := makeTestBatch(8192, mp)

	err := bats.Union(mp, bat1, []int32{})
	require.NoError(t, err)

	err = bats.Union(mp, bat1, []int32{1, 2, 3, 4, 5})
	require.NoError(t, err)
	require.Equal(t, 5, bats.RowCount())

	err = bats.Union(mp, bat1, []int32{1, 2, 3, 4, 5})
	require.NoError(t, err)
	require.Equal(t, 10, bats.RowCount())
	require.Equal(t, 1, bats.Length())

	var sels []int32
	for i := 0; i < 8192; i++ {
		sels = append(sels, int32(i))
	}
	err = bats.Union(mp, bat2, sels)
	require.NoError(t, err)
	require.Equal(t, 8192+10, bats.RowCount())
	require.Equal(t, 2, bats.Length())

	sels = nil
	for i := 0; i < 8182; i++ {
		sels = append(sels, int32(i))
	}
	err = bats.Union(mp, bat2, sels)
	require.NoError(t, err)
	require.Equal(t, 8192+10+8182, bats.RowCount())
	require.Equal(t, 2, bats.Length())

	err = bats.Union(mp, bat1, []int32{1, 2, 3, 4, 5})
	require.NoError(t, err)
	require.Equal(t, 8192+10+8182+5, bats.RowCount())
	require.Equal(t, 3, bats.Length())

	sels = nil
	for i := 0; i < 8190; i++ {
		sels = append(sels, int32(i))
	}
	err = bats.Union(mp, bat2, sels)
	require.NoError(t, err)
	require.Equal(t, 8192+10+8182+5+8190, bats.RowCount())
	require.Equal(t, 4, bats.Length())

	bat1.Clean(mp)
	bat2.Clean(mp)
	bats.Clean(mp)
}

func makeTestBatch(max int, mp *mpool.MPool) *Batch {
	bat := NewWithSize(2)
	bat.Vecs[0] = makeTestVec(max, mp)
	bat.Vecs[1] = makeConstTestVec(max, mp)
	bat.rowCount = bat.Vecs[0].Length()
	return bat
}

func makeTestVec(max int, mp *mpool.MPool) *vector.Vector {
	typ := types.T_int32.ToType()
	vec := vector.NewVec(typ)
	for i := 0; i < max; i++ {
		v := i
		if err := vector.AppendFixed(vec, int32(v), false, mp); err != nil {
			vec.Free(mp)
			return nil
		}
	}
	return vec
}

func makeConstTestVec(max int, mp *mpool.MPool) *vector.Vector {
	typ := types.T_int32.ToType()
	val := int32(10)
	vec, _ := vector.NewConstFixed(typ, val, max, mp)
	return vec
}
