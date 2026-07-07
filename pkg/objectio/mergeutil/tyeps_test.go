// Copyright 2022 Matrix Origin
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
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSortColumnsByIndex(t *testing.T) {
	mp := mpool.MustNewZero()

	const (
		vecNum = 3
		vecLen = 50
	)

	var vecs = make([]*vector.Vector, vecNum)
	for i := 0; i < vecNum; i++ {
		if vecs[i] == nil {
			vecs[i] = vector.NewVec(types.T_int32.ToType())
		}

		for j := 0; j < vecLen; j++ {
			x := rand.Int32N(10000)
			err := vector.AppendFixed[int32](vecs[i], x, false, mp)
			require.NoError(t, err)
		}
	}

	for i := 0; i < vecNum; i++ {
		err := SortColumnsByIndex(vecs, i, mp)
		require.NoError(t, err)

		for j := 0; j < vecNum; j++ {
			vals := vector.MustFixedColNoTypeCheck[int32](vecs[j])
			if j == i {
				require.True(t, slices.IsSorted(vals))
				require.True(t, vecs[j].GetSorted())
			} else {
				require.False(t, slices.IsSorted(vals))
				require.False(t, vecs[j].GetSorted())
			}
		}
	}
}

func TestMergeSortBatchesDecimal256(t *testing.T) {
	mp := mpool.MustNewZero()
	decimalTyp := types.New(types.T_decimal256, 39, 4)
	batches := []*batch.Batch{
		newDecimal256MergeBatch(t, mp, decimalTyp, []string{
			"-2.0000",
			"1234567890123456789012345678901234.5678",
			"9999999999999999999999999999999999.9999",
		}, []int32{10, 30, 50}),
		newDecimal256MergeBatch(t, mp, decimalTyp, []string{
			"0.0001",
			"1234567890123456789012345678901234.5677",
		}, []int32{20, 40}),
	}
	for _, bat := range batches {
		defer bat.Clean(mp)
	}

	buffer := batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{decimalTyp, types.T_int32.ToType()})
	defer buffer.Clean(mp)

	var gotKeys []string
	var gotPayloads []int32
	err := MergeSortBatches(batches, 0, buffer, func(out *batch.Batch) error {
		keys := vector.MustFixedColNoTypeCheck[types.Decimal256](out.Vecs[0])
		payloads := vector.MustFixedColNoTypeCheck[int32](out.Vecs[1])
		for i := 0; i < out.RowCount(); i++ {
			gotKeys = append(gotKeys, keys[i].Format(decimalTyp.Scale))
			gotPayloads = append(gotPayloads, payloads[i])
		}
		return nil
	}, mp, nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		"-2.0000",
		"0.0001",
		"1234567890123456789012345678901234.5677",
		"1234567890123456789012345678901234.5678",
		"9999999999999999999999999999999999.9999",
	}, gotKeys)
	require.Equal(t, []int32{10, 20, 40, 30, 50}, gotPayloads)
}

func newDecimal256MergeBatch(
	t *testing.T,
	mp *mpool.MPool,
	decimalTyp types.Type,
	keys []string,
	payloads []int32,
) *batch.Batch {
	t.Helper()
	require.Len(t, payloads, len(keys))

	bat := batch.NewWithSchema(false, []string{"id", "payload"}, []types.Type{decimalTyp, types.T_int32.ToType()})
	for i, key := range keys {
		value, err := types.ParseDecimal256(key, decimalTyp.Width, decimalTyp.Scale)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], payloads[i], false, mp))
	}
	bat.SetRowCount(len(keys))
	return bat
}
