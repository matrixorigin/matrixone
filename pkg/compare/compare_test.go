// Copyright 2021 - 2022 Matrix Origin
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

package compare

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 100
)

type testCase struct {
	desc bool
	proc *process.Process
	vecs []*vector.Vector
}

var (
	tcs []testCase
)

func init() {
	mp := mpool.MustNewZero()
	tcs = []testCase{
		newTestCase(true, mp, types.New(types.T_bool, 0, 0)),
		newTestCase(false, mp, types.New(types.T_bool, 0, 0)),

		newTestCase(true, mp, types.New(types.T_bit, 0, 0)),
		newTestCase(false, mp, types.New(types.T_bit, 0, 0)),

		newTestCase(true, mp, types.New(types.T_int8, 0, 0)),
		newTestCase(false, mp, types.New(types.T_int8, 0, 0)),
		newTestCase(true, mp, types.New(types.T_int16, 0, 0)),
		newTestCase(false, mp, types.New(types.T_int16, 0, 0)),
		newTestCase(true, mp, types.New(types.T_int32, 0, 0)),
		newTestCase(false, mp, types.New(types.T_int32, 0, 0)),
		newTestCase(true, mp, types.New(types.T_int64, 0, 0)),
		newTestCase(false, mp, types.New(types.T_int64, 0, 0)),

		newTestCase(true, mp, types.New(types.T_uint8, 0, 0)),
		newTestCase(false, mp, types.New(types.T_uint8, 0, 0)),
		newTestCase(true, mp, types.New(types.T_uint16, 0, 0)),
		newTestCase(false, mp, types.New(types.T_uint16, 0, 0)),
		newTestCase(true, mp, types.New(types.T_uint32, 0, 0)),
		newTestCase(false, mp, types.New(types.T_uint32, 0, 0)),
		newTestCase(true, mp, types.New(types.T_uint64, 0, 0)),
		newTestCase(false, mp, types.New(types.T_uint64, 0, 0)),

		newTestCase(true, mp, types.New(types.T_float32, 0, 0)),
		newTestCase(false, mp, types.New(types.T_float32, 0, 0)),

		newTestCase(true, mp, types.New(types.T_float64, 0, 0)),
		newTestCase(false, mp, types.New(types.T_float64, 0, 0)),

		newTestCase(true, mp, types.New(types.T_date, 0, 0)),
		newTestCase(false, mp, types.New(types.T_date, 0, 0)),

		newTestCase(true, mp, types.New(types.T_time, 0, 0)),
		newTestCase(false, mp, types.New(types.T_time, 0, 0)),

		newTestCase(true, mp, types.New(types.T_datetime, 0, 0)),
		newTestCase(false, mp, types.New(types.T_datetime, 0, 0)),

		newTestCase(true, mp, types.New(types.T_timestamp, 0, 0)),
		newTestCase(false, mp, types.New(types.T_timestamp, 0, 0)),

		newTestCase(true, mp, types.New(types.T_decimal64, 0, 0)),
		newTestCase(false, mp, types.New(types.T_decimal64, 0, 0)),

		newTestCase(true, mp, types.New(types.T_decimal128, 0, 0)),
		newTestCase(false, mp, types.New(types.T_decimal128, 0, 0)),

		newTestCase(true, mp, types.New(types.T_varchar, types.MaxVarcharLen, 0)),
		newTestCase(false, mp, types.New(types.T_varchar, types.MaxVarcharLen, 0)),

		newTestCase(true, mp, types.New(types.T_blob, 0, 0)),
		newTestCase(false, mp, types.New(types.T_blob, 0, 0)),

		newTestCase(true, mp, types.New(types.T_text, 0, 0)),
		newTestCase(false, mp, types.New(types.T_text, 0, 0)),
		newTestCase(true, mp, types.New(types.T_datalink, 0, 0)),
		newTestCase(false, mp, types.New(types.T_datalink, 0, 0)),

		newTestCase(true, mp, types.New(types.T_array_float32, types.MaxArrayDimension, 0)),
		newTestCase(false, mp, types.New(types.T_array_float32, types.MaxArrayDimension, 0)),

		newTestCase(true, mp, types.New(types.T_array_float64, types.MaxArrayDimension, 0)),
		newTestCase(false, mp, types.New(types.T_array_float64, types.MaxArrayDimension, 0)),
	}
}

func TestCompare(t *testing.T) {
	for _, tc := range tcs {
		nb0 := tc.proc.Mp().CurrNB()
		c := New(*tc.vecs[0].GetType(), tc.desc, false)
		c.Set(0, tc.vecs[0])
		c.Set(1, tc.vecs[1])
		err := c.Copy(0, 1, 0, 0, tc.proc)
		require.NoError(t, err)
		c.Compare(0, 1, 0, 0)
		nb1 := tc.proc.Mp().CurrNB()
		require.Equal(t, nb0, nb1)
		// XXX MPOOL
		// tv.vecs[0].Free modifies tc.proc.Mp()
		tc.vecs[0].Free(tc.proc.Mp())
		tc.vecs[1].Free(tc.proc.Mp())
	}
}

func newTestCase(desc bool, m *mpool.MPool, typ types.Type) testCase {
	vecs := make([]*vector.Vector, 2)
	vecs[0] = testutil.NewVector(Rows, typ, m, true, nil)
	vecs[1] = testutil.NewVector(Rows, typ, m, true, nil)
	return testCase{
		desc: desc,
		vecs: vecs,
		proc: testutil.NewProcessWithMPool("", m),
	}
}

func TestBlockRowIdsCompare(t *testing.T) {
	obj := types.NewObjectid()
	t.Run("test blk id compare", func(t *testing.T) {
		var blks1 []types.Blockid
		var blks2 []types.Blockid
		for i := 0; i < 1000; i++ {
			blks1 = append(blks1, *types.NewBlockidWithObjectID(obj, uint16(i)))
			blks2 = append(blks2, *types.NewBlockidWithObjectID(obj, uint16(i)))
		}

		for range blks1 {
			x, y := rand.Int()%len(blks1), rand.Int()%len(blks1)
			blks1[x], blks1[y] = blks1[y], blks1[x]
		}

		slices.SortFunc(blks1, blockidAscCompare)
		require.Equal(t, blks1, blks2)

		{
			slices.Reverse(blks2)
			slices.SortFunc(blks1, blockidDescCompare)
			require.Equal(t, blks1, blks2)
		}
	})

	t.Run("test row id compare", func(t *testing.T) {
		blkIdx := uint16(0)
		var rowIds1 []types.Rowid
		var rowIds2 []types.Rowid
		for i := 0; i < 1000; i++ {
			if i%10 == 0 {
				blkIdx++
			}

			rowId := types.NewRowIDWithObjectIDBlkNumAndRowID(*obj, blkIdx, uint32(i))
			rowIds1 = append(rowIds1, rowId)
			rowIds2 = append(rowIds2, rowId)
		}

		for range rowIds1 {
			x, y := rand.Int()%len(rowIds1), rand.Int()%len(rowIds1)
			rowIds1[x], rowIds1[y] = rowIds1[y], rowIds1[x]
		}

		slices.SortFunc(rowIds1, rowidAscCompare)
		require.Equal(t, rowIds1, rowIds2)

		{
			slices.Reverse(rowIds2)
			slices.SortFunc(rowIds1, rowidDescCompare)
			require.Equal(t, rowIds1, rowIds2)
		}

	})
}
