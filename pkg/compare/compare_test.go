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

func makeTestCases(t *testing.T) []testCase {
	mp := mpool.MustNewZero()
	return []testCase{
		newTestCase(t, true, mp, types.New(types.T_bool, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_bool, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_bit, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_bit, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_int8, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int8, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_int16, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int16, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_int32, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int32, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_int64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_uint8, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint8, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_uint16, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint16, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_uint32, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint32, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_uint64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_float32, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_float32, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_float64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_float64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_date, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_date, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_time, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_time, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_datetime, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_datetime, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_timestamp, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_timestamp, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_decimal64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_decimal64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_decimal128, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_decimal128, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_varchar, types.MaxVarcharLen, 0)),
		newTestCase(t, false, mp, types.New(types.T_varchar, types.MaxVarcharLen, 0)),

		newTestCase(t, true, mp, types.New(types.T_blob, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_blob, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_text, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_text, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_datalink, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_datalink, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_geometry, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_geometry, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_array_float32, types.MaxArrayDimension, 0)),
		newTestCase(t, false, mp, types.New(types.T_array_float32, types.MaxArrayDimension, 0)),

		newTestCase(t, true, mp, types.New(types.T_array_float64, types.MaxArrayDimension, 0)),
		newTestCase(t, false, mp, types.New(types.T_array_float64, types.MaxArrayDimension, 0)),
	}
}

func TestCompare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
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

func TestCopyGrowsAccountedRowMetadata(t *testing.T) {
	testCases := []struct {
		name   string
		typ    types.Type
		append func(*vector.Vector, bool, *mpool.MPool) error
	}{
		{
			name: "fixed",
			typ:  types.T_int64.ToType(),
			append: func(vec *vector.Vector, isNull bool, mp *mpool.MPool) error {
				return vector.AppendFixed(vec, int64(7), isNull, mp)
			},
		},
		{
			name: "varchar",
			typ:  types.T_varchar.ToType(),
			append: func(vec *vector.Vector, isNull bool, mp *mpool.MPool) error {
				return vector.AppendBytes(vec, []byte("value"), isNull, mp)
			},
		},
		{
			name: "array",
			typ:  types.T_array_float32.ToType(),
			append: func(vec *vector.Vector, isNull bool, mp *mpool.MPool) error {
				return vector.AppendArray(vec, []float32{1, 2}, isNull, mp)
			},
		},
	}

	for _, tc := range testCases {
		for _, metadata := range []string{"null", "const-null", "grouping"} {
			t.Run(tc.name+"/"+metadata, func(t *testing.T) {
				mp := mpool.MustNewZero()
				proc := testutil.NewProcessWithMPool(t, "", mp)
				registry, err := mpool.NewAllocationAccountRegistry(1, 16)
				require.NoError(t, err)
				account, err := registry.Open(1 << 20)
				require.NoError(t, err)
				selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
				require.NoError(t, err)

				destination := vector.NewOffHeapVecWithType(tc.typ)
				require.NoError(t, destination.SetAllocationAccount(selection))
				require.NoError(t, tc.append(destination, false, mp))
				require.Zero(t, destination.GetNulls().GetBitmap().ExternalStorageCapacity())
				require.Zero(t, destination.GetGrouping().GetBitmap().ExternalStorageCapacity())

				var source *vector.Vector
				if metadata == "const-null" {
					source = vector.NewConstNull(tc.typ, 1, mp)
				} else {
					source = vector.NewVec(tc.typ)
					require.NoError(t, tc.append(source, metadata == "null", mp))
					if metadata == "grouping" {
						source.GetGrouping().Add(0)
					}
				}

				cmp := New(tc.typ, false, false)
				cmp.Set(0, source)
				cmp.Set(1, destination)
				require.NoError(t, cmp.Copy(0, 1, 0, 0, proc))

				require.Equal(t, metadata == "null" || metadata == "const-null", destination.GetNulls().Contains(0))
				require.Equal(t, metadata == "grouping", destination.GetGrouping().Contains(0))
				require.Positive(t, account.Snapshot().Used)

				source.Free(mp)
				destination.Free(mp)
				snapshot, first, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
				require.True(t, first)
				require.Zero(t, snapshot.Used)
				proc.Free()
				require.Zero(t, mp.CurrNB())
			})
		}
	}
}

func TestCopyAccountedNullAdmissionFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	destination := vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, destination.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixed(destination, int64(7), false, mp))
	used := account.Snapshot().Used

	source := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source, int64(0), true, mp))
	cmp := New(types.T_int64.ToType(), false, false)
	cmp.Set(0, source)
	cmp.Set(1, destination)

	err = cmp.Copy(0, 1, 0, 0, proc)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.False(t, destination.GetNulls().Contains(0))
	require.Equal(t, int64(7), vector.MustFixedColWithTypeCheck[int64](destination)[0])
	require.Equal(t, used, account.Snapshot().Used)

	source.Free(mp)
	destination.Free(mp)
	snapshot, first, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.True(t, first)
	require.Zero(t, snapshot.Used)
	proc.Free()
	require.Zero(t, mp.CurrNB())
}

func newTestCase(t *testing.T, desc bool, m *mpool.MPool, typ types.Type) testCase {
	vecs := make([]*vector.Vector, 2)
	vecs[0] = testutil.NewVector(Rows, typ, m, true, nil)
	vecs[1] = testutil.NewVector(Rows, typ, m, true, nil)
	return testCase{
		desc: desc,
		vecs: vecs,
		proc: testutil.NewProcessWithMPool(t, "", m),
	}
}

func TestBlockRowIdsCompare(t *testing.T) {
	obj := types.NewObjectid()
	t.Run("test blk id compare", func(t *testing.T) {
		var blks1 []types.Blockid
		var blks2 []types.Blockid
		for i := 0; i < 1000; i++ {
			blks1 = append(blks1, types.NewBlockidWithObjectID(&obj, uint16(i)))
			blks2 = append(blks2, types.NewBlockidWithObjectID(&obj, uint16(i)))
		}

		for range blks1 {
			x, y := rand.Int()%len(blks1), rand.Int()%len(blks1)
			blks1[x], blks1[y] = blks1[y], blks1[x]
		}

		slices.SortFunc(blks1, types.BlockidAscCompare)
		require.Equal(t, blks1, blks2)

		{
			slices.Reverse(blks2)
			slices.SortFunc(blks1, types.BlockidDescCompare)
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

			rowId := types.NewRowIDWithObjectIDBlkNumAndRowID(obj, blkIdx, uint32(i))
			rowIds1 = append(rowIds1, rowId)
			rowIds2 = append(rowIds2, rowId)
		}

		for range rowIds1 {
			x, y := rand.Int()%len(rowIds1), rand.Int()%len(rowIds1)
			rowIds1[x], rowIds1[y] = rowIds1[y], rowIds1[x]
		}

		slices.SortFunc(rowIds1, types.RowidAscCompare)
		require.Equal(t, rowIds1, rowIds2)

		{
			slices.Reverse(rowIds2)
			slices.SortFunc(rowIds1, types.RowidDescCompare)
			require.Equal(t, rowIds1, rowIds2)
		}

	})
}
