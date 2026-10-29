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
	"bytes"
	"math/rand"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
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

func TestNewOrderUsesPlanUnicodeTypeWhenVectorMetadataIsPhysical(t *testing.T) {
	mp := mpool.MustNewZero()
	physical := vector.NewVec(types.T_varchar.ToType())
	defer physical.Free(mp)
	require.NoError(t, vector.AppendStringList(physical, []string{"A", "a"}, nil, mp))
	semantic := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB4UnicodeCI)
	c := NewOrder(semantic, false, false)
	c.Set(0, physical)
	c.Set(1, physical)
	require.Equal(t, 0, c.Compare(0, 1, 0, 1))
}

func TestNewOrderJSONUsesSQLComparison(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_json.ToType())
	defer vec.Free(mp)
	for _, value := range []string{"0", "1", "false", "true", "1.0"} {
		jsonValue, err := bytejson.ParseFromString(value)
		require.NoError(t, err)
		encoded, err := jsonValue.Marshal()
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(vec, encoded, false, mp))
	}

	c := NewOrder(types.T_json.ToType(), false, false)
	c.Set(0, vec)
	c.Set(1, vec)
	require.Negative(t, c.Compare(0, 1, 0, 2), "SQL JSON numbers sort before booleans")
	require.Zero(t, c.Compare(0, 1, 1, 4), "SQL JSON numeric peers compare equal")
	require.Negative(t, c.Compare(0, 1, 2, 3), "false sorts before true")

	desc := NewOrder(types.T_json.ToType(), true, false)
	desc.Set(0, vec)
	desc.Set(1, vec)
	require.Positive(t, desc.Compare(0, 1, 0, 1), "DESC reverses SQL JSON value ordering")
	require.Zero(t, desc.Compare(0, 1, 1, 4), "DESC preserves SQL JSON numeric peers")

	physical := New(types.T_json.ToType(), false, false)
	physical.Set(0, vec)
	physical.Set(1, vec)
	require.Zero(t, physical.Compare(0, 1, 1, 4),
		"generic JSON identity treats SQL numeric peers as equal")
	require.Negative(t, physical.Compare(0, 1, 0, 2),
		"generic JSON identity keeps SQL value ordering (numbers before booleans)")
}

func TestNewJSONIdentityUsesSQLValueSemantics(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_json.ToType())
	defer vec.Free(mp)
	values := []string{"1", "1.0", "1e0", `{"a":1}`, `{"a":1.0}`, "[1]", "[1.0]", "2", "2.0"}
	rowOf := make(map[string]int64)
	for i, value := range values {
		jsonValue, err := bytejson.ParseFromString(value)
		require.NoError(t, err)
		encoded, err := jsonValue.Marshal()
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(vec, encoded, false, mp))
		rowOf[value] = int64(i)
	}
	require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
	nullRow := int64(len(values))

	c := New(types.T_json.ToType(), false, false)
	c.Set(0, vec)
	c.Set(1, vec)
	require.Zero(t, c.Compare(0, 1, rowOf["1"], rowOf["1.0"]),
		"JSON numbers 1 and 1.0 are one identity")
	require.Zero(t, c.Compare(0, 1, rowOf["1"], rowOf["1e0"]),
		"JSON numbers 1 and 1e0 are one identity")
	require.Zero(t, c.Compare(0, 1, rowOf[`{"a":1}`], rowOf[`{"a":1.0}`]),
		"object members compare by their own typed semantics")
	require.Zero(t, c.Compare(0, 1, rowOf["[1]"], rowOf["[1.0]"]),
		"array elements compare by their own typed semantics")
	require.NotZero(t, c.Compare(0, 1, rowOf["1"], rowOf["2"]),
		"different JSON values keep different identities")
	require.NotZero(t, c.Compare(0, 1, rowOf["[1]"], rowOf[`{"a":1}`]),
		"an array is never an object")
	require.Zero(t, c.Compare(0, 1, nullRow, nullRow),
		"NULL equals NULL in grouping identity")
	require.NotZero(t, c.Compare(0, 1, nullRow, rowOf["1"]),
		"NULL differs from a value")

	desc := New(types.T_json.ToType(), true, false)
	desc.Set(0, vec)
	desc.Set(1, vec)
	require.Zero(t, desc.Compare(0, 1, rowOf["1.0"], rowOf["1"]),
		"DESC keeps SQL-equal JSON values as peers")
}

func TestJSONOrderMetadataAndCopy(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	source := vector.NewVec(types.T_json.ToType())
	destination := vector.NewVec(types.T_json.ToType())
	value := vector.NewVec(types.T_json.ToType())
	constNull := vector.NewConstNull(types.T_json.ToType(), 1, mp)
	t.Cleanup(func() {
		source.Free(mp)
		destination.Free(mp)
		value.Free(mp)
		constNull.Free(mp)
		proc.Free()
		require.Zero(t, mp.CurrNB())
	})

	jsonValue, err := bytejson.ParseFromString(`{"a": 1}`)
	require.NoError(t, err)
	encoded, err := jsonValue.Marshal()
	require.NoError(t, err)
	zeroValue, err := bytejson.ParseFromString(`{"a": 0}`)
	require.NoError(t, err)
	encodedZero, err := zeroValue.Marshal()
	require.NoError(t, err)
	require.NoError(t, vector.AppendBytes(source, encoded, false, mp))
	require.NoError(t, vector.AppendBytes(destination, encodedZero, false, mp))
	source.GetGrouping().Add(0)

	cmp := NewOrder(types.T_json.ToType(), false, false)
	cmp.Set(0, source)
	cmp.Set(1, destination)
	require.Same(t, source, cmp.Vector())
	require.NoError(t, cmp.Copy(0, 1, 0, 0, proc))
	require.Equal(t, source.GetBytesAt(0), destination.GetBytesAt(0))
	require.True(t, destination.GetGrouping().Contains(0))

	require.NoError(t, vector.AppendBytes(value, encodedZero, false, mp))
	grouped := NewOrder(types.T_json.ToType(), false, false)
	grouped.Set(0, destination)
	grouped.Set(1, value)
	require.Negative(t, grouped.Compare(0, 1, 0, 0), "grouping sentinels compare as SQL NULL")

	nullsFirst := NewOrder(types.T_json.ToType(), false, false)
	nullsFirst.Set(0, constNull)
	nullsFirst.Set(1, value)
	require.Negative(t, nullsFirst.Compare(0, 1, 0, 0))

	nullsLast := NewOrder(types.T_json.ToType(), true, true)
	nullsLast.Set(0, value)
	nullsLast.Set(1, constNull)
	require.Negative(t, nullsLast.Compare(0, 1, 0, 0), "explicit NULLS LAST is independent of DESC")
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
				proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
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
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
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

func TestDomainlessNullCompare(t *testing.T) {
	for _, factory := range []struct {
		name string
		new  func(types.Type, bool, bool) Compare
	}{{"identity", New}, {"order", NewOrder}} {
		t.Run(factory.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			constant := vector.NewConstNull(types.T_any.ToType(), 3, mp)
			flat := vector.NewVec(types.T_any.ToType())
			flat.SetLength(3)
			nulls.AddRange(flat.GetNulls(), 0, 3)
			defer func() { constant.Free(mp); flat.Free(mp); require.Zero(t, mp.CurrNB()) }()
			require.NoError(t, constant.SetStringSource(types.StringSourceLiteral))
			for _, desc := range []bool{false, true} {
				c := factory.new(types.T_any.ToType(), desc, desc)
				require.NotNil(t, c)
				c.Set(0, constant)
				c.Set(1, flat)
				require.Same(t, constant, c.Vector())
				require.Zero(t, c.Compare(0, 1, 2, 1))
				require.Zero(t, c.Compare(1, 0, 1, 2))
				require.NoError(t, c.Copy(0, 1, 2, 1, proc))
				require.True(t, flat.IsNull(1))
				require.Equal(t, types.StringSourceLiteral, flat.GetStringSourceAt(1))
				// ANY has no non-NULL value domain; don't broaden unsupported values.
				nulls.Del(flat.GetNulls(), 0)
				require.True(t, moerr.IsMoErrCode(c.Copy(1, 0, 0, 0, proc), moerr.ErrNotSupported))
				func() {
					defer func() {
						e, ok := recover().(error)
						require.True(t, ok)
						require.True(t, moerr.IsMoErrCode(e, moerr.ErrNotSupported))
					}()
					c.Compare(1, 0, 0, 0)
				}()
				nulls.Add(flat.GetNulls(), 0)
			}
		})
	}
}

func TestJSONCompareCrossVectorConstNullAndTypeControls(t *testing.T) {
	mp := mpool.MustNewZero()

	appendJSON := func(v *vector.Vector, s string) {
		bj, err := bytejson.ParseFromString(s)
		require.NoError(t, err)
		enc, err := bj.Marshal()
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(v, enc, false, mp))
	}

	// cross-vector: equal JSON values in distinct vectors stay peers, and
	// desc inversion keeps them peers while flipping unequal pairs
	left := vector.NewVec(types.T_json.ToType())
	defer left.Free(mp)
	right := vector.NewVec(types.T_json.ToType())
	defer right.Free(mp)
	appendJSON(left, "1")
	appendJSON(left, "2")
	appendJSON(right, "1.0")
	appendJSON(right, "3")

	c := New(types.T_json.ToType(), false, false)
	c.Set(0, left)
	c.Set(1, right)
	require.Zero(t, c.Compare(0, 1, 0, 0), "cross-vector 1 equals 1.0")
	require.Negative(t, c.Compare(0, 1, 0, 1), "cross-vector 2 sorts before 3")
	desc := New(types.T_json.ToType(), true, false)
	desc.Set(0, left)
	desc.Set(1, right)
	require.Zero(t, desc.Compare(0, 1, 0, 0), "desc cross-vector keeps 1 and 1.0 peers")
	require.Positive(t, desc.Compare(0, 1, 0, 1), "desc cross-vector inverts 2 vs 3")

	// const-null side: never equal to a value, equal to another NULL, via the
	// same nullsCompare contract strCompare applies
	constNull := vector.NewConstNull(types.T_json.ToType(), 1, mp)
	defer constNull.Free(mp)
	cc := New(types.T_json.ToType(), false, false)
	cc.Set(0, constNull)
	cc.Set(1, left)
	require.NotZero(t, cc.Compare(0, 1, 0, 0), "const NULL differs from a value")
	cc2 := New(types.T_json.ToType(), false, false)
	cc2.Set(0, constNull)
	cc2.Set(1, constNull)
	require.Zero(t, cc2.Compare(0, 1, 0, 0), "const NULL equals const NULL")

	// reverse controls: ordinary types are untouched by the JSON change
	ivec := vector.NewVec(types.T_int64.ToType())
	defer ivec.Free(mp)
	require.NoError(t, vector.AppendFixedList(ivec, []int64{1, 2}, nil, mp))
	i := New(types.T_int64.ToType(), false, false)
	i.Set(0, ivec)
	i.Set(1, ivec)
	require.Negative(t, i.Compare(0, 1, 0, 1), "int64 1 sorts before 2")
	idi := New(types.T_int64.ToType(), true, false)
	idi.Set(0, ivec)
	idi.Set(1, ivec)
	require.Positive(t, idi.Compare(0, 1, 0, 1), "desc int64 inverts")

	svec := vector.NewVec(types.T_varchar.ToType())
	defer svec.Free(mp)
	require.NoError(t, vector.AppendStringList(svec, []string{"1", "1.0"}, nil, mp))
	s := New(types.T_varchar.ToType(), false, false)
	s.Set(0, svec)
	s.Set(1, svec)
	require.Zero(t, s.Compare(0, 1, 0, 0), "string self equality")
	require.NotZero(t, s.Compare(0, 1, 0, 1), "strings keep byte identity")
}

func BenchmarkJSONCompareValueIdentityCost(b *testing.B) {
	mp := mpool.MustNewZero()
	intVec := vector.NewVec(types.T_json.ToType())
	objVec := vector.NewVec(types.T_json.ToType())
	defer func() {
		intVec.Free(mp)
		objVec.Free(mp)
	}()
	appendJSON := func(v *vector.Vector, s string) {
		bj, err := bytejson.ParseFromString(s)
		if err != nil {
			b.Fatal(err)
		}
		enc, err := bj.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		if err := vector.AppendBytes(v, enc, false, mp); err != nil {
			b.Fatal(err)
		}
	}
	appendJSON(intVec, "1")
	appendJSON(intVec, "1.0")
	appendJSON(objVec, `{"a":1}`)
	appendJSON(objVec, `{"a":1.0}`)

	intBytes0, intBytes1 := intVec.GetBytesAt(0), intVec.GetBytesAt(1)
	objBytes0, objBytes1 := objVec.GetBytesAt(0), objVec.GetBytesAt(1)

	cInt := New(types.T_json.ToType(), false, false)
	cInt.Set(0, intVec)
	cInt.Set(1, intVec)
	cObj := New(types.T_json.ToType(), false, false)
	cObj.Set(0, objVec)
	cObj.Set(1, objVec)

	b.Run("ValueIdentity-NumberPair", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = cInt.Compare(0, 1, 0, 1)
		}
	})
	b.Run("ValueIdentity-ObjectPair", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = cObj.Compare(0, 1, 0, 1)
		}
	})
	b.Run("ByteBaseline-NumberPair", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = bytes.Compare(intBytes0, intBytes1)
		}
	})
	b.Run("ByteBaseline-ObjectPair", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = bytes.Compare(objBytes0, objBytes1)
		}
	})
}
