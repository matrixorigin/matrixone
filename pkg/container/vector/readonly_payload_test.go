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

package vector

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestReadonlyRawAppendSources(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	t.Cleanup(mpool.DisableDebugPoisonOnFree)
	operations := []struct {
		name string
		run  func(*Vector, []byte, *mpool.MPool) error
	}{
		{"bytes", func(v *Vector, value []byte, mp *mpool.MPool) error { return AppendBytes(v, value, false, mp) }},
		{"source", func(v *Vector, value []byte, mp *mpool.MPool) error {
			return AppendBytesWithStringSource(v, value, false, types.StringSourceSQLPrepare, mp)
		}},
		{"multi", func(v *Vector, value []byte, mp *mpool.MPool) error { return AppendMultiBytes(v, value, false, 2, mp) }},
		{"bytes-list", func(v *Vector, value []byte, mp *mpool.MPool) error {
			return AppendBytesList(v, [][]byte{value}, nil, mp)
		}},
		{"strings-list", func(v *Vector, value []byte, mp *mpool.MPool) error {
			return AppendStringList(v, []string{util.UnsafeBytesToString(value)}, nil, mp)
		}},
		{"writer", func(v *Vector, value []byte, mp *mpool.MPool) error {
			return AppendBytesWithWriter(v, len(value), mp, func(dst []byte) error { copy(dst, value); return nil })
		}},
	}
	for _, operation := range operations {
		for _, leased := range []bool{false, true} {
			for row := range 2 {
				t.Run(operation.name+"/leased="+strconv.FormatBool(leased)+"/row="+strconv.Itoa(row), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
					values := [][]byte{[]byte("aaaaaa"), bytes.Repeat([]byte{'b'}, 64), bytes.Repeat([]byte{'c'}, 64)}
					_, destination := readonlyAliasVectors(t, mp, values, leased, nil)
					require.NoError(t, operation.run(destination, destination.GetBytesAt(row), mp))
					count := 1
					if operation.name == "multi" {
						count = 2
					}
					require.Equal(t, 3+count, destination.Length())
					for i := range 3 {
						require.Equal(t, values[i], destination.GetBytesAt(i))
					}
					for i := 3; i < destination.Length(); i++ {
						require.Equal(t, values[row], destination.GetBytesAt(i))
					}
					if operation.name == "source" {
						require.Equal(t, types.StringSourceSQLPrepare, destination.GetStringSourceAt(3))
					}
				})
			}
		}
	}
}

func readonlyPayloadVector(t *testing.T, typ types.Type, leased bool, fill func(*Vector, *mpool.MPool) error) (*mpool.MPool, *Vector) {
	t.Helper()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	source := NewOffHeapVecWithType(typ)
	t.Cleanup(func() { source.Free(mp) })
	for range 3 {
		require.NoError(t, fill(source, mp))
	}
	destination := NewOffHeapVecWithType(typ)
	t.Cleanup(func() { destination.Free(mp) })
	if leased {
		require.NoError(t, destination.CopyWithReadonlyArea(source, mp))
	} else {
		require.NoError(t, destination.UnionBatch(source, 0, 3, nil, mp))
	}
	return mp, destination
}

func TestReadonlyJSONAndArraySources(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	t.Cleanup(mpool.DisableDebugPoisonOnFree)
	for _, leased := range []bool{false, true} {
		for _, size := range []int{2, 16} {
			for _, operation := range []string{"json", "json-const", "encoded", "encoded-const", "array", "array-const", "array-list"} {
				t.Run(operation+"/leased="+strconv.FormatBool(leased)+"/size="+strconv.Itoa(size), func(t *testing.T) {
					if operation == "array" || operation == "array-const" || operation == "array-list" {
						values := make([]float32, size)
						for i := range values {
							values[i] = float32(i + 1)
						}
						mp, destination := readonlyPayloadVector(t, types.T_array_float32.ToType(), leased,
							func(v *Vector, mp *mpool.MPool) error { return AppendArray(v, values, false, mp) })
						input := GetArrayAt[float32](destination, 0)
						row := 3
						var err error
						switch operation {
						case "array":
							err = AppendArray(destination, input, false, mp)
						case "array-const":
							err = SetConstArray(destination, input, 3, mp)
							row = 0
						case "array-list":
							err = AppendArrayList(destination, [][]float32{input}, nil, mp)
						}
						require.NoError(t, err)
						require.Equal(t, values, GetArrayAt[float32](destination, row))
						return
					}
					value, err := bytejson.CreateByteJSON(string(bytes.Repeat([]byte{'j'}, size*4)))
					require.NoError(t, err)
					want, err := value.Marshal()
					require.NoError(t, err)
					mp, destination := readonlyPayloadVector(t, types.T_json.ToType(), leased,
						func(v *Vector, mp *mpool.MPool) error { return AppendByteJson(v, value, false, mp) })
					var input bytejson.ByteJson
					require.NoError(t, input.Unmarshal(destination.GetBytesAt(0)))
					row := 3
					switch operation {
					case "json":
						err = AppendByteJson(destination, input, false, mp)
					case "json-const":
						err = SetConstByteJson(destination, input, 3, mp)
						row = 0
					case "encoded":
						err = AppendByteJsonEncoded(destination, testByteJsonEncoder{value: input}, mp)
					case "encoded-const":
						err = SetConstByteJsonEncoded(destination, testByteJsonEncoder{value: input}, 3, mp)
						row = 0
					}
					require.NoError(t, err)
					require.Equal(t, want, destination.GetBytesAt(row))
				})
			}
		}
	}
}

func TestReadonlyWriterAdmissionAndFailure(t *testing.T) {
	for _, failAfter := range []int{1, 2, 3, 4, 0} {
		t.Run(strconv.Itoa(failAfter), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			controller := &readonlyWriteController{}
			state := readonlyWriteAccount(t, controller)
			values := [][]byte{[]byte("aaaaaa"), bytes.Repeat([]byte{'b'}, 64), bytes.Repeat([]byte{'c'}, 64)}
			_, destination := readonlyAliasVectors(t, mp, values, true, state.selection)
			beforeMP, beforeAccount := mp.CurrNB(), state.account.Snapshot().Used
			if failAfter > 0 {
				controller.failAt = controller.calls + failAfter
			}
			called := false
			err := AppendBytesWithWriter(destination, 24, mp, func(dst []byte) error {
				called = true
				controller.failAt = controller.calls + 1
				copy(dst, bytes.Repeat([]byte{'x'}, 24))
				return nil
			})
			if failAfter > 0 {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.False(t, called, "admit all storage before user code")
				require.Equal(t, beforeMP, mp.CurrNB())
				require.Equal(t, beforeAccount, state.account.Snapshot().Used)
				require.True(t, destination.HasBorrowedBacking())
				require.Equal(t, 3, destination.Length())
			} else {
				require.NoError(t, err)
				require.True(t, called)
				require.False(t, controller.rejected, "no allocation after writer starts")
				require.Equal(t, bytes.Repeat([]byte{'x'}, 24), destination.GetBytesAt(3))
			}
			for i, value := range values {
				require.Equal(t, value, destination.GetBytesAt(i))
			}
		})
	}
	t.Run("writer-error", func(t *testing.T) {
		mp := mpool.MustNewZero()
		t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
		values := [][]byte{[]byte("aaaaaa"), bytes.Repeat([]byte{'b'}, 64)}
		_, destination := readonlyAliasVectors(t, mp, values, true, nil)
		before := mp.CurrNB()
		sentinel := errors.New("writer failure")
		require.ErrorIs(t, AppendBytesWithWriter(destination, 24, mp, func(dst []byte) error { dst[0] = 'x'; return sentinel }), sentinel)
		require.Equal(t, before, mp.CurrNB())
		require.True(t, destination.HasBorrowedBacking())
		for i, value := range values {
			require.Equal(t, value, destination.GetBytesAt(i))
		}
	})
}

func TestReadonlyBuildersRejectUnadmittedWrites(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	values := [][]byte{bytes.Repeat([]byte{'a'}, 64)}
	_, destination := readonlyAliasVectors(t, mp, values, true, nil)
	payload := bytes.Repeat([]byte{'x'}, 24)
	json, err := bytejson.CreateByteJSON(string(payload))
	require.NoError(t, err)
	array := []float32{1, 2, 3, 4, 5, 6}
	source := types.Varlena{}
	source.SetOffsetLen(0, 24)
	for _, build := range []struct {
		name string
		run  func(*types.Varlena) error
	}{
		{"bytes", func(out *types.Varlena) error { return BuildVarlenaFromByteSlice(destination, out, &payload, mp) }},
		{"no-inline", func(out *types.Varlena) error { return BuildVarlenaNoInline(destination, out, &payload, mp) }},
		{"json", func(out *types.Varlena) error { return BuildVarlenaFromByteJson(destination, out, json, mp) }},
		{"json-no-inline", func(out *types.Varlena) error { return BuildVarlenaNoInlineFromByteJson(destination, out, json, mp) }},
		{"encoded", func(out *types.Varlena) error {
			return BuildVarlenaFromByteJsonEncoded(destination, out, testByteJsonEncoder{value: json}, mp)
		}},
		{"array", func(out *types.Varlena) error { return BuildVarlenaFromArray(destination, out, &array, mp) }},
		{"varlena", func(out *types.Varlena) error {
			return BuildVarlenaFromVarlena(destination, out, &source, &payload, mp)
		}},
	} {
		t.Run(build.name, func(t *testing.T) {
			before := mp.CurrNB()
			out := &MustFixedColNoTypeCheck[types.Varlena](destination)[0]
			require.Error(t, build.run(out))
			require.Equal(t, before, mp.CurrNB())
			require.Equal(t, values[0], destination.GetBytesAt(0))
		})
	}
}

func TestReadonlySetterAndWriterMetadata(t *testing.T) {
	for _, operation := range []string{"const", "unset", "writer"} {
		t.Run(operation, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			values := [][]byte{[]byte("aaaaaa"), bytes.Repeat([]byte{'b'}, 64), nil}
			_, destination := readonlyAliasVectors(t, mp, values, true, nil)
			destination.GetGrouping().Add(1)
			sources := []types.StringSource{types.StringSourceLiteral, types.StringSourceUserVariable, types.StringSourceSQLPrepare}
			require.NoError(t, destination.SetStringSourcesWithMP(sources, mp))
			snapshot, err := destination.RetainedReadonlyViewWithMP(mp)
			require.NoError(t, err)
			t.Cleanup(func() { snapshot.Free(mp) })
			input, err := destination.Window(1, 2)
			require.NoError(t, err)
			t.Cleanup(func() { input.Free(mp) })
			switch operation {
			case "const":
				require.NoError(t, GetConstSetFunction(*destination.GetType(), mp)(destination, input, 0, 3))
				for row := range 3 {
					require.Equal(t, values[1], destination.GetBytesAt(row))
					require.Equal(t, sources[1], destination.GetStringSourceAt(row))
					require.True(t, destination.GetGrouping().Contains(uint64(row)))
					require.False(t, destination.IsNull(uint64(row)))
				}
			case "unset":
				require.NoError(t, destination.SetRawBytesAtFromAndUnsetNull(2, input, 0, mp))
				require.Equal(t, values[1], destination.GetBytesAt(2))
				require.Equal(t, sources[1], destination.GetStringSourceAt(2))
				require.False(t, destination.IsNull(2))
				require.True(t, destination.GetGrouping().Contains(1))
			case "writer":
				value := input.GetBytesAt(0)
				require.NoError(t, AppendBytesWithWriter(destination, len(value), mp, func(dst []byte) error { copy(dst, value); return nil }))
				require.Equal(t, values[1], destination.GetBytesAt(3))
				require.Equal(t, types.StringSourceExpression, destination.GetStringSourceAt(3))
				require.True(t, destination.IsNull(2))
				require.True(t, destination.GetGrouping().Contains(1))
				for row, source := range sources {
					require.Equal(t, source, destination.GetStringSourceAt(row))
				}
			}
			for row, source := range sources {
				require.Equal(t, source, snapshot.GetStringSourceAt(row))
				require.Equal(t, row == 2, snapshot.IsNull(uint64(row)))
				require.Equal(t, row == 1, snapshot.GetGrouping().Contains(uint64(row)))
				if row != 2 {
					require.Equal(t, values[row], snapshot.GetBytesAt(row))
				}
			}
		})
	}
}

func TestReadonlyFixedRawSetter(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	t.Cleanup(mpool.DisableDebugPoisonOnFree)
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	data, err := mp.Alloc(24, true)
	require.NoError(t, err)
	copy(data, types.EncodeSlice([]int64{11, 22, 33}))
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { mp.Free(data) })
	if err != nil {
		mp.Free(data)
	}
	require.NoError(t, err)
	destination, err := NewBorrowedFixedVector(types.T_int64.ToType(), 3, data, lease)
	lease.Release()
	require.NoError(t, err)
	t.Cleanup(func() { destination.Free(mp) })
	require.NoError(t, destination.SetRawBytesAt(1, destination.GetRawBytesAt(0), mp))
	require.Equal(t, []int64{11, 11, 33}, MustFixedColNoTypeCheck[int64](destination))
	require.False(t, destination.HasBorrowedBacking())
}

func TestReadonlyFixedListSourceGrowth(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	t.Cleanup(mpool.DisableDebugPoisonOnFree)
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	destination := NewOffHeapVecWithType(types.T_int64.ToType())
	t.Cleanup(func() { destination.Free(mp) })
	values := []int64{11, 22, 33, 44}
	require.NoError(t, AppendFixedList(destination, values, nil, mp))
	for destination.Length() < destination.Capacity() {
		value := int64(destination.Length() + 1)
		values = append(values, value)
		require.NoError(t, AppendFixed(destination, value, false, mp))
	}
	require.NoError(t, AppendFixedList(destination, MustFixedColNoTypeCheck[int64](destination), nil, mp))
	require.Equal(t, append(values, values...), MustFixedColNoTypeCheck[int64](destination))
}
