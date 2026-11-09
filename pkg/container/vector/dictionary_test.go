// Copyright 2026 Matrix Origin
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

package vector

import (
	"bytes"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDictionaryReadComposeAndFlatten(t *testing.T) {
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(t, 1<<20, 128)
	t.Cleanup(func() {
		finalizeTestVectorAllocationAccount(t, state)
		require.Zero(t, mp.CurrNB())
	})
	source := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	t.Cleanup(func() { source.Free(mp) })
	for i, value := range []int64{10, 20, 30, 40} {
		require.NoError(t, AppendFixed(source, value, i == 1, mp))
	}
	require.NoError(t, source.PreExtendGrouping(4, mp))
	source.gsp.Add(3)
	source.sorted = true
	physical := &source.data[0]
	base, err := FreezeVector(source, mp, state.selection)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	require.Same(t, physical, &base.vec.data[0], "unique payload promotion must not copy")
	indices := []int64{3, 0, 3, 1}
	rows, err := NewRowSelection(indices, 4, mp, state.selection)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	indices[0] = 2
	dict, err := NewDictionaryVector(base, rows, mp, state.selection)
	require.NoError(t, err)
	t.Cleanup(func() { dict.Free(mp) })
	require.True(t, dict.IsDictionary())
	require.Equal(t, BorrowedLease, dict.DataBackingKind())
	require.False(t, dict.CanDetach(BackingData))
	require.False(t, dict.IsConst())
	require.False(t, dict.GetSorted())
	require.Equal(t, int64(40), GetFixedAtWithTypeCheck[int64](dict, 0))
	require.Equal(t, int64(10), GetFixedAtNoTypeCheck[int64](dict, 1))
	require.True(t, dict.IsNull(3))
	require.True(t, dict.gsp.Contains(0))
	require.True(t, dict.gsp.Contains(2))
	require.Panics(t, func() { _ = MustFixedColNoTypeCheck[int64](dict) })
	require.Panics(t, func() { _ = dict.GetData() })
	require.Panics(t, func() { dict.SetNulls(nil) }, "clearing validity requires writable payload")
	_, err = dict.MarshalBinary()
	require.Error(t, err, "dictionary class must never reach the old wire format")
	var legacy bytes.Buffer
	require.NoError(t, legacy.WriteByte(7))
	require.Error(t, dict.MarshalBinaryWithBufferV1(&legacy))
	require.Equal(t, []byte{7}, legacy.Bytes(), "reject before publishing any wire bytes")
	require.Error(t, dict.MarshalSelectedRowsTo(&legacy, []int32{0}))
	require.Equal(t, []byte{7}, legacy.Bytes())
	_, err = VectorToProtoVector(dict)
	require.Error(t, err)
	view := ResolveFixedReadView[int64](dict)
	require.Equal(t, []int64{3, 0, 3, 1}, view.Selection)
	require.Equal(t, int64(40), view.Values[view.Selection[0]])
	require.True(t, view.Nulls.Contains(3))
	require.NoError(t, source.EnsureWritable(mp))
	require.NoError(t, SetFixedAtNoTypeCheck(source, 3, int64(99)))
	source.gsp.Clear()
	require.Equal(t, int64(40), GetFixedAtNoTypeCheck[int64](dict, 0))
	require.True(t, dict.gsp.Contains(0))
	secondRows, err := NewRowSelection([]int64{2, 1, 3, 2}, 4, mp, state.selection)
	require.NoError(t, err)
	t.Cleanup(secondRows.Release)
	second, err := dict.SelectDictionary(secondRows, mp, state.selection)
	require.NoError(t, err)
	t.Cleanup(func() { second.Free(mp) })
	require.Same(t, base, second.dictionaryState().base)
	require.Equal(t, []int64{3, 0, 1, 3}, second.dictionaryState().rows.storageFixed())
	dict.Free(mp)
	source.ResetWithSameType()
	require.Equal(t, int64(40), GetFixedAtNoTypeCheck[int64](second, 3))
	require.NoError(t, second.EnsureFlat(mp))
	require.False(t, second.IsDictionary())
	flat := MustFixedColNoTypeCheck[int64](second)
	require.Equal(t, int64(40), flat[0])
	require.Equal(t, int64(10), flat[1])
	require.Equal(t, int64(40), flat[3])
	require.True(t, second.IsNull(2))
	require.True(t, second.gsp.Contains(3))
	require.NoError(t, SetFixedAtNoTypeCheck(second, 0, int64(123)))
	wire, err := second.MarshalBinary()
	require.NoError(t, err)
	decoded := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { decoded.Free(mp) })
	require.NoError(t, decoded.UnmarshalBinary(wire))
	require.Equal(t, int64(123), GetFixedAtNoTypeCheck[int64](decoded, 0))
	wire[0] = DICTIONARY
	require.NoError(t, decoded.UnmarshalBinary(wire), "legacy class 2 carries materialized row buffers")
	require.False(t, decoded.IsDictionary())
	require.Equal(t, int64(123), GetFixedAtNoTypeCheck[int64](decoded, 0))
}

func TestLegacyClassDecodeNormalization(t *testing.T) {
	type encodedClass struct {
		name    string
		tag     byte
		invalid bool
	}
	for _, kind := range []string{"fixed", "varlen", "json", "constant", "constant_null"} {
		t.Run(kind, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB())
			})
			var source *Vector
			var err error
			t.Cleanup(func() {
				if source != nil {
					source.Free(mp)
				}
			})
			switch kind {
			case "constant":
				source, err = NewConstFixed(types.T_int64.ToType(), int64(11), 3, mp)
				require.NoError(t, err)
			case "constant_null":
				source = NewConstNull(types.T_int64.ToType(), 3, mp)
			case "fixed":
				source = NewVec(types.T_int64.ToType())
				for row, value := range []int64{11, 22, 33} {
					require.NoError(t, AppendFixed(source, value, row == 1, mp))
				}
			default:
				typ := types.T_varchar.ToType()
				values := [][]byte{[]byte("short"), nil, []byte("a value longer than the inline string representation")}
				if kind == "json" {
					typ = types.T_json.ToType()
					values = [][]byte{jsonAdmissionValue(t, `[1]`), nil, jsonAdmissionValue(t, `{"value":"longer than inline storage"}`)}
				}
				source = NewVec(typ)
				for row, value := range values {
					require.NoError(t, AppendBytes(source, value, row == 1, mp))
				}
			}
			wire, err := source.MarshalBinary()
			require.NoError(t, err)
			var legacy bytes.Buffer
			require.NoError(t, source.MarshalBinaryWithBufferV1(&legacy))
			classes := []encodedClass{{name: "original", tag: byte(source.class)}}
			if !source.IsConst() {
				classes = append(classes, encodedClass{name: "legacy", tag: DICTIONARY})
			}
			if kind == "fixed" {
				classes = append(classes, encodedClass{name: "unknown", tag: DICTIONARY + 1, invalid: true})
			}
			for _, class := range classes {
				t.Run(class.name, func(t *testing.T) {
					for _, mode := range []string{"binary", "trusted", "copy", "legacy_v1", "reader"} {
						t.Run(mode, func(t *testing.T) {
							encoded := bytes.Clone(wire)
							if mode == "legacy_v1" {
								encoded = bytes.Clone(legacy.Bytes())
							}
							encoded[0] = class.tag
							before := bytes.Clone(encoded)
							target := NewVec(types.T_any.ToType())
							t.Cleanup(func() { target.Free(mp) })
							var decodeErr error
							switch mode {
							case "binary":
								decodeErr = target.UnmarshalBinary(encoded)
							case "trusted":
								if !class.invalid {
									checked := NewVec(types.T_any.ToType())
									t.Cleanup(func() { checked.Free(mp) })
									require.NoError(t, checked.UnmarshalBinary(encoded))
								}
								decodeErr = target.UnmarshalBinaryTrusted(encoded)
							case "copy":
								decodeErr = target.UnmarshalBinaryWithCopy(encoded, mp)
							case "legacy_v1":
								decodeErr = target.UnmarshalBinaryV1(encoded)
							case "reader":
								decodeErr = target.UnmarshalWithReader(bytes.NewReader(encoded), mp)
							}
							require.Equal(t, before, encoded, "decoding must not rewrite shared input bytes")
							require.False(t, target.IsDictionary(), "row-buffer codecs cannot construct a dictionary backing")
							if class.invalid {
								require.Error(t, decodeErr)
								return
							}
							require.NoError(t, decodeErr)
							require.Equal(t, source.class, target.class)
							require.Equal(t, *source.GetType(), *target.GetType())
							require.Equal(t, source.Length(), target.Length())
							for row := 0; row < source.Length(); row++ {
								require.Equal(t, source.IsNull(uint64(row)), target.IsNull(uint64(row)))
								if !source.IsNull(uint64(row)) {
									if source.GetType().IsVarlen() {
										require.Equal(t, source.GetBytesAt(row), target.GetBytesAt(row))
									} else {
										require.Equal(t, source.GetRawBytesAt(row), target.GetRawBytesAt(row))
									}
								}
							}
						})
					}
				})
			}
		})
	}
}

func TestDictionaryStringsAndProvenance(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { source.Free(mp) })
	values := []string{"short", "a string longer than the inline payload", "null"}
	for row, value := range values {
		require.NoError(t, AppendBytes(source, []byte(value), row == 2, mp))
	}
	require.NoError(t, source.SetPrepareParamKindsWithMP([]PrepareParamKind{PrepareParamInteger, PrepareParamDecimal, PrepareParamNone}, mp))
	require.NoError(t, source.SetStringSourcesWithMP([]types.StringSource{types.StringSourceCOMStmt, types.StringSourceSQLPrepare, types.StringSourceLiteral}, mp))
	require.NoError(t, source.SetIsBinaryStringAt(1, true, mp))
	source.SetPrepareParamType(types.T_decimal128)
	source.SetPreparedJSONComparisonParam()
	base, err := FreezeVector(source, mp, nil)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	rows, err := NewRowSelection([]int64{1, 0, 1, 2}, 3, mp, nil)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	dict, err := NewDictionaryVector(base, rows, mp, nil)
	require.NoError(t, err)
	t.Cleanup(func() { dict.Free(mp) })
	require.Equal(t, []PrepareParamKind{PrepareParamDecimal, PrepareParamInteger, PrepareParamDecimal, PrepareParamNone}, dict.GetPrepareParamKinds())
	require.Equal(t, types.StringSourceLiteral, dict.GetStringSourceAt(3), "NULL retains its source owner")
	require.True(t, dict.GetIsBinaryStringAt(0))
	require.False(t, dict.GetIsBinaryStringAt(1))
	require.Equal(t, types.T_decimal128, dict.GetPrepareParamType())
	require.True(t, dict.IsPreparedJSONComparisonParam())
	view := ResolveVarlenaReadView(dict)
	require.Equal(t, values[1], string(view.Values[view.Selection[0]].GetByteSlice(view.Area)))
	require.NoError(t, source.EnsureWritable(mp))
	require.Equal(t, types.T_decimal128, source.GetPrepareParamType())
	require.True(t, source.IsPreparedJSONComparisonParam())
	source.Free(mp)
	require.Equal(t, values[1], dict.GetStringAt(2))
	require.NoError(t, dict.EnsureWritable(mp))
	require.Equal(t, values[1], dict.GetStringAt(0))
	require.Equal(t, values[0], dict.GetStringAt(1))
	require.Equal(t, values[1], dict.GetStringAt(2))
	require.True(t, dict.IsNull(3))
	require.True(t, dict.GetIsBinaryStringAt(2))
	require.Equal(t, types.StringSourceCOMStmt, dict.GetStringSourceAt(1))
	require.Equal(t, PrepareParamDecimal, dict.GetPrepareParamKindAt(0))
	require.Equal(t, types.T_decimal128, dict.GetPrepareParamType())
	require.True(t, dict.IsPreparedJSONComparisonParam())
	require.Equal(t, len(values[1])*2, len(dict.GetArea()), "only selected non-inline payload is copied")
}

func TestDictionaryLegacyAliasCopiesPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendFixedList(source, []int64{11, 22}, nil, mp))
	alias, err := source.Window(0, 2)
	require.NoError(t, err)
	t.Cleanup(func() { alias.Free(mp) })
	base, err := FreezeVector(alias, mp, nil)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	require.NotSame(t, &source.data[0], &base.vec.data[0])
	require.NoError(t, SetFixedAtNoTypeCheck(source, 0, int64(99)))
	source.Free(mp)
	require.Equal(t, int64(11), GetFixedAtNoTypeCheck[int64](base.vec, 0))
}

func TestDictionaryConstantAndEmpty(t *testing.T) {
	for _, isNull := range []bool{false, true} {
		t.Run(map[bool]string{false: "broadcast", true: "NULL"}[isNull], func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			source, err := NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
			require.NoError(t, err)
			if isNull {
				source.Free(mp)
				source = NewConstNull(types.T_int64.ToType(), 1, mp)
			}
			t.Cleanup(func() { source.Free(mp) })
			base, err := FreezeVector(source, mp, nil)
			require.NoError(t, err)
			t.Cleanup(base.Release)
			rows, err := NewRowSelection([]int64{6, 0, 3}, 7, mp, nil)
			require.NoError(t, err)
			t.Cleanup(rows.Release)
			dict, err := NewDictionaryVector(base, rows, mp, nil)
			require.NoError(t, err)
			t.Cleanup(func() { dict.Free(mp) })
			require.Equal(t, isNull, dict.IsNull(2))
			require.Equal(t, isNull, ResolveFixedReadView[int64](dict).AllNull)
			if !isNull {
				require.Equal(t, int64(7), GetFixedAtNoTypeCheck[int64](dict, 2))
			}
			require.NoError(t, dict.EnsureFlat(mp))
			require.Equal(t, 3, dict.Length())
			require.Equal(t, isNull, dict.IsNull(1))
			emptyRows, err := NewRowSelection(nil, 1, mp, nil)
			require.NoError(t, err)
			t.Cleanup(emptyRows.Release)
			empty, err := NewDictionaryVector(base, emptyRows, mp, nil)
			require.NoError(t, err)
			t.Cleanup(func() { empty.Free(mp) })
			require.Zero(t, empty.Length())
			require.False(t, empty.IsDictionary(), "empty output must not pin the base")
			require.Zero(t, empty.Allocated())
		})
	}
}

func TestDictionarySelectionBoundsAndAccountFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	for _, rows := range [][]int64{{-1}, {2}} {
		_, err := NewRowSelection(rows, 2, mp, nil)
		require.Error(t, err)
		require.Zero(t, mp.CurrNB())
	}
	source := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendFixedList(source, []int64{11, 22}, nil, mp))
	base, err := FreezeVector(source, mp, nil)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	rows, err := NewRowSelection([]int64{1, 0}, 2, mp, nil)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	dict, err := NewDictionaryVector(base, rows, mp, nil)
	require.NoError(t, err)
	t.Cleanup(func() { dict.Free(mp) })
	state := newTestVectorAllocationAccount(t, 1, 4)
	t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
	_, err = dict.DupOffHeapWithAllocation(mp, state.selection)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.True(t, dict.IsDictionary())
	require.Equal(t, int64(22), GetFixedAtNoTypeCheck[int64](dict, 0))
	_, err = NewDictionaryVector(base, rows, mp, state.selection)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountMismatch)
}

func TestDictionarySharedOwnersConcurrentRelease(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendFixedList(source, []int64{1, 2}, nil, mp))
	base, err := FreezeVector(source, mp, nil)
	require.NoError(t, err)
	defer base.Release()
	rows, err := NewRowSelection([]int64{1, 0, 1}, 2, mp, nil)
	require.NoError(t, err)
	defer rows.Release()
	const workers = 8
	var wg sync.WaitGroup
	type result struct {
		value int64
		err   error
	}
	results := make(chan result, workers)
	for range workers {
		require.True(t, base.Retain())
		require.True(t, rows.Retain())
		wg.Go(func() {
			defer base.Release()
			defer rows.Release()
			dict, err := NewDictionaryVector(base, rows, mp, nil)
			if err != nil {
				results <- result{err: err}
				return
			}
			defer dict.Free(mp)
			view := ResolveFixedReadView[int64](dict)
			results <- result{value: view.Values[view.Selection[2]]}
		})
	}
	wg.Wait()
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		require.Equal(t, int64(2), result.value)
	}
}

func TestDictionaryForeignLeasePinAdmission(t *testing.T) {
	for _, capacity := range []uint64{256, 8192} {
		t.Run(map[bool]string{true: "pin admitted", false: "pin refusal copies"}[capacity >= 4096], func(t *testing.T) {
			mp := mpool.MustNewZero()
			state := newTestVectorAllocationAccount(t, capacity, 32)
			t.Cleanup(func() {
				finalizeTestVectorAllocationAccount(t, state)
				require.Zero(t, mp.CurrNB())
			})
			releases := 0
			lease, err := NewRefCountedBufferLease(types.EncodeSlice([]int64{11, 22}), 4096, func() { releases++ })
			require.NoError(t, err)
			source, err := NewBorrowedFixedVector(types.T_int64.ToType(), 2, lease.Bytes(), lease)
			lease.Release()
			require.NoError(t, err)
			t.Cleanup(func() { source.Free(mp) })
			base, err := FreezeVector(source, mp, state.selection)
			require.NoError(t, err)
			t.Cleanup(func() {
				if base != nil {
					base.Release()
				}
			})
			source.Free(mp)
			if capacity >= 4096 {
				require.Zero(t, releases, "query base keeps the external backing live")
				require.Equal(t, uint64(4096), state.account.Snapshot().Used, "charge the entire retained backing")
			} else {
				require.Equal(t, 1, releases, "fallback owns a private copy")
				require.Positive(t, state.account.Snapshot().Used)
				require.Less(t, state.account.Snapshot().Used, uint64(4096))
			}
			require.Equal(t, int64(22), GetFixedAtNoTypeCheck[int64](base.vec, 1))
			base.Release()
			base = nil
			require.Equal(t, 1, releases)
			require.Zero(t, state.account.Snapshot().Used)
		})
	}
}

func TestDictionaryEnsureFlatFailureIsTransactional(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	controller := &rejectNextVectorAllocation{}
	account, err := registry.OpenWithController(1<<20, controller)
	require.NoError(t, err)
	selection, err := NewAllocationAccountSelection(account, testVectorAllocationOwner,
		testVectorDataAllocationSite, testVectorAreaAllocationSite, testVectorNullAllocationSite, testVectorGroupAllocationSite)
	require.NoError(t, err)
	t.Cleanup(func() {
		finalizeTestVectorAllocationAccount(t, testVectorAllocationAccount{registry: registry, account: account, selection: selection})
		require.Zero(t, mp.CurrNB())
	})
	source := newAccountedTestVector(t, types.T_int64.ToType(), selection)
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendFixedList(source, []int64{11, 22}, nil, mp))
	base, err := FreezeVector(source, mp, selection)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	rows, err := NewRowSelection([]int64{1, 0}, 2, mp, selection)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	dict, err := NewDictionaryVector(base, rows, mp, selection)
	require.NoError(t, err)
	t.Cleanup(func() { dict.Free(mp) })
	before := account.Snapshot().Used
	backing := dict.dataLease
	controller.failAt = controller.calls + 1
	require.ErrorIs(t, dict.EnsureFlat(mp), mpool.ErrAllocationAccountCapacity)
	require.True(t, controller.rejected)
	require.Equal(t, before, account.Snapshot().Used)
	require.True(t, dict.IsDictionary())
	require.Same(t, backing, dict.dataLease, "failed COW preserves its owner")
	require.Equal(t, int64(22), GetFixedAtNoTypeCheck[int64](dict, 0))
	controller.failAt = 0
	require.NoError(t, dict.EnsureWritable(mp))
	require.False(t, backing.Retain(), "successful COW retires the descriptor")
	require.Equal(t, []int64{22, 11}, MustFixedColNoTypeCheck[int64](dict))
}

func TestDictionaryPinRefusalMaterializesOnlySelectedRows(t *testing.T) {
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(t, 32, 32)
	t.Cleanup(func() {
		finalizeTestVectorAllocationAccount(t, state)
		require.Zero(t, mp.CurrNB())
	})
	lease, err := NewRefCountedBufferLease(types.EncodeSlice([]int64{11, 22, 33, 44}), 256, nil)
	require.NoError(t, err)
	source, err := NewBorrowedFixedVector(types.T_int64.ToType(), 4, lease.Bytes(), lease)
	lease.Release()
	require.NoError(t, err)
	t.Cleanup(func() { source.Free(mp) })
	rows, err := NewRowSelection([]int64{3}, 4, mp, state.selection)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	result, err := source.SelectDictionary(rows, mp, state.selection)
	require.NoError(t, err, "a pin/full-copy refusal must not reject a fitting selected materialization")
	t.Cleanup(func() { result.Free(mp) })
	require.False(t, result.IsDictionary())
	require.Equal(t, []int64{44}, MustFixedColNoTypeCheck[int64](result))
	require.LessOrEqual(t, state.account.Snapshot().Used, uint64(32))
}
