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
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// TestTypeOpsCoverEveryOid fails when a T_ constant is neither a row of
// typeOpsRows nor listed in typeOpsUnsupported, or is both.
func TestTypeOpsCoverEveryOid(t *testing.T) {
	unsupported := map[types.T]bool{}
	for _, oid := range typeOpsUnsupported {
		require.False(t, unsupported[oid], "%s listed twice in typeOpsUnsupported", oid.OidString())
		unsupported[oid] = true
	}
	for _, oid := range types.AllOids() {
		supported := vectorTypeOps[oid].supported
		require.True(t, supported != unsupported[oid],
			"type id %d (%s): must be exactly one of a typeOpsRows row or typeOpsUnsupported", oid, oid.String())
	}
	for oid := range unsupported {
		require.Contains(t, types.AllOids(), oid, "typeOpsUnsupported lists %d, which is not a T_ constant", oid)
	}
}

// TestTypeOpsRows checks each row against the type's facts in package types,
// so a row with the wrong Go element type fails here, and that every
// operation is set.
func TestTypeOpsRows(t *testing.T) {
	for _, r := range typeOpsRows {
		name := r.oid.OidString()
		ops := vectorTypeOps[r.oid]
		require.True(t, ops.supported, name)
		require.NotNil(t, ops.getAny, name)
		require.NotNil(t, ops.appendAny, name)
		require.NotNil(t, ops.setAny, name)
		require.NotNil(t, ops.shrink, name)
		require.NotNil(t, ops.shrinkByMask, name)
		require.NotNil(t, ops.shuffle, name)
		require.NotNil(t, ops.shuffleWithBuf, name)
		require.NotNil(t, ops.unionAll, name)
		require.NotNil(t, ops.appendBytes, name)
		require.NotNil(t, ops.newResult, name)
		require.Equal(t, r.oid.ToType().IsVarlen(), ops.varlen, "%s: varlen", name)

		// The Go type GetAny returns is the row's element type (or []byte for
		// a varlena type); its size must be the type's size.
		mp := mpool.MustNewZero()
		v := randomVector(t, rand.New(rand.NewSource(1)), r.oid.ToType(), 1, mp)
		val := reflect.TypeOf(ops.getAny(v, 0, false))
		if ops.varlen {
			require.Equal(t, reflect.TypeOf([]byte(nil)), val, name)
		} else {
			require.Equal(t, uintptr(r.oid.TypeLen()), val.Size(), "%s: element size", name)
		}
		v.Free(mp)
	}
	require.Panics(t, func() {
		buildTypeOps([]typeOpsRow{{types.T_int8, fixedOps[int8]()}, {types.T_int8, fixedOps[int8]()}})
	})
}

// randomVector returns a vector of typ with n rows of random contents and
// about one row in five NULL.
func randomVector(t *testing.T, rng *rand.Rand, typ types.Type, n int, mp *mpool.MPool) *Vector {
	t.Helper()
	v := NewVec(typ, mp)
	if typ.IsVarlen() {
		for i := 0; i < n; i++ {
			b := make([]byte, rng.Intn(48)) // some inline, some in the area
			rng.Read(b)
			require.NoError(t, AppendBytes(v, b, false, mp))
		}
	} else {
		require.NoError(t, v.PreExtend(n, mp))
		v.SetLength(n)
		data := v.data[:n*typ.TypeSize()]
		rng.Read(data)
		if typ.Oid == types.T_bool {
			for i := range data {
				data[i] &= 1
			}
		}
	}
	for i := 0; i < n; i++ {
		if rng.Intn(5) == 0 {
			v.SetNull(uint64(i))
		}
	}
	return v
}

// requireSameVector compares two vectors row by row: length, NULLs, and the
// bytes of every non-null row.
func requireSameVector(t *testing.T, want, got *Vector, msg string) {
	t.Helper()
	require.Equal(t, want.Length(), got.Length(), "%s: length", msg)
	for i := 0; i < want.Length(); i++ {
		require.Equal(t, want.IsNull(uint64(i)), got.IsNull(uint64(i)), "%s: row %d null", msg, i)
		if !want.IsNull(uint64(i)) {
			require.True(t, bytes.Equal(want.GetRawBytesAt(i), got.GetRawBytesAt(i)), "%s: row %d", msg, i)
		}
	}
	require.Equal(t, want.GetSorted(), got.GetSorted(), "%s: sorted", msg)
}

// outcomeOf runs f and returns its error text, or the panic it raised.
func outcomeOf(f func() error) (s string) {
	defer func() {
		if r := recover(); r != nil {
			s = fmt.Sprintf("panic(%v)", r)
		}
	}()
	if err := f(); err != nil {
		return "error: " + err.Error()
	}
	return "ok"
}

// TestTypeOpsMatchLegacy runs each operation now dispatched through the table
// and its switch implementation on identical vectors of every type id, and
// requires the same result: equal vectors, values, errors or panics.
func TestTypeOpsMatchLegacy(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(10181))
	const n = 37

	for i := 0; i < 256; i++ {
		oid := types.T(i)
		if oid == types.T_any {
			// handled before the dispatch, by code this change does not touch
			continue
		}
		typ, ok := oid.TryToType()
		if !ok {
			typ = types.Type{Oid: oid}
		}
		name := fmt.Sprintf("T(%d) %s", i, oid.OidString())

		if !vectorTypeOps[oid].supported {
			empty := func() *Vector { return NewVec(typ, mp) }
			require.Nil(t, GetAny(empty(), 0, false), name)
			require.Equal(t, legacyGetAny(empty(), 0, false), GetAny(empty(), 0, false), name)
			oneRow := bitmapOf(t, mp, 1, 0)
			a, b := empty(), empty()
			require.Equal(t, outcomeOf(func() error { return legacyAppendAny(a, 1, false, mp) }),
				outcomeOf(func() error { return AppendAny(b, 1, false, mp) }), "%s: AppendAny", name)
			require.Equal(t, a.Length(), b.Length(), name)
			for _, op := range []struct {
				name        string
				legacy, new func(v *Vector) error
			}{
				{"Shrink", func(v *Vector) error { return legacyShrink(v, []int64{0}, false, mp) },
					func(v *Vector) error { return v.Shrink([]int64{0}, false, mp) }},
				{"ShrinkByMask", func(v *Vector) error { return legacyShrinkByMask(v, oneRow, false, 0, mp) },
					func(v *Vector) error { return v.ShrinkByMask(oneRow, false, 0, mp) }},
				{"Shuffle", func(v *Vector) error { return legacyShuffle(v, []int64{0}, mp) },
					func(v *Vector) error { return v.Shuffle([]int64{0}, mp) }},
				{"shuffleWithBufData", func(v *Vector) error { var buf []byte; return legacyShuffleWithBufData(v, nil, &buf) },
					func(v *Vector) error { var buf []byte; return v.shuffleWithBufData(nil, &buf) }},
			} {
				want := outcomeOf(func() error { return op.legacy(empty()) })
				require.Contains(t, want, "panic(unexpect type", "%s: %s", name, op.name)
				require.Equal(t, want, outcomeOf(func() error { return op.new(empty()) }), "%s: %s", name, op.name)
			}
			oneRow.Free()
			a.Free(mp)
			b.Free(mp)
			continue
		}

		src := randomVector(t, rng, typ, n, mp)
		pair := func() (*Vector, *Vector) {
			a, err := src.Dup(mp)
			require.NoError(t, err)
			b, err := src.Dup(mp)
			require.NoError(t, err)
			return a, b
		}
		check := func(op string, legacy, new func(v *Vector) error) {
			a, b := pair()
			defer a.Free(mp)
			defer b.Free(mp)
			require.Equal(t, outcomeOf(func() error { return legacy(a) }), outcomeOf(func() error { return new(b) }), "%s: %s", name, op)
			requireSameVector(t, a, b, name+": "+op)
		}

		// GetAny, with and without a deep copy
		for row := 0; row < n; row++ {
			for _, deep := range []bool{false, true} {
				require.Equal(t, legacyGetAny(src, row, deep), GetAny(src, row, deep), "%s: GetAny row %d", name, row)
			}
		}
		// AppendAny of every row's value, and of a NULL
		check("AppendAny", func(v *Vector) error {
			for row := 0; row < n; row++ {
				if err := legacyAppendAny(v, legacyGetAny(src, row, true), src.IsNull(uint64(row)), mp); err != nil {
					return err
				}
			}
			return nil
		}, func(v *Vector) error {
			for row := 0; row < n; row++ {
				if err := AppendAny(v, GetAny(src, row, true), src.IsNull(uint64(row)), mp); err != nil {
					return err
				}
			}
			return nil
		})
		// AppendAny of a value of the wrong Go type panics the same way
		check("AppendAny wrong type", func(v *Vector) error { return legacyAppendAny(v, struct{}{}, false, mp) },
			func(v *Vector) error { return AppendAny(v, struct{}{}, false, mp) })

		sels := randomSels(rng, n)
		for _, negate := range []bool{false, true} {
			check(fmt.Sprintf("Shrink negate=%v", negate),
				func(v *Vector) error { return legacyShrink(v, sels, negate, mp) },
				func(v *Vector) error { return v.Shrink(sels, negate, mp) })
			mask := bitmapOf(t, mp, n, sels...)
			check(fmt.Sprintf("ShrinkByMask negate=%v", negate),
				func(v *Vector) error { return legacyShrinkByMask(v, mask, negate, 0, mp) },
				func(v *Vector) error { return v.ShrinkByMask(mask, negate, 0, mp) })
			mask.Free()
		}
		perm := make([]int64, n)
		for j := range perm {
			perm[j] = int64(rng.Intn(n))
		}
		check("Shuffle", func(v *Vector) error { return legacyShuffle(v, perm, mp) },
			func(v *Vector) error { return v.Shuffle(perm, mp) })
		check("Shuffle shorter", func(v *Vector) error { return legacyShuffle(v, perm[:n/2], mp) },
			func(v *Vector) error { return v.Shuffle(perm[:n/2], mp) })
		check("shuffleWithBufData", func(v *Vector) error {
			var buf []byte
			return legacyShuffleWithBufData(v, perm, &buf)
		}, func(v *Vector) error {
			var buf []byte
			return v.shuffleWithBufData(perm, &buf)
		})
		src.Free(mp)
	}
	require.Zero(t, mp.CurrNB(), "vectors leaked")
}

// TestTypeOpsConstructorsMatchLegacy compares the functions that build a
// per-type function (union, append-bytes, function result) and the
// type checks moved onto the table with their switch implementations, for
// every type id.
func TestTypeOpsConstructorsMatchLegacy(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(33339))
	const n = 23

	for i := 0; i < 256; i++ {
		oid := types.T(i)
		typ, ok := oid.TryToType()
		if !ok {
			typ = types.Type{Oid: oid}
		}
		name := fmt.Sprintf("T(%d) %s", i, oid.OidString())

		require.Equal(t, legacyIsVarlenaMarshalType(oid), isVarlenaMarshalType(oid), "%s: isVarlenaMarshalType", name)
		for _, size := range []int32{typ.Size, typ.Size + 1} {
			typ2 := typ
			typ2.Size = size
			require.Equal(t, outcomeOf(func() error { _, err := legacyCanonicalVectorTypeSize(typ2); return err }),
				outcomeOf(func() error { _, err := canonicalVectorTypeSize(typ2); return err }), "%s: canonicalVectorTypeSize size %d", name, size)
		}

		if !vectorTypeOps[oid].supported {
			for _, c := range []struct {
				name        string
				legacy, new func() error
			}{
				{"union", func() error { legacyGetUnionAllFunction(typ, mp); return nil }, func() error { getUnionAllFunction(typ, mp); return nil }},
				{"MakeAppendBytesFunc", func() error { legacyMakeAppendBytesFunc(NewVec(typ, mp)); return nil },
					func() error { MakeAppendBytesFunc(NewVec(typ, mp)); return nil }},
				{"NewFunctionResultWrapper", func() error { legacyNewFunctionResultWrapper(typ, mp); return nil },
					func() error { NewFunctionResultWrapper(typ, mp); return nil }},
			} {
				want := outcomeOf(c.legacy)
				if oid != types.T_any || c.name == "MakeAppendBytesFunc" || c.name == "NewFunctionResultWrapper" {
					require.Contains(t, want, "panic(", "%s: %s", name, c.name)
				}
				require.Equal(t, want, outcomeOf(c.new), "%s: %s", name, c.name)
			}
			continue
		}

		src := randomVector(t, rng, typ, n, mp)
		// a const source
		constSrc := NewVec(typ, mp)
		require.NoError(t, getConstSetFunction(typ, mp)(constSrc, src, firstNonNull(src), 5))
		constNull := NewConstNull(typ, 4, mp)

		// union: append each kind of source to copies of another vector, whose
		// area differs from src's, so a varlena copy must move payloads
		base := randomVector(t, rng, typ, n, mp)
		for _, w := range []struct {
			name string
			vec  *Vector
		}{{"plain", src}, {"const", constSrc}, {"const null", constNull}} {
			a, err := base.Dup(mp)
			require.NoError(t, err)
			b, err := base.Dup(mp)
			require.NoError(t, err)
			require.Equal(t, outcomeOf(func() error { return legacyGetUnionAllFunction(typ, mp)(a, w.vec) }),
				outcomeOf(func() error { return getUnionAllFunction(typ, mp)(b, w.vec) }), "%s: union %s", name, w.name)
			requireSameVector(t, a, b, name+": union "+w.name)
			a.Free(mp)
			b.Free(mp)
		}

		// MakeAppendBytesFunc: append every row's raw bytes (and NULLs)
		a, b := NewVec(typ, mp), NewVec(typ, mp)
		appendA, appendB := legacyMakeAppendBytesFunc(a), MakeAppendBytesFunc(b)
		for row := 0; row < n; row++ {
			null := src.IsNull(uint64(row))
			raw := append([]byte(nil), src.GetRawBytesAt(row)...)
			require.Equal(t, outcomeOf(func() error { return appendA(raw, null, mp) }),
				outcomeOf(func() error { return appendB(raw, null, mp) }), "%s: append bytes row %d", name, row)
		}
		requireSameVector(t, a, b, name+": MakeAppendBytesFunc")
		a.Free(mp)
		b.Free(mp)

		// NewFunctionResultWrapper: same concrete type, same result vector type
		ra, rb := legacyNewFunctionResultWrapper(typ, mp), NewFunctionResultWrapper(typ, mp)
		require.Equal(t, fmt.Sprintf("%T", ra), fmt.Sprintf("%T", rb), "%s: function result", name)
		require.NoError(t, ra.PreExtendAndReset(1))
		require.NoError(t, rb.PreExtendAndReset(1))
		require.Equal(t, *ra.GetResultVector().GetType(), *rb.GetResultVector().GetType(), "%s: function result", name)
		ra.Free()
		rb.Free()

		src.Free(mp)
		constSrc.Free(mp)
		constNull.Free(mp)
		base.Free(mp)
	}
	require.Zero(t, mp.CurrNB(), "vectors leaked")
}

// TestSetAny sets a row of a vector of every supported type to another row's
// value, and checks that unsupported types are refused.
func TestSetAny(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < 256; i++ {
		oid := types.T(i)
		require.Equal(t, vectorTypeOps[oid].supported, SupportsAny(oid))
		if !SupportsAny(oid) {
			typ, ok := oid.TryToType()
			if !ok {
				typ = types.Type{Oid: oid}
			}
			require.Error(t, SetAny(NewVec(typ, mp), 0, nil, mp), oid.OidString())
			continue
		}
		typ := oid.ToType()
		v := randomVector(t, rng, typ, 8, mp)
		want := GetAny(v, 5, true)
		wasNull := v.IsNull(2)
		require.NoError(t, SetAny(v, 2, want, mp), oid.OidString())
		require.Equal(t, want, GetAny(v, 2, true), oid.OidString())
		require.Equal(t, wasNull, v.IsNull(2), "%s: SetAny leaves the NULL bit", oid.OidString())
		require.Panics(t, func() { _ = SetAny(v, 2, struct{}{}, mp) }, oid.OidString())
		v.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}

// TestConstSetCoversTypeOps ties getConstSetFunction, which stays a switch
// (its hand-written closures run per row in SetJoinBatchValues, and a generic
// version measured about 1 ns slower per call), to the table: it must return
// a function for every supported type id and panic for every other one but
// T_any, so a type added to the table cannot be missed there.
func TestConstSetCoversTypeOps(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range types.AllOids() {
		typ, ok := oid.TryToType()
		if !ok {
			typ = types.Type{Oid: oid}
		}
		got := outcomeOf(func() error {
			if getConstSetFunction(typ, mp) == nil {
				return fmt.Errorf("nil function")
			}
			return nil
		})
		if vectorTypeOps[oid].supported || oid == types.T_any {
			require.Equal(t, "ok", got, oid.OidString())
		} else {
			require.Contains(t, got, "panic(unexpect type", oid.OidString())
		}
	}
}

func firstNonNull(v *Vector) int64 {
	for i := 0; i < v.Length(); i++ {
		if !v.IsNull(uint64(i)) {
			return int64(i)
		}
	}
	return 0
}

// randomSels returns an ascending, duplicate-free random subset of [0, n).
func randomSels(rng *rand.Rand, n int) []int64 {
	var sels []int64
	for i := 0; i < n; i++ {
		if rng.Intn(2) == 0 {
			sels = append(sels, int64(i))
		}
	}
	return sels
}

// bitmapOf returns a bitmap of n rows with rows set, owned by the caller
// (Free it).
func bitmapOf(t *testing.T, mp *mpool.MPool, n int, rows ...int64) *bitmap.Bitmap {
	var bm bitmap.Bitmap
	require.NoError(t, bm.InitWithRows(int64(n), rows, mp))
	return &bm
}

// BenchmarkTypeOpsDispatch compares the table with the switch implementations
// on a per-row operation (GetAny) and a per-vector one (Shrink).
func BenchmarkTypeOpsDispatch(b *testing.B) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(1))
	t := &testing.T{}
	for _, oid := range []types.T{types.T_int64, types.T_Blockid, types.T_varchar} {
		v := randomVector(t, rng, oid.ToType(), 1024, mp)
		b.Run(oid.OidString()+"/GetAny/table", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = GetAny(v, i&1023, false)
			}
		})
		b.Run(oid.OidString()+"/GetAny/switch", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = legacyGetAny(v, i&1023, false)
			}
		})
		sels := []int64{1, 5, 9}
		b.Run(oid.OidString()+"/Shrink/table", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				w, _ := v.Dup(mp)
				_ = w.Shrink(sels, false, mp)
				w.Free(mp)
			}
		})
		b.Run(oid.OidString()+"/Shrink/switch", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				w, _ := v.Dup(mp)
				_ = legacyShrink(w, sels, false, mp)
				w.Free(mp)
			}
		})
		v.Free(mp)
	}
}

// BenchmarkTypeOpsConstructors compares the generic union function with the
// hand-written per-type closures it replaces.
func BenchmarkTypeOpsConstructors(b *testing.B) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(2))
	t := &testing.T{}
	for _, oid := range []types.T{types.T_int32, types.T_date, types.T_decimal128, types.T_varchar} {
		typ := oid.ToType()
		src := randomVector(t, rng, typ, 1024, mp)
		for _, impl := range []struct {
			name  string
			union func(types.Type, *mpool.MPool) func(v, w *Vector) error
		}{
			{"table", getUnionAllFunction},
			{"switch", legacyGetUnionAllFunction},
		} {
			b.Run(oid.OidString()+"/union/"+impl.name, func(b *testing.B) {
				dst := NewVec(typ, mp)
				union := impl.union(typ, mp)
				for i := 0; i < b.N; i++ {
					if i%64 == 0 {
						dst.CleanOnlyData()
					}
					_ = union(dst, src)
				}
				dst.Free(mp)
			})
		}
		src.Free(mp)
	}
}
