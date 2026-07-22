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

package function

import (
	"encoding/base64"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// The vector builtins gained vecbf16/vecf16/vecint8/vecuint8 overloads. Each
// overload contributes two closures to the registration table in
// list_builtIn.go — retType(...) and newOp(...) — and NEITHER is reached by the
// usual tcTemp harness, which hands the execute function to NewFunctionTestCase
// directly and so never consults the table at all.
//
// Going through GetFunctionByName -> RunFunctionDirectly is what exercises the
// registration: GetFunctionByName runs checkFn and then retType, and
// RunFunctionDirectly calls GetExecuteMethod, which is what invokes newOp.
// That path also proves the thing an overload table can get quietly wrong —
// that the narrow argument types actually RESOLVE to an overload, rather than
// falling through to a cast or a "function not supported" error.
func TestNarrowVectorFunctionOverloads(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	// Same logical vectors ([1,2,3] and [4,5,6]) in each storage type, so a
	// result that differs across types is a real dispatch bug and not an
	// artifact of the fixture.
	mkPair := func(typ types.T) (*vector.Vector, *vector.Vector) {
		switch typ {
		case types.T_array_bf16:
			a := [][]types.BF16{{types.BF16FromFloat32(1), types.BF16FromFloat32(2), types.BF16FromFloat32(3)}}
			b := [][]types.BF16{{types.BF16FromFloat32(4), types.BF16FromFloat32(5), types.BF16FromFloat32(6)}}
			return newVectorByType(mp, typ.ToType(), a, &nulls.Nulls{}),
				newVectorByType(mp, typ.ToType(), b, &nulls.Nulls{})
		case types.T_array_float16:
			a := [][]types.Float16{{types.Float16FromFloat32(1), types.Float16FromFloat32(2), types.Float16FromFloat32(3)}}
			b := [][]types.Float16{{types.Float16FromFloat32(4), types.Float16FromFloat32(5), types.Float16FromFloat32(6)}}
			return newVectorByType(mp, typ.ToType(), a, &nulls.Nulls{}),
				newVectorByType(mp, typ.ToType(), b, &nulls.Nulls{})
		case types.T_array_int8:
			return newVectorByType(mp, typ.ToType(), [][]int8{{1, 2, 3}}, &nulls.Nulls{}),
				newVectorByType(mp, typ.ToType(), [][]int8{{4, 5, 6}}, &nulls.Nulls{})
		case types.T_array_uint8:
			return newVectorByType(mp, typ.ToType(), [][]uint8{{1, 2, 3}}, &nulls.Nulls{}),
				newVectorByType(mp, typ.ToType(), [][]uint8{{4, 5, 6}}, &nulls.Nulls{})
		}
		t.Fatalf("unhandled type %v", typ)
		return nil, nil
	}

	narrow := []types.T{
		types.T_array_bf16,
		types.T_array_float16,
		types.T_array_int8,
		types.T_array_uint8,
	}

	// resolve + run, returning the result vector so the caller can assert on it.
	run := func(t *testing.T, name string, args []types.Type, inputs []*vector.Vector) *vector.Vector {
		t.Helper()
		fr, err := GetFunctionByName(proc.Ctx, name, args)
		require.NoErrorf(t, err, "%s(%v) must resolve to an overload", name, args)
		out, err := RunFunctionDirectly(proc, fr.GetEncodedOverloadID(), inputs, 1)
		require.NoErrorf(t, err, "%s(%v) must execute", name, args)
		require.NotNil(t, out)
		return out
	}

	// --- binary distance/similarity builtins -------------------------------
	// Asserted as PARITY WITH VECF32 rather than against hardcoded constants:
	// vecf32 is the reference these overloads were added to match, and a literal
	// would encode my guess at the convention instead of the engine's. That
	// distinction is not academic here — inner_product returns the NEGATED dot
	// product (-32, not 32) because it is used as a distance where smaller means
	// closer, and a hand-written 32.0 would have "found" a bug that isn't one.
	f32t := types.T_array_float32.ToType()
	for _, fn := range []string{
		"l2_distance", "l2_distance_sq", "inner_product",
		"cosine_similarity", "cosine_distance",
	} {
		af := newVectorByType(mp, f32t, [][]float32{{1, 2, 3}}, &nulls.Nulls{})
		bf := newVectorByType(mp, f32t, [][]float32{{4, 5, 6}}, &nulls.Nulls{})
		refOut := run(t, fn, []types.Type{f32t, f32t}, []*vector.Vector{af, bf})
		want := vector.MustFixedColWithTypeCheck[float64](refOut)[0]
		refOut.Free(mp)
		af.Free(mp)
		bf.Free(mp)

		for _, typ := range narrow {
			t.Run(fn+"/"+typ.String(), func(t *testing.T) {
				a, b := mkPair(typ)
				defer a.Free(mp)
				defer b.Free(mp)
				out := run(t, fn, []types.Type{typ.ToType(), typ.ToType()},
					[]*vector.Vector{a, b})
				defer out.Free(mp)

				got := vector.MustFixedColWithTypeCheck[float64](out)
				require.Len(t, got, 1)
				// int8/uint8 hold the values exactly here (1..6), and bf16/f16
				// represent small integers exactly, so parity is exact up to
				// float rounding in the kernel — not a loose approximation.
				require.InDeltaf(t, want, got[0], 0.05,
					"%s on %s must agree with vecf32 (%v)", fn, typ, want)
			})
		}
	}

	// --- unary vector builtins ---------------------------------------------
	for _, typ := range narrow {
		t.Run("vector_dims/"+typ.String(), func(t *testing.T) {
			a, b := mkPair(typ)
			defer a.Free(mp)
			defer b.Free(mp)
			out := run(t, "vector_dims", []types.Type{typ.ToType()}, []*vector.Vector{a})
			defer out.Free(mp)
			require.Equal(t, int64(3), vector.MustFixedColWithTypeCheck[int64](out)[0])
		})

		t.Run("normalize_l2/"+typ.String(), func(t *testing.T) {
			a, b := mkPair(typ)
			defer a.Free(mp)
			defer b.Free(mp)
			out := run(t, "normalize_l2", []types.Type{typ.ToType()}, []*vector.Vector{a})
			defer out.Free(mp)
			// Return type differs by storage class, deliberately: the float
			// types keep theirs, while int8/uint8 widen to vecf32 because a
			// unit vector is not representable in them at all (every component
			// of [1,2,3] normalised is < 1 and would round to 0). Pinning it
			// here so a future "consistency" cleanup cannot quietly make
			// normalize_l2 return all-zero int8 vectors.
			switch typ {
			case types.T_array_int8, types.T_array_uint8:
				require.Equal(t, types.T_array_float32, out.GetType().Oid)
			default:
				require.Equal(t, typ, out.GetType().Oid)
			}
		})
	}

	// --- least / greatest --------------------------------------------------
	// These dispatch on the ARGUMENT type and return it unchanged, so a missing
	// narrow arm shows up as "invalid argument function least, bad value
	// [VECINT8]" while the identical call on vecf32 succeeds.
	for _, fn := range []string{"least", "greatest"} {
		for _, typ := range narrow {
			t.Run(fn+"/"+typ.String(), func(t *testing.T) {
				a, b := mkPair(typ)
				defer a.Free(mp)
				defer b.Free(mp)
				out := run(t, fn, []types.Type{typ.ToType(), typ.ToType()},
					[]*vector.Vector{a, b})
				defer out.Free(mp)
				require.Equal(t, typ, out.GetType().Oid)
				// [1,2,3] vs [4,5,6]: least picks the first, greatest the second.
				want := a
				if fn == "greatest" {
					want = b
				}
				require.Equal(t, want.GetBytesAt(0), out.GetBytesAt(0))
			})
		}
	}

	// --- json constructors --------------------------------------------------
	// json_array needed TWO fixes (the value conversion AND the separate
	// jsonConstructorSupportsType gate), so it is asserted for parity with
	// vecf32: the same logical vector must serialise to the same JSON text
	// regardless of storage width.
	for _, fn := range []struct {
		name string
		args func(t types.Type) ([]types.Type, func(*vector.Vector) []*vector.Vector)
	}{
		{"json_array", func(tt types.Type) ([]types.Type, func(*vector.Vector) []*vector.Vector) {
			return []types.Type{tt}, func(v *vector.Vector) []*vector.Vector { return []*vector.Vector{v} }
		}},
		{"json_row", func(tt types.Type) ([]types.Type, func(*vector.Vector) []*vector.Vector) {
			return []types.Type{tt}, func(v *vector.Vector) []*vector.Vector { return []*vector.Vector{v} }
		}},
	} {
		ft := types.T_array_float32.ToType()
		af := newVectorByType(mp, ft, [][]float32{{1, 2, 3}}, &nulls.Nulls{})
		at, mk := fn.args(ft)
		refOut := run(t, fn.name, at, mk(af))
		wantJSON := append([]byte(nil), refOut.GetBytesAt(0)...)
		refOut.Free(mp)
		af.Free(mp)

		for _, typ := range narrow {
			t.Run(fn.name+"/"+typ.String(), func(t *testing.T) {
				a, b := mkPair(typ)
				defer a.Free(mp)
				defer b.Free(mp)
				at, mk := fn.args(typ.ToType())
				out := run(t, fn.name, at, mk(a))
				defer out.Free(mp)
				require.Equalf(t, wantJSON, out.GetBytesAt(0),
					"%s on %s must serialise like vecf32", fn.name, typ)
			})
		}
	}

	// json_object('k', v) — a separate constructor arm from json_array.
	for _, typ := range narrow {
		t.Run("json_object/"+typ.String(), func(t *testing.T) {
			a, b := mkPair(typ)
			defer a.Free(mp)
			defer b.Free(mp)
			k := newVectorByType(mp, types.T_varchar.ToType(), []string{"k"}, &nulls.Nulls{})
			defer k.Free(mp)
			out := run(t, "json_object",
				[]types.Type{types.T_varchar.ToType(), typ.ToType()},
				[]*vector.Vector{k, a})
			defer out.Free(mp)
			require.NotEmpty(t, out.GetBytesAt(0))
		})
	}

	// --- coalesce -----------------------------------------------------------
	// The overload table gained entries 25-28 for the narrow types; without them
	// COALESCE(vecint8_col, ...) failed while the vecf32 form worked.
	for _, typ := range narrow {
		t.Run("coalesce/"+typ.String(), func(t *testing.T) {
			a, b := mkPair(typ)
			defer a.Free(mp)
			defer b.Free(mp)
			out := run(t, "coalesce", []types.Type{typ.ToType(), typ.ToType()},
				[]*vector.Vector{a, b})
			defer out.Free(mp)
			require.Equal(t, typ, out.GetType().Oid)
			// Neither input is null, so the first must win.
			require.Equal(t, a.GetBytesAt(0), out.GetBytesAt(0))
		})
	}

	// --- cast ---------------------------------------------------------------
	// array->array casts dispatch on BOTH sides, so each narrow type has to
	// appear as a source and as a target. Round-tripping through vecf32 and back
	// is what proves the pair, and it is the conversion the index build path
	// actually performs when a narrow column feeds an f32 index.
	for _, typ := range narrow {
		t.Run("cast/"+typ.String()+"->f32->back", func(t *testing.T) {
			a, b := mkPair(typ)
			defer a.Free(mp)
			defer b.Free(mp)

			wide := run(t, "cast", []types.Type{typ.ToType(), f32t},
				[]*vector.Vector{a, newVectorByType(mp, f32t, [][]float32{{0, 0, 0}}, &nulls.Nulls{})})
			defer wide.Free(mp)
			require.Equal(t, types.T_array_float32, wide.GetType().Oid)
			require.InDeltaSlice(t, []float32{1, 2, 3},
				types.BytesToArray[float32](wide.GetBytesAt(0)), 0.05)

			back := run(t, "cast", []types.Type{f32t, typ.ToType()},
				[]*vector.Vector{wide, a})
			defer back.Free(mp)
			require.Equal(t, typ, back.GetType().Oid)
			require.Equal(t, a.GetBytesAt(0), back.GetBytesAt(0))
		})
	}

	// --- vecXX_from_base64 --------------------------------------------------
	// Each decodes to the matching narrow type. The base64 payloads are the raw
	// little-endian storage bytes of the three-element vectors above, which is
	// exactly what the encoder emits.
	for _, tc := range []struct {
		fn   string
		typ  types.T
		vals any
	}{
		{"vecbf16_from_base64", types.T_array_bf16, [][]types.BF16{
			{types.BF16FromFloat32(1), types.BF16FromFloat32(2), types.BF16FromFloat32(3)}}},
		{"vecf16_from_base64", types.T_array_float16, [][]types.Float16{
			{types.Float16FromFloat32(1), types.Float16FromFloat32(2), types.Float16FromFloat32(3)}}},
		{"vecint8_from_base64", types.T_array_int8, [][]int8{{1, 2, 3}}},
		{"vecuint8_from_base64", types.T_array_uint8, [][]uint8{{1, 2, 3}}},
	} {
		t.Run(tc.fn, func(t *testing.T) {
			src := newVectorByType(mp, tc.typ.ToType(), tc.vals, &nulls.Nulls{})
			defer src.Free(mp)
			raw := src.GetBytesAt(0)

			enc := newVectorByType(mp, types.T_varchar.ToType(),
				[]string{base64Encode(raw)}, &nulls.Nulls{})
			defer enc.Free(mp)

			out := run(t, tc.fn, []types.Type{types.T_varchar.ToType()},
				[]*vector.Vector{enc})
			defer out.Free(mp)
			require.Equal(t, tc.typ, out.GetType().Oid)
			require.Equal(t, raw, out.GetBytesAt(0))
		})
	}
}

func base64Encode(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
