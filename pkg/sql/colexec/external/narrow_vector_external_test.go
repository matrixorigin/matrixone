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

package external

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// The narrow vector types (vecbf16 / vecf16 / vecint8 / vecuint8) were added to
// the array switch arms in isLegalLine and getColData next to the pre-existing
// vecf32 / vecf64. vecf32 is therefore the reference behaviour: everything the
// arms are supposed to guarantee is asserted by comparing against the value the
// vecf32 arm produces for the very same input, except where the element type
// itself is legitimately stricter (see the wiring cases below).
var narrowArrayTypes = []struct {
	name string
	oid  types.T
	// elemSize is the little-endian width of one element in the stored
	// varlena; it is what distinguishes a correctly wired arm (e.g. int8 ->
	// 1 byte/element) from one that accidentally instantiates the generic
	// with float32.
	elemSize int
}{
	{"vecbf16", types.T_array_bf16, 2},
	{"vecf16", types.T_array_float16, 2},
	{"vecint8", types.T_array_int8, 1},
	{"vecuint8", types.T_array_uint8, 1},
}

func narrowArrayIsLegalLine(oid types.T, width int32, val string) bool {
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format: tree.CSV,
		},
	}
	cols := []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(oid), Width: width}}}
	return isLegalLine(param, cols, []csvparser.Field{{Val: val}})
}

// isLegalLine decides where a parallel LOAD DATA worker may cut the file: a
// line is only a valid split point if every field parses. Its switch ends in
// `default: return false`, so without a dedicated arm a table with a narrow
// vector column would have *no* legal line at all and parallel load would fail
// to find any offset - even for perfectly valid data. These cases pin the
// narrow arms to the vecf32 arm's verdict on structurally identical input.
func Test_isLegalLine_NarrowVector(t *testing.T) {
	// Values whose acceptance is a property of the array *syntax*, not of the
	// element type, so every array type must agree with vecf32 on them.
	shared := []struct {
		name string
		val  string
	}{
		{"well formed", "[1,2,3]"},
		{"inner spaces", "[1, 2, 3]"},
		// Only non-negative integers here: a negative element is an
		// element-type property (vecuint8 rightly rejects it), not syntax, so
		// it belongs in the wiring cases below rather than in the parity set.
		{"zero element", "[0,1,2]"},
		{"missing closing bracket", "[1,2,3"},
		{"no brackets", "1,2,3"},
		{"not a number", "[a,b,c]"},
		{"empty vector", "[]"},
		{"garbage", "abc"},
	}

	for _, sc := range shared {
		t.Run(sc.name, func(t *testing.T) {
			// Reference verdict, computed (not guessed) from the arm these
			// arms were copied from.
			want := narrowArrayIsLegalLine(types.T_array_float32, 3, sc.val)
			require.Equal(t, want, narrowArrayIsLegalLine(types.T_array_float64, 3, sc.val),
				"vecf64 must agree with vecf32 on %q", sc.val)
			for _, nt := range narrowArrayTypes {
				require.Equal(t, want, narrowArrayIsLegalLine(nt.oid, 3, sc.val),
					"%s must agree with vecf32 on %q", nt.name, sc.val)
			}
		})
	}

	// A parity-only test cannot tell a correctly wired arm from one that calls
	// StringToArrayToBytes[float32]: both accept "[200]". The integer element
	// types are documented (types/array_str.go stringToT) as strict - a literal
	// outside the element range or with a fraction is an error, no clamping and
	// no rounding - so these inputs are exactly where the instantiation shows.
	wiring := []struct {
		name string
		oid  types.T
		val  string
	}{
		{"int8 above range", types.T_array_int8, "[128]"},
		{"int8 below range", types.T_array_int8, "[-129]"},
		{"int8 fractional", types.T_array_int8, "[1.5]"},
		{"uint8 above range", types.T_array_uint8, "[256]"},
		{"uint8 negative", types.T_array_uint8, "[-1]"},
		{"uint8 fractional", types.T_array_uint8, "[1.5]"},
	}
	for _, wc := range wiring {
		t.Run("wiring/"+wc.name, func(t *testing.T) {
			require.True(t, narrowArrayIsLegalLine(types.T_array_float32, 1, wc.val),
				"precondition: vecf32 accepts %q, so a false below can only come from the element type", wc.val)
			require.False(t, narrowArrayIsLegalLine(wc.oid, 1, wc.val),
				"%s must reject %q (strict integer element parsing)", wc.oid, wc.val)
		})
	}
}

func narrowArrayGetColData(t *testing.T, oid types.T, width int32, val string) (*vector.Vector, error) {
	t.Helper()
	proc := testutil.NewProcess(t)
	colType := plan.Type{Id: int32(oid), Width: width}

	bat := batch.New([]string{"v"})
	bat.Vecs[0] = vector.NewVec(types.New(oid, width, 0))

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					Format: tree.CSV,
				},
			},
			Cols: []*plan.ColDef{{Name: "v", Typ: colType}},
		},
	}
	attr := plan.ExternAttr{ColName: "v", ColIndex: 0, ColFieldIndex: 0}
	err := getColData(bat, []csvparser.Field{{Val: val}}, 0, param, proc.GetMPool(), attr, proc)
	return bat.Vecs[0], err
}

func requireArrayAt[T types.ArrayElement](t *testing.T, vec *vector.Vector, want []T) {
	t.Helper()
	require.Equal(t, want, vector.GetArrayAt[T](vec, 0))
}

// getColData is the actual CSV -> vector path of LOAD DATA / external table
// scan. Its switch also ends in an error default, so without these arms every
// narrow-vector external table would fail to import. The three things an arm
// owes are checked here: it stores the value in the *element type's* encoding,
// it enforces the declared dimension, and it surfaces a parse failure - each
// compared against what the vecf32 arm does with the same input.
func Test_getColData_NarrowVector(t *testing.T) {
	// "[1,2,3]" is exactly representable in every element type, so a decoded
	// mismatch means the value went through the wrong conversion.
	t.Run("stores element-typed bytes", func(t *testing.T) {
		// The stored varlena must be sized by the column's own element type;
		// vecf32 (4 bytes/element) is included to show what a mis-instantiated
		// arm would produce.
		for _, nt := range append([]struct {
			name     string
			oid      types.T
			elemSize int
		}{{"vecf32", types.T_array_float32, 4}}, narrowArrayTypes...) {
			vec, err := narrowArrayGetColData(t, nt.oid, 3, "[1,2,3]")
			require.NoError(t, err)
			require.Equal(t, 1, vec.Length())
			require.Len(t, vec.GetBytesAt(0), 3*nt.elemSize,
				"%s must store %d bytes per element", nt.name, nt.elemSize)
		}

		vec, err := narrowArrayGetColData(t, types.T_array_bf16, 3, "[1,2,3]")
		require.NoError(t, err)
		requireArrayAt(t, vec, []types.BF16{
			types.BF16FromFloat32(1), types.BF16FromFloat32(2), types.BF16FromFloat32(3),
		})

		vec, err = narrowArrayGetColData(t, types.T_array_float16, 3, "[1,2,3]")
		require.NoError(t, err)
		requireArrayAt(t, vec, []types.Float16{
			types.Float16FromFloat32(1), types.Float16FromFloat32(2), types.Float16FromFloat32(3),
		})

		vec, err = narrowArrayGetColData(t, types.T_array_int8, 3, "[1,2,3]")
		require.NoError(t, err)
		requireArrayAt(t, vec, []int8{1, 2, 3})

		vec, err = narrowArrayGetColData(t, types.T_array_uint8, 3, "[1,2,3]")
		require.NoError(t, err)
		requireArrayAt(t, vec, []uint8{1, 2, 3})
	})

	// Dimension enforcement lives inside each arm; dropping it would let a
	// wrong-width row into a fixed-dimension column and corrupt every reader
	// that slices by the declared width.
	t.Run("dimension mismatch", func(t *testing.T) {
		_, refErr := narrowArrayGetColData(t, types.T_array_float32, 4, "[1,2,3]")
		require.Error(t, refErr)
		for _, nt := range narrowArrayTypes {
			_, err := narrowArrayGetColData(t, nt.oid, 4, "[1,2,3]")
			require.Error(t, err, "%s must reject a 3-dim value in a 4-dim column", nt.name)
			require.Equal(t, refErr.Error(), err.Error(),
				"%s must report the dimension mismatch exactly like vecf32", nt.name)
		}

		// MaxArrayDimension means "unconstrained": the check must be skipped,
		// as it is for vecf32, otherwise no dimensionless column could load.
		for _, nt := range narrowArrayTypes {
			_, err := narrowArrayGetColData(t, nt.oid, types.MaxArrayDimension, "[1,2,3]")
			require.NoError(t, err, "%s must accept any dimension when width is MaxArrayDimension", nt.name)
		}
	})

	// A malformed field must come back as an error rather than a silently
	// truncated / empty vector.
	t.Run("parse error", func(t *testing.T) {
		for _, bad := range []string{"[1,2", "abc", "[a,b,c]"} {
			_, refErr := narrowArrayGetColData(t, types.T_array_float32, 3, bad)
			require.Error(t, refErr)
			for _, nt := range narrowArrayTypes {
				_, err := narrowArrayGetColData(t, nt.oid, 3, bad)
				require.Error(t, err, "%s must reject %q", nt.name, bad)
			}
		}
	})

	// The AppendBytes error return is only reachable when the mpool refuses the
	// allocation. It matters because a swallowed error there would silently
	// drop a row from an imported table; a capped pool is the only way to make
	// a unit test reach it.
	t.Run("append error is surfaced", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		// One long row, repeatedly appended to the same vector, so the varlena
		// area outgrows the pool cap after a bounded number of rows.
		val := "[" + strings.Repeat("1,", 20000) + "1]"
		// vecf32 is included as the reference: whatever it does with an
		// exhausted pool is what the narrow arms must do too.
		probes := append([]struct {
			name     string
			oid      types.T
			elemSize int
		}{{"vecf32", types.T_array_float32, 4}}, narrowArrayTypes...)
		for _, nt := range probes {
			mp, err := mpool.NewMPool("narrow-vec-oom-"+nt.name, 1024*1024, mpool.NoFixed)
			require.NoError(t, err)

			colType := plan.Type{Id: int32(nt.oid), Width: types.MaxArrayDimension}
			bat := batch.New([]string{"v"})
			// Off-heap: the mpool only enforces its cap on off-heap
			// allocations, so this is what makes the pool actually refuse.
			bat.Vecs[0] = vector.NewOffHeapVecWithType(types.New(nt.oid, types.MaxArrayDimension, 0))
			param := &ExternalParam{
				ExParamConst: ExParamConst{
					Ctx: context.Background(),
					Extern: &tree.ExternParam{
						ExParamConst: tree.ExParamConst{Format: tree.CSV},
					},
					Cols: []*plan.ColDef{{Name: "v", Typ: colType}},
				},
			}
			attr := plan.ExternAttr{ColName: "v", ColIndex: 0, ColFieldIndex: 0}

			var gotErr error
			for i := 0; i < 200 && gotErr == nil; i++ {
				gotErr = getColData(bat, []csvparser.Field{{Val: val}}, i, param, mp, attr, proc)
			}
			require.Error(t, gotErr, "%s must propagate the mpool failure", nt.name)
			bat.Clean(mp)
			mpool.DeleteMPool(mp)
		}
	})

	// Same wiring probe as in isLegalLine: vecf32 accepts these, the integer
	// element types must not, which is only true if the arm instantiates the
	// generic with its own element type.
	t.Run("wiring", func(t *testing.T) {
		for _, wc := range []struct {
			oid types.T
			val string
		}{
			{types.T_array_int8, "[128]"},
			{types.T_array_int8, "[1.5]"},
			{types.T_array_uint8, "[256]"},
			{types.T_array_uint8, "[-1]"},
		} {
			_, refErr := narrowArrayGetColData(t, types.T_array_float32, 1, wc.val)
			require.NoError(t, refErr, "precondition: vecf32 accepts %q", wc.val)
			_, err := narrowArrayGetColData(t, wc.oid, 1, wc.val)
			require.Error(t, err, "%s must reject %q", wc.oid, wc.val)
		}
	})
}
