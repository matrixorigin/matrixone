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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestTextMinMaxUsesGeneralCICollation(t *testing.T) {
	values := []string{"a", "b", "c", "E", "C", "D"}
	testCases := []struct {
		name    string
		oid     types.T
		charset uint8
		legacy  bool
		aggID   int64
		expect  string
	}{
		{name: "char min", oid: types.T_char, aggID: AggIdOfMin, expect: "a"},
		{name: "char max", oid: types.T_char, aggID: AggIdOfMax, expect: "E"},
		{name: "varchar min", oid: types.T_varchar, aggID: AggIdOfMin, expect: "a"},
		{name: "varchar max", oid: types.T_varchar, aggID: AggIdOfMax, expect: "E"},
		{name: "text min", oid: types.T_text, aggID: AggIdOfMin, expect: "a"},
		{name: "text max", oid: types.T_text, aggID: AggIdOfMax, expect: "E"},
		{name: "varchar binary collation min", oid: types.T_varchar, charset: types.CharsetUTF8MB4Bin, aggID: AggIdOfMin, expect: "C"},
		{name: "varchar binary collation max", oid: types.T_varchar, charset: types.CharsetUTF8MB4Bin, aggID: AggIdOfMax, expect: "c"},
		{name: "legacy varchar min", oid: types.T_varchar, legacy: true, aggID: AggIdOfMin, expect: "C"},
		{name: "legacy varchar max", oid: types.T_varchar, legacy: true, aggID: AggIdOfMax, expect: "c"},
		{name: "binary min", oid: types.T_binary, aggID: AggIdOfMin, expect: "C"},
		{name: "binary max", oid: types.T_binary, aggID: AggIdOfMax, expect: "c"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.New(tc.oid, 10, 0)
			if tc.charset != types.CharsetLegacy || tc.legacy {
				typ = types.NewWithCharset(tc.oid, 10, 0, tc.charset)
			}
			vec := vector.NewVec(typ)
			for _, value := range values {
				require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
			}
			defer vec.Free(mp)

			agg := makeMinMaxExec(mp, tc.aggID, tc.aggID == AggIdOfMin, typ)
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{vec}))

			results, err := agg.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.expect, string(results[0].GetBytesAt(0)))

			agg.Free()
			for _, result := range results {
				result.Free(mp)
			}
		})
	}
}

func TestTextMinMaxGeneralCIWeights(t *testing.T) {
	mp := mpool.MustNewZero()
	exec := newTextMinMaxExec(
		mp, AggIdOfMin, true, types.New(types.T_varchar, 10, 0),
	).(*minMaxExecBytes)
	defer exec.Free()

	require.Zero(t, exec.comp([]byte("A"), []byte("a")))
	require.Zero(t, exec.comp([]byte("å"), []byte("a")))
	require.Positive(t, exec.comp([]byte("ａ"), []byte("a")))
	require.Zero(t, exec.comp([]byte("a "), []byte("a")))
	require.Zero(t, exec.comp([]byte("ß"), []byte("s")))
	require.Negative(t, exec.comp([]byte("ß"), []byte("ss")))
	require.Negative(t, exec.comp([]byte("中"), []byte("文")))
	require.Zero(t, exec.comp([]byte("😜"), []byte("😃")))
	require.Positive(t, exec.comp([]byte{0xff}, []byte{0xfe}))
}

func TestTextMinMaxGeneralCIMalformedUTF8IsTransitive(t *testing.T) {
	values := [][]byte{
		{0x80},
		[]byte("z"),
		[]byte("é"),
		{0xff},
		{0xc3},
		[]byte("A"),
		[]byte("a "),
	}
	for _, a := range values {
		for _, b := range values {
			require.Equal(t, -compareUTF8mb4GeneralCI(b, a), compareUTF8mb4GeneralCI(a, b))
			for _, c := range values {
				if compareUTF8mb4GeneralCI(a, b) > 0 && compareUTF8mb4GeneralCI(b, c) > 0 {
					require.Positive(t, compareUTF8mb4GeneralCI(a, c),
						"non-transitive ordering for %x > %x > %x", a, b, c)
				}
			}
		}
	}

	// This is the concrete cycle that raw-byte fallback used to create.
	require.Positive(t, compareUTF8mb4GeneralCI([]byte{0x80}, []byte("z")))
	require.Positive(t, compareUTF8mb4GeneralCI([]byte("z"), []byte("é")))
	require.Negative(t, compareUTF8mb4GeneralCI([]byte("é"), []byte{0x80}))
}

func TestTextMaxGeneralCIDoesNotExpandSharpS(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.New(types.T_varchar, 10, 0)
	vec := vector.NewVec(typ)
	defer vec.Free(mp)
	for _, value := range []string{"ß", "ss"} {
		require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
	}

	agg := newTextMinMaxExec(mp, AggIdOfMax, false, typ)
	defer agg.Free()
	require.NoError(t, agg.GroupGrow(1))
	require.NoError(t, agg.BulkFill(0, []*vector.Vector{vec}))

	results, err := agg.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	defer results[0].Free(mp)
	require.Equal(t, "ss", string(results[0].GetBytesAt(0)))
}

func TestTextMinMaxGeneralCIMerge(t *testing.T) {
	testCases := []struct {
		name   string
		aggID  int64
		expect string
	}{
		{name: "min", aggID: AggIdOfMin, expect: "a"},
		{name: "max", aggID: AggIdOfMax, expect: "E"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.New(types.T_varchar, 10, 0)
			newInput := func(values ...string) *vector.Vector {
				vec := vector.NewVec(typ)
				for _, value := range values {
					require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
				}
				return vec
			}

			leftVec := newInput("C", "E")
			rightVec := newInput("a", "c")
			defer leftVec.Free(mp)
			defer rightVec.Free(mp)

			left := makeMinMaxExec(mp, tc.aggID, tc.aggID == AggIdOfMin, typ)
			right := makeMinMaxExec(mp, tc.aggID, tc.aggID == AggIdOfMin, typ)
			defer left.Free()
			defer right.Free()

			require.NoError(t, left.GroupGrow(1))
			require.NoError(t, right.GroupGrow(1))
			require.NoError(t, left.BulkFill(0, []*vector.Vector{leftVec}))
			require.NoError(t, right.BulkFill(0, []*vector.Vector{rightVec}))
			require.NoError(t, left.Merge(right, 0, 0))

			results, err := left.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			defer results[0].Free(mp)
			require.Equal(t, tc.expect, string(results[0].GetBytesAt(0)))
		})
	}
}

func mustParseAggDecimal256(t *testing.T, value string, scale int32) types.Decimal256 {
	t.Helper()
	dec, err := types.ParseDecimal256(value, 65, scale)
	require.NoError(t, err)
	return dec
}

func TestDecimal256MinMax(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.New(types.T_decimal256, 65, 4)
	values := []types.Decimal256{
		mustParseAggDecimal256(t, "12.3412", 4),
		mustParseAggDecimal256(t, "-9.8765", 4),
		mustParseAggDecimal256(t, "7.7777", 4),
	}

	vec := vector.NewVec(typ)
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(vec, value, false, mp))
	}
	defer vec.Free(mp)

	testCases := []struct {
		name   string
		aggID  int64
		expect types.Decimal256
	}{
		{
			name:   "min",
			aggID:  AggIdOfMin,
			expect: mustParseAggDecimal256(t, "-9.8765", 4),
		},
		{
			name:   "max",
			aggID:  AggIdOfMax,
			expect: mustParseAggDecimal256(t, "12.3412", 4),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			agg := makeMinMaxExec(mp, tc.aggID, tc.aggID == AggIdOfMin, typ)
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{vec}))

			results, err := agg.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.expect, vector.MustFixedColNoTypeCheck[types.Decimal256](results[0])[0])

			agg.Free()
			for _, result := range results {
				result.Free(mp)
			}
		})
	}
}

func TestMinMaxPreservesWinningPrepareParamKind(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte("5"), false, mp))
	require.NoError(t, vector.AppendBytes(input, []byte("9"), false, mp))
	input.SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamNone,
	})
	defer func() {
		input.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	for _, tc := range []struct {
		name string
		id   int64
		want vector.PrepareParamKind
	}{
		{name: "min", id: AggIdOfMin, want: vector.PrepareParamInteger},
		{name: "max", id: AggIdOfMax, want: vector.PrepareParamNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agg := makeMinMaxExec(mp, tc.id, tc.id == AggIdOfMin, types.T_text.ToType())
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{input}))
			results, err := agg.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.want, results[0].GetPrepareParamKindAt(0))
			results[0].Free(mp)
			agg.Free()
		})
	}
}

func TestMinMaxEqualValuesFoldPrepareParamKinds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     int64
		isByte bool
	}{
		{name: "min-fixed", id: AggIdOfMin},
		{name: "max-fixed", id: AggIdOfMax},
		{name: "min-bytes", id: AggIdOfMin, isByte: true},
		{name: "max-bytes", id: AggIdOfMax, isByte: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, reverse := range []bool{false, true} {
				t.Run(map[bool]string{false: "forward", true: "reverse"}[reverse], func(t *testing.T) {
					mp := mpool.MustNewZero()
					var input *vector.Vector
					var agg AggFuncExec
					if tc.isByte {
						input = vector.NewVec(types.T_text.ToType())
						require.NoError(t, vector.AppendBytes(input, []byte("5"), false, mp))
						require.NoError(t, vector.AppendBytes(input, []byte("5"), false, mp))
						agg = makeMinMaxExec(mp, tc.id, tc.id == AggIdOfMin, types.T_text.ToType())
					} else {
						input = vector.NewVec(types.T_int64.ToType())
						require.NoError(t, vector.AppendFixed(input, int64(5), false, mp))
						require.NoError(t, vector.AppendFixed(input, int64(5), false, mp))
						agg = makeMinMaxExec(mp, tc.id, tc.id == AggIdOfMin, types.T_int64.ToType())
					}
					kinds := []vector.PrepareParamKind{
						vector.PrepareParamFloat,
						vector.PrepareParamNone,
					}
					if reverse {
						kinds[0], kinds[1] = kinds[1], kinds[0]
					}
					require.NoError(t, input.SetPrepareParamKindsWithMP(kinds, mp))
					require.NoError(t, agg.GroupGrow(1))
					require.NoError(t, agg.BulkFill(0, []*vector.Vector{input}))
					results, err := agg.Flush()
					require.NoError(t, err)
					require.Equal(t, vector.PrepareParamNone, results[0].GetPrepareParamKindAt(0))
					results[0].Free(mp)
					agg.Free()
					input.Free(mp)
					require.Zero(t, mp.CurrNB())
				})
			}
		})
	}
}

func TestMinMaxBatchMergeEqualValuesFoldsPrepareParamKinds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     int64
		isByte bool
	}{
		{name: "fixed", id: AggIdOfMin},
		{name: "bytes", id: AggIdOfMax, isByte: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, reverse := range []bool{false, true} {
				mp := mpool.MustNewZero()
				leftKind, rightKind := vector.PrepareParamFloat, vector.PrepareParamNone
				if reverse {
					leftKind, rightKind = rightKind, leftKind
				}
				makeInput := func(kind vector.PrepareParamKind) *vector.Vector {
					var input *vector.Vector
					if tc.isByte {
						input = vector.NewVec(types.T_text.ToType())
						require.NoError(t, vector.AppendBytes(input, []byte("5"), false, mp))
					} else {
						input = vector.NewVec(types.T_int64.ToType())
						require.NoError(t, vector.AppendFixed(input, int64(5), false, mp))
					}
					input.SetPrepareParamKind(kind)
					return input
				}
				leftInput := makeInput(leftKind)
				rightInput := makeInput(rightKind)
				var typ types.Type
				if tc.isByte {
					typ = types.T_text.ToType()
				} else {
					typ = types.T_int64.ToType()
				}
				left := makeMinMaxExec(mp, tc.id, tc.id == AggIdOfMin, typ)
				right := makeMinMaxExec(mp, tc.id, tc.id == AggIdOfMin, typ)
				require.NoError(t, left.GroupGrow(1))
				require.NoError(t, right.GroupGrow(1))
				require.NoError(t, left.BulkFill(0, []*vector.Vector{leftInput}))
				require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
				require.NoError(t, left.Merge(right, 0, 0))
				results, err := left.Flush()
				require.NoError(t, err)
				require.Equal(t, vector.PrepareParamNone, results[0].GetPrepareParamKindAt(0))
				results[0].Free(mp)
				left.Free()
				right.Free()
				leftInput.Free(mp)
				rightInput.Free(mp)
				require.Zero(t, mp.CurrNB())
			}
		})
	}
}

func TestMinMaxExtraEqualValueFoldsPrepareParamKind(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     int64
		isByte bool
	}{
		{name: "fixed", id: AggIdOfMin},
		{name: "bytes", id: AggIdOfMax, isByte: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			var input *vector.Vector
			var typ types.Type
			var extra any
			if tc.isByte {
				typ = types.T_text.ToType()
				input = vector.NewVec(typ)
				require.NoError(t, vector.AppendBytes(input, []byte("5"), false, mp))
				input.SetPrepareParamKind(vector.PrepareParamFloat)
				extra = []byte("5")
			} else {
				typ = types.T_int64.ToType()
				input = vector.NewVec(typ)
				require.NoError(t, vector.AppendFixed(input, int64(5), false, mp))
				input.SetPrepareParamKind(vector.PrepareParamFloat)
				extra = int64(5)
			}
			agg := makeMinMaxExec(mp, tc.id, tc.id == AggIdOfMin, typ)
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{input}))
			require.NoError(t, agg.SetExtraInformation(extra, 0))
			results, err := agg.Flush()
			require.NoError(t, err)
			require.Equal(t, vector.PrepareParamNone, results[0].GetPrepareParamKindAt(0))
			results[0].Free(mp)
			agg.Free()
			input.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}
