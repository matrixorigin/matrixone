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
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestFixedMinMaxPreservesAndMergesStringSources(t *testing.T) {
	for _, valueType := range []struct {
		name       string
		typ        types.Type
		low, high  any
		equalValue any
	}{
		{name: "int64", typ: types.T_int64.ToType(), low: int64(1), high: int64(2), equalValue: int64(7)},
		{name: "date", typ: types.T_date.ToType(), low: types.Date(1), high: types.Date(2), equalValue: types.Date(7)},
		{name: "decimal64", typ: types.T_decimal64.ToType(), low: types.Decimal64(1), high: types.Decimal64(2), equalValue: types.Decimal64(7)},
	} {
		for _, aggID := range []int64{AggIdOfMin, AggIdOfMax} {
			t.Run(fmt.Sprintf("%s/%d", valueType.name, aggID), func(t *testing.T) {
				mp := mpool.MustNewZero()
				input := vector.NewVec(valueType.typ)
				require.NoError(t, vector.AppendAny(input, valueType.low, false, mp))
				require.NoError(t, vector.AppendAny(input, valueType.high, false, mp))
				require.NoError(t, input.SetStringSourcesWithMP([]types.StringSource{
					types.StringSourceSQLPrepare, types.StringSourceCOMStmt,
				}, mp))
				exec := makeMinMaxExec(mp, aggID, aggID == AggIdOfMin, valueType.typ)
				require.NoError(t, exec.GroupGrow(1))
				require.NoError(t, exec.BulkFill(0, []*vector.Vector{input}))
				results, err := exec.Flush()
				require.NoError(t, err)
				want := types.StringSourceSQLPrepare
				if aggID == AggIdOfMax {
					want = types.StringSourceCOMStmt
				}
				require.Equal(t, want, results[0].GetStringSourceAt(0))
				results[0].Free(mp)
				exec.Free()
				input.Free(mp)

				leftInput := vector.NewVec(valueType.typ)
				rightInput := vector.NewVec(valueType.typ)
				require.NoError(t, vector.AppendAny(leftInput, valueType.equalValue, false, mp))
				require.NoError(t, vector.AppendAny(rightInput, valueType.equalValue, false, mp))
				require.NoError(t, leftInput.SetStringSource(types.StringSourceLiteral))
				require.NoError(t, rightInput.SetStringSource(types.StringSourceUserVariable))
				left := makeMinMaxExec(mp, aggID, aggID == AggIdOfMin, valueType.typ)
				right := makeMinMaxExec(mp, aggID, aggID == AggIdOfMin, valueType.typ)
				require.NoError(t, left.GroupGrow(1))
				require.NoError(t, right.GroupGrow(1))
				require.NoError(t, left.BulkFill(0, []*vector.Vector{leftInput}))
				require.NoError(t, right.BulkFill(0, []*vector.Vector{rightInput}))
				require.NoError(t, left.Merge(right, 0, 0))
				results, err = left.Flush()
				require.NoError(t, err)
				require.Equal(t, types.StringSourceExpression, results[0].GetStringSourceAt(0))
				results[0].Free(mp)
				left.Free()
				right.Free()
				leftInput.Free(mp)
				rightInput.Free(mp)
				require.Zero(t, mp.CurrNB())
			})
		}
	}
}

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

func TestTextMinMaxUTF8mb4BinUsesPadSpace(t *testing.T) {
	space := []byte("a ")
	nul := []byte{'a', 0}

	// Raw byte order places the trailing space after NUL. PAD SPACE removes
	// that space first, so the ordering reverses rather than merely selecting a
	// different representative from an equal pair.
	require.Positive(t, bytes.Compare(space, nul))
	require.Negative(t, compareUTF8mb4Bin(space, nul))
	require.Zero(t, compareUTF8mb4Bin([]byte("a  "), []byte("a")))

	testCases := []struct {
		name    string
		charset uint8
		aggID   int64
		expect  []byte
	}{
		{name: "utf8mb4_bin min", charset: types.CharsetUTF8MB4Bin, aggID: AggIdOfMin, expect: space},
		{name: "utf8mb4_bin max", charset: types.CharsetUTF8MB4Bin, aggID: AggIdOfMax, expect: nul},
		{name: "binary text remains raw", charset: types.CharsetBinary, aggID: AggIdOfMin, expect: nul},
		{name: "legacy text remains raw", charset: types.CharsetLegacy, aggID: AggIdOfMin, expect: nul},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.NewWithCharset(types.T_varchar, 10, 0, tc.charset)
			vec := vector.NewVec(typ)
			require.NoError(t, vector.AppendBytes(vec, space, false, mp))
			require.NoError(t, vector.AppendBytes(vec, nul, false, mp))
			defer vec.Free(mp)

			agg := makeMinMaxExec(mp, tc.aggID, tc.aggID == AggIdOfMin, typ)
			defer agg.Free()
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{vec}))

			results, err := agg.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			defer results[0].Free(mp)
			require.Equal(t, tc.expect, results[0].GetBytesAt(0))
		})
	}
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

func TestMinMaxMergePreservesSourceContract(t *testing.T) {
	for _, test := range []struct {
		name string
		typ  types.Type
		want any
		fill func(*testing.T, *vector.Vector, *mpool.MPool, int)
		read func(*vector.Vector) any
	}{
		{
			name: "fixed",
			typ:  types.T_int64.ToType(),
			want: int64(7),
			fill: func(t *testing.T, vec *vector.Vector, mp *mpool.MPool, value int) {
				require.NoError(t, vector.AppendFixed(vec, int64(value), false, mp))
			},
			read: func(vec *vector.Vector) any {
				return vector.MustFixedColWithTypeCheck[int64](vec)[0]
			},
		},
		{
			name: "bytes",
			typ:  types.T_varchar.ToType(),
			want: "7",
			fill: func(t *testing.T, vec *vector.Vector, mp *mpool.MPool, value int) {
				require.NoError(t, vector.AppendBytes(vec, []byte(fmt.Sprint(value)), false, mp))
			},
			read: func(vec *vector.Vector) any {
				return string(vec.GetBytesAt(0))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			makeState := func(value int) AggFuncExec {
				input := vector.NewVec(test.typ)
				test.fill(t, input, mp, value)
				agg := makeMinMaxExec(mp, AggIdOfMax, false, test.typ)
				require.NoError(t, agg.GroupGrow(1))
				require.NoError(t, agg.Fill(0, 0, []*vector.Vector{input}))
				input.Free(mp)
				return agg
			}

			destination, source := makeState(3), makeState(7)
			require.True(t, MergePreservesSource(source))
			require.NoError(t, destination.Merge(source, 0, 0))
			require.NoError(t, destination.Merge(source, 0, 0))

			sourceResult, err := source.Flush()
			require.NoError(t, err)
			require.Equal(t, test.want, test.read(sourceResult[0]))
			destinationResult, err := destination.Flush()
			require.NoError(t, err)
			require.Equal(t, test.want, test.read(destinationResult[0]))

			sourceResult[0].Free(mp)
			destinationResult[0].Free(mp)
			source.Free()
			destination.Free()
			require.Zero(t, mp.CurrNB())
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
	require.NoError(t, input.SetBinaryStringRowsWithMP([]bool{true, false}, mp))
	defer func() {
		input.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	for _, tc := range []struct {
		name   string
		id     int64
		want   vector.PrepareParamKind
		binary bool
	}{
		{name: "min", id: AggIdOfMin, want: vector.PrepareParamInteger, binary: true},
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
			require.Equal(t, tc.binary, results[0].GetBinaryStringMetadataAt(0))
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

func TestMinPreservesExplicitTextFromNullSlot(t *testing.T) {
	mp := mpool.MustNewZero()
	input := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte("text"), false, mp))
	require.NoError(t, input.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))
	agg := makeMinMaxExec(mp, AggIdOfMin, true, types.T_varbinary.ToType())
	require.NoError(t, agg.GroupGrow(1))
	require.NoError(t, agg.BulkFill(0, []*vector.Vector{input}))
	results, err := agg.Flush()
	require.NoError(t, err)
	require.Equal(t, types.RuntimeStringText, results[0].GetRuntimeStringDomainAt(0))
	results[0].Free(mp)
	agg.Free()
	input.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestMinMaxEqualValuesPreserveExplicitText(t *testing.T) {
	for _, id := range []int64{AggIdOfMin, AggIdOfMax} {
		t.Run(fmt.Sprintf("agg_%d", id), func(t *testing.T) {
			mp := mpool.MustNewZero()
			input := vector.NewVec(types.T_varbinary.ToType())
			require.NoError(t, vector.AppendBytes(input, []byte("same"), false, mp))
			require.NoError(t, vector.AppendBytes(input, []byte("same"), false, mp))
			require.NoError(t, input.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))
			agg := makeMinMaxExec(mp, id, id == AggIdOfMin, types.T_varbinary.ToType())
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{input}))
			results, err := agg.Flush()
			require.NoError(t, err)
			require.Equal(t, types.RuntimeStringText, results[0].GetRuntimeStringDomainAt(0))
			results[0].Free(mp)
			agg.Free()
			input.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestMinMaxEqualValuesMergeEffectiveStringDomains(t *testing.T) {
	for _, id := range []int64{AggIdOfMin, AggIdOfMax} {
		for _, textFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("agg_%d_text_first_%t", id, textFirst), func(t *testing.T) {
				mp := mpool.MustNewZero()
				input := vector.NewVec(types.T_varbinary.ToType())
				require.NoError(t, vector.AppendBytes(input, []byte("same"), false, mp))
				require.NoError(t, vector.AppendBytes(input, []byte("same"), false, mp))
				textRow := 1
				if textFirst {
					textRow = 0
				}
				require.NoError(t, input.SetRuntimeStringDomainAtWithMP(textRow, types.RuntimeStringText, mp))
				agg := makeMinMaxExec(mp, id, id == AggIdOfMin, types.T_varbinary.ToType())
				require.NoError(t, agg.GroupGrow(1))
				require.NoError(t, agg.BulkFill(0, []*vector.Vector{input}))
				results, err := agg.Flush()
				require.NoError(t, err)
				require.Equal(t, types.RuntimeStringInherit, results[0].GetRuntimeStringDomainAt(0))
				results[0].Free(mp)
				agg.Free()
				input.Free(mp)
				require.Zero(t, mp.CurrNB())
			})
		}
	}
}

func TestMinMaxEqualPartialMergeUsesEffectiveStringDomains(t *testing.T) {
	for _, id := range []int64{AggIdOfMin, AggIdOfMax} {
		for _, textLeft := range []bool{false, true} {
			t.Run(fmt.Sprintf("agg_%d_text_left_%t", id, textLeft), func(t *testing.T) {
				mp := mpool.MustNewZero()
				makeState := func(domain types.RuntimeStringDomain) AggFuncExec {
					input := vector.NewVec(types.T_varbinary.ToType())
					require.NoError(t, vector.AppendBytes(input, []byte("same"), false, mp))
					if domain != types.RuntimeStringInherit {
						require.NoError(t, input.SetRuntimeStringDomainWithMP(domain, mp))
					}
					agg := makeMinMaxExec(mp, id, id == AggIdOfMin, types.T_varbinary.ToType())
					require.NoError(t, agg.GroupGrow(1))
					require.NoError(t, agg.Fill(0, 0, []*vector.Vector{input}))
					input.Free(mp)
					return agg
				}
				leftDomain, rightDomain := types.RuntimeStringInherit, types.RuntimeStringText
				if textLeft {
					leftDomain, rightDomain = rightDomain, leftDomain
				}
				left := makeState(leftDomain)
				right := makeState(rightDomain)
				require.NoError(t, left.Merge(right, 0, 0))
				results, err := left.Flush()
				require.NoError(t, err)
				require.Equal(t, types.RuntimeStringInherit, results[0].GetRuntimeStringDomainAt(0))
				results[0].Free(mp)
				left.Free()
				right.Free()
				require.Zero(t, mp.CurrNB())
			})
		}
	}
}

func TestMinEqualMergePreservesExplicitText(t *testing.T) {
	mp := mpool.MustNewZero()
	makeState := func() AggFuncExec {
		input := vector.NewVec(types.T_varbinary.ToType())
		require.NoError(t, vector.AppendBytes(input, []byte("same"), false, mp))
		require.NoError(t, input.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))
		agg := makeMinMaxExec(mp, AggIdOfMin, true, types.T_varbinary.ToType())
		require.NoError(t, agg.GroupGrow(1))
		require.NoError(t, agg.Fill(0, 0, []*vector.Vector{input}))
		input.Free(mp)
		return agg
	}
	left, right := makeState(), makeState()
	require.NoError(t, left.Merge(right, 0, 0))
	results, err := left.Flush()
	require.NoError(t, err)
	require.Equal(t, types.RuntimeStringText, results[0].GetRuntimeStringDomainAt(0))
	results[0].Free(mp)
	left.Free()
	right.Free()
	require.Zero(t, mp.CurrNB())
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

func TestMinMaxFixedBatchPreflightsExistingMetadataAndSkipsNull(t *testing.T) {
	mp := mpool.MustNewZero()
	agg := makeMinMaxExec(mp, AggIdOfMin, true, types.T_int64.ToType())
	require.NoError(t, agg.GroupGrow(2))

	seed := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(seed, int64(10), false, mp))
	require.NoError(t, seed.SetStringSource(types.StringSourceCOMStmt))
	require.NoError(t, agg.BatchFill(0, []uint64{1}, []*vector.Vector{seed}))

	input := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(input, int64(5), false, mp))
	require.NoError(t, vector.AppendFixed(input, int64(0), true, mp))
	require.False(t, input.HasStringSourceMetadata())
	require.NoError(t, agg.BatchFill(0, []uint64{1, 2}, []*vector.Vector{input}))

	constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(3), 2, mp)
	require.NoError(t, err)
	require.NoError(t, constant.SetStringSource(types.StringSourceLiteral))
	require.NoError(t, agg.BatchFill(0, []uint64{1, 2}, []*vector.Vector{constant}))

	results, err := agg.Flush()
	require.NoError(t, err)
	for row := range 2 {
		require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](results[0], row))
		require.Equal(t, types.StringSourceLiteral,
			results[0].GetStringSourceAt(row))
	}
	results[0].Free(mp)
	agg.Free()
	seed.Free(mp)
	input.Free(mp)
	constant.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestMinMaxFixedBatchOverflowSlotsPreservesSourceMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	const groupsCount = 256
	input := vector.NewVec(types.T_int64.ToType())
	groups := make([]uint64, groupsCount)
	for row := range groupsCount {
		require.NoError(t, vector.AppendFixed(input, int64(10), false, mp))
		groups[row] = uint64(row + 1)
	}
	require.NoError(t, input.SetStringSource(types.StringSourceCOMStmt))
	agg := makeMinMaxExec(mp, AggIdOfMin, true, types.T_int64.ToType())
	require.NoError(t, agg.GroupGrow(groupsCount))
	require.NoError(t, agg.BatchFill(0, groups, []*vector.Vector{input}))

	for row := range groupsCount {
		vector.MustFixedColNoTypeCheck[int64](input)[row] = 5
	}
	require.NoError(t, input.SetStringSource(types.StringSourceLiteral))
	require.NoError(t, agg.BatchFill(0, groups, []*vector.Vector{input}))
	require.NoError(t, input.SetStringSource(types.StringSourceUserVariable))
	require.NoError(t, agg.BatchFill(0, groups, []*vector.Vector{input}))

	results, err := agg.Flush()
	require.NoError(t, err)
	for row := range groupsCount {
		require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](results[0], row))
		require.Equal(t, types.StringSourceExpression,
			results[0].GetStringSourceAt(row))
	}
	results[0].Free(mp)
	agg.Free()
	input.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestMinMaxExtraSourceOwnershipForWinsAndLosses(t *testing.T) {
	for _, test := range []struct {
		name      string
		typ       types.Type
		input     any
		extra     any
		want      types.StringSource
		appendVal func(*vector.Vector, any, *mpool.MPool) error
	}{
		{name: "fixed wins", typ: types.T_int64.ToType(), input: int64(5), extra: int64(3), want: types.StringSourceExpression,
			appendVal: func(vec *vector.Vector, value any, mp *mpool.MPool) error {
				return vector.AppendFixed(vec, value.(int64), false, mp)
			}},
		{name: "fixed loses", typ: types.T_int64.ToType(), input: int64(5), extra: int64(7), want: types.StringSourceCOMStmt,
			appendVal: func(vec *vector.Vector, value any, mp *mpool.MPool) error {
				return vector.AppendFixed(vec, value.(int64), false, mp)
			}},
		{name: "bytes wins", typ: types.T_text.ToType(), input: []byte("5"), extra: []byte("3"), want: types.StringSourceExpression,
			appendVal: func(vec *vector.Vector, value any, mp *mpool.MPool) error {
				return vector.AppendBytes(vec, value.([]byte), false, mp)
			}},
		{name: "bytes loses", typ: types.T_text.ToType(), input: []byte("5"), extra: []byte("7"), want: types.StringSourceCOMStmt,
			appendVal: func(vec *vector.Vector, value any, mp *mpool.MPool) error {
				return vector.AppendBytes(vec, value.([]byte), false, mp)
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			input := vector.NewVec(test.typ)
			require.NoError(t, test.appendVal(input, test.input, mp))
			require.NoError(t, input.SetStringSource(types.StringSourceCOMStmt))
			agg := makeMinMaxExec(mp, AggIdOfMin, true, test.typ)
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{input}))
			require.NoError(t, agg.SetExtraInformation(test.extra, 0))
			results, err := agg.Flush()
			require.NoError(t, err)
			require.Equal(t, test.want, results[0].GetStringSourceAt(0))
			results[0].Free(mp)
			agg.Free()
			input.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestMinMaxExtraPopulatesEmptyGroupsWithExpressionSource(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   types.Type
		extra any
		want  any
	}{
		{name: "fixed", typ: types.T_int64.ToType(), extra: int64(7), want: int64(7)},
		{name: "bytes", typ: types.T_text.ToType(), extra: []byte("seven"), want: []byte("seven")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			agg := makeMinMaxExec(mp, AggIdOfMin, true, tc.typ)
			require.NoError(t, agg.GroupGrow(2))
			require.NoError(t, agg.SetExtraInformation(tc.extra, 0))
			results, err := agg.Flush()
			require.NoError(t, err)
			require.Equal(t, 2, results[0].Length())
			for row := range 2 {
				require.False(t, results[0].IsNull(uint64(row)))
				require.Equal(t, types.StringSourceExpression,
					results[0].GetStringSourceAt(row))
				if tc.typ.IsVarlen() {
					require.Equal(t, tc.want, results[0].GetBytesAt(row))
				} else {
					require.Equal(t, tc.want,
						vector.GetFixedAtNoTypeCheck[int64](results[0], row))
				}
			}
			results[0].Free(mp)
			agg.Free()
			require.Zero(t, mp.CurrNB())
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
			require.NoError(t, input.SetStringSource(types.StringSourceCOMStmt))
			agg := makeMinMaxExec(mp, tc.id, tc.id == AggIdOfMin, typ)
			require.NoError(t, agg.GroupGrow(1))
			require.NoError(t, agg.BulkFill(0, []*vector.Vector{input}))
			require.NoError(t, agg.SetExtraInformation(extra, 0))
			results, err := agg.Flush()
			require.NoError(t, err)
			require.Equal(t, vector.PrepareParamNone, results[0].GetPrepareParamKindAt(0))
			require.Equal(t, types.StringSourceExpression, results[0].GetStringSourceAt(0))
			results[0].Free(mp)
			agg.Free()
			input.Free(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}
