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

package function

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type binaryStringTestFn func(
	[]*vector.Vector,
	vector.FunctionResultWrapper,
	*process.Process,
	int,
	*FunctionSelectList,
) error

func makeBinaryStringTestInput(
	t *testing.T,
	proc *process.Process,
	typ types.Type,
	values [][]byte,
	domains []types.RuntimeStringDomain,
) *vector.Vector {
	t.Helper()
	input := testutil.MakeVarlenaVector(values, nil, typ, proc.Mp())
	if domains != nil {
		require.NoError(t, input.SetRuntimeStringDomainsWithMP(domains, proc.Mp()))
	}
	return input
}

func makeBinaryStringInt64Input(t *testing.T, proc *process.Process, values []int64) *vector.Vector {
	t.Helper()
	input := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(input, values, nil, proc.Mp()))
	return input
}

func runBinaryStringBytesFn(
	t *testing.T,
	proc *process.Process,
	fn binaryStringTestFn,
	resultType types.Type,
	inputs ...*vector.Vector,
) vector.FunctionResultWrapper {
	t.Helper()
	result := vector.NewFunctionResultWrapper(resultType, proc.Mp())
	require.NoError(t, result.PreExtendAndReset(inputs[0].Length()))
	require.NoError(t, fn(inputs, result, proc, inputs[0].Length(), nil))
	return result
}

func binaryStringResultBytes(result vector.FunctionResultWrapper) [][]byte {
	vec := result.GetResultVector()
	values := make([][]byte, vec.Length())
	for row := range values {
		if !vec.IsNull(uint64(row)) {
			values[row] = append([]byte(nil), vec.GetBytesAt(row)...)
		}
	}
	return values
}

func TestBinaryStringMixedLengthOrdAndPosition(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	domains := []types.RuntimeStringDomain{types.RuntimeStringInherit, types.RuntimeStringBinary}

	input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
		[]byte("你a"), []byte("你a"),
	}, domains)
	defer input.Free(mp)

	for _, test := range []struct {
		name string
		fn   binaryStringTestFn
		want []uint64
	}{
		{name: "char length", fn: LengthUTF8, want: []uint64{2, 4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), mp)
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(input.Length()))
			require.NoError(t, test.fn([]*vector.Vector{input}, result, proc, input.Length(), nil))
			require.Equal(t, test.want, vector.MustFixedColNoTypeCheck[uint64](result.GetResultVector()))
		})
	}

	ordResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
	defer ordResult.Free()
	require.NoError(t, ordResult.PreExtendAndReset(input.Length()))
	require.NoError(t, Ord([]*vector.Vector{input}, ordResult, proc, input.Length(), nil))
	require.Equal(t, []int64{14990752, 228}, vector.MustFixedColNoTypeCheck[int64](ordResult.GetResultVector()))

	needle := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{{0xbd}, {0xbd}}, nil)
	defer needle.Free(mp)
	locateResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
	defer locateResult.Free()
	require.NoError(t, locateResult.PreExtendAndReset(input.Length()))
	require.NoError(t, buildInLocate2Args(
		[]*vector.Vector{needle, input}, locateResult, proc, input.Length(), nil))
	require.Equal(t, []int64{0, 2}, vector.MustFixedColNoTypeCheck[int64](locateResult.GetResultVector()))

	instrResult := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
	defer instrResult.Free()
	require.NoError(t, instrResult.PreExtendAndReset(input.Length()))
	require.NoError(t, Instr([]*vector.Vector{input, needle}, instrResult, proc, input.Length(), nil))
	require.Equal(t, []int64{0, 2}, vector.MustFixedColNoTypeCheck[int64](instrResult.GetResultVector()))
}

func TestBinaryStringMixedSlicingReverseAndCase(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	domains := []types.RuntimeStringDomain{types.RuntimeStringInherit, types.RuntimeStringBinary}

	t.Run("left", func(t *testing.T) {
		source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("你a"), []byte("你a"),
		}, domains)
		defer source.Free(mp)
		count := makeBinaryStringInt64Input(t, proc, []int64{1, 1})
		defer count.Free(mp)
		result := runBinaryStringBytesFn(t, proc, Left, types.T_varchar.ToType(), source, count)
		defer result.Free()
		require.Equal(t, [][]byte{[]byte("你"), {0xe4}}, binaryStringResultBytes(result))
		require.Equal(t, types.RuntimeStringBinary, result.GetResultVector().GetRuntimeStringDomainAt(1))
	})

	t.Run("right", func(t *testing.T) {
		source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("a你"), []byte("a你"),
		}, domains)
		defer source.Free(mp)
		count := makeBinaryStringInt64Input(t, proc, []int64{1, 1})
		defer count.Free(mp)
		result := runBinaryStringBytesFn(t, proc, Right, types.T_varchar.ToType(), source, count)
		defer result.Free()
		require.Equal(t, [][]byte{[]byte("你"), {0xa0}}, binaryStringResultBytes(result))
	})

	t.Run("substring", func(t *testing.T) {
		source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("你好"), []byte("你好"),
		}, domains)
		defer source.Free(mp)
		start := makeBinaryStringInt64Input(t, proc, []int64{2, 2})
		defer start.Free(mp)
		count := makeBinaryStringInt64Input(t, proc, []int64{1, 1})
		defer count.Free(mp)
		result := runBinaryStringBytesFn(
			t, proc, SubStringWith3Args, types.T_varchar.ToType(), source, start, count)
		defer result.Free()
		require.Equal(t, [][]byte{[]byte("好"), {0xbd}}, binaryStringResultBytes(result))
	})

	t.Run("reverse invalid utf8", func(t *testing.T) {
		source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("你a"), {0xff, 0xe4, 0xbd, 0xa0},
		}, domains)
		defer source.Free(mp)
		result := runBinaryStringBytesFn(t, proc, Reverse, types.T_varchar.ToType(), source)
		defer result.Free()
		require.Equal(t, [][]byte{[]byte("a你"), {0xa0, 0xbd, 0xe4, 0xff}}, binaryStringResultBytes(result))
	})

	t.Run("lower", func(t *testing.T) {
		source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("ÄA"), []byte("ÄA"),
		}, domains)
		defer source.Free(mp)
		result := runBinaryStringBytesFn(t, proc, builtInToLower, types.T_varchar.ToType(), source)
		defer result.Free()
		require.Equal(t, [][]byte{[]byte("äa"), []byte("ÄA")}, binaryStringResultBytes(result))
	})
}

func TestBinaryStringSubjectControlsInsertPadAndReplace(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	domains := []types.RuntimeStringDomain{types.RuntimeStringInherit, types.RuntimeStringBinary}
	source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
		[]byte("你a"), []byte("你a"),
	}, domains)
	defer source.Free(mp)

	t.Run("insert", func(t *testing.T) {
		position := makeBinaryStringInt64Input(t, proc, []int64{2, 2})
		defer position.Free(mp)
		remove := makeBinaryStringInt64Input(t, proc, []int64{1, 1})
		defer remove.Free(mp)
		replacement := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{{'x'}, {'x'}}, nil)
		defer replacement.Free(mp)
		result := runBinaryStringBytesFn(
			t, proc, Insert, types.T_varchar.ToType(), source, position, remove, replacement)
		defer result.Free()
		require.Equal(t, [][]byte{[]byte("你x"), {0xe4, 'x', 0xa0, 'a'}}, binaryStringResultBytes(result))
	})

	t.Run("lpad", func(t *testing.T) {
		target := makeBinaryStringInt64Input(t, proc, []int64{3, 3})
		defer target.Free(mp)
		pad := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{
			[]byte("好"), []byte("好"),
		}, nil)
		defer pad.Free(mp)
		result := runBinaryStringBytesFn(t, proc, builtInLpad, types.T_text.ToType(), source, target, pad)
		defer result.Free()
		require.Equal(t, [][]byte{append([]byte("好"), []byte("你a")...), {0xe4, 0xbd, 0xa0}}, binaryStringResultBytes(result))
		require.Equal(t, types.RuntimeStringBinary, result.GetResultVector().GetRuntimeStringDomainAt(1))
	})

	t.Run("replace auxiliary does not select domain", func(t *testing.T) {
		needle := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{{0xbd}, {0xbd}}, nil)
		defer needle.Free(mp)
		replacement := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{{'x'}, {'x'}}, nil)
		defer replacement.Free(mp)
		result := runBinaryStringBytesFn(t, proc, Replace, types.T_text.ToType(), source, needle, replacement)
		defer result.Free()
		require.Equal(t, [][]byte{{0xe4, 'x', 0xa0, 'a'}, {0xe4, 'x', 0xa0, 'a'}}, binaryStringResultBytes(result))
		require.Equal(t, types.RuntimeStringInherit, result.GetResultVector().GetRuntimeStringDomainAt(0))
		require.Equal(t, types.RuntimeStringBinary, result.GetResultVector().GetRuntimeStringDomainAt(1))
	})
}

func TestBinaryStringStaticBinaryCanCarryTextRow(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	source := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{
		[]byte("你好"), []byte("你好"),
	}, []types.RuntimeStringDomain{types.RuntimeStringText, types.RuntimeStringInherit})
	defer source.Free(mp)

	lengthResult := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), mp)
	defer lengthResult.Free()
	require.NoError(t, lengthResult.PreExtendAndReset(source.Length()))
	require.NoError(t, LengthBinary([]*vector.Vector{source}, lengthResult, proc, source.Length(), nil))
	require.Equal(t, []uint64{2, 6}, vector.MustFixedColNoTypeCheck[uint64](lengthResult.GetResultVector()))

	count := makeBinaryStringInt64Input(t, proc, []int64{1, 1})
	defer count.Free(mp)
	left := runBinaryStringBytesFn(t, proc, Left, types.T_varbinary.ToType(), source, count)
	defer left.Free()
	require.Equal(t, [][]byte{[]byte("你"), {0xe4}}, binaryStringResultBytes(left))
	require.Equal(t, types.RuntimeStringText, left.GetResultVector().GetRuntimeStringDomainAt(0))
	require.Equal(t, types.RuntimeStringInherit, left.GetResultVector().GetRuntimeStringDomainAt(1))
}

func TestBinaryStringAdditionalTransformsAndMask(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	domains := []types.RuntimeStringDomain{types.RuntimeStringInherit, types.RuntimeStringBinary}

	t.Run("upper", func(t *testing.T) {
		source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("äa"), []byte("äa"),
		}, domains)
		defer source.Free(mp)
		result := runBinaryStringBytesFn(t, proc, builtInToUpper, types.T_varchar.ToType(), source)
		defer result.Free()
		require.Equal(t, [][]byte{[]byte("ÄA"), []byte("äa")}, binaryStringResultBytes(result))
	})

	t.Run("trim and mask", func(t *testing.T) {
		source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte(" 你 "), []byte(" 你 "),
		}, domains)
		defer source.Free(mp)
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(source.Length()))
		require.NoError(t, Ltrim(
			[]*vector.Vector{source}, result, proc, source.Length(),
			&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}))
		require.Equal(t, []byte("你 "), result.GetResultVector().GetBytesAt(0))
		require.True(t, result.GetResultVector().IsNull(1))
		require.Equal(t, types.RuntimeStringInherit, result.GetResultVector().GetRuntimeStringDomainAt(1))

		right := runBinaryStringBytesFn(t, proc, Rtrim, types.T_varchar.ToType(), source)
		defer right.Free()
		require.Equal(t, [][]byte{[]byte(" 你"), []byte(" 你")}, binaryStringResultBytes(right))
		require.Equal(t, types.RuntimeStringBinary, right.GetResultVector().GetRuntimeStringDomainAt(1))
	})

	t.Run("trim explicit", func(t *testing.T) {
		mode := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("both"), []byte("both"),
		}, nil)
		defer mode.Free(mp)
		cut := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{
			[]byte("你"), []byte("你"),
		}, nil)
		defer cut.Free(mp)
		source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("你a你"), []byte("你a你"),
		}, domains)
		defer source.Free(mp)
		result := runBinaryStringBytesFn(t, proc, Trim, types.T_varchar.ToType(), mode, cut, source)
		defer result.Free()
		require.Equal(t, [][]byte{{'a'}, {'a'}}, binaryStringResultBytes(result))
		require.Equal(t, types.RuntimeStringBinary, result.GetResultVector().GetRuntimeStringDomainAt(1))
	})

	t.Run("rpad and repeat", func(t *testing.T) {
		source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("你"), []byte("你"),
		}, domains)
		defer source.Free(mp)
		target := makeBinaryStringInt64Input(t, proc, []int64{2, 2})
		defer target.Free(mp)
		pad := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{{'x'}, {'x'}}, nil)
		defer pad.Free(mp)
		padded := runBinaryStringBytesFn(t, proc, builtInRpad, types.T_text.ToType(), source, target, pad)
		defer padded.Free()
		require.Equal(t, [][]byte{[]byte("你x"), {0xe4, 0xbd}}, binaryStringResultBytes(padded))

		repeated := runBinaryStringBytesFn(t, proc, builtInRepeat, types.T_text.ToType(), source, target)
		defer repeated.Free()
		require.Equal(t, [][]byte{[]byte("你你"), []byte("你你")}, binaryStringResultBytes(repeated))
		require.Equal(t, types.RuntimeStringBinary, repeated.GetResultVector().GetRuntimeStringDomainAt(1))
	})

	t.Run("substring index and split part", func(t *testing.T) {
		source := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("a你b你c"), []byte("a你b你c"),
		}, domains)
		defer source.Free(mp)
		delimiter := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{
			[]byte("你"), []byte("你"),
		}, nil)
		defer delimiter.Free(mp)
		count := makeBinaryStringInt64Input(t, proc, []int64{1, 1})
		defer count.Free(mp)
		indexed := runBinaryStringBytesFn(t, proc, SubStrIndex[int64], types.T_varchar.ToType(), source, delimiter, count)
		defer indexed.Free()
		require.Equal(t, [][]byte{{'a'}, {'a'}}, binaryStringResultBytes(indexed))
		require.Equal(t, types.RuntimeStringBinary, indexed.GetResultVector().GetRuntimeStringDomainAt(1))

		field := vector.NewVec(types.T_uint32.ToType())
		defer field.Free(mp)
		require.NoError(t, vector.AppendFixedList(field, []uint32{2, 2}, nil, mp))
		split := runBinaryStringBytesFn(t, proc, SplitPart, types.T_varchar.ToType(), source, delimiter, field)
		defer split.Free()
		require.Equal(t, [][]byte{{'b'}, {'b'}}, binaryStringResultBytes(split))
		require.Equal(t, types.RuntimeStringBinary, split.GetResultVector().GetRuntimeStringDomainAt(1))
	})

	t.Run("locate with start", func(t *testing.T) {
		needle := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{{'b'}, {'b'}}, nil)
		defer needle.Free(mp)
		haystack := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
			[]byte("a你b"), []byte("a你b"),
		}, domains)
		defer haystack.Free(mp)
		start := makeBinaryStringInt64Input(t, proc, []int64{2, 2})
		defer start.Free(mp)
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))
		require.NoError(t, buildInLocate3Args(
			[]*vector.Vector{needle, haystack, start}, result, proc, 2, nil))
		require.Equal(t, []int64{3, 5}, vector.MustFixedColNoTypeCheck[int64](result.GetResultVector()))
	})
}

func TestBinaryStringResultDomainResetAndAllocationFailure(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	binaryRow := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{{'a'}, {'b'}},
		[]types.RuntimeStringDomain{types.RuntimeStringInherit, types.RuntimeStringBinary})
	defer binaryRow.Free(mp)
	plainText := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{{'a'}, {'b'}}, nil)
	defer plainText.Free(mp)
	count := makeBinaryStringInt64Input(t, proc, []int64{1, 1})
	defer count.Free(mp)

	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(2))
	require.NoError(t, Left([]*vector.Vector{binaryRow, count}, result, proc, 2, nil))
	require.True(t, result.GetResultVector().HasBinaryStringMetadata())

	require.NoError(t, result.PreExtendAndReset(2))
	require.NoError(t, Left([]*vector.Vector{plainText, count}, result, proc, 2, nil))
	require.False(t, result.GetResultVector().HasBinaryStringMetadata())

	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(64)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	staticBinary := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{{'a'}, {'b'}},
		[]types.RuntimeStringDomain{types.RuntimeStringText, types.RuntimeStringInherit})
	defer staticBinary.Free(mp)
	binaryResult, err := vector.NewFunctionResultWrapperWithAllocation(types.T_varbinary.ToType(), mp, selection)
	require.NoError(t, err)
	require.NoError(t, binaryResult.PreExtendAndReset(2))
	binaryRS := vector.MustFunctionResult[types.Varlena](binaryResult)
	require.NoError(t, binaryRS.AppendBytes([]byte{'a'}, false))
	require.NoError(t, binaryRS.AppendBytes([]byte{'b'}, false))
	require.ErrorIs(t, setSelectedStringResultDomain(staticBinary, binaryResult, proc), mpool.ErrAllocationAccountCapacity)
	require.False(t, binaryResult.GetResultVector().HasBinaryStringMetadata())
	binaryResult.Free()
	snapshot := account.Seal()
	require.Zero(t, snapshot.Used)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestBinaryStringBoundaryHelpers(t *testing.T) {
	left, right := substringBounds(3, math.MinInt64, math.MaxInt64, true)
	require.Equal(t, 0, left)
	require.Equal(t, 0, right)
	left, right = substringBounds(3, -1, math.MaxInt64, true)
	require.Equal(t, 2, left)
	require.Equal(t, 3, right)
	left, right = substringBounds(3, 1, math.MinInt64, true)
	require.Equal(t, 0, left)
	require.Equal(t, 0, right)
	left, right = substringBounds(3, math.MaxInt64, 1, false)
	require.Equal(t, 0, left)
	require.Equal(t, 0, right)

	size, start, end, raw := insertBinaryResultLayout([]byte("abc"), 2, math.MaxInt64, []byte("x"))
	require.Equal(t, 2, size)
	require.Equal(t, 1, start)
	require.Equal(t, 3, end)
	require.False(t, raw)
	size, _, _, raw = insertBinaryResultLayout([]byte("abc"), math.MaxInt64, math.MinInt64, []byte("x"))
	require.Equal(t, 3, size)
	require.True(t, raw)

	_, rejected := padBinaryResultByteLength([]byte("a"), -1, []byte("x"), 8)
	require.True(t, rejected)
	_, rejected = padBinaryResultByteLength([]byte("a"), 2, nil, 8)
	require.True(t, rejected)
	length, rejected := padBinaryResultByteLength([]byte("abc"), 2, nil, 8)
	require.False(t, rejected)
	require.Equal(t, 2, length)
}

func TestByteLikeUsesByteWildcardsAndEscape(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, test := range []struct {
		name    string
		pattern []byte
		value   []byte
		escape  []byte
		enabled bool
		want    bool
	}{
		{name: "one byte", pattern: []byte("____"), value: []byte("你a"), want: true},
		{name: "not one rune", pattern: []byte("__"), value: []byte("你a"), want: false},
		{name: "percent backtracks", pattern: []byte("%\xbd%a"), value: []byte("你a"), want: true},
		{name: "skipped literal segment matches", pattern: []byte("%__b%c"), value: []byte("xxbzzc"), want: true},
		{name: "skipped literal segment rejects suffix", pattern: []byte("%__b%c"), value: []byte("xxbzz"), want: false},
		{name: "skipped literal ignores too-early anchor", pattern: []byte("%__b%c"), value: []byte("xbxbzc"), want: true},
		{name: "multiple mixed segments", pattern: []byte("%__b%de_f"), value: []byte("xxbzzbdeXf"), want: true},
		{name: "escaped underscore", pattern: []byte(`\_`), value: []byte("_"), escape: []byte{'\\'}, enabled: true, want: true},
		{name: "escaped percent is literal", pattern: []byte(`\%`), value: []byte("x"), escape: []byte{'\\'}, enabled: true, want: false},
		{name: "trailing escape literal", pattern: []byte(`a\`), value: []byte(`a\`), escape: []byte{'\\'}, enabled: true, want: true},
		{name: "invalid bytes", pattern: []byte{0xff, '_'}, value: []byte{0xff, 0x80}, want: true},
		{name: "empty", pattern: nil, value: nil, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := byteLike(test.pattern, test.value, test.escape, test.enabled, mp)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestLikeEscapeValidationUsesEffectiveRowDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	values := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
		[]byte("a_"), []byte("a_"),
	}, []types.RuntimeStringDomain{types.RuntimeStringBinary, types.RuntimeStringText})
	defer values.Free(mp)
	patterns := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
		{'a', 0xff, '_'}, {'a', 0xff, '_'},
	}, nil)
	defer patterns.Free(mp)
	escape, err := vector.NewConstBytes(types.T_varbinary.ToType(), []byte{0xff}, 2, mp)
	require.NoError(t, err)
	defer escape.Free(mp)

	result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(2))
	err = newOpBuiltInRegexp().likeFn(
		[]*vector.Vector{values, patterns, escape}, result, proc, 2, nil)
	require.ErrorContains(t, err, "Incorrect arguments to ESCAPE",
		"the text row must reject an invalid UTF-8 escape")

	require.NoError(t, result.PreExtendAndReset(2))
	require.NoError(t, newOpBuiltInRegexp().likeFn(
		[]*vector.Vector{values, patterns, escape}, result, proc, 2,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}))
	require.True(t, vector.MustFixedColNoTypeCheck[bool](result.GetResultVector())[0],
		"the binary row must accept one arbitrary escape byte")
}

func TestByteLikeMatchesDynamicProgrammingOracle(t *testing.T) {
	mp := mpool.MustNewZero()
	generate := func(alphabet []byte, maxLength int) [][]byte {
		values := [][]byte{nil}
		for length := 1; length <= maxLength; length++ {
			for _, prefix := range values {
				if len(prefix) != length-1 {
					continue
				}
				for _, symbol := range alphabet {
					candidate := append(append([]byte(nil), prefix...), symbol)
					values = append(values, candidate)
				}
			}
		}
		return values
	}
	reference := func(pattern, value []byte) bool {
		reachable := make([]bool, len(pattern)+1)
		reachable[0] = true
		for i, token := range pattern {
			if token == '%' && reachable[i] {
				reachable[i+1] = true
			}
		}
		for _, b := range value {
			next := make([]bool, len(pattern)+1)
			for i, token := range pattern {
				if !reachable[i] {
					continue
				}
				switch token {
				case '%':
					next[i] = true
				case '_':
					next[i+1] = true
				default:
					if token == b {
						next[i+1] = true
					}
				}
			}
			for i, token := range pattern {
				if token == '%' && next[i] {
					next[i+1] = true
				}
			}
			reachable = next
		}
		return reachable[len(pattern)]
	}

	for _, pattern := range generate([]byte{'a', 'b', '_', '%'}, 4) {
		for _, value := range generate([]byte{'a', 'b'}, 4) {
			got, err := byteLike(pattern, value, nil, false, mp)
			require.NoError(t, err)
			require.Equalf(t, reference(pattern, value), got, "pattern=%q value=%q", pattern, value)
		}
	}
}

func TestByteLikeSegmentMatcherFindsLateValidAlignment(t *testing.T) {
	mp := mpool.MustNewZero()
	value := append(bytes.Repeat([]byte{'a'}, 8_998), 'b', 'x', 'c')
	pattern := append([]byte{'%'}, bytes.Repeat([]byte{'_'}, 5_000)...)
	pattern = append(pattern, 'b', '_', '%', 'c')
	matched, err := byteLike(pattern, value, nil, false, mp)
	require.NoError(t, err)
	require.True(t, matched)

	value[len(value)-1] = 'd'
	matched, err = byteLike(pattern, value, nil, false, mp)
	require.NoError(t, err)
	require.False(t, matched)
}

func TestByteLikeSegmentMatcherRejectsRepeatedAnchor(t *testing.T) {
	mp := mpool.MustNewZero()
	value := bytes.Repeat([]byte{'a'}, 32_000)
	pattern := []byte{'%', 'a'}
	pattern = append(pattern, bytes.Repeat([]byte{'_'}, 16_000-2)...)
	pattern = append(pattern, 'b', '%')
	matched, err := byteLike(pattern, value, nil, false, mp)
	require.NoError(t, err)
	require.False(t, matched)
}

func TestByteLikeCompiledPatternUsesLinearAccountedStorage(t *testing.T) {
	pattern := make([]byte, 64<<10)
	for i := range pattern {
		pattern[i] = byte(i)
	}
	mp := mpool.MustNewZero()
	compiled, err := compileByteLikePattern(pattern, nil, false, mp)
	require.NoError(t, err)
	require.LessOrEqual(t, len(compiled.storage), len(pattern)*2)
	require.Greater(t, len(compiled.storage), len(pattern))
	compiled.free()
	require.Zero(t, mp.CurrNB())

	limited, err := mpool.NewMPool("byte-like-compile-limit", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	largePattern := bytes.Repeat([]byte{'a'}, 1<<20)
	_, err = compileByteLikePattern(largePattern, nil, false, limited)
	require.Error(t, err)
	require.Zero(t, limited.CurrNB())

	inputMP := mpool.MustNewZero()
	value, err := vector.NewConstBytes(types.T_varbinary.ToType(), []byte{'a'}, 1, inputMP)
	require.NoError(t, err)
	defer value.Free(inputMP)
	patternVector, err := vector.NewConstBytes(types.T_varbinary.ToType(), largePattern, 1, inputMP)
	require.NoError(t, err)
	defer patternVector.Free(inputMP)
	result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), inputMP)
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	limitedProc := testutil.NewProcessWithMPool(t, "byte-like-compile-limit", limited)
	require.Error(t, newOpBuiltInRegexp().likeFn(
		[]*vector.Vector{value, patternVector}, result, limitedProc, 1, nil))
	require.Zero(t, limited.CurrNB())
}

func TestByteLikeConstantPatternCompilesOnceForMultipleRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const rows = 8
	value := append(bytes.Repeat([]byte{'a'}, (96<<10)-3), 'b', 'x', 'c')
	pattern := append([]byte{'%'}, bytes.Repeat([]byte{'_'}, (64<<10)-5)...)
	pattern = append(pattern, 'b', '_', '%', 'c')
	values, err := vector.NewConstBytes(types.T_varbinary.ToType(), value, rows, mp)
	require.NoError(t, err)
	defer values.Free(mp)
	patterns, err := vector.NewConstBytes(types.T_varbinary.ToType(), pattern, rows, mp)
	require.NoError(t, err)
	defer patterns.Free(mp)
	result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(rows))

	beforeAllocs := mp.Stats().NumAlloc.Load()
	require.NoError(t, newOpBuiltInRegexp().likeFn(
		[]*vector.Vector{values, patterns}, result, proc, rows, nil))
	require.Equal(t, int64(1), mp.Stats().NumAlloc.Load()-beforeAllocs,
		"a constant pattern must allocate one reusable compiled buffer for the whole batch")
	for _, matched := range vector.MustFixedColNoTypeCheck[bool](result.GetResultVector()) {
		require.True(t, matched)
	}
}

func TestCompiledByteLikePatternReusesRowScratch(t *testing.T) {
	mp := mpool.MustNewZero()
	compiled, err := compileByteLikePattern(bytes.Repeat([]byte{'a'}, 64<<10), nil, false, mp)
	require.NoError(t, err)
	defer compiled.free()
	storage := &compiled.storage[0]
	allocated := mp.CurrNB()
	for i := 0; i < 8; i++ {
		require.NoError(t, compiled.reset(bytes.Repeat([]byte{byte('a' + i)}, 32<<10), nil, false))
		require.Same(t, storage, &compiled.storage[0])
		require.Equal(t, allocated, mp.CurrNB())
	}
}

func BenchmarkByteLikeRepeatedAnchorRejection(b *testing.B) {
	for _, size := range []int{2_000, 4_000, 8_000, 16_000, 32_000, 64_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			value := bytes.Repeat([]byte{'a'}, size)
			pattern := []byte{'%', 'a'}
			pattern = append(pattern, bytes.Repeat([]byte{'_'}, size/2-2)...)
			pattern = append(pattern, 'b', '%')
			compiled, err := compileByteLikePattern(pattern, nil, false, mpool.MustNewZero())
			if err != nil {
				b.Fatal(err)
			}
			defer compiled.free()
			b.SetBytes(int64(len(value) + len(pattern)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if compiled.match(value) {
					b.Fatal("unexpected match")
				}
			}
		})
	}
}

func BenchmarkByteLikeSegmentLateAlignment(b *testing.B) {
	for _, size := range []int{2_000, 4_000, 8_000, 16_000, 32_000, 64_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			value := append(bytes.Repeat([]byte{'a'}, size-3), 'b', 'x', 'c')
			pattern := append([]byte{'%'}, bytes.Repeat([]byte{'_'}, size/2)...)
			pattern = append(pattern, 'b', '_', '%', 'c')
			compiled, err := compileByteLikePattern(pattern, nil, false, mpool.MustNewZero())
			if err != nil {
				b.Fatal(err)
			}
			defer compiled.free()
			b.SetBytes(int64(len(value) + len(pattern)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !compiled.match(value) {
					b.Fatal("expected match")
				}
			}
		})
	}
}

func BenchmarkByteLikeAdversarialSkippedLiteralSegment(b *testing.B) {
	for _, size := range []int{2_000, 4_000, 8_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			value := bytes.Repeat([]byte{'a'}, size)
			pattern := append([]byte{'%'}, bytes.Repeat([]byte{'_'}, size/2)...)
			pattern = append(pattern, 'b', '%', 'c')
			compiled, err := compileByteLikePattern(pattern, nil, false, mpool.MustNewZero())
			if err != nil {
				b.Fatal(err)
			}
			defer compiled.free()
			b.SetBytes(int64(len(value) + len(pattern)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if compiled.match(value) {
					b.Fatal("unexpected match")
				}
			}
		})
	}
}

func BenchmarkByteLikeAdversarialLiteralSuffix(b *testing.B) {
	value := bytes.Repeat([]byte{'a'}, 64<<10)
	pattern := append([]byte{'%'}, bytes.Repeat([]byte{'a'}, 64<<10)...)
	pattern = append(pattern, 'b')
	compiled, err := compileByteLikePattern(pattern, nil, false, mpool.MustNewZero())
	if err != nil {
		b.Fatal(err)
	}
	defer compiled.free()
	b.SetBytes(int64(len(value) + len(pattern)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if compiled.match(value) {
			b.Fatal("unexpected match")
		}
	}
}

func TestStringCharsetAndCollationNamesUseStaticType(t *testing.T) {
	for _, test := range []struct {
		name      string
		typ       types.Type
		charset   string
		collation string
	}{
		{name: "binary oid", typ: types.T_varbinary.ToType(), charset: "binary", collation: "binary"},
		{name: "opaque binary varchar", typ: types.NewWithCharset(types.T_varchar, 8, 0, types.CharsetBinary), charset: "binary", collation: "binary"},
		{name: "general ci", typ: types.T_varchar.ToType(), charset: "utf8mb4", collation: "utf8mb4_general_ci"},
		{name: "utf8mb4 bin", typ: types.NewWithCharset(types.T_varchar, 8, 0, types.CharsetUTF8MB4Bin), charset: "utf8mb4", collation: "utf8mb4_bin"},
		{name: "legacy", typ: types.NewWithCharset(types.T_varchar, 8, 0, types.CharsetLegacy), charset: "utf8", collation: "utf8_general_ci"},
		{name: "integer", typ: types.T_int64.ToType(), charset: "binary", collation: "binary"},
		{name: "untyped null", typ: types.T_any.ToType(), charset: "binary", collation: "binary"},
	} {
		t.Run(test.name, func(t *testing.T) {
			charset, collation := stringCharsetAndCollationName(test.typ)
			require.Equal(t, test.charset, charset)
			require.Equal(t, test.collation, collation)
		})
	}
}

func TestBinaryStringLikeAndConcatConsumeRuntimeDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	domains := []types.RuntimeStringDomain{types.RuntimeStringInherit, types.RuntimeStringBinary}
	value := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
		[]byte("你a"), []byte("你a"),
	}, domains)
	defer value.Free(mp)

	pattern := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
		[]byte("____"), []byte("____"),
	}, nil)
	defer pattern.Free(mp)
	likeResult := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	defer likeResult.Free()
	require.NoError(t, likeResult.PreExtendAndReset(value.Length()))
	require.NoError(t, newOpBuiltInRegexp().likeFn(
		[]*vector.Vector{value, pattern}, likeResult, proc, value.Length(), nil))
	require.Equal(t, []bool{false, true}, vector.MustFixedColNoTypeCheck[bool](likeResult.GetResultVector()))

	escapedValue := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
		[]byte("你_"), []byte("你_"),
	}, domains)
	defer escapedValue.Free(mp)
	escapedPattern := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
		[]byte(`_\_`), []byte(`_\_`),
	}, nil)
	defer escapedPattern.Free(mp)
	escape, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte{'\\'}, 2, mp)
	require.NoError(t, err)
	defer escape.Free(mp)
	escapedResult := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	defer escapedResult.Free()
	require.NoError(t, escapedResult.PreExtendAndReset(2))
	require.NoError(t, newOpBuiltInRegexp().likeFn(
		[]*vector.Vector{escapedValue, escapedPattern, escape}, escapedResult, proc, 2, nil))
	require.Equal(t, []bool{true, false}, vector.MustFixedColNoTypeCheck[bool](escapedResult.GetResultVector()))

	suffix := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{{'x'}, {'x'}}, nil)
	defer suffix.Free(mp)
	concatenated := runBinaryStringBytesFn(t, proc, builtInConcat, types.T_text.ToType(), value, suffix)
	defer concatenated.Free()
	require.Equal(t, types.RuntimeStringInherit, concatenated.GetResultVector().GetRuntimeStringDomainAt(0))
	require.Equal(t, types.RuntimeStringBinary, concatenated.GetResultVector().GetRuntimeStringDomainAt(1))

	lengthResult := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), mp)
	defer lengthResult.Free()
	require.NoError(t, lengthResult.PreExtendAndReset(value.Length()))
	require.NoError(t, LengthUTF8(
		[]*vector.Vector{concatenated.GetResultVector()}, lengthResult, proc, value.Length(), nil))
	require.Equal(t, []uint64{3, 5}, vector.MustFixedColNoTypeCheck[uint64](lengthResult.GetResultVector()))
}

func TestConcatWsDomainUsesOnlyNonNullContributors(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	separator := makeBinaryStringTestInput(t, proc, types.T_varbinary.ToType(), [][]byte{{'-'}, {'-'}}, nil)
	defer separator.Free(mp)
	first := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{{'a'}, {'a'}}, nil)
	defer first.Free(mp)
	second := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{nil, {'b'}}, nil)
	defer second.Free(mp)

	result := runBinaryStringBytesFn(
		t, proc, ConcatWs, types.T_blob.ToType(), separator, first, second)
	defer result.Free()
	require.Equal(t, [][]byte{{'a'}, []byte("a-b")}, binaryStringResultBytes(result))
	require.Equal(t, types.RuntimeStringText, result.GetResultVector().GetRuntimeStringDomainAt(0))
	require.Equal(t, types.RuntimeStringInherit, result.GetResultVector().GetRuntimeStringDomainAt(1))
}

func TestCharsetAndCollationExecutorsIgnoreRuntimeRowOverrides(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	input := makeBinaryStringTestInput(t, proc, types.T_varchar.ToType(), [][]byte{
		[]byte("你"), []byte("你"),
	}, []types.RuntimeStringDomain{types.RuntimeStringInherit, types.RuntimeStringBinary})
	defer input.Free(mp)

	for _, test := range []struct {
		name string
		fn   binaryStringTestFn
		want string
	}{
		{name: "charset", fn: Charset, want: "utf8mb4"},
		{name: "collation", fn: Collation, want: "utf8mb4_general_ci"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := runBinaryStringBytesFn(t, proc, test.fn, types.T_varchar.ToType(), input)
			defer result.Free()
			require.Equal(t, [][]byte{[]byte(test.want), []byte(test.want)}, binaryStringResultBytes(result))
		})
	}
}

func TestCharsetAndCollationNonStringInputsAndMask(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	for _, input := range []*vector.Vector{
		makeBinaryStringInt64Input(t, proc, []int64{1, 2}),
		vector.NewConstNull(types.T_any.ToType(), 2, mp),
	} {
		defer input.Free(mp)
		for _, fn := range []binaryStringTestFn{Charset, Collation} {
			result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
			require.NoError(t, result.PreExtendAndReset(2))
			require.NoError(t, fn(
				[]*vector.Vector{input}, result, proc, 2,
				&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}))
			require.Equal(t, []byte("binary"), result.GetResultVector().GetBytesAt(0))
			require.True(t, result.GetResultVector().IsNull(1))
			result.Free()
		}
	}
}

func TestBinaryStringResolverUsesSubjectDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	text := types.New(types.T_varchar, 8, 0)
	binary := types.New(types.T_varbinary, 8, 0)
	integer := types.T_int64.ToType()

	for _, test := range []struct {
		name        string
		function    string
		inputs      []types.Type
		wantOID     types.T
		wantCharset uint8
	}{
		{name: "replace text source binary auxiliary", function: "replace", inputs: []types.Type{text, binary, binary}, wantOID: types.T_varchar, wantCharset: types.CharsetUTF8},
		{name: "replace binary source text auxiliary", function: "replace", inputs: []types.Type{binary, text, text}, wantOID: types.T_varbinary, wantCharset: types.CharsetBinary},
		{name: "insert text source binary replacement", function: "insert", inputs: []types.Type{text, integer, integer, binary}, wantOID: types.T_varchar, wantCharset: types.CharsetUTF8},
		{name: "lpad text source binary pad", function: "lpad", inputs: []types.Type{text, integer, binary}, wantOID: types.T_text, wantCharset: types.CharsetUTF8},
		{name: "rpad binary source text pad", function: "rpad", inputs: []types.Type{binary, integer, text}, wantOID: types.T_blob, wantCharset: types.CharsetBinary},
		{name: "lower binary source", function: "lower", inputs: []types.Type{binary}, wantOID: types.T_varbinary, wantCharset: types.CharsetBinary},
		{name: "substring binary source", function: "substring", inputs: []types.Type{binary, integer, integer}, wantOID: types.T_varbinary, wantCharset: types.CharsetBinary},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(proc.Ctx, test.function, test.inputs)
			require.NoError(t, err)
			casts, needsCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, needsCast)
			require.Empty(t, casts)
			require.Equal(t, test.wantOID, resolved.GetReturnType().Oid)
			require.Equal(t, test.wantCharset, resolved.GetReturnType().Charset)
		})
	}

	for _, function := range []string{"char_length", "ord", "instr", "charset", "collation"} {
		t.Run(function+" preserves binary input", func(t *testing.T) {
			inputs := []types.Type{binary}
			if function == "instr" {
				inputs = []types.Type{binary, text}
			}
			resolved, err := GetFunctionByName(proc.Ctx, function, inputs)
			require.NoError(t, err)
			casts, needsCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, needsCast)
			require.Empty(t, casts)
		})
	}

	for _, input := range []types.Type{types.T_int64.ToType(), types.T_any.ToType()} {
		for _, function := range []string{"charset", "collation"} {
			resolved, err := GetFunctionByName(proc.Ctx, function, []types.Type{input})
			require.NoError(t, err)
			casts, needsCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, needsCast)
			require.Empty(t, casts)
		}
	}
}
