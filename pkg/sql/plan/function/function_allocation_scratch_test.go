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
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

const (
	testFunctionOwner       mpool.AllocationOwner = 1
	testFunctionResultData  mpool.AllocationSite  = 1
	testFunctionResultArea  mpool.AllocationSite  = 2
	testFunctionResultNulls mpool.AllocationSite  = 3
	testFunctionResultGroup mpool.AllocationSite  = 4
	testFunctionParam       mpool.AllocationSite  = 5
	testFunctionScratch     mpool.AllocationSite  = 6
)

func newAccountedFunctionResult(
	t *testing.T,
	typ types.Type,
	mp *mpool.MPool,
	limit uint64,
) (vector.FunctionResultWrapper, *mpool.AllocationAccountRegistry, *mpool.AllocationAccount) {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelectionWithBitmaps(
		account,
		testFunctionOwner,
		testFunctionResultData,
		testFunctionResultArea,
		testFunctionResultNulls,
		testFunctionResultGroup,
	)
	require.NoError(t, err)
	allocation, err := vector.NewFunctionAllocation(
		account,
		testFunctionOwner,
		testFunctionParam,
		testFunctionScratch,
	)
	require.NoError(t, err)
	result, err := vector.NewFunctionResultWrapperWithFunctionAllocation(
		typ,
		mp,
		selection,
		allocation,
	)
	require.NoError(t, err)
	return result, registry, account
}

func finalizeAccountedFunctionResult(
	t *testing.T,
	result vector.FunctionResultWrapper,
	registry *mpool.AllocationAccountRegistry,
	account *mpool.AllocationAccount,
) {
	t.Helper()
	result.Free()
	require.Zero(t, account.Snapshot().Used)
	account.Seal()
	_, err := registry.Finalize(account)
	require.NoError(t, err)
	require.Zero(t, registry.LiveAllocationMetadata())
}

func TestAppendFormattedBytesRollsBackChangedSecondPass(t *testing.T) {
	proc := testutil.NewProcess(t)
	result, registry, account := newAccountedFunctionResult(
		t,
		types.T_varchar.ToType(),
		proc.Mp(),
		1<<20,
	)
	require.NoError(t, result.PreExtendAndReset(1))
	calls := 0
	_, err := appendFormattedBytes(
		vector.MustFunctionResult[types.Varlena](result),
		func(w formatBuffer) (bool, error) {
			calls++
			if calls == 1 {
				_, err := w.WriteString("two")
				return false, err
			}
			_, err := w.WriteString("x")
			return false, err
		},
	)
	require.Error(t, err)
	require.Zero(t, result.GetResultVector().Length())
	finalizeAccountedFunctionResult(t, result, registry, account)
}

func TestBuiltInHashAccountedScratchMatchesLegacy(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	rows := 3
	nsp := nulls.NewWithSize(rows)
	nsp.Set(1)
	stringsInput := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"alpha", "unused", strings.Repeat("wide-value", 256)},
		nsp,
	)
	defer stringsInput.Free(mp)
	integersInput := newVectorByType(
		mp,
		types.T_int64.ToType(),
		[]int64{7, 8, 9},
		nil,
	)
	defer integersInput.Free(mp)
	inputs := []*vector.Vector{stringsInput, integersInput}

	legacy := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
	require.NoError(t, legacy.PreExtendAndReset(rows))
	require.NoError(t, builtInHash(inputs, legacy, proc, rows, nil))
	want := append(
		[]int64(nil),
		vector.MustFixedColWithTypeCheck[int64](legacy.GetResultVector())...,
	)
	legacy.Free()

	accounted, registry, account := newAccountedFunctionResult(
		t,
		types.T_int64.ToType(),
		mp,
		1<<20,
	)
	require.NoError(t, accounted.PreExtendAndReset(rows))
	require.NoError(t, builtInHash(inputs, accounted, proc, rows, nil))
	require.Equal(
		t,
		want,
		vector.MustFixedColWithTypeCheck[int64](accounted.GetResultVector()),
	)
	require.Positive(t, account.Snapshot().Used)
	finalizeAccountedFunctionResult(t, accounted, registry, account)
}

func TestBuiltInHashAccountedScratchRejectsCapacity(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	input := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{strings.Repeat("x", 4096)},
		nil,
	)
	defer input.Free(mp)
	result, registry, account := newAccountedFunctionResult(
		t,
		types.T_int64.ToType(),
		mp,
		1024,
	)
	require.NoError(t, result.PreExtendAndReset(1))
	err := builtInHash([]*vector.Vector{input}, result, proc, 1, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	finalizeAccountedFunctionResult(t, result, registry, account)
}

func TestPrefixInAccountedScratchMatchesLegacy(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	left := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"abc", "zzz", "other"},
		nil,
	)
	defer left.Free(mp)
	right := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"z", "ab", "a"},
		nil,
	)
	defer right.Free(mp)
	inputs := []*vector.Vector{left, right}

	legacy := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	require.NoError(t, legacy.PreExtendAndReset(3))
	require.NoError(t, newImplPrefixIn().doPrefixIn(inputs, legacy, proc, 3, nil))
	want := append(
		[]bool(nil),
		vector.MustFixedColWithTypeCheck[bool](legacy.GetResultVector())...,
	)
	legacy.Free()

	accounted, registry, account := newAccountedFunctionResult(
		t,
		types.T_bool.ToType(),
		mp,
		1<<20,
	)
	require.NoError(t, accounted.PreExtendAndReset(3))
	op := newImplPrefixIn()
	require.NoError(t, op.doPrefixIn(inputs, accounted, proc, 3, nil))
	require.Equal(
		t,
		want,
		vector.MustFixedColWithTypeCheck[bool](accounted.GetResultVector()),
	)
	require.NotEmpty(t, op.scratch)
	require.Empty(t, op.vals)
	require.Positive(t, account.Snapshot().Used)

	require.NoError(t, accounted.PreExtendAndReset(3))
	require.NoError(t, op.doPrefixIn(inputs, accounted, proc, 3, nil))
	require.Equal(
		t,
		want,
		vector.MustFixedColWithTypeCheck[bool](accounted.GetResultVector()),
	)
	finalizeAccountedFunctionResult(t, accounted, registry, account)
}

func TestPrefixInAccountedScratchRejectsCapacity(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	left := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"value"},
		nil,
	)
	defer left.Free(mp)
	right := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{strings.Repeat("x", 4096)},
		nil,
	)
	defer right.Free(mp)
	result, registry, account := newAccountedFunctionResult(
		t,
		types.T_bool.ToType(),
		mp,
		1024,
	)
	require.NoError(t, result.PreExtendAndReset(1))
	err := newImplPrefixIn().doPrefixIn(
		[]*vector.Vector{left, right},
		result,
		proc,
		1,
		nil,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	finalizeAccountedFunctionResult(t, result, registry, account)
}

func TestJqAccountedOutputMatchesLegacy(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	jsonInput := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{`{"values":[1,2,3]}`},
		nil,
	)
	defer jsonInput.Free(mp)
	queryInput := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{`.values | map(. * 2)`},
		nil,
	)
	defer queryInput.Free(mp)
	params := []*vector.Vector{jsonInput, queryInput}

	legacy := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
	require.NoError(t, legacy.PreExtendAndReset(1))
	require.NoError(t, newOpBuiltInJq().jq(params, legacy, proc, 1, nil))
	want := append([]byte(nil), legacy.GetResultVector().GetBytesAt(0)...)
	legacy.Free()
	require.Equal(t, []byte(`[2,4,6]`), want)

	accounted, registry, account := newAccountedFunctionResult(
		t,
		types.T_varchar.ToType(),
		mp,
		1<<20,
	)
	require.NoError(t, accounted.PreExtendAndReset(1))
	op := newOpBuiltInJq()
	require.NoError(t, op.jq(params, accounted, proc, 1, nil))
	require.Equal(t, want, accounted.GetResultVector().GetBytesAt(0))
	require.Positive(t, account.Snapshot().Used)
	finalizeAccountedFunctionResult(t, accounted, registry, account)
}

func TestJqAccountedOutputRejectsCapacity(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	jsonInput := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{`"` + strings.Repeat("x", 4096) + `"`},
		nil,
	)
	defer jsonInput.Free(mp)
	queryInput := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"."},
		nil,
	)
	defer queryInput.Free(mp)
	params := []*vector.Vector{jsonInput, queryInput}
	for _, test := range []struct {
		name string
		run  func(*opBuiltInJq, vector.FunctionResultWrapper) error
	}{
		{
			name: "jq",
			run: func(op *opBuiltInJq, result vector.FunctionResultWrapper) error {
				return op.jq(params, result, proc, 1, nil)
			},
		},
		{
			name: "try_jq",
			run: func(op *opBuiltInJq, result vector.FunctionResultWrapper) error {
				return op.tryJq(params, result, proc, 1, nil)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, registry, account := newAccountedFunctionResult(
				t,
				types.T_varchar.ToType(),
				mp,
				1024,
			)
			require.NoError(t, result.PreExtendAndReset(1))
			err := test.run(newOpBuiltInJq(), result)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			finalizeAccountedFunctionResult(t, result, registry, account)
		})
	}
}

func TestJSONRowAccountedOutputMatchesLegacy(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	integers := newVectorByType(
		mp,
		types.T_int64.ToType(),
		[]int64{7, 8},
		nil,
	)
	defer integers.Free(mp)
	stringsInput := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"alpha", "beta"},
		nil,
	)
	defer stringsInput.Free(mp)
	params := []*vector.Vector{integers, stringsInput}

	legacy := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
	require.NoError(t, legacy.PreExtendAndReset(2))
	require.NoError(t, newOpBuiltInJsonRow().jsonRow(params, legacy, proc, 2, nil))
	want0 := append([]byte(nil), legacy.GetResultVector().GetBytesAt(0)...)
	want1 := append([]byte(nil), legacy.GetResultVector().GetBytesAt(1)...)
	legacy.Free()

	accounted, registry, account := newAccountedFunctionResult(
		t,
		types.T_varchar.ToType(),
		mp,
		1<<20,
	)
	require.NoError(t, accounted.PreExtendAndReset(2))
	require.NoError(t, newOpBuiltInJsonRow().jsonRow(
		params,
		accounted,
		proc,
		2,
		nil,
	))
	require.Equal(t, want0, accounted.GetResultVector().GetBytesAt(0))
	require.Equal(t, want1, accounted.GetResultVector().GetBytesAt(1))
	require.Positive(t, account.Snapshot().Used)
	finalizeAccountedFunctionResult(t, accounted, registry, account)
}

func TestJSONObjectAccountedKeyScratchMatchesLegacy(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	keys := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{strings.Repeat("key", 256), "second"},
		nil,
	)
	defer keys.Free(mp)
	values := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"first", "value"},
		nil,
	)
	defer values.Free(mp)
	params := []*vector.Vector{keys, values}

	legacy := vector.NewFunctionResultWrapper(types.T_json.ToType(), mp)
	require.NoError(t, legacy.PreExtendAndReset(2))
	require.NoError(t, newOpBuiltInJsonObject().jsonObject(params, legacy, proc, 2, nil))
	want0 := append([]byte(nil), legacy.GetResultVector().GetBytesAt(0)...)
	want1 := append([]byte(nil), legacy.GetResultVector().GetBytesAt(1)...)
	legacy.Free()

	accounted, registry, account := newAccountedFunctionResult(
		t,
		types.T_json.ToType(),
		mp,
		1<<20,
	)
	require.NoError(t, accounted.PreExtendAndReset(2))
	require.NoError(t, newOpBuiltInJsonObject().jsonObject(
		params,
		accounted,
		proc,
		2,
		nil,
	))
	require.Equal(t, want0, accounted.GetResultVector().GetBytesAt(0))
	require.Equal(t, want1, accounted.GetResultVector().GetBytesAt(1))
	require.Positive(t, account.Snapshot().Used)
	finalizeAccountedFunctionResult(t, accounted, registry, account)
}

func TestJSONObjectAccountedKeyScratchRejectsCapacity(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	keys := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{strings.Repeat("key", 2048)},
		nil,
	)
	defer keys.Free(mp)
	values := newVectorByType(
		mp,
		types.T_int64.ToType(),
		[]int64{1},
		nil,
	)
	defer values.Free(mp)
	result, registry, account := newAccountedFunctionResult(
		t,
		types.T_json.ToType(),
		mp,
		1024,
	)
	require.NoError(t, result.PreExtendAndReset(1))
	err := newOpBuiltInJsonObject().jsonObject(
		[]*vector.Vector{keys, values},
		result,
		proc,
		1,
		nil,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	finalizeAccountedFunctionResult(t, result, registry, account)
}

func TestJSONModifyAccountedValueScratchRejectsCapacity(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	document := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{`{"value":0}`},
		nil,
	)
	defer document.Free(mp)
	path := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"$.value"},
		nil,
	)
	defer path.Free(mp)
	arrayType := types.T_array_float32.ToType()
	array := vector.NewVec(arrayType)
	values := make([]float32, 1024)
	for idx := range values {
		values[idx] = float32(idx)
	}
	require.NoError(t, vector.AppendBytes(
		array,
		types.ArrayToBytes(values),
		false,
		mp,
	))
	defer array.Free(mp)
	result, registry, account := newAccountedFunctionResult(
		t,
		types.T_json.ToType(),
		mp,
		1024,
	)
	require.NoError(t, result.PreExtendAndReset(1))
	err := newOpBuiltInJsonSet().buildJsonSet(
		[]*vector.Vector{document, path, array},
		result,
		proc,
		1,
		nil,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	finalizeAccountedFunctionResult(t, result, registry, account)
}

func TestFixedInAccountedScratchMatchesMapSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	left := newVectorByType(
		mp,
		types.T_float64.ToType(),
		[]float64{math.NaN(), math.Copysign(0, -1), 1, 2},
		nil,
	)
	defer left.Free(mp)
	tupleNulls := nulls.NewWithSize(3)
	tupleNulls.Set(2)
	tuple := newVectorByType(
		mp,
		types.T_float64.ToType(),
		[]float64{math.NaN(), 0, 0},
		tupleNulls,
	)
	defer tuple.Free(mp)
	params := []*vector.Vector{left, tuple}

	legacy := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	require.NoError(t, legacy.PreExtendAndReset(4))
	require.NoError(t, newOpOperatorFixedIn[float64]().operatorIn(
		params,
		legacy,
		proc,
		4,
		nil,
	))
	wantValues := append(
		[]bool(nil),
		vector.MustFixedColWithTypeCheck[bool](legacy.GetResultVector())...,
	)
	wantNulls := make([]bool, 4)
	for row := range wantNulls {
		wantNulls[row] = legacy.GetResultVector().IsNull(uint64(row))
	}
	legacy.Free()

	accounted, registry, account := newAccountedFunctionResult(
		t,
		types.T_bool.ToType(),
		mp,
		1<<20,
	)
	require.NoError(t, accounted.PreExtendAndReset(4))
	op := newOpOperatorFixedIn[float64]()
	require.NoError(t, op.operatorIn(params, accounted, proc, 4, nil))
	require.True(t, op.accounted)
	require.Nil(t, op.mp)
	require.Equal(
		t,
		wantValues,
		vector.MustFixedColWithTypeCheck[bool](accounted.GetResultVector()),
	)
	for row, wantNull := range wantNulls {
		require.Equal(t, wantNull, accounted.GetResultVector().IsNull(uint64(row)))
	}
	finalizeAccountedFunctionResult(t, accounted, registry, account)
}

func TestFixedInAccountedScratchRejectsCapacity(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	left := newVectorByType(mp, types.T_int64.ToType(), []int64{1}, nil)
	defer left.Free(mp)
	tupleValues := make([]int64, 1024)
	for idx := range tupleValues {
		tupleValues[idx] = int64(idx)
	}
	tuple := newVectorByType(mp, types.T_int64.ToType(), tupleValues, nil)
	defer tuple.Free(mp)
	result, registry, account := newAccountedFunctionResult(
		t,
		types.T_bool.ToType(),
		mp,
		1024,
	)
	require.NoError(t, result.PreExtendAndReset(1))
	err := newOpOperatorFixedIn[int64]().operatorIn(
		[]*vector.Vector{left, tuple},
		result,
		proc,
		1,
		nil,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	finalizeAccountedFunctionResult(t, result, registry, account)
}

func TestStringInAccountedScratchMatchesMapSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	left := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"alpha", "missing", "", "wide"},
		nil,
	)
	defer left.Free(mp)
	tupleNulls := nulls.NewWithSize(5)
	tupleNulls.Set(4)
	tuple := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"wide", "alpha", "alpha", "", "unused"},
		tupleNulls,
	)
	defer tuple.Free(mp)
	params := []*vector.Vector{left, tuple}

	legacy := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
	require.NoError(t, legacy.PreExtendAndReset(4))
	require.NoError(t, newOpOperatorStrIn().operatorIn(
		params,
		legacy,
		proc,
		4,
		nil,
	))
	wantValues := append(
		[]bool(nil),
		vector.MustFixedColWithTypeCheck[bool](legacy.GetResultVector())...,
	)
	wantNulls := make([]bool, 4)
	for row := range wantNulls {
		wantNulls[row] = legacy.GetResultVector().IsNull(uint64(row))
	}
	legacy.Free()

	accounted, registry, account := newAccountedFunctionResult(
		t,
		types.T_bool.ToType(),
		mp,
		1<<20,
	)
	require.NoError(t, accounted.PreExtendAndReset(4))
	op := newOpOperatorStrIn()
	require.NoError(t, op.operatorIn(params, accounted, proc, 4, nil))
	require.True(t, op.accounted)
	require.Nil(t, op.mp)
	require.Equal(
		t,
		wantValues,
		vector.MustFixedColWithTypeCheck[bool](accounted.GetResultVector()),
	)
	for row, wantNull := range wantNulls {
		require.Equal(t, wantNull, accounted.GetResultVector().IsNull(uint64(row)))
	}
	finalizeAccountedFunctionResult(t, accounted, registry, account)
}

func TestStringInAccountedScratchRejectsCapacity(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	left := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{"value"},
		nil,
	)
	defer left.Free(mp)
	tuple := newVectorByType(
		mp,
		types.T_varchar.ToType(),
		[]string{strings.Repeat("x", 4096)},
		nil,
	)
	defer tuple.Free(mp)
	result, registry, account := newAccountedFunctionResult(
		t,
		types.T_bool.ToType(),
		mp,
		1024,
	)
	require.NoError(t, result.PreExtendAndReset(1))
	err := newOpOperatorStrIn().operatorIn(
		[]*vector.Vector{left, tuple},
		result,
		proc,
		1,
		nil,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	finalizeAccountedFunctionResult(t, result, registry, account)
}

func TestNarrowCosineSimilarityAccountedScratchRejectsCapacity(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	arrayType := types.T_array_uint8.ToType()
	left := vector.NewVec(arrayType)
	right := vector.NewVec(arrayType)
	values := make([]uint8, 4096)
	for idx := range values {
		values[idx] = uint8(idx)
	}
	require.NoError(t, vector.AppendBytes(left, types.ArrayToBytes(values), false, mp))
	require.NoError(t, vector.AppendBytes(right, types.ArrayToBytes(values), false, mp))
	defer left.Free(mp)
	defer right.Free(mp)
	result, registry, account := newAccountedFunctionResult(
		t,
		types.T_float64.ToType(),
		mp,
		1024,
	)
	require.NoError(t, result.PreExtendAndReset(1))
	err := CosineSimilarityArrayViaF32[uint8](
		[]*vector.Vector{left, right},
		result,
		proc,
		1,
		nil,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	finalizeAccountedFunctionResult(t, result, registry, account)
}
