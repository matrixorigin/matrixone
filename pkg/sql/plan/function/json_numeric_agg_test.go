// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"bytes"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type jsonNumericAggExpectation struct {
	name string
	want float64
}

var jsonNumericAggExpectations = []jsonNumericAggExpectation{
	{name: "sum", want: 6.5},
	{name: "avg", want: 13.0 / 6},
	{name: "var_pop", want: 13.0 / 18},
	{name: "var_samp", want: 13.0 / 12},
	{name: "stddev_pop", want: math.Sqrt(13.0 / 18)},
	{name: "stddev_samp", want: math.Sqrt(13.0 / 12)},
}

func jsonNumericAggNulls(values []bool) *nulls.Nulls {
	if len(values) == 0 {
		return nil
	}
	nsp := nulls.NewWithSize(len(values))
	for i, isNull := range values {
		if isNull {
			nsp.Set(uint64(i))
		}
	}
	return nsp
}

// castJSONNumericAggInput deliberately binds and runs the existing CAST
// overload. Expected aggregate values in these tests are hand-computed below;
// this helper only supplies the production JSON-to-DOUBLE input boundary.
func castJSONNumericAggInput(
	t *testing.T,
	proc *process.Process,
	jsonTexts []string,
	nullList []bool,
) (*vector.Vector, error) {
	t.Helper()
	if nullList == nil {
		nullList = make([]bool, len(jsonTexts))
	}
	encoded := makeJSONEncodedFromText(t, jsonTexts, nullList)
	input := newVectorByType(
		proc.Mp(), types.T_json.ToType(), encoded, jsonNumericAggNulls(nullList))
	target := vector.NewVec(types.T_float64.ToType())
	cast, err := GetFunctionByName(proc.Ctx, "cast", []types.Type{
		types.T_json.ToType(), types.T_float64.ToType(),
	})
	if err != nil {
		input.Free(proc.Mp())
		target.Free(proc.Mp())
		return nil, err
	}
	result, err := RunFunctionDirectly(
		proc, cast.GetEncodedOverloadID(), []*vector.Vector{input, target}, len(jsonTexts))
	input.Free(proc.Mp())
	target.Free(proc.Mp())
	return result, err
}

func runJSONNumericAgg(
	t *testing.T,
	proc *process.Process,
	name string,
	jsonTexts []string,
	nullList []bool,
	distinct bool,
) (float64, bool) {
	t.Helper()
	bound, err := GetFunctionByName(proc.Ctx, name, []types.Type{types.T_json.ToType()})
	require.NoError(t, err)
	targetTypes, shouldCast := bound.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Equal(t, []types.Type{types.T_float64.ToType()}, targetTypes)

	values, err := castJSONNumericAggInput(t, proc, jsonTexts, nullList)
	require.NoError(t, err)
	defer values.Free(proc.Mp())

	exec, err := aggexec.MakeAgg(proc.Mp(), bound.GetEncodedOverloadID(), distinct, targetTypes[0])
	require.NoError(t, err)
	defer exec.Free()
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))
	results, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	defer results[0].Free(proc.Mp())
	require.Equal(t, types.T_float64, results[0].GetType().Oid)
	if results[0].IsNull(0) {
		return 0, true
	}
	return vector.GetFixedAtNoTypeCheck[float64](results[0], 0), false
}

func TestJSONNumericAggExecUsesExistingCastDomain(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := testutil.NewProcess(t, testutil.WithMPool(mp))
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})

	jsonTexts := []string{`1`, `2.5`, `3`, `null`, ``}
	nullList := []bool{false, false, false, false, true}
	for _, expectation := range jsonNumericAggExpectations {
		t.Run(expectation.name+"/mixed-null-input", func(t *testing.T) {
			got, gotNull := runJSONNumericAgg(
				t, proc, expectation.name, jsonTexts, nullList, false)
			require.False(t, gotNull)
			require.InDelta(t, expectation.want, got, 1e-14)
		})
	}

	for _, expectation := range jsonNumericAggExpectations {
		for _, input := range []struct {
			name     string
			texts    []string
			nulls    []bool
			want     float64
			wantNull bool
		}{
			{name: "empty", texts: []string{}, wantNull: true},
			{name: "all-null", texts: []string{`null`, ``}, nulls: []bool{false, true}, wantNull: true},
			{name: "singleton", texts: []string{`2.5`}, want: 2.5},
		} {
			t.Run(expectation.name+"/"+input.name, func(t *testing.T) {
				got, gotNull := runJSONNumericAgg(t, proc, expectation.name, input.texts, input.nulls, false)
				wantNull := input.wantNull ||
					(input.name == "singleton" &&
						(expectation.name == "var_samp" || expectation.name == "stddev_samp"))
				require.Equal(t, wantNull, gotNull)
				if wantNull {
					return
				}
				if expectation.name == "var_pop" || expectation.name == "stddev_pop" {
					input.want = 0
				}
				if expectation.name == "var_samp" || expectation.name == "stddev_samp" {
					require.Fail(t, "singleton sample aggregate must return SQL NULL")
				}
				require.InDelta(t, input.want, got, 1e-14)
			})
		}
	}

	for _, expectation := range jsonNumericAggExpectations {
		t.Run(expectation.name+"/partial-round-trip", func(t *testing.T) {
			bound, err := GetFunctionByName(proc.Ctx, expectation.name, []types.Type{types.T_json.ToType()})
			require.NoError(t, err)
			targetTypes, shouldCast := bound.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)

			left, err := castJSONNumericAggInput(t, proc, []string{`1`, `2.5`}, nil)
			require.NoError(t, err)
			defer left.Free(proc.Mp())
			right, err := castJSONNumericAggInput(t, proc, []string{`3`, `null`, ``}, []bool{false, false, true})
			require.NoError(t, err)
			defer right.Free(proc.Mp())

			makeExec := func() aggexec.AggFuncExec {
				exec, makeErr := aggexec.MakeAgg(
					proc.Mp(), bound.GetEncodedOverloadID(), false, targetTypes[0])
				require.NoError(t, makeErr)
				return exec
			}
			leftExec := makeExec()
			defer leftExec.Free()
			rightExec := makeExec()
			defer rightExec.Free()
			require.NoError(t, leftExec.GroupGrow(1))
			require.NoError(t, rightExec.GroupGrow(1))
			require.NoError(t, leftExec.BulkFill(0, []*vector.Vector{left}))
			require.NoError(t, rightExec.BulkFill(0, []*vector.Vector{right}))

			var leftWire, rightWire bytes.Buffer
			require.NoError(t, leftExec.SaveIntermediateResult(1, [][]uint8{{1}}, &leftWire))
			require.NoError(t, rightExec.SaveIntermediateResult(1, [][]uint8{{1}}, &rightWire))
			leftRestored := makeExec()
			defer leftRestored.Free()
			rightRestored := makeExec()
			defer rightRestored.Free()
			require.NoError(t, leftRestored.UnmarshalFromReader(bytes.NewReader(leftWire.Bytes()), proc.Mp()))
			require.NoError(t, rightRestored.UnmarshalFromReader(bytes.NewReader(rightWire.Bytes()), proc.Mp()))
			require.NoError(t, leftRestored.Merge(rightRestored, 0, 0))

			results, err := leftRestored.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			defer results[0].Free(proc.Mp())
			require.False(t, results[0].IsNull(0))
			require.Equal(t, types.T_float64, results[0].GetType().Oid)
			require.InDelta(t, expectation.want, vector.GetFixedAtNoTypeCheck[float64](results[0], 0), 1e-14)
		})
	}
}

func TestJSONNumericAggExecDistinctAndGrouped(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := testutil.NewProcess(t, testutil.WithMPool(mp))
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})

	// Four distinct JSON representations of one must be equal after the
	// JSON-to-DOUBLE boundary. This also challenges placement of DISTINCT before
	// conversion and keeps 2^53 precision out of the expected-value generator.
	distinctTexts := []string{`1`, `1.0`, `1e0`, `"1"`, `2`, `3`, `null`, ``}
	distinctNulls := []bool{false, false, false, false, false, false, false, true}
	for _, expectation := range jsonNumericAggExpectations {
		t.Run(expectation.name+"/distinct", func(t *testing.T) {
			got, gotNull := runJSONNumericAgg(t, proc, expectation.name, distinctTexts, distinctNulls, true)
			require.False(t, gotNull)
			want := map[string]float64{
				"sum":         6,
				"avg":         2,
				"var_pop":     2.0 / 3,
				"var_samp":    1,
				"stddev_pop":  math.Sqrt(2.0 / 3),
				"stddev_samp": 1,
			}[expectation.name]
			require.InDelta(t, want, got, 1e-14)
		})
	}

	groupTexts := []string{`1`, `2.5`, `3`, `null`, `4`, ``}
	groupNulls := []bool{false, false, false, false, false, true}
	groupIDs := []uint64{1, 1, 2, 2, 2, 2}
	for _, expectation := range jsonNumericAggExpectations {
		t.Run(expectation.name+"/two-groups", func(t *testing.T) {
			bound, err := GetFunctionByName(proc.Ctx, expectation.name, []types.Type{types.T_json.ToType()})
			require.NoError(t, err)
			targetTypes, shouldCast := bound.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			values, err := castJSONNumericAggInput(t, proc, groupTexts, groupNulls)
			require.NoError(t, err)
			defer values.Free(proc.Mp())
			exec, err := aggexec.MakeAgg(proc.Mp(), bound.GetEncodedOverloadID(), false, targetTypes[0])
			require.NoError(t, err)
			defer exec.Free()
			require.NoError(t, exec.GroupGrow(2))
			require.NoError(t, exec.BatchFill(0, groupIDs, []*vector.Vector{values}))
			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			defer results[0].Free(proc.Mp())
			require.Equal(t, types.T_float64, results[0].GetType().Oid)
			require.Len(t, vector.MustFixedColNoTypeCheck[float64](results[0]), 2)

			wantGroup1 := map[string]float64{
				"sum":         3.5,
				"avg":         1.75,
				"var_pop":     0.5625,
				"var_samp":    1.125,
				"stddev_pop":  math.Sqrt(0.5625),
				"stddev_samp": math.Sqrt(1.125),
			}[expectation.name]
			wantGroup2 := map[string]float64{
				"sum":         7,
				"avg":         3.5,
				"var_pop":     0.25,
				"var_samp":    0.5,
				"stddev_pop":  0.5,
				"stddev_samp": math.Sqrt(0.5),
			}[expectation.name]
			require.False(t, results[0].IsNull(0))
			require.False(t, results[0].IsNull(1))
			got := vector.MustFixedColNoTypeCheck[float64](results[0])
			require.InDelta(t, wantGroup1, got[0], 1e-14)
			require.InDelta(t, wantGroup2, got[1], 1e-14)
		})
	}
}

func TestJSONNumericAggCastErrorsDoNotPoisonRetry(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := testutil.NewProcess(t, testutil.WithMPool(mp))
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})

	for _, badJSON := range []struct {
		name string
		text string
	}{
		{name: "invalid-string", text: `"not-a-number"`},
		{name: "numeric-prefix-string", text: `"12x"`},
		{name: "empty-string", text: `""`},
		{name: "boolean", text: `true`},
		{name: "array", text: `[1]`},
		{name: "object", text: `{"v":1}`},
	} {
		t.Run(badJSON.name, func(t *testing.T) {
			for _, expectation := range jsonNumericAggExpectations {
				values, err := castJSONNumericAggInput(t, proc, []string{badJSON.text}, nil)
				require.Error(t, err)
				require.Nil(t, values)

				got, gotNull := runJSONNumericAgg(
					t, proc, expectation.name, []string{`2.5`}, nil, false)
				if expectation.name == "var_samp" || expectation.name == "stddev_samp" {
					require.True(t, gotNull, "singleton sample aggregate must return SQL NULL")
					continue
				}
				require.False(t, gotNull)
				want := float64(0)
				if expectation.name == "sum" || expectation.name == "avg" {
					want = 2.5
				}
				require.InDelta(t, want, got, 1e-14)
			}
		})
	}
}

func TestJSONNumericAggDoublePrecisionBoundary(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := testutil.NewProcess(t, testutil.WithMPool(mp))
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})

	// The JSON-to-DOUBLE contract rounds both positive values to 2^53 and
	// rounds the negative value to -2^53. The expected values are written in
	// terms of that independently known float64 boundary, rather than obtained
	// by invoking the cast implementation under test.
	texts := []string{`9007199254740992`, `9007199254740993`, `-9007199254740993`}
	sum, sumNull := runJSONNumericAgg(t, proc, "sum", texts, nil, false)
	require.False(t, sumNull)
	require.Equal(t, float64(1<<53), sum)
	distinctSum, distinctNull := runJSONNumericAgg(t, proc, "sum", texts, nil, true)
	require.False(t, distinctNull)
	require.Equal(t, float64(0), distinctSum)
}

func TestJSONNumericAggFractionExponentAndNegative(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := testutil.NewProcess(t, testutil.WithMPool(mp))
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})

	// Keep the expected values independent of both JSON encoding and the
	// aggregate implementation. The input exercises signed, fractional and
	// exponent JSON numbers in the inherited DOUBLE domain.
	texts := []string{`-2`, `0.5`, `4e0`}
	wants := map[string]float64{
		"sum":         2.5,
		"avg":         5.0 / 6,
		"var_pop":     109.0 / 18,
		"var_samp":    109.0 / 12,
		"stddev_pop":  math.Sqrt(109.0 / 18),
		"stddev_samp": math.Sqrt(109.0 / 12),
	}
	for _, expectation := range jsonNumericAggExpectations {
		t.Run(expectation.name, func(t *testing.T) {
			got, gotNull := runJSONNumericAgg(t, proc, expectation.name, texts, nil, false)
			require.False(t, gotNull)
			require.InDelta(t, wants[expectation.name], got, 1e-14)
		})
	}
}
