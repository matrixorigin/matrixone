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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// A prepared Boolean parameter must reach the same mixed JSON/BOOL kernel
// family as its equivalent SQL literal. The plan-side adapter for a direct
// parameter beside a JSON operand (__mo_json_ordering_param /
// __mo_json_comparison_param) carries the runtime provenance of the
// parameter; dropping it makes a prepared ordering predicate rank JSON
// documents in the plain JSON domain (number < boolean) instead of the
// established zero/nonzero boolean fold, so CAST('0.5' AS JSON) < TRUE is
// false as a literal and true through the prepared equivalent.
//
// Both EXECUTE channels are covered at the metadata the executor observes:
// a binary-protocol parameter (COM_STMT_EXECUTE) arrives with the concrete
// types.T_bool and the Boolean conversion kind, and a parameter without a
// protocol type still carries the Boolean kind (the text-EXECUTE user
// variable and untyped driver parameter shapes).
type preparedBooleanParamShape struct {
	name     string
	typed    bool
	paramTyp types.T
}

var preparedBooleanParamShapes = []preparedBooleanParamShape{
	{"binary_protocol_typed", true, types.T_bool},
	{"untyped_boolean_kind", false, types.T_any},
}

func preparedBooleanParam(
	t *testing.T,
	proc *process.Process,
	length int,
	shape *preparedBooleanParamShape,
	value []byte,
) *vector.Vector {
	t.Helper()
	v, err := vector.NewConstBytes(types.T_text.ToType(), value, length, proc.Mp())
	require.NoError(t, err)
	t.Cleanup(func() { v.Free(proc.Mp()) })
	v.SetPrepareParamKind(vector.PrepareParamBoolean)
	if shape.typed {
		v.SetPrepareParamType(shape.paramTyp)
	}
	return v
}

// preparedBooleanAdaptedVector runs the plan-side adapter the same way a
// prepared statement does: the raw text parameter enters, and the JSON-typed
// adapter output with its prepared marker enters the comparison operator.
func preparedBooleanAdaptedVector(
	t *testing.T,
	proc *process.Process,
	param *vector.Vector,
	length int,
	ordering bool,
) *vector.Vector {
	t.Helper()
	wrapper := vector.NewFunctionResultWrapper(types.T_json.ToType(), proc.Mp())
	t.Cleanup(wrapper.Free)
	require.NoError(t, wrapper.PreExtendAndReset(length))
	adapter := normalizeJsonComparisonParam
	if ordering {
		adapter = normalizeJsonOrderingParam
	}
	require.NoError(t, adapter([]*vector.Vector{param}, wrapper, proc, length, nil))
	out := wrapper.GetResultVector()
	// The marker, not the encoded value, is what routes the operator to the
	// prepared JSON comparison executor with the parameter's SQL category.
	require.True(t, out.IsPreparedJSONComparisonParam(),
		"adapter must keep the prepared marker for a Boolean parameter")
	require.Equal(t, vector.PrepareParamBoolean, out.GetPrepareParamKind())
	require.Equal(t, param.GetPrepareParamType(), out.GetPrepareParamType())
	return out
}

func preparedBooleanJSONOperand(
	t *testing.T,
	proc *process.Process,
) (*vector.Vector, []bool) {
	t.Helper()
	rows := jsonScalarForms(t)
	nulls := jsonFormsNulls(rows)
	v := vector.NewVec(types.T_json.ToType())
	t.Cleanup(func() { v.Free(proc.Mp()) })
	for i, row := range rows {
		require.NoError(t, vector.AppendBytes(v, []byte(row), nulls[i], proc.Mp()))
	}
	return v, nulls
}

func runPreparedBooleanOperator(
	t *testing.T,
	proc *process.Process,
	executor executeLogicOfOverload,
	rows []*vector.Vector,
	length int,
) ([]bool, []bool) {
	t.Helper()
	wrapper := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
	require.NoError(t, wrapper.PreExtendAndReset(length))
	require.NoError(t, executor(rows, wrapper, proc, length, nil))
	result := wrapper.GetResultVector()
	values := make([]bool, length)
	nulls := make([]bool, length)
	column := vector.MustFixedColNoTypeCheck[bool](result)
	for i := 0; i < length; i++ {
		values[i] = column[i]
		nulls[i] = result.IsNull(uint64(i))
	}
	wrapper.Free()
	return values, nulls
}

// TestPreparedBooleanOrderingMatchesLiteral runs the mixed JSON/BOOL
// comparison family through the prepared-parameter channel and expects the
// exact receipt table the SQL literal produces: category ranking for
// strings, objects and arrays, JSON literal null as a comparable value, the
// numeric zero/nonzero fold, and the SQL NULL contract, in both operand
// orders.
func TestPreparedBooleanOrderingMatchesLiteral(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	const length = 8
	jsonVector, _ := preparedBooleanJSONOperand(t, proc)

	type boolCase struct {
		name        string
		executor    executeLogicOfOverload
		ordering    bool
		paramValue  []byte
		expected    []bool
		resultNulls []bool
		paramLeft   bool
	}
	cases := []boolCase{
		{"json < TRUE", lessThanFn, true, []byte("true"),
			[]bool{false, true, false, true, true, false, false, false},
			[]bool{false, false, false, false, false, false, false, true}, false},
		{"json <= TRUE", lessEqualFn, true, []byte("true"),
			[]bool{true, true, true, true, true, true, true, false},
			[]bool{false, false, false, false, false, false, false, true}, false},
		{"json > TRUE", greatThanFn, true, []byte("true"),
			[]bool{false, false, false, false, false, false, false, false},
			[]bool{false, false, false, false, false, false, false, true}, false},
		{"json >= TRUE", greatEqualFn, true, []byte("true"),
			[]bool{true, false, true, false, false, true, true, false},
			[]bool{false, false, false, false, false, false, false, true}, false},
		{"json < FALSE", lessThanFn, true, []byte("false"),
			[]bool{false, true, false, false, true, false, false, false},
			[]bool{false, false, false, false, false, false, false, true}, false},
		{"json = TRUE", equalFn, false, []byte("true"),
			[]bool{true, false, true, false, false, true, true, false},
			[]bool{false, false, false, false, false, false, false, true}, false},
		{"json != TRUE", notEqualFn, false, []byte("true"),
			[]bool{false, true, false, true, true, false, false, false},
			[]bool{false, false, false, false, false, false, false, true}, false},
		{"json <=> TRUE", nullSafeEqualFn, false, []byte("true"),
			[]bool{true, false, true, false, false, true, true, false},
			[]bool{false, false, false, false, false, false, false, false}, false},
		{"json = FALSE", equalFn, false, []byte("false"),
			[]bool{false, false, false, true, false, false, false, false},
			[]bool{false, false, false, false, false, false, false, true}, false},
		{"json <=> FALSE", nullSafeEqualFn, false, []byte("false"),
			[]bool{false, false, false, true, false, false, false, false},
			[]bool{false, false, false, false, false, false, false, false}, false},
		// Reversed operand order: a Boolean parameter on the left exercises
		// the executor's orientation flip.
		{"TRUE > json", greatThanFn, true, []byte("true"),
			[]bool{false, true, false, true, true, false, false, false},
			[]bool{false, false, false, false, false, false, false, true}, true},
		{"TRUE >= json", greatEqualFn, true, []byte("true"),
			[]bool{true, true, true, true, true, true, true, false},
			[]bool{false, false, false, false, false, false, false, true}, true},
		{"TRUE < json", lessThanFn, true, []byte("true"),
			[]bool{false, false, false, false, false, false, false, false},
			[]bool{false, false, false, false, false, false, false, true}, true},
		{"TRUE <= json", lessEqualFn, true, []byte("true"),
			[]bool{true, false, true, false, false, true, true, false},
			[]bool{false, false, false, false, false, false, false, true}, true},
		{"FALSE > json", greatThanFn, true, []byte("false"),
			[]bool{false, true, false, false, true, false, false, false},
			[]bool{false, false, false, false, false, false, false, true}, true},
	}
	for si := range preparedBooleanParamShapes {
		shape := &preparedBooleanParamShapes[si]
		for _, tc := range cases {
			t.Run(shape.name+" "+tc.name, func(t *testing.T) {
				param := preparedBooleanParam(t, proc, length, shape, tc.paramValue)
				adapted := preparedBooleanAdaptedVector(t, proc, param, length, tc.ordering)
				operands := []*vector.Vector{jsonVector, adapted}
				if tc.paramLeft {
					operands = []*vector.Vector{adapted, jsonVector}
				}
				values, nulls := runPreparedBooleanOperator(t, proc, tc.executor, operands, length)
				require.Equal(t, tc.expected, values, "prepared %s must match the literal semantics", tc.name)
				require.Equal(t, tc.resultNulls, nulls, "prepared %s must keep the SQL NULL contract", tc.name)
			})
		}
	}
}

// TestPreparedBooleanOrderingFoldControls pins the zero and fractional
// number rows whose plain-JSON ranking diverges from the boolean fold: the
// JSON-domain order ranks every number below JSON true, while the mixed
// JSON/BOOL kernel folds each number to its zero/nonzero boolean.
func TestPreparedBooleanOrderingFoldControls(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	forms := []string{`0`, `0.0`, `1`, `0.5`, `-2`}
	length := len(forms)
	buildJSON := func() *vector.Vector {
		v := vector.NewVec(types.T_json.ToType())
		t.Cleanup(func() { v.Free(proc.Mp()) })
		for _, form := range forms {
			require.NoError(t, vector.AppendBytes(v, []byte(mustJsonBinaryString(t, form)), false, proc.Mp()))
		}
		return v
	}
	type foldCase struct {
		name       string
		executor   executeLogicOfOverload
		ordering   bool
		paramValue []byte
		expected   []bool
	}
	// Each case states its Boolean constant through the parameter itself, so
	// the fold rows above stay a receipt of the constant they name.
	cases := []foldCase{
		{"json < TRUE", lessThanFn, true, []byte("true"), []bool{true, true, false, false, false}},
		{"json <= TRUE", lessEqualFn, true, []byte("true"), []bool{true, true, true, true, true}},
		{"json > TRUE", greatThanFn, true, []byte("true"), []bool{false, false, false, false, false}},
		{"json >= TRUE", greatEqualFn, true, []byte("true"), []bool{false, false, true, true, true}},
		{"json = TRUE", equalFn, false, []byte("true"), []bool{false, false, true, true, true}},
		{"json = FALSE", equalFn, false, []byte("false"), []bool{true, true, false, false, false}},
		{"json < FALSE", lessThanFn, true, []byte("false"), []bool{false, false, false, false, false}},
	}
	for si := range preparedBooleanParamShapes {
		shape := &preparedBooleanParamShapes[si]
		for _, tc := range cases {
			t.Run(shape.name+" "+tc.name, func(t *testing.T) {
				param := preparedBooleanParam(t, proc, length, shape, tc.paramValue)
				adapted := preparedBooleanAdaptedVector(t, proc, param, length, tc.ordering)
				values, nulls := runPreparedBooleanOperator(t, proc, tc.executor,
					[]*vector.Vector{buildJSON(), adapted}, length)
				require.Equal(t, tc.expected, values)
				for _, null := range nulls {
					require.False(t, null)
				}
			})
		}
	}
}

// TestPreparedBooleanOrderingLiteralParity anchors the prepared receipt
// tables above to the SQL literal kernel: the same rows run through the
// direct literal operands, including the reversed order.
func TestPreparedBooleanOrderingLiteralParity(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	const length = 8
	jsonVector, _ := preparedBooleanJSONOperand(t, proc)

	for _, tc := range []struct {
		name      string
		executor  executeLogicOfOverload
		boolean   bool
		expected  []bool
		nulls     []bool
		paramLeft bool
	}{
		{"json < TRUE", lessThanFn, true,
			[]bool{false, true, false, true, true, false, false, false},
			[]bool{false, false, false, false, false, false, false, true}, false},
		{"json >= TRUE", greatEqualFn, true,
			[]bool{true, false, true, false, false, true, true, false},
			[]bool{false, false, false, false, false, false, false, true}, false},
		{"TRUE <= json", lessEqualFn, true,
			[]bool{true, false, true, false, false, true, true, false},
			[]bool{false, false, false, false, false, false, false, true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operand, err := vector.NewConstFixed(types.T_bool.ToType(), tc.boolean, length, proc.Mp())
			require.NoError(t, err)
			t.Cleanup(func() { operand.Free(proc.Mp()) })
			operands := []*vector.Vector{jsonVector, operand}
			if tc.paramLeft {
				operands = []*vector.Vector{operand, jsonVector}
			}
			values, nulls := runPreparedBooleanOperator(t, proc, tc.executor, operands, length)
			require.Equal(t, tc.expected, values)
			require.Equal(t, tc.nulls, nulls)
		})
	}
}
