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
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// A direct prepared parameter beside a JSON operand bypasses the static
// operator check that rejects a typed binary-string peer (BINARY, VARBINARY,
// BLOB): the plan wraps the parameter in the private JSON comparison adapters
// (__mo_json_comparison_param / __mo_json_ordering_param) before the
// operator ever sees the pair, so the rejection belongs at the runtime
// boundary those adapters feed. Every comparison operator must refuse a
// binary-domain parameter with the same invalid-argument operator error the
// equivalent CAST('...' AS BINARY) form receives, through both EXECUTE
// channels: a binary-protocol parameter (COM_STMT_EXECUTE) carries the
// concrete binary type, and an untyped parameter attached to the
// binary-string sidecar keeps the same opaque byte domain without a
// protocol type.
type preparedBinaryParamSpec struct {
	name            string
	paramTyp        types.T
	rendered        types.T
	setBinaryString bool
	typed           bool
	value           []byte
}

func preparedBinaryParamSpecs() []preparedBinaryParamSpec {
	return []preparedBinaryParamSpec{
		// COM_STMT_EXECUTE attaches the concrete protocol type of the bound
		// value, exactly the metadata JDBC setBytes produces.
		{"binary_protocol", types.T_binary, types.T_binary, false, true, []byte("3")},
		{"binary_protocol", types.T_varbinary, types.T_varbinary, false, true, []byte("3")},
		{"binary_protocol", types.T_blob, types.T_blob, false, true, []byte("3")},
		// The text channel attaches the binary-string sidecar instead; its
		// bytes need not parse as JSON, and the literal spelling carries the
		// BINARY domain.
		{"binary_string_axis", types.T_any, types.T_binary, true, false, []byte{0x00, 0xff, 0x13}},
	}
}

func preparedBinaryParamVector(
	t *testing.T,
	proc *process.Process,
	length int,
	spec *preparedBinaryParamSpec,
) *vector.Vector {
	t.Helper()
	v, err := vector.NewConstBytes(types.T_text.ToType(), spec.value, length, proc.Mp())
	require.NoError(t, err)
	t.Cleanup(func() { v.Free(proc.Mp()) })
	v.SetPrepareParamKind(vector.PrepareParamNone)
	if spec.typed {
		v.SetPrepareParamType(spec.paramTyp)
	}
	if spec.setBinaryString {
		require.NoError(t, v.SetIsBinaryStringAt(0, true, proc.Mp()))
	}
	return v
}

func preparedBinaryJSONOperand(
	t *testing.T,
	proc *process.Process,
) *vector.Vector {
	t.Helper()
	rows := []string{`3`, `"str"`, `true`, `{}`, `null`}
	v := vector.NewVec(types.T_json.ToType())
	t.Cleanup(func() { v.Free(proc.Mp()) })
	for _, row := range rows {
		require.NoError(t, vector.AppendBytes(v, []byte(mustJsonBinaryString(t, row)), false, proc.Mp()))
	}
	return v
}

func adaptPreparedBinaryParam(
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
	return wrapper.GetResultVector()
}

// TestPreparedBinaryParamComparisonRejected walks every comparison operator,
// binary family member, EXECUTE channel and operand order: the adapter must
// route the binary provenance into the prepared JSON comparison executor, and
// that executor must refuse the operand pair with the same invalid-argument
// operator error the equivalent CAST form receives from the static check.
func TestPreparedBinaryParamComparisonRejected(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	const length = 5
	jsonVector := preparedBinaryJSONOperand(t, proc)

	operators := []struct {
		name     string
		executor executeLogicOfOverload
		ordering bool
	}{
		{"=", equalFn, false},
		{"<=>", nullSafeEqualFn, false},
		{"<>", notEqualFn, false}, // "<>" is the canonical operator name
		{">", greatThanFn, true},
		{">=", greatEqualFn, true},
		{"<", lessThanFn, true},
		{"<=", lessEqualFn, true},
	}
	for _, op := range operators {
		specs := preparedBinaryParamSpecs()
		for i := range specs {
			spec := &specs[i]
			for _, paramLeft := range []bool{false, true} {
				label := fmt.Sprintf("%s %s param_left=%v", op.name, spec.name, paramLeft)
				pair := fmt.Sprintf("[%s %s]", types.T_json.String(), spec.rendered.String())
				if paramLeft {
					pair = fmt.Sprintf("[%s %s]", spec.rendered.String(), types.T_json.String())
				}
				t.Run(label, func(t *testing.T) {
					param := preparedBinaryParamVector(t, proc, length, spec)
					adapted := adaptPreparedBinaryParam(t, proc, param, length, op.ordering)
					wrapper := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
					require.NoError(t, wrapper.PreExtendAndReset(length))
					operands := []*vector.Vector{jsonVector, adapted}
					if paramLeft {
						operands = []*vector.Vector{adapted, jsonVector}
					}
					err := op.executor(operands, wrapper, proc, length, nil)
					require.Error(t, err, label+" must reject the binary-domain parameter")
					require.Contains(t, err.Error(),
						"invalid argument operator "+op.name, label)
					require.Contains(t, err.Error(),
						"bad value "+pair, label+" must mirror the static pair spelling")
					wrapper.Free()
				})
			}
		}
	}
}

// TestPreparedBinaryParamNullRowsKept pins the boundary the rejection must
// not disturb: a SQL NULL parameter keeps the SQL NULL comparison contract
// whatever its declared domain, because MySQL evaluates a NULL prepared
// parameter as NULL, and the static gate only ever spoke about operand
// types.
func TestPreparedBinaryParamNullRowsKept(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	const length = 5
	jsonVector := preparedBinaryJSONOperand(t, proc)

	for _, op := range []struct {
		name     string
		executor executeLogicOfOverload
		ordering bool
	}{
		{"=", equalFn, false},
		{"<=>", nullSafeEqualFn, false},
		{"<", lessThanFn, true},
	} {
		t.Run(op.name, func(t *testing.T) {
			param := vector.NewConstNull(types.T_text.ToType(), length, proc.Mp())
			t.Cleanup(func() { param.Free(proc.Mp()) })
			param.SetPrepareParamKind(vector.PrepareParamNone)
			param.SetPrepareParamType(types.T_blob)
			adapted := adaptPreparedBinaryParam(t, proc, param, length, op.ordering)
			wrapper := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
			t.Cleanup(wrapper.Free)
			require.NoError(t, wrapper.PreExtendAndReset(length))
			require.NoError(t, op.executor([]*vector.Vector{jsonVector, adapted}, wrapper, proc, length, nil))
			result := wrapper.GetResultVector()
			for i := 0; i < length; i++ {
				if op.name == "<=>" {
					require.False(t, result.IsNull(uint64(i)), "row %d", i)
				} else {
					require.True(t, result.IsNull(uint64(i)), "row %d stays SQL NULL", i)
				}
			}
			require.True(t, adapted.IsPreparedJSONComparisonParam())
		})
	}
}

// TestPreparedBinaryParamEncodingContracts pins that the rejection is the
// comparison operator's admission decision alone: binary parameters keep
// their opaque JSON encoding for JSON construction consumers, a JSON-typed
// parameter may still carry a top-level binary-derived document with the
// ordinary JSON/JSON comparison executing, and character parameters keep
// comparing through the JSON string scalar contract.
func TestPreparedBinaryParamEncodingContracts(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })

	t.Run("binary parameter keeps its binary JSON encoding", func(t *testing.T) {
		scalar, err := PreparedJSONScalarValue(
			proc.Ctx, []byte("3"), vector.PrepareParamNone, types.T_blob, false,
			bytejson.MySQLOpaqueProtocolVersion)
		require.NoError(t, err)
		encoded, err := bytejson.CreateByteJSON(scalar)
		require.NoError(t, err)
		require.Equal(t, bytejson.TpCodeBlob, encoded.Type)
	})

	const length = 5
	jsonVector := preparedBinaryJSONOperand(t, proc)

	t.Run("JSON-typed binary document parameter compares", func(t *testing.T) {
		// CAST(x'..' AS JSON) persists a binary-derived document at the top
		// level; the parameter's declared domain is JSON, not binary, and its
		// text transport carries the document's textual spelling.
		document, err := bytejson.NewMySQLOpaque(
			bytejson.MySQLOpaqueProtocolVersion, 252, []byte("opaque-member-payload"))
		require.NoError(t, err)
		payload, err := document.MarshalJSON()
		require.NoError(t, err)
		require.NotEmpty(t, payload)
		param, err := vector.NewConstBytes(types.T_text.ToType(), payload, length, proc.Mp())
		require.NoError(t, err)
		t.Cleanup(func() { param.Free(proc.Mp()) })
		param.SetPrepareParamKind(vector.PrepareParamNone)
		param.SetPrepareParamType(types.T_json)
		adapted := adaptPreparedBinaryParam(t, proc, param, length, false)
		wrapper := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
		t.Cleanup(wrapper.Free)
		require.NoError(t, wrapper.PreExtendAndReset(length))
		require.NoError(t, equalFn([]*vector.Vector{jsonVector, adapted}, wrapper, proc, length, nil))
	})

	t.Run("character parameter keeps the JSON string scalar contract", func(t *testing.T) {
		param, err := vector.NewConstBytes(types.T_text.ToType(), []byte("str"), length, proc.Mp())
		require.NoError(t, err)
		t.Cleanup(func() { param.Free(proc.Mp()) })
		param.SetPrepareParamKind(vector.PrepareParamNone)
		adapted := adaptPreparedBinaryParam(t, proc, param, length, false)
		wrapper := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
		t.Cleanup(wrapper.Free)
		require.NoError(t, wrapper.PreExtendAndReset(length))
		require.NoError(t, equalFn([]*vector.Vector{jsonVector, adapted}, wrapper, proc, length, nil))
		result := wrapper.GetResultVector()
		require.True(t, vector.MustFixedColNoTypeCheck[bool](result)[1],
			`"str" = 'str' through a prepared character parameter`)
	})
}

// TestPreparedBinaryParamStaticParity keeps the runtime rejection and the
// static gate spelled the same way: the CAST form of the binary peer fails
// binding with the invalid-argument operator error, so the prepared form must
// fail execution with the same operator error and pair rendering.
func TestPreparedBinaryParamStaticParity(t *testing.T) {
	ctx := context.Background()
	for _, op := range []string{"=", "<=>", "<>", ">", ">=", "<", "<="} {
		for _, peer := range []types.Type{
			types.T_binary.ToType(), types.T_varbinary.ToType(), types.T_blob.ToType(),
		} {
			for _, args := range [][]types.Type{
				{types.T_json.ToType(), peer},
				{peer, types.T_json.ToType()},
			} {
				_, err := GetFunctionByName(ctx, op, args)
				require.Error(t, err, "operator "+op+" static binary control")
			}
		}
	}
}
