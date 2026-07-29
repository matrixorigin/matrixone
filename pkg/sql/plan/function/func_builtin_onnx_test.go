// Copyright 2021 - 2025 Matrix Origin
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
	"context"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/mlai/onnx"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const onnxTestdata = "../../../mlai/onnx/testdata/"

func skipNoOnnxRuntime(t *testing.T) {
	if err := onnx.Available(); err != nil {
		t.Skipf("onnxruntime not available: %v", err)
	}
}

func onnxTestModel(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(onnxTestdata + name)
	require.NoError(t, err)
	return b
}

// onnxEval builds the four argument vectors, runs the operator, and returns
// the result json texts (empty string for NULL rows).
func onnxEval(t *testing.T, proc *process.Process, op *opOnnxRun,
	params []*vector.Vector, n int, selectList *FunctionSelectList) ([]string, error) {
	t.Helper()
	wrapper := vector.NewFunctionResultWrapper(types.T_json.ToType(), proc.Mp())
	require.NoError(t, wrapper.PreExtendAndReset(n))
	if err := op.onnxRun(params, wrapper, proc, n, selectList); err != nil {
		return nil, err
	}
	out := make([]string, n)
	sp := vector.GenerateFunctionStrParameter(wrapper.GetResultVector())
	for i := 0; i < n; i++ {
		b, isnull := sp.GetStrValue(uint64(i))
		if isnull {
			out[i] = ""
			continue
		}
		bj := types.DecodeJson(append([]byte(nil), b...))
		out[i] = bj.String()
	}
	return out, nil
}

func constStr(t *testing.T, proc *process.Process, typ types.T, val string, n int) *vector.Vector {
	t.Helper()
	v, err := vector.NewConstBytes(typ.ToType(), []byte(val), n, proc.Mp())
	require.NoError(t, err)
	return v
}

func constNull(t *testing.T, proc *process.Process, typ types.T, n int) *vector.Vector {
	t.Helper()
	return vector.NewConstNull(typ.ToType(), n, proc.Mp())
}

const (
	sumDiffInShape  = `{"dim":[1,1,4],"dtype":"float32"}`
	sumDiffOutShape = `{"dim":[1,1,2],"dtype":"float32"}`
	sumDiffInput    = `[0.2,0.3,0.6,0.9]`
)

func sumDiffParams(t *testing.T, proc *process.Process, model []byte, n int) []*vector.Vector {
	t.Helper()
	mv, err := vector.NewConstBytes(types.T_varbinary.ToType(), model, n, proc.Mp())
	require.NoError(t, err)
	return []*vector.Vector{
		mv,
		constStr(t, proc, types.T_varchar, sumDiffInput, n),
		constStr(t, proc, types.T_varchar, sumDiffInShape, n),
		constStr(t, proc, types.T_varchar, sumDiffOutShape, n),
	}
}

// TestOnnxRunVarbinaryConst runs the const-model fast path across several rows:
// the session is built once and reused, results are identical.
func TestOnnxRunVarbinaryConst(t *testing.T) {
	skipNoOnnxRuntime(t)
	proc := testutil.NewProcess(t)
	op := newOpOnnxRun()
	defer func() { require.NoError(t, op.Close()) }()

	model := onnxTestModel(t, "sum_and_difference.onnx")
	out, err := onnxEval(t, proc, op, sumDiffParams(t, proc, model, 3), 3, nil)
	require.NoError(t, err)
	for _, s := range out {
		require.Contains(t, s, "1.9999883")
	}
	// Const path: the raw model argument is not retained.
	require.False(t, op.hasArg)
	require.NotNil(t, op.sess)
}

// TestOnnxRunVarbinaryNonConst exercises the bytes.Equal session-reuse branch.
func TestOnnxRunVarbinaryNonConst(t *testing.T) {
	skipNoOnnxRuntime(t)
	proc := testutil.NewProcess(t)
	op := newOpOnnxRun()
	defer func() { require.NoError(t, op.Close()) }()

	model := onnxTestModel(t, "sum_and_difference.onnx")
	mv := newVectorByType(proc.Mp(), types.T_varbinary.ToType(),
		[]string{string(model), string(model)}, nil)
	params := []*vector.Vector{
		mv,
		constStr(t, proc, types.T_varchar, sumDiffInput, 2),
		constStr(t, proc, types.T_varchar, sumDiffInShape, 2),
		constStr(t, proc, types.T_varchar, sumDiffOutShape, 2),
	}
	out, err := onnxEval(t, proc, op, params, 2, nil)
	require.NoError(t, err)
	require.Equal(t, out[0], out[1])
	// Non-const path retains the model argument for the reuse check.
	require.True(t, op.hasArg)
}

// TestOnnxRunNullHandling covers NULL model / NULL output_shape rows.
func TestOnnxRunNullHandling(t *testing.T) {
	skipNoOnnxRuntime(t)
	proc := testutil.NewProcess(t)

	// NULL model -> NULL result.
	{
		op := newOpOnnxRun()
		params := []*vector.Vector{
			constNull(t, proc, types.T_varbinary, 1),
			constStr(t, proc, types.T_varchar, sumDiffInput, 1),
			constStr(t, proc, types.T_varchar, sumDiffInShape, 1),
			constStr(t, proc, types.T_varchar, sumDiffOutShape, 1),
		}
		out, err := onnxEval(t, proc, op, params, 1, nil)
		require.NoError(t, err)
		require.Equal(t, "", out[0])
		require.NoError(t, op.Close())
	}

	// NULL output_shape -> non-tensor rendering keyed by output name.
	{
		op := newOpOnnxRun()
		model := onnxTestModel(t, "sklearn_randomforest.onnx")
		mv, err := vector.NewConstBytes(types.T_varbinary.ToType(), model, 1, proc.Mp())
		require.NoError(t, err)
		params := []*vector.Vector{
			mv,
			constStr(t, proc, types.T_varchar, `[5.9,3.0,5.1,1.8]`, 1),
			constStr(t, proc, types.T_varchar, `{"dim":[1,4],"dtype":"float32"}`, 1),
			constNull(t, proc, types.T_varchar, 1),
		}
		out, err := onnxEval(t, proc, op, params, 1, nil)
		require.NoError(t, err)
		require.Contains(t, out[0], "output_label")
		require.Contains(t, out[0], "output_probability")
		require.NoError(t, op.Close())
	}
}

// TestOnnxRunErrors covers the error paths of the eval loop.
func TestOnnxRunErrors(t *testing.T) {
	skipNoOnnxRuntime(t)
	proc := testutil.NewProcess(t)
	model := onnxTestModel(t, "sum_and_difference.onnx")

	run := func(modelVal, input, inShape, outShape string, outNull bool) error {
		op := newOpOnnxRun()
		defer op.Close()
		var out *vector.Vector
		if outNull {
			out = constNull(t, proc, types.T_varchar, 1)
		} else {
			out = constStr(t, proc, types.T_varchar, outShape, 1)
		}
		mv, err := vector.NewConstBytes(types.T_varbinary.ToType(), []byte(modelVal), 1, proc.Mp())
		require.NoError(t, err)
		params := []*vector.Vector{
			mv,
			constStr(t, proc, types.T_varchar, input, 1),
			constStr(t, proc, types.T_varchar, inShape, 1),
			out,
		}
		_, err = onnxEval(t, proc, op, params, 1, nil)
		return err
	}

	// invalid input shape json
	require.ErrorContains(t,
		run(string(model), sumDiffInput, `{bad json`, sumDiffOutShape, false),
		"invalid shape json")
	// unsupported dtype
	require.ErrorContains(t,
		run(string(model), sumDiffInput, `{"dim":[1,1,4],"dtype":"complex64"}`, sumDiffOutShape, false),
		"unsupported dtype")
	// invalid output shape json
	require.ErrorContains(t,
		run(string(model), sumDiffInput, sumDiffInShape, `{"dim":[]}`, false),
		"onnx")
	// input length mismatch
	require.ErrorContains(t,
		run(string(model), `[1,2]`, sumDiffInShape, sumDiffOutShape, false),
		"shape needs")
	// corrupt model bytes
	require.ErrorContains(t,
		run("not a model", sumDiffInput, sumDiffInShape, sumDiffOutShape, false),
		"cannot read model")
	// datalink-scheme model string routes through datalink resolution
	require.Error(t,
		run("file:///nonexistent/model.onnx", sumDiffInput, sumDiffInShape, sumDiffOutShape, false))
	require.True(t, looksLikeDatalink([]byte("stage://s/m.onnx")))
	require.False(t, looksLikeDatalink([]byte("\x08\x04\x12\x07pytorch")))
}

// TestOnnxRunLifecycle covers Reset/Close and session rebuild.
func TestOnnxRunLifecycle(t *testing.T) {
	skipNoOnnxRuntime(t)
	proc := testutil.NewProcess(t)
	op := newOpOnnxRun()
	model := onnxTestModel(t, "sum_and_difference.onnx")

	_, err := onnxEval(t, proc, op, sumDiffParams(t, proc, model, 1), 1, nil)
	require.NoError(t, err)
	require.NotNil(t, op.sess)

	// Reset drops the session; a new eval rebuilds it.
	require.NoError(t, op.Reset())
	require.Nil(t, op.sess)
	out, err := onnxEval(t, proc, op, sumDiffParams(t, proc, model, 1), 1, nil)
	require.NoError(t, err)
	require.Contains(t, out[0], "1.9999883")

	// Close is idempotent.
	require.NoError(t, op.Close())
	require.NoError(t, op.Close())
}

// TestOnnxRunSelectList covers the filtered-row paths.
func TestOnnxRunSelectList(t *testing.T) {
	skipNoOnnxRuntime(t)
	proc := testutil.NewProcess(t)
	model := onnxTestModel(t, "sum_and_difference.onnx")

	// All rows filtered out.
	{
		op := newOpOnnxRun()
		sl := &FunctionSelectList{AllNull: true}
		out, err := onnxEval(t, proc, op, sumDiffParams(t, proc, model, 2), 2, sl)
		require.NoError(t, err)
		require.Equal(t, []string{"", ""}, out)
		require.NoError(t, op.Close())
	}
	// One row filtered, one evaluated.
	{
		op := newOpOnnxRun()
		sl := &FunctionSelectList{SelectList: []bool{false, true}}
		out, err := onnxEval(t, proc, op, sumDiffParams(t, proc, model, 2), 2, sl)
		require.NoError(t, err)
		require.Equal(t, "", out[0])
		require.Contains(t, out[1], "1.9999883")
		require.NoError(t, op.Close())
	}
}

// TestOnnxRunRegistration covers the list_builtIn.go registration: overload
// lookup, return type, and the newOpWithFree lifecycle closures.
func TestOnnxRunRegistration(t *testing.T) {
	skipNoOnnxRuntime(t)
	proc := testutil.NewProcess(t)
	model := onnxTestModel(t, "sum_and_difference.onnx")

	for _, overloadId := range []int32{0, 1} {
		ov, err := GetFunctionById(context.TODO(), encodeOverloadID(ONNX_RUN, overloadId))
		require.NoError(t, err)
		require.Equal(t, types.T_json,
			ov.retType([]types.Type{types.T_varbinary.ToType()}).Oid)

		evalFn, resetFn, freeFn := ov.GetExecuteMethod()
		require.NotNil(t, evalFn)
		require.NotNil(t, resetFn)
		require.NotNil(t, freeFn)

		// Drive one evaluation through the registered closure (the model is
		// varbinary bytes; the datalink overload accepts them too via the
		// scheme check).
		mv, err := vector.NewConstBytes(types.T_varbinary.ToType(), model, 1, proc.Mp())
		require.NoError(t, err)
		params := []*vector.Vector{
			mv,
			constStr(t, proc, types.T_varchar, sumDiffInput, 1),
			constStr(t, proc, types.T_varchar, sumDiffInShape, 1),
			constStr(t, proc, types.T_varchar, sumDiffOutShape, 1),
		}
		wrapper := vector.NewFunctionResultWrapper(types.T_json.ToType(), proc.Mp())
		require.NoError(t, wrapper.PreExtendAndReset(1))
		require.NoError(t, evalFn(params, wrapper, proc, 1, nil))
		require.NoError(t, resetFn())
		require.NoError(t, freeFn())
	}
}
