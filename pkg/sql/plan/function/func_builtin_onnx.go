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
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/mlai/onnx"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// onnx_run(model, input, input_shape, output_shape) evaluates an ONNX model.
//
//	model        - varbinary model bytes, or a datalink to a model file in a stage
//	input        - json flat array of scalars for the input tensor
//	input_shape  - json shape, e.g. '{"dim":[1,1,4],"dtype":"float32"}'
//	output_shape - json shape of the (single) tensor output, or NULL when the
//	               output is not a plain tensor (returns whatever the model
//	               produced, keyed by output name)
//
// The operator caches one onnx.Session per expression instance and reuses it
// across rows; the session is closed when the expression executor is freed
// (Close) or reset for the next query (Reset). See the serial/serial_full
// builtins for the same newOpWithFree lifecycle.
type opOnnxRun struct {
	sess    *onnx.Session
	lastArg []byte // raw model argument (datalink string or bytes) of the cached session
	hasArg  bool
}

func newOpOnnxRun() *opOnnxRun {
	return &opOnnxRun{}
}

// Close releases the cached session (freeFn).
func (op *opOnnxRun) Close() error {
	op.lastArg = nil
	op.hasArg = false
	if op.sess != nil {
		err := op.sess.Close()
		op.sess = nil
		return err
	}
	return nil
}

// Reset drops the cached session so the next query rebuilds it (resetFn).
func (op *opOnnxRun) Reset() error {
	return op.Close()
}

// ensureSession (re)builds the cached session when the raw model argument
// changes. The common case is a constant model argument (a datalink literal or
// a constant varbinary): the model file is resolved and loaded once on the
// first row and the session is reused for every subsequent row. Only when the
// argument bytes differ from the cached one do we resolve/reload.
func (op *opOnnxRun) ensureSession(proc *process.Process, rawArg []byte, isDatalink bool) error {
	if op.sess != nil && op.hasArg && bytes.Equal(op.lastArg, rawArg) {
		return nil
	}
	model := rawArg
	if isDatalink {
		dl, err := datalink.NewDatalink(string(rawArg), proc)
		if err != nil {
			return err
		}
		if model, err = dl.GetBytes(proc); err != nil {
			return err
		}
	}
	if err := op.Close(); err != nil {
		return err
	}
	sess, err := onnx.NewSession(model)
	if err != nil {
		return err
	}
	op.sess = sess
	op.lastArg = append([]byte(nil), rawArg...) // copy: GetStrValue bytes are not owned
	op.hasArg = true
	return nil
}

func (op *opOnnxRun) onnxRun(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	// If the onnxruntime library failed to load, every call fails cleanly while
	// the database keeps running.
	if err := onnx.Available(); err != nil {
		return err
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	pModel := vector.GenerateFunctionStrParameter(params[0])
	pInput := vector.GenerateFunctionStrParameter(params[1])
	pInShape := vector.GenerateFunctionStrParameter(params[2])
	pOutShape := vector.GenerateFunctionStrParameter(params[3])
	// The datalink overload may be coerced to the varbinary overload by
	// function resolution, in which case the model argument arrives as the
	// datalink URL string rather than as type T_datalink. Detect a datalink by
	// its URL scheme as well; a real ONNX model (protobuf) never begins with
	// one of these ASCII schemes.
	declaredDatalink := params[0].GetType().Oid == types.T_datalink

	if selectList.IgnoreAllRow() {
		rs.AddNullRange(0, uint64(length))
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		modelArg, mnull := pModel.GetStrValue(i)
		inputJSON, inull := pInput.GetStrValue(i)
		inShapeJSON, snull := pInShape.GetStrValue(i)
		if mnull || inull || snull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		inShape, err := onnx.ParseShape(inShapeJSON)
		if err != nil {
			return err
		}
		// A NULL output_shape means "non-tensor output": return whatever the
		// model produced.
		var outShape *onnx.Shape
		if outShapeJSON, onull := pOutShape.GetStrValue(i); !onull {
			outShape, err = onnx.ParseShape(outShapeJSON)
			if err != nil {
				return err
			}
		}

		isDatalink := declaredDatalink || looksLikeDatalink(modelArg)
		if err := op.ensureSession(proc, modelArg, isDatalink); err != nil {
			return err
		}
		out, err := op.sess.Run(inputJSON, inShape, outShape)
		if err != nil {
			return err
		}
		// out is json text; store it as a proper T_json (ByteJson) value.
		bj, err := bytejson.ParseFromByteSlice(out)
		if err != nil {
			return err
		}
		dt, err := bj.Marshal()
		if err != nil {
			return err
		}
		if err := rs.AppendBytes(dt, false); err != nil {
			return err
		}
	}
	return nil
}

// datalinkSchemes are the URL schemes datalink understands. Used to recognize a
// datalink argument that was coerced to varbinary/varchar by function
// resolution (see the comment in onnxRun).
var datalinkSchemes = []string{"file://", "stage://", "hdfs://", "s3://"}

func looksLikeDatalink(arg []byte) bool {
	for _, s := range datalinkSchemes {
		if len(arg) >= len(s) && string(arg[:len(s)]) == s {
			return true
		}
	}
	return false
}
