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

package onnx

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	ort "github.com/yalue/onnxruntime_go"
)

// Session wraps a loaded ONNX model. It is not safe for concurrent use; the
// onnx_run operator holds one Session and evaluates rows sequentially.
type Session struct {
	s           *ort.DynamicAdvancedSession
	inputNames  []string
	outputNames []string
}

// NewSession builds a session from raw model bytes (varbinary or the content
// of a datalink). Input and output names are discovered from the model so the
// SQL surface does not have to specify them.
func NewSession(modelBytes []byte) (*Session, error) {
	if err := Available(); err != nil {
		return nil, err
	}
	if len(modelBytes) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("onnx: empty model")
	}
	// Note: this parses the model twice (GetInputOutputInfo builds a throwaway
	// ORT session to read the names, then NewDynamicAdvancedSession builds the
	// real one). That is a deliberate one-time cost per distinct model — the
	// SQL surface does not take input/output names, so they must be discovered.
	inInfo, outInfo, err := ort.GetInputOutputInfoWithONNXData(modelBytes)
	if err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("onnx: cannot read model: %v", err)
	}
	if len(inInfo) == 0 || len(outInfo) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("onnx: model has no inputs or outputs")
	}
	inNames := names(inInfo)
	outNames := names(outInfo)
	s, err := ort.NewDynamicAdvancedSessionWithONNXData(modelBytes, inNames, outNames, nil)
	if err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("onnx: cannot load model: %v", err)
	}
	return &Session{s: s, inputNames: inNames, outputNames: outNames}, nil
}

func names(info []ort.InputOutputInfo) []string {
	out := make([]string, len(info))
	for i := range info {
		out[i] = info[i].Name
	}
	return out
}

// Close releases the underlying onnxruntime session.
func (s *Session) Close() error {
	if s == nil || s.s == nil {
		return nil
	}
	err := s.s.Destroy()
	s.s = nil
	return err
}

// Run evaluates the model on one input tensor and returns the result as a
// json-encodable value tree (nil / bool / int64 / uint64 / float64 / string /
// []any / map[string]any — exactly the scalar set bytejson.CreateByteJSON
// accepts, so the caller can build a ByteJson directly with no text
// round-trip). When outShape is non-nil the (single) output is reshaped into a
// nested array of that shape; when outShape is nil every output is rendered by
// structure (tensor/sequence/map) into an object keyed by output name.
func (s *Session) Run(inputJSON []byte, inShape, outShape *Shape) (any, error) {
	if len(s.inputNames) != 1 {
		return nil, moerr.NewNotSupportedNoCtxf(
			"onnx: model has %d inputs, only single-input models are supported",
			len(s.inputNames))
	}
	inTensor, err := buildInputTensor(inputJSON, inShape)
	if err != nil {
		return nil, err
	}
	defer inTensor.Destroy()

	outputs := make([]ort.Value, len(s.outputNames))
	// Register cleanup before Run: on a Run error the pre-allocated output
	// tensor below must still be destroyed (auto-allocated slots stay nil and
	// are skipped).
	defer func() {
		for _, o := range outputs {
			if o != nil {
				_ = o.Destroy()
			}
		}
	}()
	if outShape != nil {
		// Declared tensor output: only single-output models are supported in
		// this mode; pre-allocate the output so its type/shape is predictable
		// (also works around an onnxruntime_go sizing bug for auto-allocated
		// float16 outputs).
		if len(s.outputNames) != 1 {
			return nil, moerr.NewNotSupportedNoCtxf(
				"onnx: model has %d outputs; pass NULL output_shape to get all of them",
				len(s.outputNames))
		}
		outTensor, err := buildOutputTensor(outShape)
		if err != nil {
			return nil, err
		}
		outputs[0] = outTensor
	}
	if err := s.s.Run([]ort.Value{inTensor}, outputs); err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("onnx: run failed: %v", err)
	}

	if outShape != nil {
		// Declared tensor output: reshape the first output.
		return tensorToNested(outputs[0], outShape)
	}
	// Undeclared: render every output by structure, keyed by name.
	obj := make(map[string]any, len(outputs))
	for i, o := range outputs {
		j, err := valueToJSON(o)
		if err != nil {
			return nil, err
		}
		obj[s.outputNames[i]] = j
	}
	return obj, nil
}
