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
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	ort "github.com/yalue/onnxruntime_go"
)

// Session wraps a loaded ONNX model. It is not safe for concurrent use; the
// onnx_run operator holds one Session and evaluates rows sequentially.
type Session struct {
	s           *ort.DynamicAdvancedSession
	inputNames  []string
	outputNames []string
	// hasSeqOutput: at least one output is a sequence or map. Such outputs are
	// materialized EAGERLY by the binding inside Run, so their size must be
	// bounded BEFORE Run — see the input-element guard in Session.Run.
	hasSeqOutput bool
}

// MaxSequenceInputElements caps the input tensor's element count for models with
// sequence/map outputs. A ZipMap-style output emits one sequence member per input
// ROW with no Loop/Scan involved, so the sequence length is input-sized; the
// binding then eagerly materializes per-member Go/C objects (roughly a KB each,
// NOT arena-bounded payload) inside Run, before resultBudget can act. Bounding the
// input elements bounds the member count (members <= rows <= elements), keeping
// the eager-conversion peak in the tens of MB. 64K elements is generous for
// classical-ML batches (the sklearn BVT uses 24) while an 8M-element input would
// otherwise admit millions of members.
const MaxSequenceInputElements = 64 << 10

// NewSession builds a session from raw model bytes (varbinary or the content
// of a datalink). Input and output names are discovered from the model so the
// SQL surface does not have to specify them.
//
// MaxModelBytes caps the model size accepted by NewSession, enforced at the
// package boundary (the operator's varbinary path is blob-capped to the same
// value; the datalink path is capped before reading). This is also the
// resource bound for session construction: the binding's discovery helper
// builds a throwaway ORT session with default (uncapped-allocator) options,
// and the real session's graph optimization has transient peaks — both are
// proportional to model size, so capping the model bounds them.
const MaxModelBytes = 64 << 20

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
	if len(modelBytes) > MaxModelBytes {
		return nil, moerr.NewInvalidInputNoCtxf(
			"onnx: model is %d bytes, exceeds the %d MB limit",
			len(modelBytes), MaxModelBytes>>20)
	}
	// Note: this parses the model twice (GetInputOutputInfo builds a throwaway
	// ORT session to read the names, then NewDynamicAdvancedSession builds the
	// real one). That is a deliberate one-time cost per distinct model — the
	// SQL surface does not take input/output names, so they must be discovered —
	// and its transient memory is bounded by MaxModelBytes above.
	inInfo, outInfo, err := ort.GetInputOutputInfoWithONNXData(modelBytes)
	if err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("onnx: cannot read model: %v", err)
	}
	if len(inInfo) == 0 || len(outInfo) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("onnx: model has no inputs or outputs")
	}
	// Enforced model contract for sequence/map outputs: the Go binding converts a
	// sequence output EAGERLY inside Run — allocating per-child Go/C objects for the
	// sequence's full length before any of our budget checks can run — and neither the
	// ORT arena (which bounds tensor payload, not per-value metadata) nor resultBudget
	// (which runs after conversion) bounds that peak. A sequence can only exceed the
	// input-derived bound (see MaxSequenceInputElements) via Loop/Scan iteration or a
	// chained static Sequence* builder, so those operators are rejected for models that
	// declare sequence/map outputs. Detection scans for the NodeProto op_type protobuf
	// encoding, which also covers ops inside subgraphs and local functions; a false
	// positive (e.g. an attribute string with identical bytes) fails conservatively
	// with a clean error. Tensor-only-output models are unaffected: their conversion
	// peak is payload bytes, bounded by the capped arena.
	hasSeqOutput := false
	for _, o := range outInfo {
		if o.OrtValueType == ort.ONNXTypeSequence || o.OrtValueType == ort.ONNXTypeMap {
			hasSeqOutput = true
			if op := containsUnboundedSeqOp(modelBytes); op != "" {
				return nil, moerr.NewNotSupportedNoCtxf(
					"onnx: models with sequence or map outputs may not contain the %s operator", op)
			}
			break
		}
	}
	inNames := names(inInfo)
	outNames := names(outInfo)
	// Use the environment's shared, memory-capped allocator (registered in
	// ensureInit) so allocations made while building and running the session —
	// graph optimization, constant folding, outputs, intermediates — are bounded
	// by MaxRuntimeMemoryBytes.
	opts, err := ort.NewSessionOptions()
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("onnx: cannot create session options: %v", err)
	}
	defer opts.Destroy()
	if err := opts.AddSessionConfigEntry("session.use_env_allocators", "1"); err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("onnx: cannot enable env allocators: %v", err)
	}
	s, err := ort.NewDynamicAdvancedSessionWithONNXData(modelBytes, inNames, outNames, opts)
	if err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("onnx: cannot load model: %v", err)
	}
	return &Session{
		s:            s,
		inputNames:   inNames,
		outputNames:  outNames,
		hasSeqOutput: hasSeqOutput,
	}, nil
}

// unboundedSeqOps are the operators that can grow a sequence beyond what the
// INPUT bounds: Loop/Scan iterate (runtime amplification, including inside
// subgraphs), and the static Sequence* builders can be chained node-by-node so
// a modest-sized model still yields a sequence whose per-member materialization
// cost far exceeds the model bytes. The remaining sequence producers — ZipMap
// and SplitToSequence — emit at most one member per input row/element, which
// MaxSequenceInputElements bounds. The byte scan is flat, so ops inside
// subgraphs and local functions are seen too.
var unboundedSeqOps = []string{"Loop", "Scan", "SequenceInsert", "SequenceConstruct", "SequenceEmpty"}

// containsUnboundedSeqOp reports the first unbounded-sequence operator present
// in the serialized model ("" if none), matching the NodeProto op_type
// protobuf encoding (field 4, length-delimited). A false positive (e.g. an
// attribute string with identical bytes) fails conservatively with a clean
// error. See the contract comment in NewSession.
func containsUnboundedSeqOp(model []byte) string {
	for _, op := range unboundedSeqOps {
		marker := append([]byte{0x22, byte(len(op))}, op...)
		if bytes.Contains(model, marker) {
			return op
		}
	}
	return ""
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

// runWithCancel executes the inference with a RunOptions whose terminate flag
// is raised if ctx is cancelled mid-run. The watcher goroutine touches only
// the RunOptions, and it is joined before the RunOptions is destroyed and
// before this function returns — so it can never race a later Close/Reset of
// the session.
func (s *Session) runWithCancel(ctx context.Context, inTensor ort.Value, outputs []ort.Value) error {
	ro, err := ort.NewRunOptions()
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("onnx: cannot create run options: %v", err)
	}
	runDone := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			_ = ro.Terminate()
		case <-runDone:
		}
	}()
	runErr := s.s.RunWithOptions([]ort.Value{inTensor}, outputs, ro)
	close(runDone)
	<-watcherDone
	_ = ro.Destroy()

	// A cancelled query reports cancellation regardless of whether the run
	// happened to finish first.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return moerr.NewInternalErrorNoCtxf("onnx: inference cancelled: %v", ctxErr)
	}
	if runErr != nil {
		return moerr.NewInternalErrorNoCtxf("onnx: run failed: %v", runErr)
	}
	return nil
}

// Run evaluates the model on one input tensor and returns the result as a
// json-encodable value tree (nil / bool / int64 / uint64 / float64 / string /
// []any / map[string]any — exactly the scalar set bytejson.CreateByteJSON
// accepts, so the caller can build a ByteJson directly with no text
// round-trip). When outShape is non-nil the (single) output is reshaped into a
// nested array of that shape; when outShape is nil every output is rendered by
// structure (tensor/sequence/map) into an object keyed by output name.
//
// Cancelling ctx terminates an in-flight inference: KILL, statement timeout,
// or client disconnect must not leave a long-running model (e.g. a large
// Loop) occupying the executor and ORT worker threads.
func (s *Session) Run(ctx context.Context, inputJSON []byte, inShape, outShape *Shape) (any, error) {
	if len(s.inputNames) != 1 {
		return nil, moerr.NewNotSupportedNoCtxf(
			"onnx: model has %d inputs, only single-input models are supported",
			len(s.inputNames))
	}
	// Sequence/map outputs are input-sized (e.g. ZipMap emits one member per input
	// row) and the binding materializes every member eagerly inside Run, before
	// resultBudget exists. Bound the member count BEFORE Run by bounding the input
	// elements (members <= rows <= elements) — the only pre-materialization handle
	// this binding offers. Checked first so an oversized request fails fast.
	if s.hasSeqOutput {
		if n, err := inShape.NumElements(); err != nil {
			return nil, err
		} else if n > MaxSequenceInputElements {
			return nil, moerr.NewInvalidInputNoCtxf(
				"onnx: input of %d elements exceeds the %d-element limit for models with sequence/map outputs (their outputs are materialized per input row)",
				n, MaxSequenceInputElements)
		}
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
	if err := s.runWithCancel(ctx, inTensor, outputs); err != nil {
		return nil, err
	}

	if outShape != nil {
		// Declared tensor output: reshape the first output.
		return tensorToNested(outputs[0], outShape)
	}
	// Undeclared: render every output by structure, keyed by name. All outputs
	// share one conversion budget so the aggregate result — across many named
	// outputs and recursively through sequences/maps — stays bounded even when
	// each tensor is individually within the per-tensor limit.
	budget := newResultBudget()
	obj := make(map[string]any, len(outputs))
	for i, o := range outputs {
		j, err := valueToJSON(o, budget)
		if err != nil {
			return nil, err
		}
		obj[s.outputNames[i]] = j
	}
	return obj, nil
}
