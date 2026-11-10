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

package colexec

import (
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// JSONReaderExpressionExecutor supplies an explicit immutable column version.
// A producer binds each new version; Reset releases it, requiring a fresh Bind.
// It never infers validity from an ordinary Vector or a backing address.
type JSONReaderExpressionExecutor struct {
	original     ExpressionExecutor // only projection-owned sources may fall back before binding
	reader       vector.JSONReader
	materialized *vector.Vector
	proc         *process.Process
}

func NewJSONReaderExpressionExecutor(proc *process.Process, reader vector.JSONReader) (*JSONReaderExpressionExecutor, error) {
	if proc == nil {
		return nil, moerr.NewInvalidInputNoCtx("JSON reader expression requires a process")
	}
	source := &JSONReaderExpressionExecutor{proc: proc}
	if err := source.Bind(reader); err != nil {
		return nil, err
	}
	return source, nil
}

// Bind retains first, so a failed replacement preserves the previous version.
func (source *JSONReaderExpressionExecutor) Bind(reader vector.JSONReader) error {
	if reader == nil || source.proc == nil {
		return io.ErrClosedPipe
	}
	retained, err := reader.Window(0, reader.Length())
	if err != nil {
		return err
	}
	source.ResetForNextQuery()
	source.reader = retained
	return nil
}

// jsonReaderInput is private to the evaluator. Only sealed reader sources may
// supply this path; ordinary ExpressionExecutor implementations keep Eval.
func (source *JSONReaderExpressionExecutor) jsonReaderInput(_ *process.Process, _ []*batch.Batch, _ []bool) (vector.JSONReader, error) {
	if source.reader == nil {
		if source.original != nil {
			return nil, nil
		}
		return nil, io.ErrClosedPipe
	}
	reader, err := source.reader.Window(0, source.reader.Length())
	return reader, err
}

func (source *JSONReaderExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, selected []bool) (*vector.Vector, error) {
	if source.reader == nil {
		if source.original != nil {
			return source.original.Eval(proc, batches, selected)
		}
		return nil, io.ErrClosedPipe
	}
	if source.materialized != nil {
		source.materialized.Free(source.proc.Mp())
		source.materialized = nil
	}
	result, err := source.reader.Materialize(source.proc.Mp())
	if err != nil {
		return nil, err
	}
	source.materialized = result
	return result, nil
}

func (source *JSONReaderExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selected []bool) (*vector.Vector, error) {
	if source.reader == nil {
		if source.original != nil {
			return source.original.EvalWithoutResultReusing(proc, batches, selected)
		}
		return nil, io.ErrClosedPipe
	}
	return source.reader.Materialize(source.proc.Mp())
}

func (source *JSONReaderExpressionExecutor) ResetForNextQuery() {
	if source.original != nil {
		source.original.ResetForNextQuery()
	}
	if source.reader != nil {
		source.reader.Close()
		source.reader = nil
	}
	if source.materialized != nil {
		source.materialized.Free(source.proc.Mp())
		source.materialized = nil
	}
}
func (source *JSONReaderExpressionExecutor) Free() {
	source.ResetForNextQuery()
	if source.original != nil {
		source.original.Free()
		source.original = nil
	}
	source.proc = nil
}
func (source *JSONReaderExpressionExecutor) IsColumnExpr() bool { return true }
func (source *JSONReaderExpressionExecutor) TypeName() string   { return "json-reader-column" }

// A constructor used directly by JSON_VALUE writes into a private owner before
// any mutable result can escape. Other functions keep the ordinary Eval path.
func (expr *FunctionExpressionExecutor) supportsJSONReaderConstructor() bool {
	if expr.resultType.Oid != types.T_json {
		return false
	}
	if expr.fid == function.JSON_ARRAY || expr.fid == function.JSON_OBJECT {
		return true
	}
	if expr.fid == function.CAST && len(expr.parameterTypes) == 2 {
		source := expr.parameterTypes[0].Oid
		return source.IsMySQLString() || source == types.T_json || source == types.T_any
	}
	return false
}

func jsonConstantReaderView(reader vector.JSONReader, length int) (vector.JSONReader, error) {
	if length <= 1 {
		return reader.Window(0, length)
	}
	return reader.Repeat(0, length)
}

// jsonConstructorExpressionExecutor exists only for a constructor consumed
// directly by JSON_VALUE. Its metadata and constant version belong to the
// current execution, rather than every ordinary function executor.
type jsonConstructorExpressionExecutor struct {
	*FunctionExpressionExecutor
	proc      *process.Process
	selection *vector.AllocationAccountSelection
	constant  vector.JSONReader
	release   func()
}

func newJSONConstructorExpressionExecutor(proc *process.Process, expr *FunctionExpressionExecutor, selection *vector.AllocationAccountSelection) (*jsonConstructorExpressionExecutor, error) {
	admission, err := function.JSONReaderMemoryAdmissionForEvaluation(proc)
	if err != nil {
		return nil, err
	}
	release, err := vector.ReserveJSONReaderMetadata(selection, admission, vector.JSONReaderMetadataCapacity(unsafe.Sizeof(jsonConstructorExpressionExecutor{})))
	if err != nil {
		return nil, err
	}
	return &jsonConstructorExpressionExecutor{FunctionExpressionExecutor: expr, proc: proc, selection: selection, release: release}, nil
}

func (source *jsonConstructorExpressionExecutor) ensureMetadata() error {
	if source.release != nil {
		return nil
	}
	admission, err := function.JSONReaderMemoryAdmissionForEvaluation(source.proc)
	if err != nil {
		return err
	}
	source.release, err = vector.ReserveJSONReaderMetadata(source.selection, admission, vector.JSONReaderMetadataCapacity(unsafe.Sizeof(jsonConstructorExpressionExecutor{})))
	return err
}

func (source *jsonConstructorExpressionExecutor) ResetForNextQuery() {
	if source.constant != nil {
		source.constant.Close()
		source.constant = nil
	}
	if source.release != nil {
		source.release()
		source.release = nil
	}
	if source.FunctionExpressionExecutor != nil {
		source.FunctionExpressionExecutor.ResetForNextQuery()
	}
}

func (source *jsonConstructorExpressionExecutor) Free() {
	source.ResetForNextQuery()
	if source.FunctionExpressionExecutor != nil {
		source.FunctionExpressionExecutor.Free()
		source.FunctionExpressionExecutor = nil
	}
	source.proc = nil
}

func (source *jsonConstructorExpressionExecutor) jsonReaderInput(proc *process.Process, batches []*batch.Batch, selected []bool) (vector.JSONReader, error) {
	if err := source.ensureMetadata(); err != nil {
		return nil, err
	}
	expr := source.FunctionExpressionExecutor
	if expr.folded.needFoldingCheck {
		if err := expr.doFoldWithJSONConstructor(proc, proc.GetBaseProcessRunningStatus(), source); err != nil {
			return nil, err
		}
	}
	if reader := source.constant; reader != nil {
		// A scalar constructor has one physical document. Views repeat its
		// row coordinate, without repeating payload copies or admission.
		return jsonConstantReaderView(reader, expressionRowCount(batches))
	}
	for i, parameter := range expr.parameterExecutor {
		result, err := parameter.Eval(proc, batches, selected)
		if err != nil {
			return nil, err
		}
		expr.parameterResults[i] = result
	}
	var selection *function.FunctionSelectList
	var mask function.FunctionSelectList
	if selected != nil {
		mask.SelectList = selected
		mask.AllNull = true
		for _, keep := range selected {
			if keep {
				mask.AllNull = false
			} else {
				mask.AnyNull = true
			}
		}
		selection = &mask
	}
	reader, err := function.EvalJSONConstructorReader(expr.fid, expr.semanticParameterVectors(expr.parameterResults), proc, expressionRowCount(batches), selection, expr.allocation)
	if err != nil {
		return nil, err
	}
	return reader, nil
}
