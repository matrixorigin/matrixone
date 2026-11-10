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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// JSONProjectionInputs owns bindings inside the expression executors supplied
// at Prepare. Their caller owns executor cleanup; Release ends one evaluation.
type JSONProjectionInputs []projectionJSONInput

type projectionJSONInput struct {
	column   int
	source   *JSONReaderExpressionExecutor
	executor *FunctionExpressionExecutor
	scope    *JSONMetadataScope
}

// jsonProjectionRoot keeps JSON-only state alongside an existing executor.
// Ordinary projections retain exactly their existing executor storage.
type jsonProjectionRoot struct {
	ExpressionExecutor
	inputs     JSONProjectionInputs
	columns    []int
	allocation *vector.AllocationAccountSelection
}

// PrepareJSONProjectionExecutors installs an accounted wrapper only when a
// projection has JSON_VALUE document-column demand.
func PrepareJSONProjectionExecutors(proc *process.Process, roots []ExpressionExecutor, selection *vector.AllocationAccountSelection) error {
	if len(roots) == 0 || jsonProjectionState(roots) != nil {
		return nil
	}
	inputs, err := PrepareJSONProjectionInputs(proc, roots, selection)
	if err != nil || len(inputs) == 0 {
		return err
	}
	roots[0] = &jsonProjectionRoot{ExpressionExecutor: roots[0], inputs: inputs, columns: inputs.Columns(), allocation: selection}
	return nil
}

func jsonProjectionState(roots []ExpressionExecutor) *jsonProjectionRoot {
	if len(roots) == 0 {
		return nil
	}
	state, _ := roots[0].(*jsonProjectionRoot)
	return state
}

func JSONProjectionColumns(roots []ExpressionExecutor) []int {
	if state := jsonProjectionState(roots); state != nil {
		return state.columns
	}
	return nil
}

func BindJSONProjectionExecutors(proc *process.Process, roots []ExpressionExecutor, input *batch.Batch, published map[int]vector.JSONReader) error {
	if state := jsonProjectionState(roots); state != nil {
		return state.inputs.Bind(proc, input, state.allocation, published)
	}
	return nil
}

func ReleaseJSONProjectionExecutors(roots []ExpressionExecutor) {
	if state := jsonProjectionState(roots); state != nil {
		state.inputs.Release()
	}
}

func CloseJSONProjectionExecutors(roots []ExpressionExecutor) {
	if state := jsonProjectionState(roots); state != nil {
		roots[0] = state.ExpressionExecutor
		state.inputs.Close()
		state.inputs = nil
		state.columns = nil
		state.allocation = nil
		state.ExpressionExecutor = nil
	}
}

func projectionJSONSource(expr *FunctionExpressionExecutor) *ColumnExpressionExecutor {
	if expr.fid != function.JSON_VALUE || len(expr.parameterExecutor) == 0 {
		return nil
	}
	column := projectionJSONColumn(expr.parameterExecutor[0])
	if column == nil || column.relIndex != 0 || column.typ.Oid != types.T_json {
		return nil
	}
	return column
}

// hasJSONProjectionSource uses only preparation-local marks in existing
// executors. The second walk clears every mark, including shared memo nodes.
// It does not allocate a discovery map for projections without JSON_VALUE.
func hasJSONProjectionSource(roots []ExpressionExecutor) bool {
	var visit func(ExpressionExecutor) bool
	visit = func(executor ExpressionExecutor) bool {
		switch expr := executor.(type) {
		case *memoRootExpressionExecutor:
			return visit(expr.executor)
		case *memoExpressionExecutor:
			return expr.state != nil && visit(expr.state.executor)
		case *jsonConstructorExpressionExecutor:
			return expr.FunctionExpressionExecutor != nil && visit(expr.FunctionExpressionExecutor)
		case *FunctionExpressionExecutor:
			if expr.jsonProjectionProbe {
				return false
			}
			expr.jsonProjectionProbe = true
			if projectionJSONSource(expr) != nil {
				return true
			}
			for _, parameter := range expr.parameterExecutor {
				if visit(parameter) {
					return true
				}
			}
		}
		return false
	}
	var clearMarks func(ExpressionExecutor)
	clearMarks = func(executor ExpressionExecutor) {
		switch expr := executor.(type) {
		case *memoRootExpressionExecutor:
			clearMarks(expr.executor)
		case *memoExpressionExecutor:
			if expr.state != nil {
				clearMarks(expr.state.executor)
			}
		case *jsonConstructorExpressionExecutor:
			if expr.FunctionExpressionExecutor != nil {
				clearMarks(expr.FunctionExpressionExecutor)
			}
		case *FunctionExpressionExecutor:
			if !expr.jsonProjectionProbe {
				return
			}
			expr.jsonProjectionProbe = false
			for _, parameter := range expr.parameterExecutor {
				clearMarks(parameter)
			}
		}
	}
	defer func() {
		for _, root := range roots {
			clearMarks(root)
		}
	}()
	for _, root := range roots {
		if visit(root) {
			return true
		}
	}
	return false
}

// PrepareJSONProjectionInputs discovers bindings once, not per row or lookup.
// Only a JSON_VALUE document column is replaced; other consumers retain their ordinary input.
func PrepareJSONProjectionInputs(proc *process.Process, roots []ExpressionExecutor, selections ...*vector.AllocationAccountSelection) (JSONProjectionInputs, error) {
	if !hasJSONProjectionSource(roots) {
		return nil, nil
	}
	var selection *vector.AllocationAccountSelection
	if len(selections) != 0 {
		selection = selections[0]
	}
	admission, err := function.JSONReaderMemoryAdmissionForEvaluation(proc)
	if err != nil {
		return nil, err
	}
	scope, err := newJSONMetadataScopeWithAdmission(selection, admission, 4)
	if err != nil {
		return nil, err
	}
	inputs := make(JSONProjectionInputs, 0)
	seen := make(map[*FunctionExpressionExecutor]bool)
	var walkErr error
	var visit func(ExpressionExecutor)
	visit = func(executor ExpressionExecutor) {
		if executor == nil || walkErr != nil {
			return
		}
		switch expr := executor.(type) {
		case *memoRootExpressionExecutor:
			visit(expr.executor)
		case *memoExpressionExecutor:
			if expr.state != nil {
				visit(expr.state.executor)
			}
		case *jsonConstructorExpressionExecutor:
			if expr.FunctionExpressionExecutor != nil {
				visit(expr.FunctionExpressionExecutor)
			}
		case *FunctionExpressionExecutor:
			if seen[expr] {
				return
			}
			if walkErr = scope.Ensure(len(seen) + 1); walkErr != nil {
				return
			}
			seen[expr] = true
			if column := projectionJSONSource(expr); column != nil {
				source := &JSONReaderExpressionExecutor{original: expr.parameterExecutor[0], proc: proc}
				expr.parameterExecutor[0] = source
				inputs = append(inputs, projectionJSONInput{column: column.colIndex, source: source, executor: expr, scope: scope})
			}
			for _, parameter := range expr.parameterExecutor {
				visit(parameter)
			}
		}
	}
	for _, root := range roots {
		visit(root)
	}
	if walkErr != nil {
		if len(inputs) == 0 {
			scope.Close()
		} else {
			inputs.Close()
		}
		return nil, walkErr
	}
	if len(inputs) == 0 {
		scope.Close()
		return nil, nil
	}
	return inputs, nil
}

// Bind creates one version per source column for this read-only
// projection call, independent of whether the batch came from disk, memory or
// a remote receiver. No version is cached across calls or keyed by an address.
// This includes publication costs; it is not the final zero-extra-copy scan
// optimization and must be included in end-to-end performance measurements.
func (inputs JSONProjectionInputs) Bind(proc *process.Process, input *batch.Batch, selection *vector.AllocationAccountSelection, published map[int]vector.JSONReader) (err error) {
	if len(inputs) == 0 {
		return nil
	}
	admission, err := function.JSONReaderMemoryAdmission(proc)
	if err != nil {
		return err
	}
	capacity, err := vector.JSONReaderContainerMetadataCapacity(unsafe.Sizeof(map[int]vector.JSONReader(nil)), len(inputs))
	if err != nil {
		return err
	}
	releaseMeta, err := vector.ReserveJSONReaderMetadata(nil, admission, capacity)
	if err != nil {
		return err
	}
	defer releaseMeta()
	readers := make(map[int]vector.JSONReader)
	var declined map[int]struct{}
	defer func() {
		for _, reader := range readers {
			reader.Close()
		}
		if err != nil {
			inputs.Release()
		}
	}()
	for _, binding := range inputs {
		if _, skip := declined[binding.column]; skip {
			continue
		}
		if binding.column < 0 || binding.column >= len(input.Vecs) || input.Vecs[binding.column] == nil {
			return moerr.NewInvalidInputNoCtx("JSON projection column out of range")
		}
		reader := readers[binding.column]
		supplied, present := published[binding.column]
		if reader == nil && present && supplied == nil {
			// Publication found malformed mutable bytes. The original evaluator
			// must determine whether this expression actually reads them.
			if declined == nil {
				declined = make(map[int]struct{})
			}
			declined[binding.column] = struct{}{}
			continue
		}
		if reader == nil && supplied != nil {
			if supplied.Length() != input.RowCount() {
				return moerr.NewInvalidInputNoCtx("published JSON row mapping mismatch")
			}
			reader, err = supplied.Window(0, supplied.Length())
			if err != nil {
				return err
			}
			readers[binding.column] = reader
		}
		if reader == nil {
			allocation := selection
			if allocation == nil {
				allocation = input.Vecs[binding.column].AllocationAccountSelection()
			}
			var invalid bool
			reader, invalid, err = vector.TryNewJSONReaderSnapshotForRows(input.Vecs[binding.column], input.RowCount(), proc.Mp(), allocation, admission)
			if err != nil {
				if invalid {
					err = nil
					if declined == nil {
						declined = make(map[int]struct{})
					}
					declined[binding.column] = struct{}{}
					continue
				}
				return err
			}
			readers[binding.column] = reader
		}
		if err = binding.source.Bind(reader); err != nil {
			return err
		}
	}
	return nil
}

func (inputs JSONProjectionInputs) Release() {
	for _, binding := range inputs {
		if binding.source != nil {
			binding.source.ResetForNextQuery()
		}
	}
}

// Close removes preparation-only wrappers and releases their statement budget.
// It is used at Reset/Free so a later execution rebuilds its own bindings.
func (inputs JSONProjectionInputs) Close() {
	if len(inputs) == 0 {
		return
	}
	scope := inputs[0].scope
	for i := range inputs {
		binding := &inputs[i]
		if binding.source == nil || binding.executor == nil {
			continue
		}
		if len(binding.executor.parameterExecutor) != 0 && binding.executor.parameterExecutor[0] == binding.source {
			binding.executor.parameterExecutor[0] = binding.source.original
		}
		binding.source.original = nil
		binding.source.Free()
		binding.source = nil
		binding.executor = nil
		binding.scope = nil
	}
	scope.Close()
}

func projectionJSONColumn(source ExpressionExecutor) *ColumnExpressionExecutor {
	for {
		switch expr := source.(type) {
		case *ColumnExpressionExecutor:
			return expr
		case *memoRootExpressionExecutor:
			source = expr.executor
		case *memoExpressionExecutor:
			source = expr.state.executor
		default:
			return nil
		}
	}
}

// JSONInputColumns reports prepared JSON document column ordinals for producers.
func (inputs JSONProjectionInputs) Columns() []int {
	var columns []int
	seen := make(map[int]bool)
	for _, binding := range inputs {
		if !seen[binding.column] {
			columns = append(columns, binding.column)
			seen[binding.column] = true
		}
	}
	return columns
}

func (projection *Projection) bindJSONInputs(proc *process.Process, input *batch.Batch, published ...map[int]vector.JSONReader) error {
	var readers map[int]vector.JSONReader
	if len(published) != 0 {
		readers = published[0]
	}
	return BindJSONProjectionExecutors(proc, projection.ProjectExecutors, input, readers)
}
func (projection *Projection) releaseJSONInputs() {
	ReleaseJSONProjectionExecutors(projection.ProjectExecutors)
}
func (projection *Projection) JSONInputColumns() []int {
	if len(projection.ProjectList) == 0 {
		return nil
	}
	return JSONProjectionColumns(projection.ProjectExecutors)
}
