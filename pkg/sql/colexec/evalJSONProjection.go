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
	column int
	source *JSONReaderExpressionExecutor
}

// PrepareJSONProjectionInputs discovers bindings once, not per row or lookup.
// Only a JSON_VALUE document column is replaced; other consumers retain their ordinary input.
func PrepareJSONProjectionInputs(proc *process.Process, roots []ExpressionExecutor) JSONProjectionInputs {
	inputs := make(JSONProjectionInputs, 0)
	seen := make(map[*FunctionExpressionExecutor]bool)
	var visit func(ExpressionExecutor)
	visit = func(executor ExpressionExecutor) {
		if executor == nil {
			return
		}
		switch expr := executor.(type) {
		case *memoRootExpressionExecutor:
			visit(expr.executor)
		case *memoExpressionExecutor:
			visit(expr.state.executor)
		case *FunctionExpressionExecutor:
			if seen[expr] {
				return
			}
			seen[expr] = true
			if expr.fid == function.JSON_VALUE && len(expr.parameterExecutor) > 0 {
				if column := projectionJSONColumn(expr.parameterExecutor[0]); column != nil && column.relIndex == 0 && column.typ.Oid == types.T_json {
					source := &JSONReaderExpressionExecutor{original: expr.parameterExecutor[0], proc: proc}
					expr.parameterExecutor[0] = source
					inputs = append(inputs, projectionJSONInput{column: column.colIndex, source: source})
				}
			}
			for _, parameter := range expr.parameterExecutor {
				visit(parameter)
			}
		}
	}
	for _, root := range roots {
		visit(root)
	}
	return inputs
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
			reader, invalid, err = vector.TryNewJSONReaderSnapshotForRows(input.Vecs[binding.column], input.RowCount(), proc.Mp(), allocation)
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
		binding.source.ResetForNextQuery()
	}
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
	return projection.jsonInputs.Bind(proc, input, projection.jsonAllocation, readers)
}
func (projection *Projection) releaseJSONInputs() { projection.jsonInputs.Release() }
func (projection *Projection) JSONInputColumns() []int {
	if len(projection.ProjectList) == 0 {
		return nil
	}
	return projection.jsonInputs.Columns()
}
