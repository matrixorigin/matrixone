// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"errors"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ExpressionsAcceptDictionary closes the actual expression tree, including
// shared subjects. Specialized executors remain behind a flat boundary until
// their physical reads have been converted.
func ExpressionsAcceptDictionary(executors []ExpressionExecutor) bool {
	for _, executor := range executors {
		switch e := executor.(type) {
		case *ColumnExpressionExecutor, *FixedVectorExpressionExecutor:
		case *FunctionExpressionExecutor:
			if !ExpressionsAcceptDictionary(e.parameterExecutor) {
				return false
			}
		case *decimalTypedExecutor:
			if !ExpressionsAcceptDictionary([]ExpressionExecutor{e.root}) {
				return false
			}
		case *decimalMulSubExecutor:
			if !ExpressionsAcceptDictionary([]ExpressionExecutor{e.root}) {
				return false
			}
		case *decimalChainExecutor:
			if !ExpressionsAcceptDictionary([]ExpressionExecutor{e.root}) {
				return false
			}
		case *memoExpressionExecutor:
			if !ExpressionsAcceptDictionary([]ExpressionExecutor{e.state.executor}) {
				return false
			}
		case *memoRootExpressionExecutor:
			if !ExpressionsAcceptDictionary([]ExpressionExecutor{e.executor}) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// SelectBatchColumns owns only the listed populated columns. Other schema slots
// remain nil, matching early-reader column positions. Declined retention gathers
// the same rows once without reevaluating predicates or copying the full source.
func SelectBatchColumns(input *batch.Batch, positions []int, rows []int64, mp *mpool.MPool, account *vector.AllocationAccountSelection, retain bool) (_ *batch.Batch, retained bool, retErr error) {
	if len(rows) == 0 || len(input.Vecs) == 0 {
		retain = false
	}
	output := batch.NewOffHeapWithSize(len(input.Vecs))
	defer func() {
		if retErr != nil {
			output.Clean(mp)
		}
	}()
	if err := output.SetAllocationAccount(account); err != nil {
		return nil, false, err
	}
	if positions == nil {
		if retain {
			admitted, err := vector.TrySelectDictionaryVectors(input.Vecs, output.Vecs, rows, input.RowCount(), mp, account)
			if err != nil {
				return nil, false, err
			}
			retained = admitted
		}
		if !retained {
			for i, source := range input.Vecs {
				if source == nil {
					continue
				}
				dst, err := vector.NewOffHeapVecWithTypeAndAllocation(*source.GetType(), account)
				if err != nil {
					return nil, false, err
				}
				output.Vecs[i] = dst
				if err := dst.Union(source, rows, mp); err != nil {
					return nil, false, err
				}
				source.CopyExpressionMetadataTo(dst)
			}
		}
	} else {
		sources := make([]*vector.Vector, len(positions))
		destinations := make([]*vector.Vector, len(positions))
		for i, pos := range positions {
			if pos < 0 || pos >= len(input.Vecs) || input.Vecs[pos] == nil {
				return nil, false, moerr.NewInvalidInputNoCtx("invalid selected reader column")
			}
			sources[i] = input.Vecs[pos]
		}
		if retain {
			admitted, err := vector.TrySelectDictionaryVectors(sources, destinations, rows, input.RowCount(), mp, account)
			if err != nil {
				return nil, false, err
			}
			retained = admitted
		}
		if retained {
			for i, pos := range positions {
				output.Vecs[pos] = destinations[i]
			}
		} else {
			for i, pos := range positions {
				source := sources[i]
				dst, err := vector.NewOffHeapVecWithTypeAndAllocation(*source.GetType(), account)
				if err != nil {
					return nil, false, err
				}
				output.Vecs[pos] = dst
				if err := dst.Union(source, rows, mp); err != nil {
					return nil, false, err
				}
				source.CopyExpressionMetadataTo(dst)
			}
		}
	}
	output.Attrs = append(output.Attrs, input.Attrs...)
	output.Recursive, output.ShuffleIDX = input.Recursive, input.ShuffleIDX
	output.SetRowCount(len(rows))
	return output, retained, nil
}

// SelectPredicateRows preserves evaluation order and only evaluates survivors
// of preceding predicates. Temporary views have one local owner. Final rows are
// borrowed from scratch until the caller next evaluates this chain.
func SelectPredicateRows(proc *process.Process, input *batch.Batch, loaded []int, executors []ExpressionExecutor, account *vector.AllocationAccountSelection, scratch *[]int64, parameter *vector.FunctionParameterWrapper[bool], analyzer process.Analyzer, beforeEval func(int, int), frame *DictionaryInputFrame) ([]int64, bool, error) {
	*scratch = (*scratch)[:0]
	current := input
	var bases []*vector.DictionaryBase
	var declined []bool
	defer func() {
		for _, base := range bases {
			if base != nil {
				base.Release()
			}
		}
	}()

	if frame != nil {
		frame.Begin(current, account, proc.Mp())
		defer func() { frame.Observe(analyzer); frame.End() }()
	}
	var owned *batch.Batch
	defer func() {
		if owned != nil {
			if frame != nil {
				frame.recyclePredicate(owned, proc.Mp())
			} else {
				owned.Clean(proc.Mp())
			}
		}
	}()
	selected := false
	for i := range executors {
		rows := input.RowCount()
		if selected {
			rows = len(*scratch)
		}
		if beforeEval != nil {
			beforeEval(i, rows)
		}
		if selected {
			if owned != nil {
				if frame != nil {
					frame.recyclePredicate(owned, proc.Mp())
				} else {
					owned.Clean(proc.Mp())
				}
				owned = nil
			}
			if bases == nil {
				bases = make([]*vector.DictionaryBase, len(input.Vecs))
				declined = make([]bool, len(input.Vecs))
			}
			var reuse []*vector.Vector
			if frame != nil {
				if frame.predicateHeaders == nil {
					frame.predicateHeaders = make([]*vector.Vector, len(input.Vecs))
				}
				reuse = frame.predicateHeaders
			}
			next, err := selectPredicateInput(proc, input, loaded, executors[i], *scratch, account, bases, declined, reuse, frame)
			if err != nil {
				return nil, false, err
			}
			owned, current = next, next
			if frame != nil {
				frame.Begin(current, account, proc.Mp())
			}
		} else if !ExpressionsAcceptDictionary(executors[i:i+1]) && current.HasDictionary() {
			flat, err := current.FlatCopy(proc.Mp(), account)
			if err != nil {
				return nil, false, err
			}
			owned, current = flat, flat
			if frame != nil {
				frame.Begin(current, account, proc.Mp())
			}
		}

		if frame != nil {
			frame.Bind(executors[i : i+1])
		}
		value, err := executors[i].Eval(proc, []*batch.Batch{current}, nil)
		if err != nil {
			return nil, false, err
		}
		if !value.GetType().IsBoolean() {
			return nil, false, moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
		}
		if proc.OperatorOutofMemory(int64(value.Size())) {
			return nil, false, moerr.NewOOM(proc.Ctx)
		}
		if analyzer != nil {
			analyzer.Alloc(int64(value.Size()))
		}
		if *parameter == nil || !vector.ReuseFunctionFixedTypeParameter(value, *parameter) {
			*parameter = vector.GenerateFunctionFixedTypeParameter[bool](value)
		}
		if value.IsConst() {
			keep, isNull := (*parameter).GetValue(0)
			if isNull || !keep {
				*scratch = (*scratch)[:0]
				return *scratch, false, nil
			}
			continue
		}

		*scratch = compactPredicateRows(value, current.RowCount(), *scratch, selected)
		if len(*scratch) == current.RowCount() {
			if !selected {
				*scratch = (*scratch)[:0]
			}
			continue
		}
		selected = true
		if len(*scratch) == 0 {
			return *scratch, false, nil
		}

	}
	return *scratch, !selected, nil
}

// compactPredicateRows writes each surviving original position once. A selected
// input's positions are compacted in place: the write index never exceeds the
// current read index, including when the original row domain contains repeats.
func compactPredicateRows(value *vector.Vector, rows int, scratch []int64, selected bool) []int64 {
	if !selected {
		scratch = slices.Grow(scratch[:0], rows)[:rows]
	}
	view := vector.ResolveFixedReadView[bool](value)
	kept := 0
	if view.Selection == nil && !view.Constant && !view.Nulls.Any() {
		if selected {
			for row, keep := range view.Values[:rows] {
				if keep {
					scratch[kept] = scratch[row]
					kept++
				}
			}
		} else {
			for row, keep := range view.Values[:rows] {
				if keep {
					scratch[kept] = int64(row)
					kept++
				}
			}
		}
	} else {
		for row := 0; row < rows; row++ {
			if view.AllNull || view.Nulls.Contains(uint64(row)) {
				continue
			}
			physical := row
			if view.Selection != nil {
				physical = int(view.Selection[row])
			}
			if view.Constant {
				physical = 0
			}
			if view.Values[physical] {
				original := int64(row)
				if selected {
					original = scratch[row]
				}
				scratch[kept] = original
				kept++
			}
		}
	}
	return scratch[:kept]
}

// Collect only the current predicate's columns. Unknown executors retain the
// full-schema owned fallback rather than guessing their dependencies.
func predicateColumns(executor ExpressionExecutor, seen []bool, columns *[]int) bool {
	switch e := executor.(type) {
	case *ColumnExpressionExecutor:
		if e.relIndex != 0 || e.colIndex < 0 || e.colIndex >= len(seen) {
			return false
		}
		if !seen[e.colIndex] {
			seen[e.colIndex] = true
			*columns = append(*columns, e.colIndex)
		}
	case *FixedVectorExpressionExecutor:
	case *FunctionExpressionExecutor:
		for _, child := range e.parameterExecutor {
			if !predicateColumns(child, seen, columns) {
				return false
			}
		}
	case *decimalTypedExecutor:
		return predicateColumns(e.root, seen, columns)
	case *decimalMulSubExecutor:
		return predicateColumns(e.root, seen, columns)
	case *decimalChainExecutor:
		return predicateColumns(e.root, seen, columns)
	case *memoExpressionExecutor:
		return predicateColumns(e.state.executor, seen, columns)
	case *memoRootExpressionExecutor:
		return predicateColumns(e.executor, seen, columns)
	default:
		return false
	}
	return true
}

func selectPredicateInput(proc *process.Process, input *batch.Batch, loaded []int, executor ExpressionExecutor, rows []int64, account *vector.AllocationAccountSelection, bases []*vector.DictionaryBase, declined []bool, reuse []*vector.Vector, frame *DictionaryInputFrame) (result *batch.Batch, retErr error) {
	var columns []int
	if !predicateColumns(executor, make([]bool, len(input.Vecs)), &columns) {
		result, _, err := SelectBatchColumns(input, loaded, rows, proc.Mp(), account, false)
		return result, err
	}
	output := batch.NewOffHeapWithSize(len(input.Vecs))
	defer func() {
		if retErr != nil {
			output.Clean(proc.Mp())
			if errors.Is(retErr, mpool.ErrAllocationAccountCapacity) || errors.Is(retErr, mpool.ErrAllocationAccountMismatch) {
				result, _, retErr = SelectBatchColumns(input, columns, rows, proc.Mp(), account, false)
			}
		}
	}()
	if err := output.SetAllocationAccount(account); err != nil {
		return nil, err
	}
	output.Attrs = append(output.Attrs, input.Attrs...)
	output.Recursive, output.ShuffleIDX = input.Recursive, input.ShuffleIDX
	output.SetRowCount(len(rows))
	if len(columns) == 0 {
		return output, nil
	}
	var rowSlot *vector.DictionaryVectorSlot
	if frame != nil {
		rowSlot = frame.selectionSlot
	}
	selection, err := vector.NewRowSelectionReuse(rows, input.RowCount(), proc.Mp(), account, rowSlot)
	if err != nil {
		return nil, err
	}
	defer selection.Release()
	for _, col := range columns {
		var spare **vector.Vector
		if reuse != nil {
			spare = &reuse[col]
		}
		source := input.Vecs[col]
		if source == nil {
			return nil, moerr.NewInvalidInput(proc.Ctx, "predicate references unloaded column")
		}
		if source.IsDictionary() {
			output.Vecs[col], err = source.SelectDictionaryReuse(selection, proc.Mp(), account, spare)
		} else {
			if bases[col] == nil && !declined[col] {
				var admitted bool
				var slot *vector.DictionaryVectorSlot
				if frame != nil {
					slot = frame.snapshots[col]
				}
				bases[col], admitted, err = vector.TryFreezeVectorReuse(source, proc.Mp(), account, slot)
				if err != nil {
					return nil, err
				}
				declined[col] = !admitted
			}
			if bases[col] != nil {
				output.Vecs[col], err = vector.NewDictionaryVectorReuse(bases[col], selection, proc.Mp(), account, spare)
			} else {
				output.Vecs[col], err = vector.NewOffHeapVecWithTypeAndAllocation(*source.GetType(), account)
				if err == nil {
					err = output.Vecs[col].Union(source, rows, proc.Mp())
					source.CopyExpressionMetadataTo(output.Vecs[col])
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}
