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
	if frame != nil {
		frame.Begin(current, account, proc.Mp())
		defer func() { frame.Observe(analyzer); frame.End() }()
	}
	var owned *batch.Batch
	defer func() {
		if owned != nil {
			owned.Clean(proc.Mp())
		}
	}()
	var matches []int64
	defer func() {
		if matches != nil {
			vector.PutSels(matches)
		}
	}()
	selected := false
	for i := range executors {
		if !ExpressionsAcceptDictionary(executors[i:i+1]) && current.HasDictionary() {
			flat, err := current.FlatCopy(proc.Mp(), account)
			if err != nil {
				return nil, false, err
			}
			if owned != nil {
				owned.Clean(proc.Mp())
			}
			owned, current = flat, flat
			if frame != nil {
				frame.Begin(current, account, proc.Mp())
			}
		}
		if beforeEval != nil {
			beforeEval(i, current.RowCount())
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
		if matches == nil {
			matches = vector.GetSels()
		}
		matches = matches[:0]
		view := vector.ResolveFixedReadView[bool](value)
		if view.Selection == nil && !view.Nulls.Any() {
			// Ordinary predicate results keep the existing direct flat loop.
			for row, keep := range view.Values[:current.RowCount()] {
				if keep {
					matches = append(matches, int64(row))
				}
			}
		} else {
			for row := 0; row < current.RowCount(); row++ {
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
					matches = append(matches, int64(row))
				}
			}
		}
		if len(matches) == current.RowCount() {
			continue
		}
		if !selected {
			*scratch = append((*scratch)[:0], matches...)
			selected = true
		} else {
			for j, row := range matches {
				(*scratch)[j] = (*scratch)[row]
			}
			*scratch = (*scratch)[:len(matches)]
		}
		if len(matches) == 0 {
			return *scratch, false, nil
		}
		if i+1 < len(executors) {
			next, _, err := SelectBatchColumns(current, loaded, matches, proc.Mp(), account, ExpressionsAcceptDictionary(executors[i+1:]))
			if err != nil {
				return nil, false, err
			}
			if owned != nil {
				owned.Clean(proc.Mp())
			}
			owned, current = next, next
			if frame != nil {
				frame.Begin(current, account, proc.Mp())
			}
		}
	}
	return *scratch, !selected, nil
}
