// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package minusall

import (
	"bytes"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "minus_all"

func (minusAll *MinusAll) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": minus all ")
}

func (minusAll *MinusAll) OpType() vm.OpType { return vm.MinusAll }

func (minusAll *MinusAll) Prepare(proc *process.Process) error {
	var err error
	if minusAll.OpAnalyzer == nil {
		minusAll.OpAnalyzer = process.NewAnalyzer(minusAll.GetIdx(), minusAll.IsFirst, minusAll.IsLast, "minusAll")
	} else {
		minusAll.OpAnalyzer.Reset()
	}
	minusAll.ctr.hashTable, err = hashmap.NewStrHashMap(true, proc.Mp())
	if err != nil {
		return err
	}
	if err = minusAll.ctr.keyEvaluator.Prepare(proc, minusAll.KeyExprs); err != nil {
		minusAll.ctr.cleanHashMap()
		return err
	}
	if len(minusAll.ctr.selected) == 0 {
		minusAll.ctr.selected = make([]uint8, hashmap.UnitLimit)
		minusAll.ctr.zeroes = make([]uint8, hashmap.UnitLimit)
	}
	return nil
}

func (minusAll *MinusAll) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := minusAll.OpAnalyzer
	for {
		switch minusAll.ctr.state {
		case buildingHashMap:
			if err := minusAll.build(proc, analyzer); err != nil {
				return vm.CancelResult, err
			}
			analyzer.Alloc(minusAll.ctr.hashTable.Size())
			minusAll.ctr.state = probingHashMap
		case probingHashMap:
			result := vm.NewCallResult()
			last, err := minusAll.probe(proc, analyzer, &result)
			if err != nil {
				return result, err
			}
			if last {
				minusAll.ctr.state = operatorEnd
				continue
			}
			return result, nil
		case operatorEnd:
			return vm.CancelResult, nil
		}
	}
}

func (minusAll *MinusAll) build(proc *process.Process, analyzer process.Analyzer) error {
	ctr := &minusAll.ctr
	for {
		input, err := vm.ChildrenCall(minusAll.GetChildren(1), proc, analyzer)
		if err != nil {
			return err
		}
		if input.Batch == nil {
			return nil
		}
		if input.Batch.IsEmpty() {
			continue
		}
		keyVecs, err := ctr.keyEvaluator.Eval(proc, input.Batch)
		if err != nil {
			return err
		}
		itr := ctr.hashTable.NewIterator()
		count := input.Batch.RowCount()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := min(count-i, hashmap.UnitLimit)
			values, _, err := itr.Insert(i, n, keyVecs)
			if err != nil {
				return err
			}
			if err = ctr.ensureRemaining(int(ctr.hashTable.GroupCount()), proc.Mp()); err != nil {
				return err
			}
			for _, value := range values {
				if value == 0 {
					continue
				}
				idx := value - 1
				if ctr.remaining[idx] == math.MaxUint64 {
					return moerr.NewInternalErrorNoCtx("EXCEPT ALL multiplicity overflow")
				}
				ctr.remaining[idx]++
			}
		}
	}
}

func (minusAll *MinusAll) probe(proc *process.Process, analyzer process.Analyzer, result *vm.CallResult) (bool, error) {
	ctr := &minusAll.ctr
	for {
		input, err := vm.ChildrenCall(minusAll.GetChildren(0), proc, analyzer)
		if err != nil {
			return false, err
		}
		if input.Batch == nil {
			return true, nil
		}
		if input.Batch.Last() {
			result.Batch = input.Batch
			return false, nil
		}
		if input.Batch.IsEmpty() {
			continue
		}
		if ctr.bat == nil {
			ctr.bat = batch.NewWithSize(len(input.Batch.Vecs))
			for i := range input.Batch.Vecs {
				ctr.bat.Vecs[i] = vector.NewVec(*input.Batch.Vecs[i].GetType())
			}
		}
		ctr.bat.CleanOnlyData()
		keyVecs, err := ctr.keyEvaluator.Eval(proc, input.Batch)
		if err != nil {
			return false, err
		}
		itr := ctr.hashTable.NewIterator()
		count := input.Batch.RowCount()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := min(count-i, hashmap.UnitLimit)
			copy(ctr.selected[:n], ctr.zeroes[:n])
			values, _, err := itr.Find(i, n, keyVecs)
			if err != nil {
				return false, err
			}
			selected := 0
			for row, value := range values {
				if value != 0 && ctr.remaining[value-1] > 0 {
					ctr.remaining[value-1]--
					continue
				}
				ctr.selected[row] = 1
				selected++
			}
			ctr.bat.AddRowCount(selected)
			if selected > 0 {
				for col := range input.Batch.Vecs {
					if err := ctr.bat.Vecs[col].UnionBatch(input.Batch.Vecs[col], int64(i), selected, ctr.selected[:n], proc.Mp()); err != nil {
						return false, err
					}
				}
			}
		}
		if ctr.bat.IsEmpty() {
			continue
		}
		analyzer.Alloc(int64(ctr.bat.Size()))
		result.Batch = ctr.bat
		return false, nil
	}
}

func (ctr *container) ensureRemaining(length int, mp *mpool.MPool) error {
	if length <= len(ctr.remaining) {
		return nil
	}
	oldLength := len(ctr.remaining)
	if length <= cap(ctr.remaining) {
		ctr.remaining = ctr.remaining[:length]
		clear(ctr.remaining[oldLength:])
		return nil
	}
	capacity := cap(ctr.remaining)
	if capacity == 0 {
		capacity = 1
	}
	for capacity < length {
		if capacity > math.MaxInt/2 {
			capacity = length
			break
		}
		capacity *= 2
	}
	next, err := mpool.MakeSlice[uint64](capacity, mp, true)
	if err != nil {
		return err
	}
	copy(next, ctr.remaining)
	clear(next[oldLength:])
	if cap(ctr.remaining) > 0 {
		mpool.FreeSlice(mp, ctr.remaining)
	}
	ctr.remaining = next[:length]
	return nil
}
