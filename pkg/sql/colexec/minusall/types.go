// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package minusall

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MinusAll)

const (
	buildingHashMap = iota
	probingHashMap
	operatorEnd
)

type container struct {
	state int

	hashTable *hashmap.StrHashMap
	remaining []uint64
	selected  []uint8
	zeroes    []uint8
	bat       *batch.Batch

	keyEvaluator colexec.SetOperationKeyEvaluator
}

type MinusAll struct {
	ctr      container
	KeyExprs []*plan.Expr

	vm.OperatorBase
}

func (minusAll *MinusAll) GetOperatorBase() *vm.OperatorBase { return &minusAll.OperatorBase }

func init() {
	reuse.CreatePool[MinusAll](
		func() *MinusAll { return &MinusAll{} },
		func(a *MinusAll) { *a = MinusAll{} },
		reuse.DefaultOptions[MinusAll]().WithEnableChecker(),
	)
}

func (minusAll MinusAll) TypeName() string { return opName }

func NewArgument() *MinusAll { return reuse.Alloc[MinusAll](nil) }

func (minusAll *MinusAll) Release() {
	if minusAll != nil {
		reuse.Free[MinusAll](minusAll, nil)
	}
}

func (minusAll *MinusAll) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &minusAll.ctr
	ctr.state = buildingHashMap
	ctr.cleanHashMap()
	ctr.cleanRemaining(proc.Mp())
	ctr.keyEvaluator.Reset()
	if ctr.bat != nil {
		ctr.bat.CleanOnlyData()
	}
}

func (minusAll *MinusAll) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &minusAll.ctr
	ctr.cleanHashMap()
	ctr.cleanRemaining(proc.Mp())
	ctr.keyEvaluator.Free()
	if ctr.bat != nil {
		ctr.bat.Clean(proc.Mp())
		ctr.bat = nil
	}
}

func (minusAll *MinusAll) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) cleanHashMap() {
	if ctr.hashTable != nil {
		ctr.hashTable.Free()
		ctr.hashTable = nil
	}
}

func (ctr *container) cleanRemaining(mp *mpool.MPool) {
	if cap(ctr.remaining) > 0 {
		mpool.FreeSlice(mp, ctr.remaining)
	}
	ctr.remaining = nil
}
