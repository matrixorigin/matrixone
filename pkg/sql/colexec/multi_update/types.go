// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MultiUpdate)

const opName = "MultiUpdate"

type UpdateAction int

const (
	UpdateWriteTable UpdateAction = iota
	UpdateWriteS3
	UpdateFlushS3Info
)

type UpdateTableType int

const (
	UpdateMainTable UpdateTableType = iota
	UpdateUniqueIndexTable
	UpdateSecondaryIndexTable
)

type actionType int

const (
	actionInsert actionType = iota
	actionDelete
	actionUpdate
)

func init() {
	reuse.CreatePool[MultiUpdate](
		func() *MultiUpdate {
			return &MultiUpdate{}
		},
		func(a *MultiUpdate) {
			*a = MultiUpdate{}
		},
		reuse.DefaultOptions[MultiUpdate]().
			WithEnableChecker(),
	)
}

type MultiUpdate struct {
	delegated      bool
	input          vm.CallResult
	ctr            container
	MultiUpdateCtx []*MultiUpdateCtx
	mainTable      uint64

	Action                 UpdateAction
	IsOnduplicateKeyUpdate bool
	IsRemote               bool

	Engine engine.Engine

	getS3WriterFunc          func(sid string, id uint64) (*s3WriterDelegate, error)
	getFlushableS3WriterFunc func() *s3WriterDelegate
	addAffectedRowsFunc      func(uint64)

	vm.OperatorBase
}

type updateCtxInfo struct {
	Source      engine.Relation
	tableType   UpdateTableType
	insertAttrs []string
}

type container struct {
	state        vm.CtrState
	affectedRows uint64
	action       actionType

	flushed        bool
	s3Writer       *s3WriterDelegate
	updateCtxInfos map[string]*updateCtxInfo
	sources        map[uint64]engine.Relation

	insertBuf []*batch.Batch
	deleteBuf []*batch.Batch
}

type MultiUpdateCtx struct {
	ObjRef        *plan.ObjectRef
	TableDef      *plan.TableDef
	InsertCols    []int
	DeleteCols    []int
	PartitionCols []int
}

func (update MultiUpdate) TypeName() string {
	return opName
}

func NewArgument() *MultiUpdate {
	return reuse.Alloc[MultiUpdate](nil)
}

func (update *MultiUpdate) Release() {
	if update != nil {
		reuse.Free[MultiUpdate](update, nil)
	}
}

func (update *MultiUpdate) GetOperatorBase() *vm.OperatorBase {
	return &update.OperatorBase
}

func (update *MultiUpdate) Reset(proc *process.Process, pipelineFailed bool, err error) {
	for _, buf := range update.ctr.insertBuf {
		if buf != nil {
			buf.CleanOnlyData()
		}
	}

	for _, buf := range update.ctr.deleteBuf {
		if buf != nil {
			buf.CleanOnlyData()
		}
	}
	if update.ctr.s3Writer != nil {
		update.ctr.s3Writer.reset(proc)
	}

	update.ctr.state = vm.Build
}

func (update *MultiUpdate) Free(proc *process.Process, pipelineFailed bool, err error) {
	mp := proc.GetMPool()
	for _, buf := range update.ctr.insertBuf {
		if buf != nil {
			buf.Clean(mp)
		}
	}
	update.ctr.insertBuf = nil

	for _, buf := range update.ctr.deleteBuf {
		if buf != nil {
			buf.Clean(mp)
		}
	}
	update.ctr.deleteBuf = nil

	if update.ctr.s3Writer != nil {
		update.ctr.s3Writer.free(proc)
		update.ctr.s3Writer = nil
	}

	update.ctr.updateCtxInfos = nil
	update.ctr.sources = nil
}

func (update *MultiUpdate) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (update *MultiUpdate) GetAffectedRows() uint64 {
	return update.ctr.affectedRows
}

func (update *MultiUpdate) SetAffectedRows(affectedRows uint64) {
	update.ctr.affectedRows = affectedRows
}

func (update *MultiUpdate) addInsertAffectRows(tableType UpdateTableType, rowCount uint64) {
	if tableType != UpdateMainTable {
		return
	}
	// For REPLACE INTO, we always count INSERT rows, regardless of update.ctr.action
	// because REPLACE INTO should return at least the number of rows being inserted
	switch update.ctr.action {
	case actionInsert:
		update.addAffectedRowsFunc(rowCount)
	case actionUpdate:
		// For REPLACE INTO with both DELETE and INSERT, count INSERT rows
		update.addAffectedRowsFunc(rowCount)
	}
}

func (update *MultiUpdate) addDeleteAffectRows(tableType UpdateTableType, rowCount uint64) {
	if tableType != UpdateMainTable {
		return
	}
	// For REPLACE INTO, we don't count DELETE rows in affected rows
	// REPLACE INTO should only return the number of INSERT rows
	// Only count DELETE rows for regular UPDATE operations
	switch update.ctr.action {
	case actionDelete:
		// Regular DELETE operation, count it
		update.addAffectedRowsFunc(rowCount)
	case actionUpdate:
		// For UPDATE operations (not REPLACE INTO), count DELETE rows
		// But for REPLACE INTO, this should not be called or should be ignored
		// REPLACE INTO uses actionUpdate but should only count INSERT
		// So we don't count DELETE here for actionUpdate
	}
}

func (update *MultiUpdate) doAddAffectedRows(affectedRows uint64) {
	update.ctr.affectedRows += affectedRows
}
