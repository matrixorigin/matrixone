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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func updateCtxKey(ctx *MultiUpdateCtx) string {
	if ctx == nil {
		return ""
	}
	tableID := uint64(0)
	if ctx.TableDef != nil {
		tableID = ctx.TableDef.TblId
	}
	if ctx.ObjRef != nil {
		return fmt.Sprintf(
			"%d/%d/%d/%s/%s/%d",
			ctx.ObjRef.Db,
			ctx.ObjRef.Schema,
			ctx.ObjRef.Obj,
			ctx.ObjRef.SchemaName,
			ctx.ObjRef.ObjName,
			tableID,
		)
	}
	if ctx.TableDef != nil {
		return fmt.Sprintf("%s/%s/%d", ctx.TableDef.DbName, ctx.TableDef.Name, ctx.TableDef.TblId)
	}
	return ""
}

func lookupUpdateCtxInfo(infos map[string]*updateCtxInfo, ctx *MultiUpdateCtx) *updateCtxInfo {
	if info := infos[updateCtxKey(ctx)]; info != nil {
		return info
	}
	if ctx != nil && ctx.TableDef != nil {
		return infos[ctx.TableDef.Name]
	}
	return nil
}

var _ vm.Operator = new(MultiUpdate)

const opName = "MultiUpdate"

const multiUpdateAllocationOwner mpool.AllocationOwner = 1

const (
	multiUpdateAllocationSiteHashCell mpool.AllocationSite = iota + 1
	multiUpdateAllocationSiteHashDescriptor
	multiUpdateAllocationSiteHashIterator
)

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
	CountDeleteAffectRows  bool
	RejectZeroTemporal     bool
	Engine                 engine.Engine

	getS3WriterFunc          func(sid string, id uint64) (*s3WriterDelegate, error)
	getFlushableS3WriterFunc func() *s3WriterDelegate
	addAffectedRowsFunc      func(uint64)

	allocationAccount  *mpool.AllocationAccount
	mapAllocation      *hashtable.AllocationAccountSelection
	iteratorAllocation *hashmap.IteratorAllocation

	vm.OperatorBase
}

func (update *MultiUpdate) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if update.allocationAccount != nil {
		if update.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	selection, err := hashtable.NewAllocationAccountSelection(
		account,
		multiUpdateAllocationOwner,
		multiUpdateAllocationSiteHashCell,
		multiUpdateAllocationSiteHashDescriptor,
	)
	if err != nil {
		return err
	}
	iteratorAllocation, err := hashmap.NewIteratorAllocation(
		account,
		multiUpdateAllocationOwner,
		multiUpdateAllocationSiteHashIterator,
	)
	if err != nil {
		return err
	}
	update.allocationAccount = account
	update.mapAllocation = selection
	update.iteratorAllocation = iteratorAllocation
	return nil
}

func (update *MultiUpdate) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if update.allocationAccount == nil {
		return nil
	}
	if update.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if len(update.ctr.seenTargetRows) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	update.allocationAccount = nil
	update.mapAllocation = nil
	update.iteratorAllocation = nil
	return nil
}

// MultiUpdate participates in an allocation generation when another operator
// (normally the join feeding a multi-target UPDATE) activates it. It must not
// activate the statement-wide lifecycle by itself because ordinary single-
// table writes do not need the physical-target deduplication map.
func (update *MultiUpdate) ActivatesAllocationAccountLifecycle() bool {
	return false
}

type updateCtxInfo struct {
	Source       engine.Relation
	tableType    UpdateTableType
	insertAttrs  []string
	isContiguous bool
	refBatch     *batch.Batch
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

	seenTargetRows map[uint64]*hashmap.StrHashMap
	seenRowsGrant  int64
	seenRowsRSC    rscthrottler.RSCThrottler
}

type MultiUpdateCtx struct {
	ObjRef             *plan.ObjectRef
	TableDef           *plan.TableDef
	InsertCols         []int
	DeleteCols         []int
	PartitionCols      []int
	SkipInsertOnNullPk bool
	// InsertPkColIdx is the PK column's index within InsertCols. It is only
	// used with SkipInsertOnNullPk for REPLACE delete-only rows.
	InsertPkColIdx     int
	IgnoreAffectedRows bool
	// DedupByTargetRowID makes this context consume only the whole input row
	// selected for its physical target row. The planner supplies an independent
	// row_number() partition for every updated target table.
	DedupByTargetRowID bool
	TargetUpdateCtxIdx int
	// TargetTableID stays logical when a partition wrapper replaces TableDef
	// with a physical partition definition.
	TargetTableID uint64
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

func (update *MultiUpdate) SetRejectZeroTemporal(reject bool) {
	update.RejectZeroTemporal = reject
	if update.ctr.s3Writer != nil {
		update.ctr.s3Writer.rejectZeroTemporal = reject
	}
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
	update.freeSeenTargetRows()
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
	update.freeSeenTargetRows()

	update.ctr.updateCtxInfos = nil
	update.ctr.sources = nil
}

func (update *MultiUpdate) freeSeenTargetRows() {
	if update.ctr.seenRowsGrant > 0 {
		update.ctr.seenRowsRSC.Release(update.ctr.seenRowsGrant)
		update.ctr.seenRowsGrant = 0
	}
	update.ctr.seenRowsRSC = nil
	for _, seen := range update.ctr.seenTargetRows {
		seen.Free()
	}
	update.ctr.seenTargetRows = nil
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
	switch update.ctr.action {
	case actionDelete:
		update.addAffectedRowsFunc(rowCount)
	case actionUpdate:
		if update.CountDeleteAffectRows {
			update.addAffectedRowsFunc(rowCount)
		}
	}
}

func (update *MultiUpdate) doAddAffectedRows(affectedRows uint64) {
	if len(update.MultiUpdateCtx) > 0 && update.MultiUpdateCtx[0].IgnoreAffectedRows {
		return
	}
	update.ctr.affectedRows += affectedRows
}
