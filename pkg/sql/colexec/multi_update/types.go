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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
		return fmt.Sprintf("%s/%s/%d", ctx.TableDef.DbName, ctx.TableDef.Name, tableID)
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

	vm.OperatorBase
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
	// ChangedRowsCol is the input bool column containing the final row-image
	// change marker. Nil requests the legacy matched-row count.
	ChangedRowsCol *int
	// AffectedRowsCols contains one semantic selector for every writable alias
	// coalesced into this physical target. DeleteCols[3] independently controls
	// physical write eligibility, so implicit cascade rows can be written without
	// contributing to SQL affected-row accounting.
	AffectedRowsCols []int
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

func physicalInsertAffectedRows(updateCtx *MultiUpdateCtx, rowCount uint64) uint64 {
	if updateCtx != nil && len(updateCtx.AffectedRowsCols) > 0 {
		return 0
	}
	return rowCount
}

func (update *MultiUpdate) insertAffectedRows(updateCtx *MultiUpdateCtx, input *batch.Batch) uint64 {
	return insertAffectedRows(updateCtx, input)
}

func insertAffectedRows(updateCtx *MultiUpdateCtx, input *batch.Batch) uint64 {
	if updateCtx.ChangedRowsCol == nil {
		return uint64(input.RowCount())
	}
	changed := vector.MustFixedColWithTypeCheck[bool](input.Vecs[*updateCtx.ChangedRowsCol])
	var count uint64
	for row := 0; row < input.RowCount(); row++ {
		if changed[row] {
			count++
		}
	}
	return count
}

func hasChangedRowsCol(updateCtxs []*MultiUpdateCtx) bool {
	for _, updateCtx := range updateCtxs {
		if updateCtx.ChangedRowsCol != nil {
			return true
		}
	}
	return false
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
