// Copyright 2021 Matrix Origin
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

package txnimpl

import (
	"context"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

var _ handle.RelationIt = (*txnRelationIt)(nil)

type txnRelationIt struct {
	*sync.RWMutex
	txnDB  *txnDB
	linkIt *common.GenericSortedDListIt[*catalog.TableEntry]
	itered bool // linkIt has no dummy head, use this to avoid duplicate filter logic for the very first entry
	curr   *catalog.TableEntry
	err    error
}

func newRelationIt(db *txnDB) *txnRelationIt {
	it := &txnRelationIt{
		RWMutex: db.entry.RWMutex,
		linkIt:  db.entry.MakeTableIt(true),
		txnDB:   db,
	}
	it.Next()
	return it
}

func (it *txnRelationIt) Close() error { return nil }

func (it *txnRelationIt) GetError() error { return it.err }
func (it *txnRelationIt) Valid() bool {
	if it.err != nil {
		return false
	}
	return it.linkIt.Valid()
}

func (it *txnRelationIt) Next() {
	var err error
	var valid bool
	txn := it.txnDB.store.txn
	for {
		if it.itered {
			it.linkIt.Next()
		}
		it.itered = true
		node := it.linkIt.Get()
		if node == nil {
			it.curr = nil
			break
		}
		entry := node.GetPayload()
		entry.RLock()
		// SystemDB can hold table created by different tenant, filter needed.
		// while the 3 shared tables are not affected
		if it.txnDB.entry.IsSystemDB() && !isSysTable(entry.GetLastestSchemaLocked(false).Name) &&
			entry.GetLastestSchemaLocked(false).AcInfo.TenantID != txn.GetTenantID() {
			entry.RUnlock()
			continue
		}
		valid, err = entry.IsVisibleWithLock(it.txnDB.store.txn, entry.RWMutex)
		entry.RUnlock()
		if err != nil {
			it.err = err
			break
		}
		if valid {
			it.curr = entry
			break
		}
	}
}

func (it *txnRelationIt) GetCurr() *catalog.TableEntry {
	return it.curr
}

func (it *txnRelationIt) GetRelation() handle.Relation {
	table, _ := it.txnDB.getOrSetTable(it.curr.ID)
	return newRelation(table)
}

type txnRelation struct {
	*txnbase.TxnRelation
	table *txnTable
}

func newRelation(table *txnTable) *txnRelation {
	rel := &txnRelation{
		TxnRelation: &txnbase.TxnRelation{
			Txn: table.store.txn,
		},
		table: table,
	}
	return rel
}

func (h *txnRelation) ID() uint64     { return h.table.entry.GetID() }
func (h *txnRelation) String() string { return h.table.entry.String() }
func (h *txnRelation) SimplePPString(level common.PPLevel) string {
	s := h.table.entry.String()
	if level < common.PPL1 {
		return s
	}
	it := h.MakeObjectIt(false)
	for it.Next() {
		object := it.GetObject()
		defer object.Close()
		s = fmt.Sprintf("%s\n%s", s, object.String())
	}
	it = h.MakeObjectIt(true)
	for it.Next() {
		object := it.GetObject()
		defer object.Close()
		s = fmt.Sprintf("%s\n%s", s, object.String())
	}
	return s
}

func (h *txnRelation) Close() error { return nil }
func (h *txnRelation) GetMeta() any { return h.table.entry }

// Schema return schema in txnTable, not the lastest schema in TableEntry
func (h *txnRelation) Schema(isTombstone bool) any { return h.table.GetLocalSchema(isTombstone) }

func (h *txnRelation) GetCardinality(attr string) int64 { return 0 }

func (h *txnRelation) BatchDedup(col containers.Vector) error {
	return h.Txn.GetStore().BatchDedup(h.table.entry.GetDB().ID, h.table.entry.GetID(), col)
}

func (h *txnRelation) Append(ctx context.Context, data *containers.Batch) error {
	if !h.table.GetLocalSchema(false).IsSameColumns(h.table.GetMeta().GetLastestSchemaLocked(false)) {
		return moerr.NewInternalErrorNoCtx("schema changed, please rollback and retry")
	}
	return h.Txn.GetStore().Append(ctx, h.table.entry.GetDB().ID, h.table.entry.GetID(), data)
}

func (h *txnRelation) AddObjsWithMetaLoc(ctx context.Context, stats containers.Vector) error {
	return h.Txn.GetStore().AddObjsWithMetaLoc(
		ctx,
		h.table.entry.GetDB().ID,
		h.table.entry.GetID(),
		stats,
	)
}

func (h *txnRelation) GetObject(id *types.Objectid, isTombstone bool) (obj handle.Object, err error) {
	fp := h.table.entry.AsCommonID()
	fp.SetObjectID(id)
	return h.Txn.GetStore().GetObject(fp, isTombstone)
}

func (h *txnRelation) CreateObject(isTombstone bool) (obj handle.Object, err error) {
	return h.Txn.GetStore().CreateObject(h.table.entry.GetDB().ID, h.table.entry.GetID(), isTombstone)
}

func (h *txnRelation) CreateNonAppendableObject(isTombstone bool, opt *objectio.CreateObjOpt) (obj handle.Object, err error) {
	return h.Txn.GetStore().CreateNonAppendableObject(h.table.entry.GetDB().ID, h.table.entry.GetID(), isTombstone, opt)
}

func (h *txnRelation) SoftDeleteObject(id *types.Objectid, isTombstone bool) (err error) {
	fp := h.table.entry.AsCommonID()
	fp.SetObjectID(id)
	return h.Txn.GetStore().SoftDeleteObject(isTombstone, fp)
}

func (h *txnRelation) MakeObjectItOnSnap(isTombstone bool) handle.ObjectIt {
	return newObjectItOnSnap(h.table, isTombstone)
}

func (h *txnRelation) MakeObjectIt(isTombstone bool) handle.ObjectIt {
	return newObjectIt(h.table, isTombstone)
}

func (h *txnRelation) GetByFilter(
	ctx context.Context, filter *handle.Filter,
) (*common.ID, uint32, error) {
	return h.Txn.GetStore().GetByFilter(ctx, h.table.entry.GetDB().ID, h.table.entry.GetID(), filter)
}

func (h *txnRelation) GetValueByFilter(
	ctx context.Context, filter *handle.Filter, col int,
) (v any, isNull bool, err error) {
	id, row, err := h.GetByFilter(ctx, filter)
	if err != nil {
		return
	}
	v, isNull, err = h.GetValue(id, row, uint16(col), false)
	return
}

func (h *txnRelation) UpdateByFilter(ctx context.Context, filter *handle.Filter, col uint16, v any, isNull bool) (err error) {
	id, row, err := h.table.GetByFilter(ctx, filter)
	if err != nil {
		return
	}
	schema := h.table.GetLocalSchema(false)
	pkDef := schema.GetPrimaryKey()
	pkVec := makeWorkspaceVector(pkDef.Type)
	defer pkVec.Close()
	pkVal, _, err := h.table.GetValue(ctx, id, row, uint16(pkDef.Idx), true)
	if err != nil {
		return err
	}
	pkVec.Append(pkVal, false)
	bat := containers.NewBatch()
	defer bat.Close()
	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		var colVal any
		var colValIsNull bool
		if int(col) == def.Idx {
			colVal = v
			colValIsNull = isNull
		} else {
			colVal, colValIsNull, err = h.table.GetValue(ctx, id, row, uint16(def.Idx), true)
			if err != nil {
				return err
			}
		}
		vec := makeWorkspaceVector(def.Type)
		vec.Append(colVal, colValIsNull)
		bat.AddVector(def.Name, vec)
	}
	if err = h.table.RangeDelete(id, row, row, pkVec, handle.DT_Normal); err != nil {
		return
	}
	err = h.Append(ctx, bat)
	// FIXME!: We need to revert previous delete if append fails.
	return
}

func (h *txnRelation) DeleteByFilter(ctx context.Context, filter *handle.Filter) (err error) {
	id, row, err := h.GetByFilter(ctx, filter)
	if err != nil {
		return
	}
	return h.RangeDelete(id, row, row, handle.DT_Normal)
}

func (h *txnRelation) DeleteByPhyAddrKeys(keys containers.Vector, pkVec containers.Vector) (err error) {
	id := h.table.entry.AsCommonID()
	var row uint32
	var pk containers.Vector
	err = containers.ForeachVectorWindow(
		keys, 0, keys.Length(),
		func(rid types.Rowid, _ bool, offset int) (err error) {
			id.BlockID, row = rid.Decode()
			if pkVec != nil && pkVec.Length() > 0 {
				pk = pkVec.Window(offset, 1)
			}
			err = h.Txn.GetStore().RangeDelete(
				id,
				row,
				row,
				pk,
				handle.DT_Normal,
			)
			return
		}, nil, nil)
	return
}

// Only used by test.
func (h *txnRelation) DeleteByPhyAddrKey(key any) error {
	rid := key.(types.Rowid)
	bid, row := rid.Decode()
	id := h.table.entry.AsCommonID()
	id.BlockID = bid
	schema := h.table.GetLocalSchema(false)
	pkDef := schema.GetPrimaryKey()
	pkVec := makeWorkspaceVector(pkDef.Type)
	defer pkVec.Close()
	val, _, err := h.table.GetValue(h.table.store.ctx, id, row, uint16(pkDef.Idx), true)
	if err != nil {
		return err
	}
	pkVec.Append(val, false)
	return h.Txn.GetStore().RangeDelete(id, row, row, pkVec, handle.DT_Normal)
}

func (h *txnRelation) RangeDelete(id *common.ID, start, end uint32, dt handle.DeleteType) error {
	schema := h.table.GetLocalSchema(false)
	pkDef := schema.GetPrimaryKey()
	pkVec := h.table.store.rt.VectorPool.Small.GetVector(&pkDef.Type)
	defer pkVec.Close()
	for row := start; row <= end; row++ {
		pkVal, _, err := h.table.GetValue(h.table.store.GetContext(), id, row, uint16(pkDef.Idx), true)
		if err != nil {
			return err
		}
		pkVec.Append(pkVal, false)
	}
	return h.Txn.GetStore().RangeDelete(id, start, end, pkVec, dt)
}
func (h *txnRelation) TryDeleteByDeltaloc(id *common.ID, deltaloc objectio.Location) (ok bool, err error) {
	return h.Txn.GetStore().TryDeleteByDeltaloc(id, deltaloc)
}

// Only used by test.
func (h *txnRelation) GetValueByPhyAddrKey(key any, col int) (any, bool, error) {
	rid := key.(types.Rowid)
	bid, row := rid.Decode()
	id := h.table.entry.AsCommonID()
	id.BlockID = bid
	return h.Txn.GetStore().GetValue(id, row, uint16(col), false)
}

func (h *txnRelation) GetValue(id *common.ID, row uint32, col uint16, skipCheckDelete bool) (any, bool, error) {
	return h.Txn.GetStore().GetValue(id, row, col, skipCheckDelete)
}

func (h *txnRelation) LogTxnEntry(entry txnif.TxnEntry, readedObject, readedTombstone []*common.ID) (err error) {
	return h.Txn.GetStore().LogTxnEntry(h.table.entry.GetDB().ID, h.table.entry.GetID(), entry, readedObject, readedTombstone)
}

func (h *txnRelation) GetDB() (handle.Database, error) {
	return h.Txn.GetStore().GetDatabase(h.GetMeta().(*catalog.TableEntry).GetDB().GetName())
}

func (h *txnRelation) AlterTable(ctx context.Context, req *apipb.AlterTableReq) (err error) {
	return h.table.AlterTable(ctx, req)
}

func (h *txnRelation) FillInWorkspaceDeletes(blkID types.Blockid, view **nulls.Nulls) error {
	return h.table.FillInWorkspaceDeletes(blkID, view)
}
