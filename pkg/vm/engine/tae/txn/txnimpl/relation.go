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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnRelationIt struct {
	*sync.RWMutex
	txnDB  *txnDB
	linkIt *common.LinkIt
	curr   *catalog.TableEntry
	err    error
}

func newRelationIt(db *txnDB) *txnRelationIt {
	it := &txnRelationIt{
		RWMutex: db.entry.RWMutex,
		linkIt:  db.entry.MakeTableIt(true),
		txnDB:   db,
	}
	var err error
	var ok bool
	for it.linkIt.Valid() {
		curr := it.linkIt.Get().GetPayload().(*catalog.TableEntry)
		curr.RLock()
		ok, err = curr.TxnCanRead(it.txnDB.store.txn, curr.RWMutex)
		if err != nil {
			curr.RUnlock()
			it.err = err
			return it
		}
		if ok {
			curr.RUnlock()
			it.curr = curr
			break
		}
		curr.RUnlock()
		it.linkIt.Next()
	}
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
	valid := true
	for {
		it.linkIt.Next()
		node := it.linkIt.Get()
		if node == nil {
			it.curr = nil
			break
		}
		entry := node.GetPayload().(*catalog.TableEntry)
		entry.RLock()
		valid, err = entry.TxnCanRead(it.txnDB.store.txn, entry.RWMutex)
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
	it := h.MakeBlockIt()
	for it.Valid() {
		block := it.GetBlock()
		s = fmt.Sprintf("%s\n%s", s, block.String())
		it.Next()
	}
	return s
}

func (h *txnRelation) Close() error   { return nil }
func (h *txnRelation) GetMeta() any   { return h.table.entry }
func (h *txnRelation) GetSchema() any { return h.table.entry.GetSchema() }

func (h *txnRelation) Rows() int64 {
	if h.table.entry.GetDB().IsSystemDB() && h.table.entry.IsVirtual() {
		if h.table.entry.GetSchema().Name == catalog.SystemTable_DB_Name {
			return int64(h.table.entry.GetCatalog().CoarseDBCnt())
		} else if h.table.entry.GetSchema().Name == catalog.SystemTable_Table_Name {
			return int64(h.table.entry.GetCatalog().CoarseTableCnt())
		} else if h.table.entry.GetSchema().Name == catalog.SystemTable_Columns_Name {
			return int64(h.table.entry.GetCatalog().CoarseColumnCnt())
		}
		panic("logic error")
	}
	return int64(h.table.entry.GetRows())
}
func (h *txnRelation) Size(attr string) int64           { return 0 }
func (h *txnRelation) GetCardinality(attr string) int64 { return 0 }

func (h *txnRelation) BatchDedup(cols ...containers.Vector) error {
	return h.Txn.GetStore().BatchDedup(h.table.entry.GetDB().ID, h.table.entry.GetID(), cols...)
}

func (h *txnRelation) Append(data *containers.Batch) error {
	return h.Txn.GetStore().Append(h.table.entry.GetDB().ID, h.table.entry.GetID(), data)
}

func (h *txnRelation) GetSegment(id uint64) (seg handle.Segment, err error) {
	fp := h.table.entry.AsCommonID()
	fp.SegmentID = id
	return h.Txn.GetStore().GetSegment(h.table.entry.GetDB().ID, fp)
}

func (h *txnRelation) CreateSegment() (seg handle.Segment, err error) {
	return h.Txn.GetStore().CreateSegment(h.table.entry.GetDB().ID, h.table.entry.GetID())
}

func (h *txnRelation) CreateNonAppendableSegment() (seg handle.Segment, err error) {
	return h.Txn.GetStore().CreateNonAppendableSegment(h.table.entry.GetDB().ID, h.table.entry.GetID())
}

func (h *txnRelation) SoftDeleteSegment(id uint64) (err error) {
	fp := h.table.entry.AsCommonID()
	fp.SegmentID = id
	return h.Txn.GetStore().SoftDeleteSegment(h.table.entry.GetDB().ID, fp)
}

func (h *txnRelation) MakeSegmentIt() handle.SegmentIt {
	return newSegmentIt(h.table)
}

func (h *txnRelation) MakeBlockIt() handle.BlockIt {
	return newRelationBlockIt(h)
}

func (h *txnRelation) GetByFilter(filter *handle.Filter) (*common.ID, uint32, error) {
	return h.Txn.GetStore().GetByFilter(h.table.entry.GetDB().ID, h.table.entry.GetID(), filter)
}

func (h *txnRelation) GetValueByFilter(filter *handle.Filter, col int) (v any, err error) {
	id, row, err := h.GetByFilter(filter)
	if err != nil {
		return
	}
	v, err = h.GetValue(id, row, uint16(col))
	return
}

func (h *txnRelation) UpdateByFilter(filter *handle.Filter, col uint16, v any) (err error) {
	id, row, err := h.table.GetByFilter(filter)
	if err != nil {
		return
	}
	schema := h.table.entry.GetSchema()
	if !schema.IsPartOfPK(int(col)) {
		err = h.Update(id, row, col, v)
		return
	}
	bat := containers.NewBatch()
	for _, def := range schema.ColDefs {
		if def.IsHidden() {
			continue
		}
		colVal, err := h.table.GetValue(id, row, uint16(def.Idx))
		if err != nil {
			return err
		}
		vec := containers.MakeVector(def.Type, def.Nullable())
		vec.Append(colVal)
		bat.AddVector(def.Name, vec)
	}
	if err = h.table.RangeDelete(id, row, row); err != nil {
		return
	}
	err = h.Append(bat)
	// FIXME!: We need to revert previous delete if append fails.
	return
}

func (h *txnRelation) UpdateByHiddenKey(key any, col int, v any) error {
	sid, bid, row := model.DecodeHiddenKeyFromValue(key)
	id := &common.ID{
		TableID:   h.table.entry.ID,
		SegmentID: sid,
		BlockID:   bid,
	}
	return h.Txn.GetStore().Update(h.table.entry.GetDB().ID, id, row, uint16(col), v)
}

func (h *txnRelation) Update(id *common.ID, row uint32, col uint16, v any) error {
	return h.Txn.GetStore().Update(h.table.entry.GetDB().ID, id, row, col, v)
}

func (h *txnRelation) DeleteByFilter(filter *handle.Filter) (err error) {
	id, row, err := h.GetByFilter(filter)
	if err != nil {
		return
	}
	return h.RangeDelete(id, row, row)
}

func (h *txnRelation) DeleteByHiddenKeys(keys containers.Vector) (err error) {
	id := &common.ID{
		TableID: h.table.entry.ID,
	}
	var row uint32
	dbId := h.table.entry.GetDB().ID
	err = keys.Foreach(func(key any, _ int) (err error) {
		id.SegmentID, id.BlockID, row = model.DecodeHiddenKeyFromValue(key)
		err = h.Txn.GetStore().RangeDelete(dbId, id, row, row)
		return
	}, nil)
	return
}

func (h *txnRelation) DeleteByHiddenKey(key any) error {
	sid, bid, row := model.DecodeHiddenKeyFromValue(key)
	id := &common.ID{
		TableID:   h.table.entry.ID,
		SegmentID: sid,
		BlockID:   bid,
	}
	return h.Txn.GetStore().RangeDelete(h.table.entry.GetDB().ID, id, row, row)
}

func (h *txnRelation) RangeDelete(id *common.ID, start, end uint32) error {
	return h.Txn.GetStore().RangeDelete(h.table.entry.GetDB().ID, id, start, end)
}

func (h *txnRelation) GetValueByHiddenKey(key any, col int) (any, error) {
	sid, bid, row := model.DecodeHiddenKeyFromValue(key)
	id := &common.ID{
		TableID:   h.table.entry.ID,
		SegmentID: sid,
		BlockID:   bid,
	}
	return h.Txn.GetStore().GetValue(h.table.entry.GetDB().ID, id, row, uint16(col))
}

func (h *txnRelation) GetValue(id *common.ID, row uint32, col uint16) (any, error) {
	return h.Txn.GetStore().GetValue(h.table.entry.GetDB().ID, id, row, col)
}

func (h *txnRelation) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return h.Txn.GetStore().LogTxnEntry(h.table.entry.GetDB().ID, h.table.entry.GetID(), entry, readed)
}
