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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnRelationIt struct {
	*sync.RWMutex
	txn    txnif.AsyncTxn
	linkIt *common.LinkIt
	curr   *catalog.TableEntry
}

func newRelationIt(txn txnif.AsyncTxn, meta *catalog.DBEntry) *txnRelationIt {
	it := &txnRelationIt{
		RWMutex: meta.RWMutex,
		txn:     txn,
		linkIt:  meta.MakeTableIt(true),
	}
	for it.linkIt.Valid() {
		curr := it.linkIt.Get().GetPayload().(*catalog.TableEntry)
		curr.RLock()
		if curr.TxnCanRead(it.txn, curr.RWMutex) {
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

func (it *txnRelationIt) Valid() bool { return it.linkIt.Valid() }

func (it *txnRelationIt) Next() {
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
		valid = entry.TxnCanRead(it.txn, entry.RWMutex)
		entry.RUnlock()
		if valid {
			it.curr = entry
			break
		}
	}
}

func (it *txnRelationIt) GetRelation() handle.Relation {
	return newRelation(it.txn, it.curr)
}

type txnRelation struct {
	*txnbase.TxnRelation
	entry *catalog.TableEntry
}

func newRelation(txn txnif.AsyncTxn, meta *catalog.TableEntry) *txnRelation {
	rel := &txnRelation{
		TxnRelation: &txnbase.TxnRelation{
			Txn: txn,
		},
		entry: meta,
	}
	return rel
}

func (h *txnRelation) ID() uint64     { return h.entry.GetID() }
func (h *txnRelation) String() string { return h.entry.String() }
func (h *txnRelation) SimplePPString(level common.PPLevel) string {
	s := h.entry.String()
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

func (h *txnRelation) GetMeta() interface{}   { return h.entry }
func (h *txnRelation) GetSchema() interface{} { return h.entry.GetSchema() }

func (h *txnRelation) Close() error                     { return nil }
func (h *txnRelation) Rows() int64                      { return 0 }
func (h *txnRelation) Size(attr string) int64           { return 0 }
func (h *txnRelation) GetCardinality(attr string) int64 { return 0 }
func (h *txnRelation) MakeReader() handle.Reader        { return nil }

func (h *txnRelation) BatchDedup(col *vector.Vector) error {
	return h.Txn.GetStore().BatchDedup(h.entry.GetID(), col)
}

func (h *txnRelation) Append(data *batch.Batch) error {
	return h.Txn.GetStore().Append(h.entry.GetID(), data)
}

func (h *txnRelation) GetSegment(id uint64) (seg handle.Segment, err error) {
	fp := h.entry.AsCommonID()
	fp.SegmentID = id
	return h.Txn.GetStore().GetSegment(fp)
}

func (h *txnRelation) CreateSegment() (seg handle.Segment, err error) {
	return h.Txn.GetStore().CreateSegment(h.entry.GetID())
}

func (h *txnRelation) CreateNonAppendableSegment() (seg handle.Segment, err error) {
	return h.Txn.GetStore().CreateNonAppendableSegment(h.entry.GetID())
}

func (h *txnRelation) SoftDeleteSegment(id uint64) (err error) {
	fp := h.entry.AsCommonID()
	fp.SegmentID = id
	return h.Txn.GetStore().SoftDeleteSegment(fp)
}

func (h *txnRelation) MakeSegmentIt() handle.SegmentIt {
	return newSegmentIt(h.Txn, h.entry)
}

func (h *txnRelation) MakeBlockIt() handle.BlockIt {
	return newRelationBlockIt(h)
}

func (h *txnRelation) GetByFilter(filter *handle.Filter) (*common.ID, uint32, error) {
	return h.Txn.GetStore().GetByFilter(h.entry.GetID(), filter)
}

func (h *txnRelation) Update(id *common.ID, row uint32, col uint16, v interface{}) error {
	return h.Txn.GetStore().Update(id, row, col, v)
}

func (h *txnRelation) RangeDelete(id *common.ID, start, end uint32) error {
	return h.Txn.GetStore().RangeDelete(id, start, end)
}

func (h *txnRelation) GetValue(id *common.ID, row uint32, col uint16) (interface{}, error) {
	return h.Txn.GetStore().GetValue(id, row, col)
}

func (h *txnRelation) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return h.Txn.GetStore().LogTxnEntry(h.entry.GetID(), entry, readed)
}
