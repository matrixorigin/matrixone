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

package txnbase

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var NoopStoreFactory = func() txnif.TxnStore { return new(NoopTxnStore) }

type NoopTxnStore struct{}

func (store *NoopTxnStore) BindTxn(txn txnif.AsyncTxn)                              {}
func (store *NoopTxnStore) Close() error                                            { return nil }
func (store *NoopTxnStore) RangeDeleteLocalRows(id uint64, start, end uint32) error { return nil }
func (store *NoopTxnStore) Append(id uint64, data *batch.Batch) error               { return nil }
func (store *NoopTxnStore) UpdateLocalValue(id uint64, row uint32, col uint16, v interface{}) error {
	return nil
}
func (store *NoopTxnStore) AddUpdateNode(id uint64, node txnif.UpdateNode) error { return nil }
func (store *NoopTxnStore) PrepareRollback() error                               { return nil }
func (store *NoopTxnStore) PreCommit() error                                     { return nil }
func (store *NoopTxnStore) PrepareCommit() error                                 { return nil }
func (store *NoopTxnStore) ApplyRollback() error                                 { return nil }
func (store *NoopTxnStore) ApplyCommit() error                                   { return nil }

func (store *NoopTxnStore) AddTxnEntry(t txnif.TxnEntryType, entry txnif.TxnEntry) {}

func (store *NoopTxnStore) CreateRelation(def interface{}) (rel handle.Relation, err error) { return }
func (store *NoopTxnStore) DropRelationByName(name string) (rel handle.Relation, err error) { return }
func (store *NoopTxnStore) GetRelationByName(name string) (rel handle.Relation, err error)  { return }
func (store *NoopTxnStore) CreateDatabase(name string) (db handle.Database, err error)      { return }
func (store *NoopTxnStore) DropDatabase(name string) (db handle.Database, err error)        { return }
func (store *NoopTxnStore) GetDatabase(name string) (db handle.Database, err error)         { return }
func (store *NoopTxnStore) UseDatabase(name string) (err error)                             { return }
func (store *NoopTxnStore) CurrentDatabase() (db handle.Database)                           { return }
func (store *NoopTxnStore) DatabaseNames() (names []string)                                 { return }
func (store *NoopTxnStore) GetSegment(id *common.ID) (seg handle.Segment, err error)        { return }

func (store *NoopTxnStore) CreateSegment(uint64) (seg handle.Segment, err error)              { return }
func (store *NoopTxnStore) CreateNonAppendableSegment(uint64) (seg handle.Segment, err error) { return }
func (store *NoopTxnStore) GetBlock(id *common.ID) (blk handle.Block, err error)              { return }
func (store *NoopTxnStore) CreateBlock(uint64, uint64) (blk handle.Block, err error)          { return }
func (store *NoopTxnStore) CreateNonAppendableBlock(id *common.ID) (blk handle.Block, err error) {
	return
}
func (store *NoopTxnStore) SoftDeleteBlock(id *common.ID) (err error)                  { return }
func (store *NoopTxnStore) SoftDeleteSegment(id *common.ID) (err error)                { return }
func (store *NoopTxnStore) BatchDedup(uint64, *vector.Vector) (err error)              { return }
func (store *NoopTxnStore) Update(*common.ID, uint32, uint16, interface{}) (err error) { return }
func (store *NoopTxnStore) RangeDelete(*common.ID, uint32, uint32) (err error)         { return }
func (store *NoopTxnStore) GetByFilter(uint64, *handle.Filter) (id *common.ID, offset uint32, err error) {
	return
}
func (store *NoopTxnStore) GetValue(*common.ID, uint32, uint16) (v interface{}, err error) {
	return
}

func (store *NoopTxnStore) LogSegmentID(tid, sid uint64) {}
func (store *NoopTxnStore) LogBlockID(tid, bid uint64)   {}
func (store *NoopTxnStore) LogTxnEntry(tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return
}

func (store *NoopTxnStore) IsReadonly() bool      { return false }
func (store *NoopTxnStore) IncreateWriteCnt() int { return 0 }
