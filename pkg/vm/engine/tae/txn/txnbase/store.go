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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

var NoopStoreFactory = func() txnif.TxnStore { return new(NoopTxnStore) }

type NoopTxnStore struct{}

func (store *NoopTxnStore) WaitPrepared() (err error)                            { return }
func (store *NoopTxnStore) GetLSN() uint64                                       { return 0 }
func (store *NoopTxnStore) BindTxn(txn txnif.AsyncTxn)                           {}
func (store *NoopTxnStore) Close() error                                         { return nil }
func (store *NoopTxnStore) Append(dbId, id uint64, data *containers.Batch) error { return nil }
func (store *NoopTxnStore) AddBlksWithMetaLoc(
	dbId, tid uint64,
	zm []objectio.ZoneMap,
	metaLocs []objectio.Location,
) error {
	return nil
}
func (store *NoopTxnStore) PrepareRollback() error { return nil }
func (store *NoopTxnStore) PrePrepare() error      { return nil }
func (store *NoopTxnStore) PrepareCommit() error   { return nil }
func (store *NoopTxnStore) ApplyRollback() error   { return nil }
func (store *NoopTxnStore) PreApplyCommit() error  { return nil }
func (store *NoopTxnStore) ApplyCommit() error     { return nil }
func (store *NoopTxnStore) Apply2PCPrepare() error { return nil }

func (store *NoopTxnStore) DoneWaitEvent(cnt int)                                  {}
func (store *NoopTxnStore) AddWaitEvent(cnt int)                                   {}
func (store *NoopTxnStore) AddTxnEntry(t txnif.TxnEntryType, entry txnif.TxnEntry) {}

func (store *NoopTxnStore) CreateRelation(dbId uint64, def any) (rel handle.Relation, err error) {
	return
}
func (store *NoopTxnStore) CreateRelationWithTableId(dbId uint64, tableId uint64, def any) (rel handle.Relation, err error) {
	return
}
func (store *NoopTxnStore) DropRelationByName(dbId uint64, name string) (rel handle.Relation, err error) {
	return
}
func (store *NoopTxnStore) DropRelationByID(dbId uint64, id uint64) (rel handle.Relation, err error) {
	return
}
func (store *NoopTxnStore) GetRelationByName(dbId uint64, name string) (rel handle.Relation, err error) {
	return
}
func (store *NoopTxnStore) GetRelationByID(dbId uint64, id uint64) (rel handle.Relation, err error) {
	return
}

func (store *NoopTxnStore) CreateDatabase(name, creatSql, datTyp string) (db handle.Database, err error) {
	return
}
func (store *NoopTxnStore) CreateDatabaseWithID(name, createSql, datTyp string, id uint64) (db handle.Database, err error) {
	return
}
func (store *NoopTxnStore) DropDatabase(name string) (db handle.Database, err error)    { return }
func (store *NoopTxnStore) DropDatabaseByID(id uint64) (db handle.Database, err error)  { return }
func (store *NoopTxnStore) UnsafeGetDatabase(id uint64) (db handle.Database, err error) { return }
func (store *NoopTxnStore) UnsafeGetRelation(dbId, id uint64) (rel handle.Relation, err error) {
	return
}
func (store *NoopTxnStore) GetDatabase(name string) (db handle.Database, err error)   { return }
func (store *NoopTxnStore) GetDatabaseByID(id uint64) (db handle.Database, err error) { return }
func (store *NoopTxnStore) DatabaseNames() (names []string)                           { return }
func (store *NoopTxnStore) GetSegment(id *common.ID) (seg handle.Segment, err error) {
	return
}

func (store *NoopTxnStore) CreateSegment(dbId, tid uint64, is1PC bool) (seg handle.Segment, err error) {
	return
}
func (store *NoopTxnStore) CreateNonAppendableSegment(dbId, tid uint64, _ bool) (seg handle.Segment, err error) {
	return
}
func (store *NoopTxnStore) GetBlock(id *common.ID) (blk handle.Block, err error) { return }
func (store *NoopTxnStore) CreateBlock(*common.ID, bool) (blk handle.Block, err error) {
	return
}
func (store *NoopTxnStore) CreateNonAppendableBlock(*common.ID, *objectio.CreateBlockOpt) (blk handle.Block, err error) {
	return
}

func (store *NoopTxnStore) UpdateMetaLoc(id *common.ID, un objectio.Location) (err error) {
	return
}
func (store *NoopTxnStore) UpdateDeltaLoc(id *common.ID, un objectio.Location) (err error) {
	return
}
func (store *NoopTxnStore) SoftDeleteBlock(id *common.ID) (err error)                { return }
func (store *NoopTxnStore) SoftDeleteSegment(id *common.ID) (err error)              { return }
func (store *NoopTxnStore) BatchDedup(uint64, uint64, containers.Vector) (err error) { return }
func (store *NoopTxnStore) Update(uint64, *common.ID, uint32, uint16, any) (err error) {
	return
}
func (store *NoopTxnStore) RangeDelete(*common.ID, uint32, uint32, handle.DeleteType) (err error) {
	return
}
func (store *NoopTxnStore) GetByFilter(uint64, uint64, *handle.Filter) (id *common.ID, offset uint32, err error) {
	return
}
func (store *NoopTxnStore) GetValue(*common.ID, uint32, uint16) (v any, isNull bool, err error) {
	return
}

func (store *NoopTxnStore) LogTxnEntry(dbId, tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return
}
func (store *NoopTxnStore) LogTxnState(sync bool) (logEntry entry.Entry, err error) {
	return
}

func (store *NoopTxnStore) IsReadonly() bool      { return false }
func (store *NoopTxnStore) IncreateWriteCnt() int { return 0 }

func (store *NoopTxnStore) HasAnyTableDataChanges() bool                 { return false }
func (store *NoopTxnStore) GetDirty() *model.Tree                        { return nil }
func (store *NoopTxnStore) HasTableDataChanges(id uint64) bool           { return false }
func (store *NoopTxnStore) GetDirtyTableByID(id uint64) *model.TableTree { return nil }
func (store *NoopTxnStore) HasCatalogChanges() bool                      { return false }

func (store *NoopTxnStore) ObserveTxn(
	visitDatabase func(db any),
	visitTable func(tbl any),
	rotateTable func(dbName, tblName string, dbid, tid uint64),
	visitMetadata func(block any),
	visitAppend func(bat any),
	visitDelete func(deletes []uint32, prefix []byte)) {
}

func (store *NoopTxnStore) GetTransactionType() txnif.TxnType {
	return txnif.TxnType_Normal
}
