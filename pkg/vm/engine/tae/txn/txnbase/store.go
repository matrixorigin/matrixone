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
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
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

func (store *NoopTxnStore) StartTrace()        {}
func (store *NoopTxnStore) TriggerTrace(uint8) {}
func (store *NoopTxnStore) EndTrace()          {}

func (store *NoopTxnStore) Freeze() error                                { return nil }
func (store *NoopTxnStore) WaitPrepared(ctx context.Context) (err error) { return }
func (store *NoopTxnStore) GetLSN() uint64                               { return 0 }
func (store *NoopTxnStore) BindTxn(txn txnif.AsyncTxn)                   {}
func (store *NoopTxnStore) Close() error                                 { return nil }
func (store *NoopTxnStore) Append(ctx context.Context, dbId, id uint64, data *containers.Batch) error {
	return nil
}
func (store *NoopTxnStore) AddObjsWithMetaLoc(
	ctx context.Context,
	dbId, tid uint64,
	stats containers.Vector,
) error {
	return nil
}
func (store *NoopTxnStore) GetContext() context.Context          { return nil }
func (store *NoopTxnStore) SetContext(context.Context)           {}
func (store *NoopTxnStore) PrepareRollback() error               { return nil }
func (store *NoopTxnStore) PrePrepare(ctx context.Context) error { return nil }
func (store *NoopTxnStore) PrepareCommit() error                 { return nil }
func (store *NoopTxnStore) ApplyRollback() error                 { return nil }
func (store *NoopTxnStore) PreApplyCommit() error                { return nil }
func (store *NoopTxnStore) ApplyCommit() error                   { return nil }
func (store *NoopTxnStore) Apply2PCPrepare() error               { return nil }
func (store *NoopTxnStore) PrepareWAL() error                    { return nil }

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
func (store *NoopTxnStore) CreateDatabaseWithID(ctx context.Context, s1, s2, s3 string, id uint64) (db handle.Database, err error) {
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
func (store *NoopTxnStore) GetObject(id *common.ID, isTombstone bool) (obj handle.Object, err error) {
	return
}

func (store *NoopTxnStore) CreateObject(dbId, tid uint64, isTombstone bool) (obj handle.Object, err error) {
	return
}
func (store *NoopTxnStore) CreateNonAppendableObject(dbId, tid uint64, _ bool, _ *objectio.CreateObjOpt) (obj handle.Object, err error) {
	return
}
func (store *NoopTxnStore) UpdateMetaLoc(id *common.ID, un objectio.Location) (err error) {
	return
}
func (store *NoopTxnStore) UpdateDeltaLoc(id *common.ID, un objectio.Location) (err error) {
	return
}
func (store *NoopTxnStore) SoftDeleteBlock(id *common.ID) (err error)                    { return }
func (store *NoopTxnStore) SoftDeleteObject(isTombstone bool, id *common.ID) (err error) { return }
func (store *NoopTxnStore) BatchDedup(
	uint64, uint64, containers.Vector,
) (err error) {
	return
}
func (store *NoopTxnStore) Update(uint64, *common.ID, uint32, uint16, any) (err error) {
	return
}
func (store *NoopTxnStore) RangeDelete(
	*common.ID, uint32, uint32, containers.Vector, handle.DeleteType,
) (err error) {
	return
}
func (store *NoopTxnStore) TryDeleteByDeltaloc(id *common.ID, deltaloc objectio.Location) (ok bool, err error) {
	return
}
func (store *NoopTxnStore) GetByFilter(
	context.Context, uint64, uint64, *handle.Filter,
) (id *common.ID, offset uint32, err error) {
	return
}
func (store *NoopTxnStore) GetValue(
	*common.ID, uint32, uint16, bool,
) (v any, isNull bool, err error) {
	return
}

func (store *NoopTxnStore) LogTxnEntry(dbId, tableId uint64, entry txnif.TxnEntry, readedObject, readedTombstone []*common.ID) (err error) {
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
	rotateTable func(aid uint32, dbName, tblName string, dbid, tid uint64, pkSeqnum uint16),
	visitObject func(any),
	visitAppend func(bat any, isTombstone bool)) {
}

func (store *NoopTxnStore) GetTransactionType() txnif.TxnType {
	return txnif.TxnType_Normal
}

func (store *NoopTxnStore) UpdateObjectStats(*common.ID, *objectio.ObjectStats, bool) error {
	return nil
}
func (store *NoopTxnStore) FillInWorkspaceDeletes(id *common.ID, deletes **nulls.Nulls) error {
	return nil
}
func (store *NoopTxnStore) IsDeletedInWorkSpace(id *common.ID, row uint32) (bool, error) {
	return false, nil
}
