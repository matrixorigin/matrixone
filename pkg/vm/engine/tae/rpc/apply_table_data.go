// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"
	"fmt"
	"path"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"go.uber.org/zap"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

/*
read object list

object entry
object data
logtail
*/

type ApplyTableDataArg struct {
	ctx            context.Context
	dir            string
	inspectContext *inspectContext
	mp             *mpool.MPool
	fs             fileservice.FileService

	txn     txnif.AsyncTxn
	catalog *catalog.Catalog

	tableName    string
	tableID      uint64
	databaseName string
	databaseID   uint64
}

func NewApplyTableDataArg(
	ctx context.Context,
	dir string,
	inspectContext *inspectContext,
	catalog *catalog.Catalog,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (*ApplyTableDataArg, error) {
	a := &ApplyTableDataArg{
		ctx:            ctx,
		dir:            dir,
		inspectContext: inspectContext,
		mp:             mp,
		fs:             fs,
		catalog:        catalog,
	}
	var err error
	if a.txn, err = a.inspectContext.db.StartTxn(nil); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *ApplyTableDataArg) Run() (err error) {
	logutil.Info(
		"APPLY-TABLE-DATA-START",
		zap.String("dir", a.dir),
		zap.String("start ts", a.txn.GetStartTS().ToString()),
	)
	a.createDatabase()
	a.createTable()

	objectlistBatch, release, err := a.readBatch(CopyTableObjectList)
	if err != nil {
		return
	}
	defer release()
	defer objectlistBatch.Clean(a.mp)
	objTypes := vector.MustFixedColNoTypeCheck[int8](objectlistBatch.Vecs[ObjectListAttr_ObjectType_Idx])
	idVec := objectlistBatch.Vecs[ObjectListAttr_ID_Idx]
	createTSs := vector.MustFixedColNoTypeCheck[types.TS](objectlistBatch.Vecs[ObjectListAttr_CreateTS_Idx])
	deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](objectlistBatch.Vecs[ObjectListAttr_DeleteTS_Idx])
	isPersisted := vector.MustFixedColNoTypeCheck[bool](objectlistBatch.Vecs[ObjectListAttr_IsPersisted_Idx])

	var table *catalog.TableEntry
	var dbEntry *catalog.DBEntry
	if dbEntry, err = a.catalog.GetDatabaseByID(a.databaseID); err != nil {
		return
	}
	if table, err = dbEntry.GetTableEntryByID(a.tableID); err != nil {
		return
	}

	for i := 0; i < objectlistBatch.RowCount(); i++ {
		var isTombstone bool
		if objTypes[i] == ckputil.ObjectType_Data {
			isTombstone = false
		} else if objTypes[i] == ckputil.ObjectType_Tombstone {
			isTombstone = true
		} else {
			panic(fmt.Sprintf("invalid object type: %d", objTypes[i]))
		}
		objectEntry := &catalog.ObjectEntry{
			ObjectNode: catalog.ObjectNode{IsTombstone: isTombstone},
			EntryMVCCNode: catalog.EntryMVCCNode{
				CreatedAt: createTSs[i],
				DeletedAt: deleteTSs[i],
			},
			ObjectMVCCNode: catalog.ObjectMVCCNode{
				ObjectStats: objectio.ObjectStats(idVec.GetBytesAt(i)),
			},
			CreateNode:  txnbase.NewTxnMVCCNodeWithTS(createTSs[i]),
			DeleteNode:  txnbase.NewTxnMVCCNodeWithTS(deleteTSs[i]),
			ObjectState: catalog.ObjectState_Create_ApplyCommit,
		}
		objectEntry.SetTable(table)
		table.AddEntryLocked(objectEntry)

		if !isPersisted[i] {
			name := objectEntry.ObjectName().String()
			var bat *batch.Batch
			var objectRelease func()
			if bat, objectRelease, err = a.readBatch(name); err != nil {
				return
			}
			defer objectRelease()
			defer bat.Clean(a.mp)
			tnBat := containers.ToTNBatch(bat, a.mp)
			objectEntry.GetObjectData().ApplyDebugBatch(tnBat)
		}
	}
	if err = a.txn.Commit(a.ctx); err != nil {
		return
	}
	logutil.Info(
		"APPLY-TABLE-DATA-END",
		zap.String("dir", a.dir),
		zap.String(
			"table",
			fmt.Sprintf(
				"%d-%v, %d-%s",
				a.databaseID,
				a.databaseName,
				a.tableID,
				a.tableName,
			),
		),
		zap.String("end ts", a.txn.GetCommitTS().ToString()),
	)
	return

}

func (a *ApplyTableDataArg) createDatabase() (err error) {

	var bat *batch.Batch
	var release func()
	if bat, release, err = a.readBatch(CopyTableDatabase); err != nil {
		return
	}
	defer release()
	defer bat.Clean(a.mp)
	tnBat := containers.ToTNBatch(bat, a.mp)

	a.databaseName = tnBat.GetVectorByName(pkgcatalog.SystemDBAttr_Name).GetDownstreamVector().GetStringAt(0)
	a.databaseID = tnBat.GetVectorByName(pkgcatalog.SystemDBAttr_ID).Get(0).(uint64)

	panguEpoch := types.BuildTS(42424242, 0)
	txnNode := &txnbase.TxnMVCCNode{
		Start:   panguEpoch,
		Prepare: panguEpoch,
		End:     panguEpoch,
	}

	a.catalog.ReplayMODatabase(a.ctx, txnNode, tnBat)

	var db handle.Database
	if db, err = a.txn.GetDatabase(pkgcatalog.MO_CATALOG); err != nil {
		return
	}
	var table handle.Relation
	if table, err = db.GetRelationByName(pkgcatalog.MO_DATABASE); err != nil {
		return
	}
	if err = table.Append(a.ctx, tnBat); err != nil {
		return
	}
	return
}

func (a *ApplyTableDataArg) createTable() (err error) {

	var schemaBatch, tableBatch *batch.Batch
	var schemaRelease, tableRelease func()
	if schemaBatch, schemaRelease, err = a.readBatch(CopyTableSchema); err != nil {
		return
	}
	defer schemaRelease()
	defer schemaBatch.Clean(a.mp)
	tnSchemaBatch := containers.ToTNBatch(schemaBatch, a.mp)
	if tableBatch, tableRelease, err = a.readBatch(CopyTableTable); err != nil {
		return
	}
	defer tableRelease()
	defer tableBatch.Clean(a.mp)
	tnTableBatch := containers.ToTNBatch(tableBatch, a.mp)

	a.tableName = tnTableBatch.GetVectorByName(pkgcatalog.SystemRelAttr_Name).GetDownstreamVector().GetStringAt(0)
	a.tableID = tnTableBatch.GetVectorByName(pkgcatalog.SystemRelAttr_ID).Get(0).(uint64)

	panguEpoch := types.BuildTS(42424242, 0)
	txnNode := &txnbase.TxnMVCCNode{
		Start:   panguEpoch,
		Prepare: panguEpoch,
		End:     panguEpoch,
	}

	a.catalog.ReplayMOTables(a.ctx, txnNode, tnTableBatch, tnSchemaBatch, &catalog.BaseReplayer{})

	var db handle.Database
	if db, err = a.txn.GetDatabase(pkgcatalog.MO_CATALOG); err != nil {
		return
	}
	var table handle.Relation
	if table, err = db.GetRelationByName(pkgcatalog.MO_COLUMNS); err != nil {
		return
	}
	if err = table.Append(a.ctx, tnSchemaBatch); err != nil {
		return
	}
	if table, err = db.GetRelationByName(pkgcatalog.MO_TABLES); err != nil {
		return
	}
	if err = table.Append(a.ctx, tnTableBatch); err != nil {
		return
	}
	return
}

func (a *ApplyTableDataArg) readBatch(name string) (bat *batch.Batch, release func(), err error) {
	logutil.Info(
		"APPLY-TABLE-DATA-READ-BATCH",
		zap.String("dir", a.dir),
		zap.String("name", name),
	)
	fname := path.Join(a.dir, name)
	var reader *ioutil.BlockReader
	if reader, err = ioutil.NewFileReader(
		a.fs,
		fname,
	); err != nil {
		return
	}
	var bats []*batch.Batch
	if bats, release, err = reader.LoadAllColumns(
		a.ctx, nil, a.mp,
	); err != nil {
		return
	}
	if len(bats) != 1 {
		release()
		for _, bat := range bats {
			bat.Clean(a.mp)
		}
		return nil, nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid object list batch, %d", len(bats)))
	}
	bat = bats[0]
	return
}
