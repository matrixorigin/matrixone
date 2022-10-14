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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestHandle_HandlePreCommit1PC(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	handle := mockTAEHandle(t, opts)
	defer handle.HandleClose(context.TODO())
	txnEngine := handle.GetTxnEngine()
	schema := catalog.MockSchema(2, 1)
	schema.Name = "tbtest"
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	//DDL
	//create db;
	dbName := "dbtest"
	ac := AccessInfo{
		accountId: 0,
		userId:    0,
		roleId:    0,
	}
	createDbEntries, err := makeCreateDatabaseEntries(
		"",
		ac,
		dbName,
		handle.m)
	assert.Nil(t, err)
	createDbTxn := mock1PCTxn(txnEngine)
	err = handle.HandlePreCommit(
		context.TODO(),
		createDbTxn,
		api.PrecommitWriteCmd{
			UserId:    ac.userId,
			AccountId: ac.accountId,
			RoleId:    ac.roleId,
			EntryList: createDbEntries,
		},
		new(api.SyncLogTailResp),
	)
	assert.Nil(t, err)
	err = handle.HandleCommit(context.TODO(), createDbTxn)
	assert.Nil(t, err)

	//start txn ,read "dbtest"'s ID
	ctx := context.TODO()
	txn, err := txnEngine.StartTxn(nil)
	assert.Nil(t, err)
	names, _ := txnEngine.DatabaseNames(ctx, txn)
	assert.Equal(t, 2, len(names))
	dbHandle, err := txnEngine.GetDatabase(ctx, dbName, txn)
	assert.Nil(t, err)
	dbTestId := dbHandle.GetDatabaseID(ctx)
	err = txn.Commit()
	assert.Nil(t, err)

	//create table from "dbtest"
	defs, err := moengine.SchemaToDefs(schema)
	defs[0].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: true,
		Expr: &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: "expr1",
					},
				},
			},
		},
		OriginString: "expr1",
	}
	defs[1].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: false,
		Expr: &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: "expr2",
					},
				},
			},
		},
		OriginString: "expr2",
	}
	assert.Nil(t, err)

	createTbTxn := mock1PCTxn(txnEngine)

	createTbEntries, err := makeCreateTableEntries(
		"",
		ac,
		schema.Name,
		new(common.IdAllocator).Alloc(),
		dbTestId,
		dbName,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	err = handle.HandlePreCommit(
		context.TODO(),
		createTbTxn,
		api.PrecommitWriteCmd{
			UserId:    ac.userId,
			AccountId: ac.accountId,
			RoleId:    ac.roleId,
			EntryList: createTbEntries,
		},
		new(api.SyncLogTailResp))
	assert.Nil(t, err)
	err = handle.HandleCommit(context.TODO(), createTbTxn)
	assert.Nil(t, err)
	//start txn ,read table ID
	txn, err = txnEngine.StartTxn(nil)
	assert.Nil(t, err)
	dbHandle, err = txnEngine.GetDatabase(ctx, dbName, txn)
	assert.NoError(t, err)
	dbId := dbHandle.GetDatabaseID(ctx)
	assert.True(t, dbTestId == dbId)
	names, _ = dbHandle.RelationNames(ctx)
	assert.Equal(t, 1, len(names))
	tbHandle, err := dbHandle.GetRelation(ctx, schema.Name)
	assert.NoError(t, err)
	tbTestId := tbHandle.GetRelationID(ctx)
	rDefs, _ := tbHandle.TableDefs(ctx)
	//assert.Equal(t, 3, len(rDefs))
	rAttr := rDefs[0].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.NullAbility)
	rAttr = rDefs[1].(*engine.AttributeDef).Attr
	assert.Equal(t, "expr2", rAttr.Default.OriginString)

	err = txn.Commit()
	assert.NoError(t, err)

	//DML: insert batch into table
	insertTxn := mock1PCTxn(txnEngine)
	moBat := containers.CopyToMoBatch(catalog.MockBatch(schema, 100))
	insertEntry, err := makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, moBat)
	assert.NoError(t, err)
	err = handle.HandlePreCommit(
		context.TODO(),
		insertTxn,
		api.PrecommitWriteCmd{
			UserId:    ac.userId,
			AccountId: ac.accountId,
			RoleId:    ac.roleId,
			EntryList: []*api.Entry{insertEntry},
		},
		new(api.SyncLogTailResp),
	)
	assert.NoError(t, err)
	// TODO:: Dml delete
	//bat := batch.NewWithSize(1)
	err = handle.HandleCommit(context.TODO(), insertTxn)
	assert.NoError(t, err)
	//TODO::DML:delete by primary key.
	// physcial addr + primary key
	//bat = batch.NewWithSize(2)

	//start txn ,read table ID
	txn, err = txnEngine.StartTxn(nil)
	assert.NoError(t, err)
	dbHandle, err = txnEngine.GetDatabase(ctx, dbName, txn)
	assert.NoError(t, err)
	tbHandle, err = dbHandle.GetRelation(ctx, schema.Name)
	assert.NoError(t, err)
	tbReaders, _ := tbHandle.NewReader(ctx, 1, nil, nil)
	for _, reader := range tbReaders {
		bat, err := reader.Read([]string{schema.ColDefs[1].Name}, nil, handle.m)
		assert.Nil(t, err)
		if bat != nil {
			len := vector.Length(bat.Vecs[0])
			assert.Equal(t, 100, len)
		}
	}
	// read row ids
	hideCol, err := tbHandle.GetHideKeys(ctx)
	assert.NoError(t, err)
	reader, _ := tbHandle.NewReader(ctx, 1, nil, nil)
	hideBat, err := reader[0].Read([]string{hideCol[0].Name}, nil, handle.m)
	assert.Nil(t, err)
	err = txn.Commit()
	assert.Nil(t, err)

	//delete 20 rows
	deleteTxn := mock1PCTxn(handle.GetTxnEngine())
	batch.SetLength(hideBat, 20)
	deleteEntry, _ := makePBEntry(
		DELETE,
		dbId,
		tbTestId,
		dbName,
		schema.Name,
		hideBat,
	)
	err = handle.HandlePreCommit(
		context.TODO(),
		deleteTxn,
		api.PrecommitWriteCmd{
			UserId:    ac.userId,
			AccountId: ac.accountId,
			RoleId:    ac.roleId,
			EntryList: append([]*api.Entry{}, deleteEntry),
		},
		new(api.SyncLogTailResp),
	)
	assert.Nil(t, err)
	err = handle.HandleCommit(context.TODO(), deleteTxn)
	assert.Nil(t, err)
	//read, there should be 80 rows left.
	txn, err = txnEngine.StartTxn(nil)
	assert.NoError(t, err)
	dbHandle, err = txnEngine.GetDatabase(ctx, dbName, txn)
	assert.NoError(t, err)
	tbHandle, err = dbHandle.GetRelation(ctx, schema.Name)
	assert.NoError(t, err)
	tbReaders, _ = tbHandle.NewReader(ctx, 2, nil, nil)
	for _, reader := range tbReaders {
		bat, err := reader.Read([]string{schema.ColDefs[1].Name}, nil, handle.m)
		assert.Nil(t, err)
		if bat != nil {
			len := vector.Length(bat.Vecs[0])
			assert.Equal(t, 80, len)
		}
	}
	err = txn.Commit()
	assert.Nil(t, err)
}

func TestHandle_HandlePreCommit2PCForCoordinator(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	handle := mockTAEHandle(t, opts)
	defer handle.HandleClose(context.TODO())
	txnEngine := handle.GetTxnEngine()
	schema := catalog.MockSchema(2, 1)
	schema.Name = "tbtest"
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	dbName := "dbtest"
	ac := AccessInfo{
		accountId: 0,
		userId:    0,
		roleId:    0,
	}
	//make create db cmd;
	createDbEntries, err := makeCreateDatabaseEntries(
		"",
		ac,
		dbName,
		handle.m)
	assert.Nil(t, err)
	txnCmds := []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: createDbEntries},
		},
		{typ: CmdPrepare},
		{typ: CmdCommitting},
		{typ: CmdCommit},
	}
	txnMeta := mock2PCTxn(txnEngine)
	ctx := context.TODO()
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)

	//start 1pc txn ,read "dbtest"'s ID
	ctx = context.TODO()
	txn, err := txnEngine.StartTxn(nil)
	assert.Nil(t, err)
	names, _ := txnEngine.DatabaseNames(ctx, txn)
	assert.Equal(t, 2, len(names))
	dbHandle, err := txnEngine.GetDatabase(ctx, dbName, txn)
	assert.Nil(t, err)
	dbTestId := dbHandle.GetDatabaseID(ctx)
	err = txn.Commit()
	assert.Nil(t, err)

	//create table from "dbtest"
	defs, err := moengine.SchemaToDefs(schema)
	defs[0].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: true,
		Expr: &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: "expr1",
					},
				},
			},
		},
		OriginString: "expr1",
	}
	defs[1].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: false,
		Expr: &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: "expr2",
					},
				},
			},
		},
		OriginString: "expr2",
	}
	assert.Nil(t, err)
	createTbEntries, err := makeCreateTableEntries(
		"",
		ac,
		schema.Name,
		new(common.IdAllocator).Alloc(),
		dbTestId,
		dbName,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: createTbEntries},
		},
		{typ: CmdPrepare},
		{typ: CmdCommitting},
		{typ: CmdCommit},
	}
	txnMeta = mock2PCTxn(txnEngine)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)

	//start 1pc txn ,read table ID
	txn, err = txnEngine.StartTxn(nil)
	assert.Nil(t, err)
	dbHandle, err = txnEngine.GetDatabase(ctx, dbName, txn)
	assert.NoError(t, err)
	dbId := dbHandle.GetDatabaseID(ctx)
	assert.True(t, dbTestId == dbId)
	names, _ = dbHandle.RelationNames(ctx)
	assert.Equal(t, 1, len(names))
	tbHandle, err := dbHandle.GetRelation(ctx, schema.Name)
	assert.NoError(t, err)
	tbTestId := tbHandle.GetRelationID(ctx)
	rDefs, _ := tbHandle.TableDefs(ctx)
	assert.Equal(t, 3, len(rDefs))
	rAttr := rDefs[0].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.NullAbility)
	rAttr = rDefs[1].(*engine.AttributeDef).Attr
	assert.Equal(t, "expr2", rAttr.Default.OriginString)
	err = txn.Commit()
	assert.NoError(t, err)

	//DML::insert batch into table
	moBat := containers.CopyToMoBatch(catalog.MockBatch(schema, 100))
	insertEntry, err := makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdCommitting},
		{typ: CmdCommit},
	}
	insertTxn := mock2PCTxn(txnEngine)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, insertTxn, txnCmds)
	assert.Nil(t, err)

	//start 2PC txn ,rollback it after prepared
	rollbackTxn := mock2PCTxn(txnEngine)
	//insert 20 rows, then rollback the txn
	//FIXME::??
	//batch.SetLength(moBat, 20)
	moBat = containers.CopyToMoBatch(catalog.MockBatch(schema, 20))
	insertEntry, err = makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdRollback},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, rollbackTxn, txnCmds)
	assert.Nil(t, err)

	//start 1PC txn , read table
	txn, err = txnEngine.StartTxn(nil)
	assert.NoError(t, err)
	dbHandle, err = txnEngine.GetDatabase(ctx, dbName, txn)
	assert.NoError(t, err)
	tbHandle, err = dbHandle.GetRelation(ctx, schema.Name)
	assert.NoError(t, err)
	tbReaders, _ := tbHandle.NewReader(ctx, 1, nil, nil)
	for _, reader := range tbReaders {
		bat, err := reader.Read([]string{schema.ColDefs[1].Name}, nil, handle.m)
		assert.Nil(t, err)
		if bat != nil {
			len := vector.Length(bat.Vecs[0])
			assert.Equal(t, 100, len)
		}
	}
	// read row ids
	hideCol, err := tbHandle.GetHideKeys(ctx)
	assert.NoError(t, err)
	reader, _ := tbHandle.NewReader(ctx, 1, nil, nil)
	hideBat, err := reader[0].Read([]string{hideCol[0].Name}, nil, handle.m)
	assert.Nil(t, err)
	err = txn.Commit()
	assert.Nil(t, err)

	hideBats := containers.SplitBatch(hideBat, 5)
	//delete 20 rows by 2PC txn
	//batch.SetLength(hideBats[0], 20)
	deleteEntry, err := makePBEntry(
		DELETE,
		dbId,
		tbTestId,
		dbName,
		schema.Name,
		hideBats[0],
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{deleteEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdCommitting},
		{typ: CmdCommit},
	}
	deleteTxn := mock2PCTxn(txnEngine)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, deleteTxn, txnCmds)
	assert.Nil(t, err)

	//start a 2PC txn ,rollback it after prepared.
	rollbackTxn = mock2PCTxn(txnEngine)
	deleteEntry, _ = makePBEntry(
		DELETE,
		dbId,
		tbTestId,
		dbName,
		schema.Name,
		hideBats[1],
	)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{deleteEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdRollback},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, rollbackTxn, txnCmds)
	assert.Nil(t, err)

	//read, there should be 80 rows left.
	txn, err = txnEngine.StartTxn(nil)
	assert.NoError(t, err)
	dbHandle, err = txnEngine.GetDatabase(ctx, dbName, txn)
	assert.NoError(t, err)
	tbHandle, err = dbHandle.GetRelation(ctx, schema.Name)
	assert.NoError(t, err)
	tbReaders, _ = tbHandle.NewReader(ctx, 2, nil, nil)
	for _, reader := range tbReaders {
		bat, err := reader.Read([]string{schema.ColDefs[1].Name}, nil, handle.m)
		assert.Nil(t, err)
		if bat != nil {
			len := vector.Length(bat.Vecs[0])
			assert.Equal(t, 80, len)
		}
	}
	err = txn.Commit()
	assert.Nil(t, err)
}

func TestHandle_HandlePreCommit2PCForParticipant(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	handle := mockTAEHandle(t, opts)
	defer handle.HandleClose(context.TODO())
	txnEngine := handle.GetTxnEngine()
	schema := catalog.MockSchema(2, 1)
	schema.Name = "tbtest"
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	dbName := "dbtest"
	ac := AccessInfo{
		accountId: 0,
		userId:    0,
		roleId:    0,
	}
	//make create db cmd;
	createDbEntries, err := makeCreateDatabaseEntries(
		"",
		ac,
		dbName,
		handle.m)
	assert.Nil(t, err)
	txnCmds := []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: createDbEntries},
		},
		{typ: CmdPrepare},
		{typ: CmdCommit},
	}
	txnMeta := mock2PCTxn(txnEngine)
	ctx := context.TODO()
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)

	//start 1pc txn ,read "dbtest"'s ID
	ctx = context.TODO()
	txn, err := txnEngine.StartTxn(nil)
	assert.Nil(t, err)
	names, _ := txnEngine.DatabaseNames(ctx, txn)
	assert.Equal(t, 2, len(names))
	dbHandle, err := txnEngine.GetDatabase(ctx, dbName, txn)
	assert.Nil(t, err)
	dbTestId := dbHandle.GetDatabaseID(ctx)
	err = txn.Commit()
	assert.Nil(t, err)

	//create table from "dbtest"
	defs, err := moengine.SchemaToDefs(schema)
	defs[0].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: true,
		Expr: &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: "expr1",
					},
				},
			},
		},
		OriginString: "expr1",
	}
	defs[1].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: false,
		Expr: &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: "expr2",
					},
				},
			},
		},
		OriginString: "expr2",
	}
	assert.Nil(t, err)
	createTbEntries, err := makeCreateTableEntries(
		"",
		ac,
		schema.Name,
		new(common.IdAllocator).Alloc(),
		dbTestId,
		dbName,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: createTbEntries},
		},
		{typ: CmdPrepare},
		{typ: CmdCommit},
	}
	txnMeta = mock2PCTxn(txnEngine)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)

	//start 1pc txn ,read table ID
	txn, err = txnEngine.StartTxn(nil)
	assert.Nil(t, err)
	dbHandle, err = txnEngine.GetDatabase(ctx, dbName, txn)
	assert.NoError(t, err)
	dbId := dbHandle.GetDatabaseID(ctx)
	assert.True(t, dbTestId == dbId)
	names, _ = dbHandle.RelationNames(ctx)
	assert.Equal(t, 1, len(names))
	tbHandle, err := dbHandle.GetRelation(ctx, schema.Name)
	assert.NoError(t, err)
	tbTestId := tbHandle.GetRelationID(ctx)
	rDefs, _ := tbHandle.TableDefs(ctx)
	assert.Equal(t, 3, len(rDefs))
	rAttr := rDefs[0].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.NullAbility)
	rAttr = rDefs[1].(*engine.AttributeDef).Attr
	assert.Equal(t, "expr2", rAttr.Default.OriginString)
	err = txn.Commit()
	assert.NoError(t, err)

	//DML::insert batch into table
	moBat := containers.CopyToMoBatch(catalog.MockBatch(schema, 100))
	insertEntry, err := makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdCommit},
	}
	insertTxn := mock2PCTxn(txnEngine)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, insertTxn, txnCmds)
	assert.Nil(t, err)

	//start 2PC txn ,rollback it after prepared
	rollbackTxn := mock2PCTxn(txnEngine)
	//insert 20 rows ,then rollback
	//FIXME::??
	//batch.SetLength(moBat, 20)
	moBat = containers.CopyToMoBatch(catalog.MockBatch(schema, 20))
	insertEntry, err = makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdRollback},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, rollbackTxn, txnCmds)
	assert.Nil(t, err)

	//start 2PC txn , rollback it when it is ACTIVE.
	rollbackTxn = mock2PCTxn(txnEngine)
	//insert 10 rows ,then rollback
	//batch.SetLength(moBat, 10)
	moBat = containers.CopyToMoBatch(catalog.MockBatch(schema, 10))
	insertEntry, err = makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdRollback},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, rollbackTxn, txnCmds)
	assert.Nil(t, err)

	//start 1PC txn , read table
	txn, err = txnEngine.StartTxn(nil)
	assert.NoError(t, err)
	dbHandle, err = txnEngine.GetDatabase(ctx, dbName, txn)
	assert.NoError(t, err)
	tbHandle, err = dbHandle.GetRelation(ctx, schema.Name)
	assert.NoError(t, err)
	tbReaders, _ := tbHandle.NewReader(ctx, 1, nil, nil)
	for _, reader := range tbReaders {
		bat, err := reader.Read([]string{schema.ColDefs[1].Name}, nil, handle.m)
		assert.Nil(t, err)
		if bat != nil {
			len := vector.Length(bat.Vecs[0])
			assert.Equal(t, 100, len)
		}
	}
	// read row ids
	hideCol, err := tbHandle.GetHideKeys(ctx)
	assert.NoError(t, err)
	reader, _ := tbHandle.NewReader(ctx, 1, nil, nil)
	hideBat, err := reader[0].Read([]string{hideCol[0].Name}, nil, handle.m)
	assert.Nil(t, err)
	err = txn.Commit()
	assert.Nil(t, err)

	hideBats := containers.SplitBatch(hideBat, 5)
	//delete 20 rows by 2PC txn
	//batch.SetLength(hideBat, 20)
	deleteEntry, err := makePBEntry(
		DELETE,
		dbId,
		tbTestId,
		dbName,
		schema.Name,
		hideBats[0],
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{deleteEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdCommitting},
		{typ: CmdCommit},
	}
	deleteTxn := mock2PCTxn(txnEngine)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, deleteTxn, txnCmds)
	assert.Nil(t, err)

	//start a 2PC txn ,rollback it after prepared.
	// delete 20 rows ,then rollback
	rollbackTxn = mock2PCTxn(txnEngine)
	deleteEntry, _ = makePBEntry(
		DELETE,
		dbId,
		tbTestId,
		dbName,
		schema.Name,
		hideBats[1],
	)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{deleteEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdRollback},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, rollbackTxn, txnCmds)
	assert.Nil(t, err)

	//read, there should be 80 rows left.
	txn, err = txnEngine.StartTxn(nil)
	assert.NoError(t, err)
	dbHandle, err = txnEngine.GetDatabase(ctx, dbName, txn)
	assert.NoError(t, err)
	tbHandle, err = dbHandle.GetRelation(ctx, schema.Name)
	assert.NoError(t, err)
	tbReaders, _ = tbHandle.NewReader(ctx, 2, nil, nil)
	for _, reader := range tbReaders {
		bat, err := reader.Read([]string{schema.ColDefs[1].Name}, nil, handle.m)
		assert.Nil(t, err)
		if bat != nil {
			len := vector.Length(bat.Vecs[0])
			assert.Equal(t, 80, len)
		}
	}
	err = txn.Commit()
	assert.Nil(t, err)
}

func TestHandle_MVCCVisibility(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	handle := mockTAEHandle(t, opts)
	defer handle.HandleClose(context.TODO())
	txnEngine := handle.GetTxnEngine()
	schema := catalog.MockSchema(2, 1)
	schema.Name = "tbtest"
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	dbName := "dbtest"
	ac := AccessInfo{
		accountId: 0,
		userId:    0,
		roleId:    0,
	}
	//make create db cmd;
	createDbEntries, err := makeCreateDatabaseEntries(
		"",
		ac,
		dbName,
		handle.m)
	assert.Nil(t, err)
	txnCmds := []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: createDbEntries},
		},
	}
	txnMeta := mock2PCTxn(txnEngine)
	ctx := context.TODO()
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)
	var dbTestId uint64
	var dbNames []string
	wg := new(sync.WaitGroup)
	wg.Add(1)
	//start a db reader.
	go func() {
		//start 1pc txn ,read "dbtest"'s ID
		ctx := context.TODO()
		txn, err := txnEngine.StartTxn(nil)
		assert.Nil(t, err)
		dbNames, _ = txnEngine.DatabaseNames(ctx, txn)
		err = txn.Commit()
		assert.Nil(t, err)
		wg.Done()

	}()
	wg.Wait()
	assert.Equal(t, 1, len(dbNames))

	err = handle.HandlePrepare(ctx, txnMeta)
	assert.Nil(t, err)
	//start reader after preparing success.
	startTime := time.Now()
	wg.Add(1)
	go func() {
		//start 1pc txn ,read "dbtest"'s ID
		ctx := context.TODO()
		txn, err := txnEngine.StartTxn(nil)
		assert.Nil(t, err)
		//reader should wait until the writer committed.
		dbNames, _ = txnEngine.DatabaseNames(ctx, txn)
		assert.Equal(t, 2, len(dbNames))
		dbHandle, err := txnEngine.GetDatabase(ctx, dbName, txn)
		assert.Nil(t, err)
		dbTestId = dbHandle.GetDatabaseID(ctx)
		err = txn.Commit()
		assert.Nil(t, err)
		wg.Done()
		//To check whether reader had waited.
		assert.True(t, time.Since(startTime) > 1*time.Second)

	}()
	//sleep 1 second
	time.Sleep(1 * time.Second)
	//CommitTS = PreparedTS + 1
	err = handle.handleCmds(ctx, txnMeta, []txnCommand{
		{typ: CmdCommitting}, {typ: CmdCommit},
	})
	assert.Nil(t, err)
	wg.Wait()

	//create table from "dbtest"
	defs, err := moengine.SchemaToDefs(schema)
	defs[0].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: true,
		Expr: &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: "expr1",
					},
				},
			},
		},
		OriginString: "expr1",
	}
	defs[1].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: false,
		Expr: &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: "expr2",
					},
				},
			},
		},
		OriginString: "expr2",
	}
	assert.Nil(t, err)
	createTbEntries, err := makeCreateTableEntries(
		"",
		ac,
		schema.Name,
		new(common.IdAllocator).Alloc(),
		dbTestId,
		dbName,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: createTbEntries},
		},
		{typ: CmdPrepare},
	}
	txnMeta = mock2PCTxn(txnEngine)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)
	var tbTestId uint64
	startTime = time.Now()
	wg.Add(1)
	go func() {
		//start 1pc txn ,read table ID
		txn, err := txnEngine.StartTxn(nil)
		assert.Nil(t, err)
		ctx := context.TODO()
		dbHandle, err := txnEngine.GetDatabase(ctx, dbName, txn)
		assert.NoError(t, err)
		dbId := dbHandle.GetDatabaseID(ctx)
		assert.True(t, dbTestId == dbId)
		//txn should wait here.
		names, _ := dbHandle.RelationNames(ctx)
		assert.Equal(t, 1, len(names))
		tbHandle, err := dbHandle.GetRelation(ctx, schema.Name)
		assert.NoError(t, err)
		tbTestId = tbHandle.GetRelationID(ctx)
		rDefs, _ := tbHandle.TableDefs(ctx)
		assert.Equal(t, 3, len(rDefs))
		rAttr := rDefs[0].(*engine.AttributeDef).Attr
		assert.Equal(t, true, rAttr.Default.NullAbility)
		rAttr = rDefs[1].(*engine.AttributeDef).Attr
		assert.Equal(t, "expr2", rAttr.Default.OriginString)
		err = txn.Commit()
		assert.NoError(t, err)
		wg.Done()
		//To check whether reader had waited.
		assert.True(t, time.Since(startTime) > 1*time.Second)
	}()
	time.Sleep(1 * time.Second)
	err = handle.handleCmds(ctx, txnMeta, []txnCommand{
		{typ: CmdCommitting}, {typ: CmdCommit},
	})
	assert.Nil(t, err)
	wg.Wait()

	//DML::insert batch into table
	moBat := containers.CopyToMoBatch(catalog.MockBatch(schema, 100))
	insertEntry, err := makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdPrepare},
	}
	insertTxn := mock2PCTxn(txnEngine)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, insertTxn, txnCmds)
	assert.Nil(t, err)
	startTime = time.Now()
	wg.Add(1)
	go func() {
		//start 1PC txn , read table
		txn, err := txnEngine.StartTxn(nil)
		assert.NoError(t, err)
		ctx := context.TODO()
		dbHandle, err := txnEngine.GetDatabase(ctx, dbName, txn)
		assert.NoError(t, err)
		tbHandle, err := dbHandle.GetRelation(ctx, schema.Name)
		assert.NoError(t, err)
		tbReaders, _ := tbHandle.NewReader(ctx, 1, nil, nil)
		for _, reader := range tbReaders {
			bat, err := reader.Read(
				[]string{schema.ColDefs[1].Name},
				nil,
				handle.m)
			assert.Nil(t, err)
			if bat != nil {
				len := vector.Length(bat.Vecs[0])
				assert.Equal(t, 100, len)
			}
		}
		txn.Commit()
		//To check whether reader had waited.
		assert.True(t, time.Since(startTime) > 1*time.Second)
		wg.Done()
	}()
	time.Sleep(1 * time.Second)
	//insertTxn 's CommitTS = PreparedTS + 1.
	err = handle.handleCmds(ctx, insertTxn, []txnCommand{
		{typ: CmdCommitting}, {typ: CmdCommit},
	})
	assert.Nil(t, err)
	wg.Wait()

	//DML:delete rows
	//read row ids
	var hideBat *batch.Batch
	{
		txn, err := txnEngine.StartTxn(nil)
		assert.NoError(t, err)
		ctx = context.TODO()
		dbHandle, err := txnEngine.GetDatabase(ctx, dbName, txn)
		assert.NoError(t, err)
		tbHandle, err := dbHandle.GetRelation(ctx, schema.Name)
		assert.NoError(t, err)
		hideCol, err := tbHandle.GetHideKeys(ctx)
		assert.NoError(t, err)
		reader, _ := tbHandle.NewReader(ctx, 1, nil, nil)
		hideBat, err = reader[0].Read([]string{hideCol[0].Name}, nil, handle.m)
		assert.Nil(t, err)
		err = txn.Commit()
		assert.Nil(t, err)
	}

	hideBats := containers.SplitBatch(hideBat, 5)
	//delete 20 rows by 2PC txn
	deleteTxn := mock2PCTxn(txnEngine)
	//batch.SetLength(hideBat, 20)
	deleteEntry, err := makePBEntry(
		DELETE,
		dbTestId,
		tbTestId,
		dbName,
		schema.Name,
		hideBats[0],
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				UserId:    ac.userId,
				AccountId: ac.accountId,
				RoleId:    ac.roleId,
				EntryList: []*api.Entry{deleteEntry}},
		},
		{typ: CmdPrepare},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, deleteTxn, txnCmds)
	assert.Nil(t, err)
	startTime = time.Now()
	wg.Add(1)
	go func() {
		//read, there should be 80 rows left.
		txn, err := txnEngine.StartTxn(nil)
		assert.NoError(t, err)
		ctx := context.TODO()
		dbHandle, err := txnEngine.GetDatabase(ctx, dbName, txn)
		assert.NoError(t, err)
		tbHandle, err := dbHandle.GetRelation(ctx, schema.Name)
		assert.NoError(t, err)
		tbReaders, _ := tbHandle.NewReader(ctx, 2, nil, nil)
		for _, reader := range tbReaders {
			bat, err := reader.Read([]string{schema.ColDefs[1].Name}, nil, handle.m)
			assert.Nil(t, err)
			if bat != nil {
				len := vector.Length(bat.Vecs[0])
				assert.Equal(t, 80, len)
			}
		}
		err = txn.Commit()
		assert.Nil(t, err)
		//To check whether reader had waited.
		assert.True(t, time.Since(startTime) > 1*time.Second)
		wg.Done()

	}()
	time.Sleep(1 * time.Second)
	//deleteTxn 's CommitTS = PreparedTS + 1
	err = handle.handleCmds(ctx, deleteTxn, []txnCommand{
		{typ: CmdCommitting}, {typ: CmdCommit},
	})
	assert.Nil(t, err)
	wg.Wait()
}
