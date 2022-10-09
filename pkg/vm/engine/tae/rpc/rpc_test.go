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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHandle_HandlePreCommit1PC(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	handle := mockTAEHandle(t, opts)
	defer handle.HandleClose()
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
	err = handle.HandleCommit(createDbTxn)
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
		dbTestId,
		dbName,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	err = handle.HandlePreCommit(
		createTbTxn,
		api.PrecommitWriteCmd{
			UserId:    ac.userId,
			AccountId: ac.accountId,
			RoleId:    ac.roleId,
			EntryList: createTbEntries,
		},
		new(api.SyncLogTailResp))
	assert.Nil(t, err)
	err = handle.HandleCommit(createTbTxn)
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
	assert.Equal(t, 3, len(rDefs))
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
	var insertEntries []*api.Entry
	insertEntries = append(insertEntries, insertEntry)
	err = handle.HandlePreCommit(
		insertTxn,
		api.PrecommitWriteCmd{
			UserId:    ac.userId,
			AccountId: ac.accountId,
			RoleId:    ac.roleId,
			EntryList: insertEntries,
		},
		new(api.SyncLogTailResp),
	)
	assert.NoError(t, err)
	// TODO:: Dml delete
	//bat := batch.NewWithSize(1)
	err = handle.HandleCommit(insertTxn)
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

}

func TestHandle_HandlePreCommit2PC(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	handle := mockTAEHandle(t, opts)
	defer handle.HandleClose()
	txnEngine := handle.GetTxnEngine()
	//create db/tb;
	_, err := mock2PCTxn(txnEngine)
	assert.Nil(t, err)

}
