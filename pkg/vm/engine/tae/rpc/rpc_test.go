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
	"github.com/stretchr/testify/require"
	"strconv"
	"sync"
	"testing"
	"time"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/mocks"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestHandle_HandleCommitPerformanceForS3Load(t *testing.T) {
	defer testutils.AfterTest(t)()
	opts := config.WithLongScanAndCKPOpts(nil)
	ctx := context.Background()

	handle := mockTAEHandle(ctx, t, opts)
	defer handle.HandleClose(context.TODO())
	fs := handle.db.Opts.Fs
	IDAlloc := catalog.NewIDAllocator()

	schema := catalog.MockSchema(2, 1)
	schema.Name = "tbtest"
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
	//100 objs, one obj contains 50 blocks, one block contains 10 rows.
	taeBat := catalog.MockBatch(schema, 100*50*10)
	defer taeBat.Close()
	taeBats := taeBat.Split(100 * 50)

	var objNames []objectio.ObjectName
	var stats []objectio.ObjectStats
	offset := 0
	for i := 0; i < 100; i++ {
		name := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
		objNames = append(objNames, name)
		writer, err := blockio.NewBlockWriterNew(fs, objNames[i], 0, nil)
		assert.Nil(t, err)
		for j := 0; j < 50; j++ {
			_, err = writer.WriteBatch(containers.ToCNBatch(taeBats[offset+j]))
			assert.Nil(t, err)
		}
		offset += 50
		blocks, _, err := writer.Sync(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, 50, len(blocks))

		ss := writer.GetObjectStats(objectio.WithCNCreated())
		stats = append(stats, ss)

		require.Equal(t, int(50), int(ss.BlkCnt()))
		require.Equal(t, int(50*10), int(ss.Rows()))
	}

	//create dbtest and tbtest;
	dbName := "dbtest"
	ac := AccessInfo{
		accountId: 0,
		userId:    0,
		roleId:    0,
	}
	//var entries []*api.Entry
	entries := make([]*api.Entry, 0)
	txn := mock1PCTxn(handle.db)
	dbTestID := IDAlloc.NextDB()
	createDbEntries, err := makeCreateDatabaseEntries(
		"",
		ac,
		dbName,
		dbTestID,
		handle.m)
	assert.Nil(t, err)
	entries = append(entries, createDbEntries...)
	//create table from "dbtest"
	defs, err := catalog.SchemaToDefs(schema)
	for i := 0; i < len(defs); i++ {
		if attrdef, ok := defs[i].(*engine.AttributeDef); ok {
			attrdef.Attr.Default = &plan.Default{
				NullAbility: true,
				Expr: &plan.Expr{
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_Sval{
								Sval: "expr" + strconv.Itoa(i),
							},
						},
					},
				},
				OriginString: "expr" + strconv.Itoa(i),
			}
		}
	}

	assert.Nil(t, err)
	tbTestID := IDAlloc.NextTable()
	createTbEntries, err := makeCreateTableEntries(
		"",
		ac,
		schema.Name,
		tbTestID,
		dbTestID,
		dbName,
		schema.Constraint,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	entries = append(entries, createTbEntries...)

	//add 100 * 50 blocks from S3 into "tbtest" table
	attrs := []string{catalog2.ObjectMeta_ObjectStats}
	vecTypes := []types.Type{types.New(types.T_varchar, types.MaxVarcharLen, 0)}
	vecOpts := containers.Options{}
	vecOpts.Capacity = 0
	for i, obj := range objNames {
		metaLocBat := containers.BuildBatch(attrs, vecTypes, vecOpts)
		metaLocBat.Vecs[0].Append([]byte(stats[i][:]), false)
		metaLocMoBat := containers.ToCNBatch(metaLocBat)
		addS3BlkEntry, err := makePBEntry(INSERT, dbTestID,
			tbTestID, dbName, schema.Name, obj.String(), metaLocMoBat)
		assert.NoError(t, err)
		entries = append(entries, addS3BlkEntry)
		defer metaLocBat.Close()
	}
	err = handle.HandlePreCommit(
		context.TODO(),
		txn,
		&api.PrecommitWriteCmd{
			//UserId:    ac.userId,
			//AccountId: ac.accountId,
			//RoleId:    ac.roleId,
			EntryList: entries,
		},
		nil,
	)
	assert.Nil(t, err)
	//t.FailNow()
	start := time.Now()
	_, err = handle.HandleCommit(context.TODO(), txn)
	assert.Nil(t, err)
	t.Logf("Commit 10w blocks spend: %d", time.Since(start).Microseconds())
}

func TestHandle_HandlePreCommitWriteS3(t *testing.T) {
	defer testutils.AfterTest(t)()
	opts := config.WithLongScanAndCKPOpts(nil)
	ctx := context.Background()

	handle := mockTAEHandle(ctx, t, opts)
	defer handle.HandleClose(context.TODO())
	fs := handle.db.Opts.Fs
	defer fs.Close()
	IDAlloc := catalog.NewIDAllocator()

	schema := catalog.MockSchema(2, 1)
	schema.Name = "tbtest"
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
	taeBat := catalog.MockBatch(schema, 40)
	defer taeBat.Close()

	taeBats := taeBat.Split(4)
	taeBats[0] = taeBats[0].CloneWindow(0, 10)
	taeBats[1] = taeBats[1].CloneWindow(0, 10)
	taeBats[2] = taeBats[2].CloneWindow(0, 10)
	taeBats[3] = taeBats[3].CloneWindow(0, 10)

	//sort by primary key
	_, err := mergesort.SortBlockColumns(taeBats[0].Vecs, 1, mocks.GetTestVectorPool())
	assert.Nil(t, err)
	_, err = mergesort.SortBlockColumns(taeBats[1].Vecs, 1, mocks.GetTestVectorPool())
	assert.Nil(t, err)
	_, err = mergesort.SortBlockColumns(taeBats[2].Vecs, 1, mocks.GetTestVectorPool())
	assert.Nil(t, err)

	moBats := make([]*batch.Batch, 4)
	moBats[0] = containers.ToCNBatch(taeBats[0])
	moBats[1] = containers.ToCNBatch(taeBats[1])
	moBats[2] = containers.ToCNBatch(taeBats[2])
	moBats[3] = containers.ToCNBatch(taeBats[3])

	//write taeBats[0], taeBats[1] two blocks into file service
	objName1 := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
	writer, err := blockio.NewBlockWriterNew(fs, objName1, 0, nil)
	assert.Nil(t, err)
	writer.SetPrimaryKey(1)
	for i, bat := range taeBats {
		if i == 2 {
			break
		}
		_, err := writer.WriteBatch(containers.ToCNBatch(bat))
		assert.Nil(t, err)
	}
	blocks, _, err := writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 2, len(blocks))
	stats1 := writer.GetObjectStats(objectio.WithCNCreated())
	require.Equal(t, int(2), int(stats1.BlkCnt()))
	require.Equal(t, int(20), int(stats1.Rows()))

	//write taeBats[3] into file service
	objName2 := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
	writer, err = blockio.NewBlockWriterNew(fs, objName2, 0, nil)
	assert.Nil(t, err)
	writer.SetPrimaryKey(1)
	_, err = writer.WriteBatch(containers.ToCNBatch(taeBats[3]))
	assert.Nil(t, err)
	blocks, _, err = writer.Sync(context.Background())
	assert.Equal(t, 1, len(blocks))
	assert.Nil(t, err)

	stats3 := writer.GetObjectStats(objectio.WithCNCreated())
	require.Equal(t, int(1), int(stats3.BlkCnt()))
	require.Equal(t, int(10), int(stats3.Rows()))

	//create db;
	dbName := "dbtest"
	ac := AccessInfo{
		accountId: 0,
		userId:    0,
		roleId:    0,
	}
	var entries []*api.Entry
	txn := mock1PCTxn(handle.db)
	dbTestID := IDAlloc.NextDB()
	createDbEntries, err := makeCreateDatabaseEntries(
		"",
		ac,
		dbName,
		dbTestID,
		handle.m)
	assert.Nil(t, err)
	entries = append(entries, createDbEntries...)
	//create table from "dbtest"
	defs, err := catalog.SchemaToDefs(schema)
	for i := 0; i < len(defs); i++ {
		if attrdef, ok := defs[i].(*engine.AttributeDef); ok {
			attrdef.Attr.Default = &plan.Default{
				NullAbility: true,
				Expr: &plan.Expr{
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_Sval{
								Sval: "expr" + strconv.Itoa(i),
							},
						},
					},
				},
				OriginString: "expr" + strconv.Itoa(i),
			}
		}
	}

	assert.Nil(t, err)
	tbTestID := IDAlloc.NextTable()
	createTbEntries, err := makeCreateTableEntries(
		"",
		ac,
		schema.Name,
		tbTestID,
		dbTestID,
		dbName,
		schema.Constraint,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	entries = append(entries, createTbEntries...)
	err = handle.HandlePreCommit(
		context.TODO(),
		txn,
		&api.PrecommitWriteCmd{
			//UserId:    ac.userId,
			//AccountId: ac.accountId,
			//RoleId:    ac.roleId,
			EntryList: entries,
		},
		nil,
	)
	assert.Nil(t, err)
	//t.FailNow()
	_, err = handle.HandleCommit(context.TODO(), txn)
	assert.Nil(t, err)
	entries = entries[:0]

	// set blockmaxrow as 10
	p := &catalog.LoopProcessor{}
	p.TableFn = func(te *catalog.TableEntry) error {
		schema := te.GetLastestSchemaLocked(false)
		if schema.Name == "tbtest" {
			schema.Extra.BlockMaxRows = 10
		}
		return nil
	}
	handle.db.Catalog.RecurLoop(p)
	txn = mock1PCTxn(handle.db)
	//append data into "tbtest" table
	insertEntry, err := makePBEntry(INSERT, dbTestID,
		tbTestID, dbName, schema.Name, "", moBats[2])
	assert.NoError(t, err)
	entries = append(entries, insertEntry)

	//add two non-appendable blocks from S3 into "tbtest" table
	attrs := []string{catalog2.ObjectMeta_ObjectStats}
	vecTypes := []types.Type{types.New(types.T_varchar, types.MaxVarcharLen, 0)}
	vecOpts := containers.Options{}
	vecOpts.Capacity = 0

	metaLocBat1 := containers.BuildBatch(attrs, vecTypes, vecOpts)
	metaLocBat1.Vecs[0].Append([]byte(stats1[:]), false)

	metaLocMoBat1 := containers.ToCNBatch(metaLocBat1)
	addS3BlkEntry1, err := makePBEntry(INSERT, dbTestID,
		tbTestID, dbName, schema.Name, objName1.String(), metaLocMoBat1)
	assert.NoError(t, err)

	entries = append(entries, addS3BlkEntry1)

	//add one non-appendable block from S3 into "tbtest" table
	metaLocBat2 := containers.BuildBatch(attrs, vecTypes, vecOpts)
	metaLocBat2.Vecs[0].Append([]byte(stats3[:]), false)

	metaLocMoBat2 := containers.ToCNBatch(metaLocBat2)
	addS3BlkEntry2, err := makePBEntry(INSERT, dbTestID,
		tbTestID, dbName, schema.Name, objName2.String(), metaLocMoBat2)
	assert.NoError(t, err)
	entries = append(entries, addS3BlkEntry2)

	err = handle.HandlePreCommit(
		context.TODO(),
		txn,
		&api.PrecommitWriteCmd{
			//UserId:    ac.userId,
			//AccountId: ac.accountId,
			//RoleId:    ac.roleId,
			EntryList: entries,
		},
		nil,
	)
	assert.Nil(t, err)
	//t.FailNow()
	_, err = handle.HandleCommit(context.TODO(), txn)
	assert.Nil(t, err)
	t.Log(handle.db.Catalog.SimplePPString(3))
	//check rows of "tbtest" which should has three blocks.
	txnR, err := handle.db.StartTxn(nil)
	assert.NoError(t, err)
	dbH, err := txnR.GetDatabase(dbName)
	assert.NoError(t, err)
	tbH, err := dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	hideDef, err := GetHideKeysOfTable(tbH)
	assert.NoError(t, err)

	rows := 0
	it := tbH.MakeObjectIt(false)
	for it.Next() {
		var cv *containers.Batch
		blk := it.GetObject()
		for j := 0; j < blk.BlkCnt(); j++ {
			err := blk.Scan(ctx, &cv, uint16(j), []int{schema.ColDefs[1].Idx}, common.DefaultAllocator)
			assert.NoError(t, err)
		}
		rows += cv.Length()
		cv.Close()
	}
	_ = it.Close()
	assert.Equal(t, taeBat.Length(), rows)

	var physicals []*containers.Batch
	it = tbH.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		for j := 0; j < blk.BlkCnt(); j++ {
			var bv *containers.Batch
			err := blk.GetMeta().(*catalog.ObjectEntry).GetObjectData().Scan(
				ctx, &bv, txnR, schema, uint16(j), []int{schema.GetColIdx(hideDef[0].Name), schema.GetPrimaryKey().Idx}, common.DefaultAllocator,
			)
			assert.NoError(t, err)
			physicals = append(physicals, bv)
		}
	}
	_ = it.Close()

	//read physical addr column
	assert.Equal(t, len(taeBats), len(physicals))
	err = txnR.Commit(context.Background())
	assert.Nil(t, err)

	//write deleted row ids into FS
	objName3 := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
	writer, err = blockio.NewBlockWriterNew(fs, objName3, 0, nil)
	assert.Nil(t, err)
	writer.SetDataType(objectio.SchemaTombstone)
	writer.SetPrimaryKeyWithType(uint16(catalog.TombstonePrimaryKeyIdx), index.HBF,
		index.ObjectPrefixFn,
		index.BlockPrefixFn)
	for _, view := range physicals {
		bat := batch.New(true, []string{hideDef[0].Name, schema.GetPrimaryKey().GetName()})
		bat.Vecs[0], _ = view.Vecs[0].GetDownstreamVector().Window(0, 5)
		bat.Vecs[1], _ = view.Vecs[1].GetDownstreamVector().Window(0, 5)
		_, err := writer.WriteBatch(bat)
		assert.Nil(t, err)
	}
	blocks, _, err = writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, len(physicals), len(blocks))

	stats := writer.GetObjectStats()
	require.False(t, stats.IsZero())

	//prepare delete locations.
	attrs = []string{catalog2.ObjectMeta_ObjectStats}
	vecTypes = []types.Type{types.New(types.T_varchar, types.MaxVarcharLen, 0)}

	vecOpts = containers.Options{}
	vecOpts.Capacity = 0
	delLocBat := containers.BuildBatch(attrs, vecTypes, vecOpts)
	delLocBat.Vecs[0].Append(stats.Marshal(), false)

	delLocMoBat := containers.ToCNBatch(delLocBat)
	var delApiEntries []*api.Entry
	deleteS3BlkEntry, err := makePBEntry(DELETE, dbTestID,
		tbTestID, dbName, schema.Name, objName3.String(), delLocMoBat)
	assert.NoError(t, err)
	delApiEntries = append(delApiEntries, deleteS3BlkEntry)

	txn = mock1PCTxn(handle.db)
	err = handle.HandlePreCommit(
		context.TODO(),
		txn,
		&api.PrecommitWriteCmd{
			//UserId:    ac.userId,
			//AccountId: ac.accountId,
			//RoleId:    ac.roleId,
			EntryList: delApiEntries,
		},
		nil,
	)
	assert.Nil(t, err)
	_, err = handle.HandleCommit(context.TODO(), txn)
	assert.Nil(t, err)
	//Now, the "tbtest" table has 20 rows left.
	txnR, err = handle.db.StartTxn(nil)
	assert.NoError(t, err)
	dbH, err = txnR.GetDatabase(dbName)
	assert.NoError(t, err)
	tbH, err = dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)

	rows = 0
	it = tbH.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		for j := 0; j < blk.BlkCnt(); j++ {
			var cv *containers.Batch
			err := blk.HybridScan(ctx, &cv, uint16(j), []int{schema.ColDefs[1].Idx}, common.DefaultAllocator)
			assert.NoError(t, err)
			defer cv.Close()
			cv.Compact()
			rows += cv.Length()
		}
	}
	assert.Equal(t, len(taeBats)*taeBats[0].Length()-5*len(taeBats), rows)
	err = txnR.Commit(context.Background())
	assert.Nil(t, err)
}

func TestHandle_HandlePreCommit1PC(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	handle := mockTAEHandle(ctx, t, opts)
	defer handle.HandleClose(context.TODO())
	IDAlloc := catalog.NewIDAllocator()
	schema := catalog.MockSchema(2, 1)
	schema.Name = "tbtest"
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
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
		IDAlloc.NextDB(),
		handle.m)
	assert.Nil(t, err)
	createDbTxn := mock1PCTxn(handle.db)
	err = handle.HandlePreCommit(
		context.TODO(),
		createDbTxn,
		&api.PrecommitWriteCmd{
			//UserId:    ac.userId,
			//AccountId: ac.accountId,
			//RoleId:    ac.roleId,
			EntryList: createDbEntries,
		},
		nil,
	)
	assert.Nil(t, err)
	_, err = handle.HandleCommit(context.TODO(), createDbTxn)
	assert.Nil(t, err)

	//start txn ,read "dbtest"'s ID
	txn, err := handle.db.StartTxn(nil)
	assert.Nil(t, err)
	names := txn.DatabaseNames()
	assert.Equal(t, 2, len(names))
	dbH, err := txn.GetDatabase(dbName)
	assert.Nil(t, err)
	dbTestId := dbH.GetID()
	err = txn.Commit(context.Background())
	assert.Nil(t, err)

	//create table from "dbtest"
	defs, err := catalog.SchemaToDefs(schema)
	for i := 0; i < len(defs); i++ {
		if attrdef, ok := defs[i].(*engine.AttributeDef); ok {
			attrdef.Attr.Default = &plan.Default{
				NullAbility: true,
				Expr: &plan.Expr{
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_Sval{
								Sval: "expr" + strconv.Itoa(i),
							},
						},
					},
				},
				OriginString: "expr" + strconv.Itoa(i),
			}
		}
	}
	assert.Nil(t, err)

	createTbTxn := mock1PCTxn(handle.db)

	createTbEntries, err := makeCreateTableEntries(
		"",
		ac,
		schema.Name,
		IDAlloc.NextTable(),
		dbTestId,
		dbName,
		schema.Constraint,
		handle.m,
		defs,
	)
	assert.Nil(t, err)

	createTbEntries1, err := makeCreateTableEntries(
		"",
		ac,
		"tbtest1",
		IDAlloc.NextTable(),
		dbTestId,
		dbName,
		schema.Constraint,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	createTbEntries = append(createTbEntries, createTbEntries1...)
	err = handle.HandlePreCommit(
		context.TODO(),
		createTbTxn,
		&api.PrecommitWriteCmd{
			//UserId:    ac.userId,
			//AccountId: ac.accountId,
			//RoleId:    ac.roleId,
			EntryList: createTbEntries,
		},
		nil)
	assert.Nil(t, err)
	_, err = handle.HandleCommit(context.TODO(), createTbTxn)
	assert.Nil(t, err)
	//start txn ,read table ID
	txn, err = handle.db.StartTxn(nil)
	assert.Nil(t, err)
	dbH, err = txn.GetDatabase(dbName)
	assert.NoError(t, err)
	dbId := dbH.GetID()
	assert.True(t, dbTestId == dbId)
	names, _ = TableNamesOfDB(dbH)
	assert.Equal(t, 2, len(names))
	tbH, err := dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	tbTestId := tbH.ID()
	rDefs, _ := TableDefs(tbH)
	//assert.Equal(t, 3, len(rDefs))
	rAttr := rDefs[1].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.NullAbility)
	rAttr = rDefs[2].(*engine.AttributeDef).Attr
	assert.Equal(t, "expr2", rAttr.Default.OriginString)

	err = txn.Commit(context.Background())
	assert.NoError(t, err)

	//DML: insert batch into table
	insertTxn := mock1PCTxn(handle.db)
	moBat := containers.ToCNBatch(catalog.MockBatch(schema, 100))
	insertEntry, err := makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, "", moBat)
	assert.NoError(t, err)
	err = handle.HandlePreCommit(
		context.TODO(),
		insertTxn,
		&api.PrecommitWriteCmd{
			//UserId:    ac.userId,
			//AccountId: ac.accountId,
			//RoleId:    ac.roleId,
			EntryList: []*api.Entry{insertEntry},
		},
		nil,
	)
	assert.NoError(t, err)
	// TODO:: Dml delete
	//bat := batch.NewWithSize(1)
	_, err = handle.HandleCommit(context.TODO(), insertTxn)
	assert.NoError(t, err)
	//TODO::DML:delete by primary key.
	// physcial addr + primary key
	//bat = batch.NewWithSize(2)

	//start txn ,read table ID
	txn, err = handle.db.StartTxn(nil)
	assert.NoError(t, err)
	dbH, err = txn.GetDatabase(dbName)
	assert.NoError(t, err)
	tbH, err = dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)

	it := tbH.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		for j := 0; j < blk.BlkCnt(); j++ {
			var cv *containers.Batch
			err := blk.Scan(ctx, &cv, uint16(j), []int{schema.ColDefs[1].Idx}, common.DefaultAllocator)
			assert.NoError(t, err)
			defer cv.Close()
			assert.Equal(t, 100, cv.Length())
		}
	}
	_ = it.Close()

	// read row ids
	hideCol, err := GetHideKeysOfTable(tbH)
	assert.NoError(t, err)

	hideColIdx := schema.GetColIdx(hideCol[0].Name)
	var cv *containers.Batch
	err = testutil.GetOneObject(tbH).Scan(ctx, &cv, 0, []int{hideColIdx, schema.GetPrimaryKey().Idx}, common.DefaultAllocator)
	assert.NoError(t, err)
	defer cv.Close()

	assert.NoError(t, txn.Commit(context.Background()))
	delBat := batch.New(true, []string{hideCol[0].Name, schema.GetPrimaryKey().GetName()})
	delBat.Vecs[0], _ = cv.Vecs[0].GetDownstreamVector().Window(0, 20)
	delBat.Vecs[1], _ = cv.Vecs[1].GetDownstreamVector().Window(0, 20)

	//delete 20 rows
	deleteTxn := mock1PCTxn(handle.db)
	deleteEntry, _ := makePBEntry(
		DELETE,
		dbId,
		tbTestId,
		dbName,
		schema.Name,
		"",
		delBat,
	)
	err = handle.HandlePreCommit(
		context.TODO(),
		deleteTxn,
		&api.PrecommitWriteCmd{
			//UserId:    ac.userId,
			//AccountId: ac.accountId,
			//RoleId:    ac.roleId,
			EntryList: append([]*api.Entry{}, deleteEntry),
		},
		nil,
	)
	assert.Nil(t, err)
	_, err = handle.HandleCommit(context.TODO(), deleteTxn)
	assert.Nil(t, err)
	//read, there should be 80 rows left.
	txn, err = handle.db.StartTxn(nil)
	assert.NoError(t, err)
	dbH, err = txn.GetDatabase(dbName)
	assert.NoError(t, err)
	tbH, err = dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)

	it = tbH.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		for j := 0; j < blk.BlkCnt(); j++ {
			var v *containers.Batch
			err := blk.HybridScan(ctx, &v, uint16(j), []int{schema.ColDefs[1].Idx}, common.DefaultAllocator)
			assert.NoError(t, err)
			defer v.Close()
			v.Compact()
			assert.Equal(t, 80, v.Length())
		}
	}
	it.Close()
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestHandle_HandlePreCommit2PCForCoordinator(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	handle := mockTAEHandle(ctx, t, opts)
	defer handle.HandleClose(context.TODO())
	IDAlloc := catalog.NewIDAllocator()
	schema := catalog.MockSchemaAll(2, -1)
	schema.Name = "tbtest"
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
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
		IDAlloc.NextDB(),
		handle.m)
	assert.Nil(t, err)
	txnCmds := []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: createDbEntries},
		},
		{typ: CmdPrepare},
		{typ: CmdCommitting},
		{typ: CmdCommit},
	}
	txnMeta := mock2PCTxn(handle.db)
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)

	//start 1pc txn ,read "dbtest"'s ID
	txn, err := handle.db.StartTxn(nil)
	assert.Nil(t, err)
	names := txn.DatabaseNames()
	assert.Equal(t, 2, len(names))
	dbH, err := txn.GetDatabase(dbName)
	assert.Nil(t, err)
	dbTestId := dbH.GetID()
	err = txn.Commit(ctx)
	assert.Nil(t, err)

	//create table from "dbtest"
	defs, err := catalog.SchemaToDefs(schema)
	defs[0].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: true,
		Expr: &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_Sval{
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
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_Sval{
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
		IDAlloc.NextTable(),
		dbTestId,
		dbName,
		schema.Constraint,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: createTbEntries},
		},
		{typ: CmdPrepare},
		{typ: CmdCommitting},
		{typ: CmdCommit},
	}
	txnMeta = mock2PCTxn(handle.db)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)

	//start 1pc txn ,read table ID
	txn, err = handle.db.StartTxn(nil)
	assert.Nil(t, err)
	dbH, err = txn.GetDatabase(dbName)
	assert.NoError(t, err)
	dbId := dbH.GetID()
	assert.True(t, dbTestId == dbId)
	names, _ = TableNamesOfDB(dbH)
	assert.Equal(t, 1, len(names))
	tbH, err := dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	tbTestId := tbH.ID()
	rDefs, _ := TableDefs(tbH)
	assert.Equal(t, 4, len(rDefs))
	rAttr := rDefs[0].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.NullAbility)
	rAttr = rDefs[1].(*engine.AttributeDef).Attr
	assert.Equal(t, "expr2", rAttr.Default.OriginString)
	err = txn.Commit(ctx)
	assert.NoError(t, err)

	//DML::insert batch into table
	moBat := containers.ToCNBatch(catalog.MockBatch(schema, 100))
	insertEntry, err := makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, "", moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdCommitting},
		{typ: CmdCommit},
	}
	insertTxn := mock2PCTxn(handle.db)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, insertTxn, txnCmds)
	assert.Nil(t, err)

	//start 2PC txn ,rollback it after prepared
	rollbackTxn := mock2PCTxn(handle.db)
	//insert 20 rows, then rollback the txn
	//FIXME::??
	//batch.SetLength(moBat, 20)
	moBat = containers.ToCNBatch(catalog.MockBatch(schema, 20))
	insertEntry, err = makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, "", moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdRollback},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, rollbackTxn, txnCmds)
	assert.Nil(t, err)

	//start 1PC txn , read table
	txn, err = handle.db.StartTxn(nil)
	assert.NoError(t, err)
	dbH, err = txn.GetDatabase(dbName)
	assert.NoError(t, err)
	tbH, err = dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)

	it := tbH.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		for j := 0; j < blk.BlkCnt(); j++ {
			var v *containers.Batch
			err := blk.Scan(ctx, &v, uint16(j), []int{schema.ColDefs[1].Idx}, common.DefaultAllocator)
			assert.NoError(t, err)
			defer v.Close()
			assert.Equal(t, 100, v.Length())
		}
	}
	_ = it.Close()

	// read row ids
	hideCol, err := GetHideKeysOfTable(tbH)
	assert.NoError(t, err)
	hideColIdx := schema.GetColIdx(hideCol[0].Name)
	var cv *containers.Batch
	err = testutil.GetOneObject(tbH).Scan(ctx, &cv, 0, []int{hideColIdx, schema.GetPrimaryKey().Idx}, common.DefaultAllocator)
	assert.NoError(t, err)
	defer cv.Close()

	delBat := batch.New(true, []string{hideCol[0].Name, schema.GetPrimaryKey().GetName()})
	delBat.Vecs[0] = cv.Vecs[0].GetDownstreamVector()
	delBat.Vecs[1] = cv.Vecs[1].GetDownstreamVector()

	assert.NoError(t, txn.Commit(ctx))

	hideBats := containers.SplitBatch(delBat, 5)
	//delete 20 rows by 2PC txn
	//batch.SetLength(hideBats[0], 20)
	deleteEntry, err := makePBEntry(
		DELETE,
		dbId,
		tbTestId,
		dbName,
		schema.Name,
		"",
		hideBats[0],
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: []*api.Entry{deleteEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdCommitting},
		{typ: CmdCommit},
	}
	deleteTxn := mock2PCTxn(handle.db)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, deleteTxn, txnCmds)
	assert.Nil(t, err)

	//start a 2PC txn ,rollback it after prepared.
	rollbackTxn = mock2PCTxn(handle.db)
	deleteEntry, _ = makePBEntry(
		DELETE,
		dbId,
		tbTestId,
		dbName,
		schema.Name,
		"",
		hideBats[1],
	)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: []*api.Entry{deleteEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdRollback},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, rollbackTxn, txnCmds)
	assert.Nil(t, err)

	//read, there should be 80 rows left.
	txn, err = handle.db.StartTxn(nil)
	assert.NoError(t, err)
	dbH, err = txn.GetDatabase(dbName)
	assert.NoError(t, err)
	tbH, err = dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)

	it = tbH.MakeObjectIt(false)
	for it.Next() {
		obj := it.GetObject()
		for j := 0; j < obj.BlkCnt(); j++ {
			var v *containers.Batch
			err := obj.HybridScan(ctx, &v, uint16(0), []int{schema.ColDefs[1].Idx}, common.DefaultAllocator)
			assert.NoError(t, err)
			defer v.Close()
			v.Compact()
			assert.Equal(t, 80, v.Length())
		}
	}
	_ = it.Close()
	assert.NoError(t, txn.Commit(ctx))
}

func TestHandle_HandlePreCommit2PCForParticipant(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	handle := mockTAEHandle(ctx, t, opts)
	defer handle.HandleClose(context.TODO())
	IDAlloc := catalog.NewIDAllocator()
	schema := catalog.MockSchemaAll(2, -1)
	schema.Name = "tbtest"
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
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
		IDAlloc.NextDB(),
		handle.m)
	assert.Nil(t, err)
	txnCmds := []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: createDbEntries},
		},
		{typ: CmdPrepare},
		{typ: CmdCommit},
	}
	txnMeta := mock2PCTxn(handle.db)
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)

	//start 1pc txn ,read "dbtest"'s ID
	txn, err := handle.db.StartTxn(nil)
	assert.Nil(t, err)
	names := txn.DatabaseNames()
	assert.Equal(t, 2, len(names))
	dbH, err := txn.GetDatabase(dbName)
	assert.Nil(t, err)
	dbTestId := dbH.GetID()
	err = txn.Commit(ctx)
	assert.Nil(t, err)

	//create table from "dbtest"
	defs, err := catalog.SchemaToDefs(schema)
	defs[0].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: true,
		Expr: &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_Sval{
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
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_Sval{
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
		IDAlloc.NextTable(),
		dbTestId,
		dbName,
		schema.Constraint,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: createTbEntries},
		},
		{typ: CmdPrepare},
		{typ: CmdCommit},
	}
	txnMeta = mock2PCTxn(handle.db)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)

	//start 1pc txn ,read table ID
	txn, err = handle.db.StartTxn(nil)
	assert.Nil(t, err)
	dbH, err = txn.GetDatabase(dbName)
	assert.NoError(t, err)
	dbId := dbH.GetID()
	assert.True(t, dbTestId == dbId)
	names, _ = TableNamesOfDB(dbH)
	assert.Equal(t, 1, len(names))
	tbH, err := dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	tbTestId := tbH.ID()
	rDefs, _ := TableDefs(tbH)
	assert.Equal(t, 4, len(rDefs))
	rAttr := rDefs[0].(*engine.AttributeDef).Attr
	assert.Equal(t, true, rAttr.Default.NullAbility)
	rAttr = rDefs[1].(*engine.AttributeDef).Attr
	assert.Equal(t, "expr2", rAttr.Default.OriginString)
	err = txn.Commit(ctx)
	assert.NoError(t, err)

	//DML::insert batch into table
	moBat := containers.ToCNBatch(catalog.MockBatch(schema, 100))
	insertEntry, err := makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, "", moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdCommit},
	}
	insertTxn := mock2PCTxn(handle.db)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, insertTxn, txnCmds)
	assert.Nil(t, err)

	//start 2PC txn ,rollback it after prepared
	rollbackTxn := mock2PCTxn(handle.db)
	//insert 20 rows ,then rollback
	//FIXME::??
	//batch.SetLength(moBat, 20)
	moBat = containers.ToCNBatch(catalog.MockBatch(schema, 20))
	insertEntry, err = makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, "", moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdRollback},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, rollbackTxn, txnCmds)
	assert.Nil(t, err)

	//start 2PC txn , rollback it when it is ACTIVE.
	rollbackTxn = mock2PCTxn(handle.db)
	//insert 10 rows ,then rollback
	//batch.SetLength(moBat, 10)
	moBat = containers.ToCNBatch(catalog.MockBatch(schema, 10))
	insertEntry, err = makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, "", moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdRollback},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, rollbackTxn, txnCmds)
	assert.Nil(t, err)

	//start 1PC txn , read table
	txn, err = handle.db.StartTxn(nil)
	assert.NoError(t, err)
	dbH, err = txn.GetDatabase(dbName)
	assert.NoError(t, err)
	tbH, err = dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	it := tbH.MakeObjectIt(false)
	for it.Next() {
		obj := it.GetObject()
		for j := 0; j < obj.BlkCnt(); j++ {
			var v *containers.Batch
			err := it.GetObject().Scan(ctx, &v, uint16(0), []int{schema.ColDefs[1].Idx}, common.DefaultAllocator)
			assert.NoError(t, err)
			defer v.Close()
			assert.Equal(t, 100, v.Length())
		}
	}
	_ = it.Close()

	hideCol, err := GetHideKeysOfTable(tbH)
	assert.NoError(t, err)
	hideColIdx := schema.GetColIdx(hideCol[0].Name)
	var v *containers.Batch
	err = testutil.GetOneObject(tbH).Scan(ctx, &v, 0, []int{hideColIdx, schema.GetPrimaryKey().Idx}, common.DefaultAllocator)
	assert.NoError(t, err)
	defer v.Close()

	_ = it.Close()
	delBat := batch.New(true, []string{hideCol[0].Name, schema.GetPrimaryKey().GetName()})
	delBat.Vecs[0] = v.Vecs[0].GetDownstreamVector()
	delBat.Vecs[1] = v.Vecs[1].GetDownstreamVector()

	assert.NoError(t, txn.Commit(ctx))

	hideBats := containers.SplitBatch(delBat, 5)
	//delete 20 rows by 2PC txn
	//batch.SetLength(delBat, 20)
	deleteEntry, err := makePBEntry(
		DELETE,
		dbId,
		tbTestId,
		dbName,
		schema.Name,
		"",
		hideBats[0],
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: []*api.Entry{deleteEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdCommitting},
		{typ: CmdCommit},
	}
	deleteTxn := mock2PCTxn(handle.db)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, deleteTxn, txnCmds)
	assert.Nil(t, err)

	//start a 2PC txn ,rollback it after prepared.
	// delete 20 rows ,then rollback
	rollbackTxn = mock2PCTxn(handle.db)
	deleteEntry, _ = makePBEntry(
		DELETE,
		dbId,
		tbTestId,
		dbName,
		schema.Name,
		"",
		hideBats[1],
	)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: []*api.Entry{deleteEntry}},
		},
		{typ: CmdPrepare},
		{typ: CmdRollback},
	}
	ctx = context.TODO()
	err = handle.handleCmds(ctx, rollbackTxn, txnCmds)
	assert.Nil(t, err)

	//read, there should be 80 rows left.
	txn, err = handle.db.StartTxn(nil)
	assert.NoError(t, err)
	dbH, err = txn.GetDatabase(dbName)
	assert.NoError(t, err)
	tbH, err = dbH.GetRelationByName(schema.Name)
	assert.NoError(t, err)

	it = tbH.MakeObjectIt(false)
	for it.Next() {
		obj := it.GetObject()
		for j := 0; j < obj.BlkCnt(); j++ {
			var v *containers.Batch
			err := obj.HybridScan(ctx, &v, uint16(j), []int{schema.ColDefs[1].Idx}, common.DefaultAllocator)
			assert.NoError(t, err)
			defer v.Close()
			v.Compact()
			assert.Equal(t, 80, v.Length())
		}
	}
	_ = it.Close()

	assert.NoError(t, txn.Commit(ctx))
}

func TestHandle_MVCCVisibility(t *testing.T) {
	t.Skip("debug later")
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	opts := config.WithLongScanAndCKPOpts(nil)
	handle := mockTAEHandle(ctx, t, opts)
	defer handle.HandleClose(context.TODO())
	IDAlloc := catalog.NewIDAllocator()
	schema := catalog.MockSchemaAll(2, -1)
	schema.Name = "tbtest"
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
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
		IDAlloc.NextDB(),
		handle.m)
	assert.Nil(t, err)
	txnCmds := []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: createDbEntries},
		},
	}
	txnMeta := mock2PCTxn(handle.db)
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)
	var dbTestId uint64
	var dbNames []string
	wg := new(sync.WaitGroup)
	wg.Add(1)
	//start a db reader.
	go func() {
		//start 1pc txn ,read "dbtest"'s ID
		txn, err := handle.db.StartTxn(nil)
		assert.Nil(t, err)
		dbNames = txn.DatabaseNames()
		err = txn.Commit(ctx)
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
		txn, err := handle.db.StartTxn(nil)
		assert.Nil(t, err)
		//reader should wait until the writer committed.
		dbNames = txn.DatabaseNames()
		assert.Equal(t, 2, len(dbNames))
		dbH, err := txn.GetDatabase(dbName)
		assert.Nil(t, err)
		dbTestId = dbH.GetID()
		err = txn.Commit(ctx)
		assert.Nil(t, err)
		//wg.Done()
		//To check whether reader had waited.
		assert.True(t, time.Since(startTime) > 1*time.Second)
		wg.Done()

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
	defs, err := catalog.SchemaToDefs(schema)
	defs[0].(*engine.AttributeDef).Attr.Default = &plan.Default{
		NullAbility: true,
		Expr: &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_Sval{
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
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: false,
					Value: &plan.Literal_Sval{
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
		IDAlloc.NextTable(),
		dbTestId,
		dbName,
		schema.Constraint,
		handle.m,
		defs,
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: createTbEntries},
		},
		{typ: CmdPrepare},
	}
	txnMeta = mock2PCTxn(handle.db)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, txnMeta, txnCmds)
	assert.Nil(t, err)
	var tbTestId uint64
	startTime = time.Now()
	wg.Add(1)
	go func() {
		//start 1pc txn ,read table ID
		txn, err := handle.db.StartTxn(nil)
		assert.Nil(t, err)
		dbH, err := txn.GetDatabase(dbName)
		assert.NoError(t, err)
		dbId := dbH.GetID()
		assert.True(t, dbTestId == dbId)
		//txn should wait here.
		names, _ := TableNamesOfDB(dbH)
		assert.Equal(t, 1, len(names))
		tbH, err := dbH.GetRelationByName(schema.Name)
		assert.NoError(t, err)
		tbTestId = tbH.ID()
		rDefs, _ := TableDefs(tbH)
		assert.Equal(t, 4, len(rDefs))
		rAttr := rDefs[0].(*engine.AttributeDef).Attr
		assert.Equal(t, true, rAttr.Default.NullAbility)
		rAttr = rDefs[1].(*engine.AttributeDef).Attr
		assert.Equal(t, "expr2", rAttr.Default.OriginString)
		err = txn.Commit(ctx)
		assert.NoError(t, err)
		//wg.Done()
		//To check whether reader had waited.
		assert.True(t, time.Since(startTime) > 1*time.Second)
		wg.Done()
	}()
	time.Sleep(1 * time.Second)
	err = handle.handleCmds(ctx, txnMeta, []txnCommand{
		{typ: CmdCommitting}, {typ: CmdCommit},
	})
	assert.Nil(t, err)
	wg.Wait()

	//DML::insert batch into table
	moBat := containers.ToCNBatch(catalog.MockBatch(schema, 100))
	insertEntry, err := makePBEntry(INSERT, dbTestId,
		tbTestId, dbName, schema.Name, "", moBat)
	assert.NoError(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
				//UserId:    ac.userId,
				//AccountId: ac.accountId,
				//RoleId:    ac.roleId,
				EntryList: []*api.Entry{insertEntry}},
		},
		{typ: CmdPrepare},
	}
	insertTxn := mock2PCTxn(handle.db)
	ctx = context.TODO()
	err = handle.handleCmds(ctx, insertTxn, txnCmds)
	assert.Nil(t, err)
	startTime = time.Now()
	wg.Add(1)
	go func() {
		//start 1PC txn , read table
		txn, err := handle.db.StartTxn(nil)
		assert.NoError(t, err)
		dbH, err := txn.GetDatabase(dbName)
		assert.NoError(t, err)
		tbH, err := dbH.GetRelationByName(schema.Name)
		assert.NoError(t, err)

		it := tbH.MakeObjectIt(false)
		for it.Next() {
			obj := it.GetObject()
			for j := 0; j < obj.BlkCnt(); j++ {
				var v *containers.Batch
				err := obj.Scan(ctx, &v, 0, []int{schema.ColDefs[1].Idx}, common.DefaultAllocator)
				assert.NoError(t, err)
				defer v.Close()
				assert.Equal(t, 100, v.Length())
			}
		}
		_ = it.Close()
		txn.Commit(ctx)
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
	var delBat *batch.Batch
	{
		txn, err := handle.db.StartTxn(nil)
		assert.NoError(t, err)
		dbH, err := txn.GetDatabase(dbName)
		assert.NoError(t, err)
		tbH, err := dbH.GetRelationByName(schema.Name)
		assert.NoError(t, err)
		hideCol, err := GetHideKeysOfTable(tbH)
		assert.NoError(t, err)

		hideColIdx := schema.GetColIdx(hideCol[0].Name)
		var v *containers.Batch
		err = testutil.GetOneObject(tbH).Scan(ctx, &v, 0, []int{hideColIdx, schema.GetPrimaryKey().Idx}, common.DefaultAllocator)
		assert.NoError(t, err)
		defer v.Close()

		delBat = batch.New(true, []string{hideCol[0].Name, schema.GetPrimaryKey().GetName()})
		delBat.Vecs[0] = v.Vecs[0].GetDownstreamVector()
		delBat.Vecs[1] = v.Vecs[1].GetDownstreamVector()

		assert.NoError(t, txn.Commit(ctx))
	}

	hideBats := containers.SplitBatch(delBat, 5)
	//delete 20 rows by 2PC txn
	deleteTxn := mock2PCTxn(handle.db)
	//batch.SetLength(delBat, 20)
	deleteEntry, err := makePBEntry(
		DELETE,
		dbTestId,
		tbTestId,
		dbName,
		schema.Name,
		"",
		hideBats[0],
	)
	assert.Nil(t, err)
	txnCmds = []txnCommand{
		{
			typ: CmdPreCommitWrite,
			cmd: api.PrecommitWriteCmd{
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
		txn, err := handle.db.StartTxn(nil)
		assert.NoError(t, err)
		dbH, err := txn.GetDatabase(dbName)
		assert.NoError(t, err)
		tbH, err := dbH.GetRelationByName(schema.Name)
		assert.NoError(t, err)

		it := tbH.MakeObjectIt(false)
		for it.Next() {
			obj := it.GetObject()
			for j := 0; j < obj.BlkCnt(); j++ {
				var v *containers.Batch
				err := obj.HybridScan(ctx, &v, uint16(0), []int{schema.ColDefs[1].Idx}, common.DefaultAllocator)
				assert.NoError(t, err)
				defer v.Close()
				v.Compact()
				assert.Equal(t, 80, v.Length())
			}
		}
		_ = it.Close()

		assert.NoError(t, txn.Commit(ctx))
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

func TestApplyDeltaloc(t *testing.T) {
	defer testutils.AfterTest(t)()
	opts := config.WithLongScanAndCKPOpts(nil)
	ctx := context.Background()

	h := mockTAEHandle(ctx, t, opts)
	defer h.HandleClose(context.TODO())
	defer opts.Fs.Close()

	schema := catalog.MockSchema(2, 1)
	schema.Name = "tbtest"
	schema.Extra.BlockMaxRows = 5
	schema.Extra.ObjectMaxBlocks = 2
	//5 objs, one obj contains 2 blocks, one block contains 10 rows.
	rowCount := 100 * 2 * 5
	taeBat := catalog.MockBatch(schema, rowCount)
	defer taeBat.Close()
	// taeBats := taeBat.Split(rowCount)

	// create relation
	txn0, err := h.db.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn0.CreateDatabase("db", "create", "typ")
	assert.NoError(t, err)
	_, err = db.CreateRelation(schema)
	assert.NoError(t, err)
	assert.NoError(t, txn0.Commit(context.Background()))

	// append
	txn0, err = h.db.StartTxn(nil)
	assert.NoError(t, err)
	db, err = txn0.GetDatabase("db")
	dbID := db.GetID()
	assert.NoError(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	tid := rel.GetMeta().(*catalog.TableEntry).GetID()
	assert.NoError(t, rel.Append(context.Background(), taeBat))
	assert.NoError(t, txn0.Commit(context.Background()))

	// compact
	txn0, err = h.db.StartTxn(nil)
	assert.NoError(t, err)
	db, err = txn0.GetDatabase("db")
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	var metas []*catalog.ObjectEntry
	it := rel.MakeObjectIt(false)
	for it.Next() {
		blk := it.GetObject()
		meta := blk.GetMeta().(*catalog.ObjectEntry)
		metas = append(metas, meta)
	}
	assert.NoError(t, txn0.Commit(context.Background()))

	txn0, err = h.db.StartTxn(nil)
	assert.NoError(t, err)
	task, err := jobs.NewFlushTableTailTask(nil, txn0, metas, nil, h.db.Runtime)
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, txn0.Commit(context.Background()))

	txn0, err = h.db.StartTxn(nil)
	assert.NoError(t, err)
	db, err = txn0.GetDatabase("db")
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	inMemoryDeleteTxns := make([]*txn.TxnMeta, 0)
	makeDeleteTxnFn := func(val any) {
		filter := handle.NewEQFilter(val)
		id, offset, err := rel.GetByFilter(context.Background(), filter)
		assert.NoError(t, err)
		rowIDVec := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
		rowIDVec.Append(*objectio.NewRowid(&id.BlockID, offset), false)
		pkVec := containers.MakeVector(schema.GetPrimaryKey().GetType(), common.DefaultAllocator)
		pkVec.Append(val, false)
		bat := containers.NewBatch()
		bat.AddVector(catalog.AttrRowID, rowIDVec)
		bat.AddVector(schema.GetPrimaryKey().GetName(), pkVec)
		insertEntry, err := makePBEntry(DELETE, dbID, tid, "db", schema.Name, "", containers.ToCNBatch(bat))
		assert.NoError(t, err)

		txn := mock1PCTxn(h.db)
		err = h.HandlePreCommit(context.Background(), txn, &api.PrecommitWriteCmd{EntryList: []*api.Entry{insertEntry}}, nil)
		assert.NoError(t, err)
		inMemoryDeleteTxns = append(inMemoryDeleteTxns, txn)
	}
	assert.NoError(t, txn0.Commit(context.Background()))

	pkVec := containers.MakeVector(schema.GetPrimaryKey().Type, common.DebugAllocator)
	defer pkVec.Close()
	rowIDVec := containers.MakeVector(types.T_Rowid.ToType(), common.DebugAllocator)
	defer rowIDVec.Close()
	for i := 0; i < rowCount; i++ {
		val := taeBat.Vecs[schema.GetSingleSortKeyIdx()].Get(i)
		if i%5 == 0 {
			// try apply deltaloc
			filter := handle.NewEQFilter(val)
			id, offset, err := rel.GetByFilter(context.Background(), filter)
			assert.NoError(t, err)
			rowid := types.NewRowIDWithObjectIDBlkNumAndRowID(*id.ObjectID(), id.BlockID.Sequence(), offset)
			pkVec.Append(val, false)
			rowIDVec.Append(rowid, false)
		} else {
			// in memory deletes
			makeDeleteTxnFn(val)
		}
	}

	// make txn for apply deltaloc

	txn0, err = h.db.StartTxn(nil)
	assert.NoError(t, err)
	db, err = txn0.GetDatabase("db")
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	attrs := []string{catalog2.ObjectMeta_ObjectStats}
	vecTypes := []types.Type{types.New(types.T_varchar, types.MaxVarcharLen, 0)}

	vecOpts := containers.Options{}
	vecOpts.Capacity = 0
	delLocBat := containers.BuildBatch(attrs, vecTypes, vecOpts)
	stats, err := testutil.MockCNDeleteInS3(h.db.Runtime.Fs, rowIDVec, pkVec, schema, txn0)
	assert.NoError(t, err)
	require.False(t, stats.IsZero())
	delLocBat.Vecs[0].Append(stats.Marshal(), false)
	deleteS3Entry, err := makePBEntry(DELETE, dbID, tid, "db", schema.Name, "file", containers.ToCNBatch(delLocBat))
	assert.NoError(t, err)
	deleteS3Txn := mock1PCTxn(h.db)
	err = h.HandlePreCommit(
		context.Background(),
		deleteS3Txn, &api.PrecommitWriteCmd{
			EntryList: []*api.Entry{deleteS3Entry}}, nil)
	assert.NoError(t, err)
	assert.NoError(t, txn0.Commit(context.Background()))

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		_, err := h.HandleCommit(context.Background(), deleteS3Txn)
		assert.NoError(t, err)
	})

	commitMemoryFn := func(txnMeta *txn.TxnMeta) func() {
		return func() {
			defer wg.Done()
			_, err := h.HandleCommit(context.Background(), txnMeta)
			assert.NoError(t, err)
		}
	}
	for _, deleteTxn := range inMemoryDeleteTxns {
		wg.Add(1)
		pool.Submit(commitMemoryFn(deleteTxn))
	}
	wg.Wait()

	txn0, err = h.db.StartTxn(nil)
	assert.NoError(t, err)
	db, err = txn0.GetDatabase("db")
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	it = rel.MakeObjectIt(false)
	for _, def := range schema.ColDefs {
		length := 0
		for it.Next() {
			blk := it.GetObject()
			meta := blk.GetMeta().(*catalog.ObjectEntry)
			for j := 0; j < blk.BlkCnt(); j++ {
				var view *containers.Batch
				blkID := objectio.NewBlockidWithObjectID(meta.ID(), uint16(j))
				err := tables.HybridScanByBlock(ctx, meta.GetTable(), txn0, &view, schema, []int{def.Idx}, blkID, common.DefaultAllocator)
				assert.NoError(t, err)
				view.Compact()
				length += view.Length()
			}
		}
		assert.Equal(t, 0, length)
	}
	assert.NoError(t, txn0.Commit(context.Background()))
}
