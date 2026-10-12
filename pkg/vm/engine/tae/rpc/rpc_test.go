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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestAutoIncrEpochFenceIsModeIndependent(t *testing.T) {
	defer testutils.AfterTest(t)()
	for _, mode := range []txnpb.TxnMode{txnpb.TxnMode_Optimistic, txnpb.TxnMode_Pessimistic} {
		t.Run(mode.String(), func(t *testing.T) {
			ctx := context.Background()
			h := mockTAEHandle(ctx, t, config.WithLongScanAndCKPOpts(nil))
			defer h.HandleClose(ctx)

			schema := catalog.MockSchemaAll(3, 1)
			schema.Name = "mode_fence"
			_, createdRel := testutil.CreateRelation(t, h.db, testutil.DefaultTestDB, schema, true)
			tableID := createdRel.ID()
			databaseID := createdRel.GetMeta().(*catalog.TableEntry).GetDB().ID

			alterTxn, alterRel := testutil.GetDefaultRelation(t, h.db, schema.Name)
			require.NoError(t, alterRel.AlterTable(ctx, api.NewUpdateAutoIncrementReq(0, tableID, 10, 1)))
			require.NoError(t, alterTxn.Commit(ctx))

			insertBatch := catalog.MockBatch(schema, 1)
			defer insertBatch.Close()
			entry, err := makePBEntry(INSERT, databaseID, tableID, testutil.DefaultTestDB,
				schema.Name, "", containers.ToCNBatch(insertBatch))
			require.NoError(t, err)
			entry.AutoIncrEpoch = 0
			entry.AutoIncrEpochKnown = true
			payload, err := (&api.PrecommitWriteCmd{EntryList: []*api.Entry{entry}}).MarshalBinary()
			require.NoError(t, err)
			commitReq := &txnpb.TxnCommitRequest{Payload: []*txnpb.TxnRequest{{
				CNRequest: &txnpb.CNOpRequest{OpCode: uint32(api.OpCode_OpPreCommit), Payload: payload},
			}}}
			meta := mock1PCTxn(h.db)
			meta.Mode = mode

			_, err = h.HandleCommit(ctx, meta, nil, commitReq)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), err)

			// Legacy writers are accepted while the table is at epoch zero, but
			// fail closed after the first allocator reset.
			entry.AutoIncrEpochKnown = false
			payload, err = (&api.PrecommitWriteCmd{EntryList: []*api.Entry{entry}}).MarshalBinary()
			require.NoError(t, err)
			commitReq.Payload[0].CNRequest.Payload = payload
			meta = mock1PCTxn(h.db)
			meta.Mode = mode
			_, err = h.HandleCommit(ctx, meta, nil, commitReq)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), err)
		})
	}
}

func TestHandleCommitStaleTableGenerationRequestsDefinitionRetry(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	h := mockTAEHandle(ctx, t, config.WithLongScanAndCKPOpts(nil))
	defer h.HandleClose(ctx)

	schema := catalog.MockSchemaAll(3, 1)
	schema.Name = "stale_generation"
	_, oldRel := testutil.CreateRelation(t, h.db, testutil.DefaultTestDB, schema, true)
	oldTableID := oldRel.ID()
	databaseID := oldRel.GetMeta().(*catalog.TableEntry).GetDB().ID

	// Replace the physical table while preserving its logical name, as copy-based
	// ALTER TABLE does. The write below deliberately represents a plan compiled
	// against the previous generation.
	replacementSchema := schema.Clone()
	replacementSchema.Name = "stale_generation_replacement"
	replaceTxn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	replaceDB, err := replaceTxn.GetDatabase(testutil.DefaultTestDB)
	require.NoError(t, err)
	replacement, err := replaceDB.CreateRelation(replacementSchema)
	require.NoError(t, err)
	_, err = replaceDB.DropRelationByID(oldTableID)
	require.NoError(t, err)
	require.NoError(t, replacement.AlterTable(ctx,
		api.NewRenameTableReq(0, 0, replacementSchema.Name, schema.Name)))
	require.NoError(t, replaceTxn.Commit(ctx))
	require.NotEqual(t, oldTableID, replacement.ID())

	insertBatch := catalog.MockBatch(schema, 1)
	defer insertBatch.Close()
	insertEntry, err := makePBEntry(INSERT, databaseID, oldTableID,
		testutil.DefaultTestDB, schema.Name, "", containers.ToCNBatch(insertBatch))
	require.NoError(t, err)

	softDeleteBatch := batch.NewWithSize(1)
	softDeleteBatch.SetAttributes([]string{"object_id"})
	softDeleteBatch.Vecs[0] = vector.NewVec(types.T_binary.ToType())
	objectID := types.NewObjectid()
	require.NoError(t, vector.AppendBytes(softDeleteBatch.Vecs[0], objectID[:], false, h.m))
	softDeleteBatch.SetRowCount(1)
	defer softDeleteBatch.Clean(h.m)
	softDeleteEntry, err := makePBEntry(DELETE, databaseID, oldTableID,
		testutil.DefaultTestDB, schema.Name, softDeleteObjectPrefix+"false", softDeleteBatch)
	require.NoError(t, err)

	for _, entryCase := range []struct {
		name  string
		entry *api.Entry
	}{
		{name: "insert", entry: insertEntry},
		{name: "soft-delete-object", entry: softDeleteEntry},
	} {
		t.Run(entryCase.name, func(t *testing.T) {
			payload, err := (&api.PrecommitWriteCmd{EntryList: []*api.Entry{entryCase.entry}}).MarshalBinary()
			require.NoError(t, err)
			commitReq := &txnpb.TxnCommitRequest{Payload: []*txnpb.TxnRequest{{
				CNRequest: &txnpb.CNOpRequest{
					OpCode:  uint32(api.OpCode_OpPreCommit),
					Payload: payload,
				},
			}}}

			for _, mode := range []txnpb.TxnMode{txnpb.TxnMode_Optimistic, txnpb.TxnMode_Pessimistic} {
				t.Run(mode.String(), func(t *testing.T) {
					meta := mock1PCTxn(h.db)
					meta.Mode = mode
					_, commitErr := h.HandleCommit(ctx, meta, nil, commitReq)
					require.True(t,
						moerr.IsMoErrCode(commitErr, moerr.ErrTxnNeedRetryWithDefChanged),
						commitErr)
				})
			}
		})
	}
}

func TestHandleSoftDeleteObjectMarksCNProvenance(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	h := mockTAEHandle(ctx, t, config.WithLongScanAndCKPOpts(nil))
	defer h.HandleClose(ctx)

	schema := catalog.MockSchemaAll(1, 0)
	schema.Name = "cn_soft_delete"
	_, createdRel := testutil.CreateRelation(
		t, h.db, testutil.DefaultTestDB, schema, true,
	)
	tableEntry := createdRel.GetMeta().(*catalog.TableEntry)
	databaseID := tableEntry.GetDB().ID
	tableID := tableEntry.ID

	sourceID := objectio.NewObjectid()
	createTxn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	createDB, err := createTxn.GetDatabaseByID(databaseID)
	require.NoError(t, err)
	createRel, err := createDB.GetRelationByID(tableID)
	require.NoError(t, err)
	obj, err := createRel.CreateNonAppendableObject(true, &objectio.CreateObjOpt{
		Stats:       objectio.NewObjectStatsWithObjectID(&sourceID, false, true, true),
		IsTombstone: true,
	})
	require.NoError(t, err)
	require.NoError(t, obj.Close())
	require.NoError(t, createTxn.Commit(ctx))

	deleteTxn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	require.NoError(t, h.HandleSoftDeleteObject(ctx, deleteTxn, &cmd_util.WriteReq{
		DatabaseId:   databaseID,
		TableID:      tableID,
		DatabaseName: testutil.DefaultTestDB,
		TableName:    schema.Name,
		ObjectID:     &sourceID,
		IsTombstone:  true,
	}))
	require.NoError(t, deleteTxn.Commit(ctx))

	readTxn, err := h.db.StartTxn(nil)
	require.NoError(t, err)
	readDB, err := readTxn.GetDatabaseByID(databaseID)
	require.NoError(t, err)
	readRel, err := readDB.GetRelationByID(tableID)
	require.NoError(t, err)
	it := readRel.GetMeta().(*catalog.TableEntry).MakeTombstoneObjectIt()
	marked := false
	for ok := it.Last(); ok; ok = it.Prev() {
		entry := it.Item()
		if entry.IsDEntry() && *entry.ID() == sourceID {
			marked = entry.ObjectStats.GetCNDeleted()
			break
		}
	}
	it.Release()
	require.True(t, marked)
	require.NoError(t, readTxn.Commit(ctx))
}

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
		noid := objectio.NewObjectid()
		name := objectio.BuildObjectNameWithObjectID(&noid)
		objNames = append(objNames, name)
		writer, err := ioutil.NewBlockWriterNew(fs, objNames[i], 0, nil, false)
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
	txn := mock1PCTxn(handle.db)
	dbTestID := IDAlloc.NextDB()
	createDbEntries, err := makeCreateDatabaseEntries(
		"",
		ac,
		dbName,
		dbTestID,
		handle.m)
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
	entries := make([]*api.Entry, 0, len(createDbEntries)+len(createTbEntries)+len(objNames))
	entries = append(entries, createDbEntries...)
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
	// Create TxnCommitRequest with entries
	precommitWriteCmd := &api.PrecommitWriteCmd{
		EntryList: entries,
	}
	payload, err := precommitWriteCmd.MarshalBinary()
	assert.Nil(t, err)

	commitReq := &txnpb.TxnCommitRequest{
		Payload: []*txnpb.TxnRequest{
			{
				CNRequest: &txnpb.CNOpRequest{
					OpCode:  uint32(api.OpCode_OpPreCommit),
					Payload: payload,
				},
			},
		},
	}

	start := time.Now()
	_, err = handle.HandleCommit(context.TODO(), txn, nil, commitReq)
	assert.Nil(t, err)
	t.Logf("Commit 10w blocks spend: %d", time.Since(start).Microseconds())
}

func TestApplyDeltaloc(t *testing.T) {
	defer testutils.AfterTest(t)()
	opts := config.WithLongScanAndCKPOpts(nil)
	ctx := context.Background()

	h := mockTAEHandle(ctx, t, opts)
	defer h.HandleClose(context.TODO())
	defer opts.Fs.Close(ctx)

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
	inMemoryDeleteTxns := make([]*txnpb.TxnMeta, 0)
	inMemoryDeleteReqs := make([]*txnpb.TxnCommitRequest, 0)
	makeDeleteTxnFn := func(val any) {
		filter := handle.NewEQFilter(val)
		id, offset, err := rel.GetByFilter(context.Background(), filter)
		assert.NoError(t, err)
		rowIDVec := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
		rowIDVec.Append(objectio.NewRowid(&id.BlockID, offset), false)
		pkVec := containers.MakeVector(schema.GetPrimaryKey().GetType(), common.DefaultAllocator)
		pkVec.Append(val, false)
		bat := containers.NewBatch()
		bat.AddVector(objectio.TombstoneAttr_Rowid_Attr, rowIDVec)
		bat.AddVector(schema.GetPrimaryKey().GetName(), pkVec)
		insertEntry, err := makePBEntry(DELETE, dbID, tid, "db", schema.Name, "", containers.ToCNBatch(bat))
		assert.NoError(t, err)

		txn := mock1PCTxn(h.db)
		precommitWriteCmd := &api.PrecommitWriteCmd{EntryList: []*api.Entry{insertEntry}}
		payload, err := precommitWriteCmd.MarshalBinary()
		assert.NoError(t, err)
		commitReq := &txnpb.TxnCommitRequest{
			Payload: []*txnpb.TxnRequest{
				{
					CNRequest: &txnpb.CNOpRequest{
						OpCode:  uint32(api.OpCode_OpPreCommit),
						Payload: payload,
					},
				},
			},
		}
		// Store txn and commitReq for later commit
		inMemoryDeleteTxns = append(inMemoryDeleteTxns, txn)
		inMemoryDeleteReqs = append(inMemoryDeleteReqs, commitReq)
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
	precommitWriteCmd := &api.PrecommitWriteCmd{EntryList: []*api.Entry{deleteS3Entry}}
	payload, err := precommitWriteCmd.MarshalBinary()
	assert.NoError(t, err)
	deleteS3CommitReq := &txnpb.TxnCommitRequest{
		Payload: []*txnpb.TxnRequest{
			{
				CNRequest: &txnpb.CNOpRequest{
					OpCode:  uint32(api.OpCode_OpPreCommit),
					Payload: payload,
				},
			},
		},
	}
	// Don't commit here, will commit in goroutine below
	assert.NoError(t, txn0.Commit(context.Background()))

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		_, err := h.HandleCommit(context.Background(), deleteS3Txn, nil, deleteS3CommitReq)
		assert.NoError(t, err)
	})

	commitMemoryFn := func(idx int) func() {
		return func() {
			defer wg.Done()
			_, err := h.HandleCommit(context.Background(), inMemoryDeleteTxns[idx], nil, inMemoryDeleteReqs[idx])
			assert.NoError(t, err)
		}
	}
	for i := range inMemoryDeleteTxns {
		wg.Add(1)
		pool.Submit(commitMemoryFn(i))
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
				err := tables.HybridScanByBlock(ctx, meta.GetTable(), txn0, &view, schema, []int{def.Idx}, &blkID, common.DefaultAllocator)
				assert.NoError(t, err)
				view.Compact()
				length += view.Length()
			}
		}
		assert.Equal(t, 0, length)
	}
	assert.NoError(t, txn0.Commit(context.Background()))
}
