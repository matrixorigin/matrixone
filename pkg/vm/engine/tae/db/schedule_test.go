package db

import (
	"testing"
	"time"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

type mockIOTask struct {
	*tasks.BaseTask
	scope    *common.ID
	duration time.Duration
}

func newMockIOTask(ctx *tasks.Context, scope *common.ID, duration time.Duration) *mockIOTask {
	task := &mockIOTask{
		scope:    scope,
		duration: duration,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *mockIOTask) Scope() *common.ID { return task.scope }

func (task *mockIOTask) Execute() error {
	time.Sleep(task.duration)
	return nil
}

func TestIOSchedule1(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()
	pendings := make([]tasks.Task, 0)
	now := time.Now()
	for i := 0; i < 5; i++ {
		ctx := tasks.Context{Waitable: true}
		scope := &common.ID{TableID: 1}
		task := newMockIOTask(&ctx, scope, time.Millisecond*5)
		db.Scheduler.Schedule(task)
		pendings = append(pendings, task)
	}
	for _, task := range pendings {
		task.WaitDone()
	}
	duration := time.Since(now)
	assert.True(t, duration > time.Millisecond*5*5)
	t.Log(time.Since(now))
	pendings = pendings[:0]
	now = time.Now()
	for i := 0; i < 5; i++ {
		ctx := tasks.Context{Waitable: true}
		scope := &common.ID{TableID: uint64(i)}
		task := newMockIOTask(&ctx, scope, time.Millisecond*5)
		db.Scheduler.Schedule(task)
		pendings = append(pendings, task)
	}
	for _, task := range pendings {
		task.WaitDone()
	}
	duration = time.Since(now)
	assert.True(t, duration < time.Millisecond*5*2)
	t.Log(time.Since(now))
}

func TestCheckpoint1(t *testing.T) {
	opts := new(options.Options)
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScannerInterval = 10
	opts.CheckpointCfg.ExecutionLevels = 2
	opts.CheckpointCfg.ExecutionInterval = 1
	db := initDB(t, opts)
	defer db.Close()
	schema := catalog.MockSchema(13)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		err := blk.Update(1, 3, int32(333))
		assert.Nil(t, err)
		err = blk.RangeDelete(3, 3)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	time.Sleep(time.Millisecond * 20)
	{
		blockCnt := 0
		blockFn := func(entry *catalog.BlockEntry) error {
			blockCnt++
			return nil
		}
		processor := new(catalog.LoopProcessor)
		processor.BlockFn = blockFn
		db.Opts.Catalog.RecurLoop(processor)
		assert.Equal(t, 2, blockCnt)
	}
}

func TestCheckpoint2(t *testing.T) {
	opts := new(options.Options)
	opts.CacheCfg = new(options.CacheCfg)
	opts.CacheCfg.IndexCapacity = 1000000
	opts.CacheCfg.TxnCapacity = 1000000
	opts.CacheCfg.InsertCapacity = 200
	// opts.CheckpointCfg = new(options.CheckpointCfg)
	// opts.CheckpointCfg.ScannerInterval = 10
	// opts.CheckpointCfg.ExecutionLevels = 2
	// opts.CheckpointCfg.ExecutionInterval = 1
	tae := initDB(t, opts)
	defer tae.Close()
	schema1 := catalog.MockSchema(4)
	schema1.BlockMaxRows = 10
	schema1.SegmentMaxBlocks = 2
	schema1.PrimaryKey = 2
	schema2 := catalog.MockSchema(4)
	schema2.BlockMaxRows = 10
	schema2.SegmentMaxBlocks = 2
	schema2.PrimaryKey = 2
	bat := compute.MockBatch(schema1.Types(), uint64(schema1.BlockMaxRows)*2, int(schema1.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 10)
	var (
		meta1 *catalog.TableEntry
		meta2 *catalog.TableEntry
	)
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		rel1, _ := db.CreateRelation(schema1)
		rel2, _ := db.CreateRelation(schema2)
		meta1 = rel1.GetMeta().(*catalog.TableEntry)
		meta2 = rel2.GetMeta().(*catalog.TableEntry)
		t.Log(meta1.String())
		t.Log(meta2.String())
		assert.Nil(t, txn.Commit())
	}
	doAppend := func(data *gbat.Batch, name string) {
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(name)
		err := rel.Append(data)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	for i, data := range bats[0:8] {
		var name string
		if i%2 == 0 {
			name = schema1.Name
		} else {
			name = schema2.Name
		}
		doAppend(data, name)
	}
	var meta *catalog.BlockEntry
	testutils.WaitExpect(1000, func() bool {
		return tae.Wal.GetPenddingCnt() == 4
	})
	assert.Equal(t, uint64(4), tae.Wal.GetPenddingCnt())
	t.Log(tae.Wal.GetPenddingCnt())
	doAppend(bats[8], schema1.Name)
	// tae.Catalog.Checkpoint(tae.TxnMgr.StatSafeTS())
	// t.Log(tae.MTBufMgr.String())
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema1.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		meta = blk.GetMeta().(*catalog.BlockEntry)
		assert.Equal(t, 10, blk.Rows())
		task, err := jobs.NewCompactBlockTask(tasks.WaitableCtx, txn, meta, tae.Scheduler)
		assert.Nil(t, err)
		tae.Scheduler.Schedule(task)
		err = task.WaitDone()
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	// testutils.WaitExpect(1000, func() bool {
	// 	return tae.Wal.GetPenddingCnt() == 1
	// })
	t.Log(tae.Wal.GetPenddingCnt())
	meta.GetBlockData().Destroy()
	task, err := tae.Scheduler.ScheduleScopedFn(tasks.WaitableCtx, tasks.CheckpointCatalogTask, nil, tae.Catalog.CheckpointClosure(tae.TxnMgr.StatSafeTS()))
	assert.Nil(t, err)
	err = task.WaitDone()
	assert.Nil(t, err)
	testutils.WaitExpect(1000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})
	t.Log(tae.Wal.GetPenddingCnt())
	assert.Equal(t, uint64(0), tae.Wal.GetPenddingCnt())
}

func TestSchedule1(t *testing.T) {
	db := initDB(t, nil)
	schema := catalog.MockSchema(13)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		blkMeta := blk.GetMeta().(*catalog.BlockEntry)
		factory := jobs.CompactBlockTaskFactory(blkMeta, db.Scheduler)
		ctx := tasks.Context{Waitable: true}
		task, err := db.Scheduler.ScheduleTxnTask(&ctx, factory)
		assert.Nil(t, err)
		err = task.WaitDone()
		assert.Nil(t, err)
	}
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	db.Close()
}
