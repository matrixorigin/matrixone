package db

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
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
		db.IOScheduler.Schedule(task)
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
		db.IOScheduler.Schedule(task)
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
	opts.CheckpointCfg.CalibrationInterval = 10
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
		factory := jobs.CompactBlockTaskFactory(blkMeta)
		ctx := tasks.Context{Waitable: true}
		task, err := db.TaskScheduler.ScheduleTxnTask(&ctx, factory)
		assert.Nil(t, err)
		err = task.WaitDone()
		assert.Nil(t, err)
	}
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	db.Close()
}
