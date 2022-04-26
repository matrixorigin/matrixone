package db

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/stretchr/testify/assert"
)

func TestSchedule1(t *testing.T) {
	db := initDB(t, nil)
	schema := catalog.MockSchema(13)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn, _ := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		blkMeta := blk.GetMeta().(*catalog.BlockEntry)
		factory := tables.CompactBlockTaskFactory(blkMeta)
		ctx := tasks.Context{Waitable: true}
		task, err := db.TaskScheduler.ScheduleTxnTask(&ctx, factory)
		assert.Nil(t, err)
		err = task.WaitDone()
		assert.Nil(t, err)
	}
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	db.Close()
}
