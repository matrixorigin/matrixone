package db

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestReplayCatalog1(t *testing.T) {
	tae := initDB(t, nil)
	schemas := make([]*catalog.Schema, 4)
	for i := range schemas {
		schemas[i] = catalog.MockSchema(2)
	}

	txn := tae.StartTxn(nil)
	_, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	createTable := func(schema *catalog.Schema, wg *sync.WaitGroup, forceCkp bool) func() {
		return func() {
			defer wg.Done()
			txn := tae.StartTxn(nil)
			db, err := txn.GetDatabase("db")
			assert.Nil(t, err)
			_, err = db.CreateRelation(schema)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
			txn = tae.StartTxn(nil)
			db, err = txn.GetDatabase("db")
			assert.Nil(t, err)
			rel, err := db.GetRelationByName(schema.Name)
			assert.Nil(t, err)
			segCnt := rand.Intn(5) + 1
			for i := 0; i < segCnt; i++ {
				seg, err := rel.CreateSegment()
				assert.Nil(t, err)
				blkCnt := rand.Intn(5) + 1
				for j := 0; j < blkCnt; j++ {
					_, err = seg.CreateBlock()
					assert.Nil(t, err)
				}
			}
			assert.Nil(t, txn.Commit())
			if forceCkp || rand.Intn(100) > 80 {
				err := tae.Catalog.Checkpoint(tae.Scheduler.GetSafeTS())
				assert.Nil(t, err)
			}
		}
	}

	var wg sync.WaitGroup
	pool, err := ants.NewPool(1)
	assert.Nil(t, err)
	for i, schema := range schemas {
		wg.Add(1)
		ckp := false
		if i == len(schemas)/2 {
			ckp = true
		}
		err := pool.Submit(createTable(schema, &wg, ckp))
		assert.Nil(t, err)
	}
	wg.Wait()
	logutil.Info(tae.Catalog.SimplePPString(common.PPL1))
	t.Logf("GetPenddingLSNCnt: %d", tae.Scheduler.GetPenddingLSNCnt())
	t.Logf("GetCheckpointed: %d", tae.Scheduler.GetCheckpointedLSN())
	// ckpTs := tae.Catalog.GetCheckpointed().MaxTS
	// ckpEntry := tae.Catalog.PrepareCheckpoint(0, ckpTs)
	tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	defer tae2.Close()

	c := tae2.Catalog
	defer c.Close()

	logutil.Info(c.SimplePPString(common.PPL1))
	t.Logf("GetCatalogCheckpointed: %v", tae.Catalog.GetCheckpointed())
	t.Logf("GetCatalogCheckpointed2: %v", c.GetCheckpointed())
	assert.Equal(t, tae.Catalog.GetCheckpointed(), c.GetCheckpointed())
}

func TestReplayCatalog2(t *testing.T) {
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2)
	schema2 := catalog.MockSchema(2)
	txn := tae.StartTxn(nil)
	_, err := txn.CreateDatabase("db2")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	seg, err := rel.CreateSegment()
	assert.Nil(t, err)
	blk1, err := seg.CreateBlock()
	assert.Nil(t, err)
	blk1Meta := blk1.GetMeta().(*catalog.BlockEntry)
	_, err = seg.CreateBlock()
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema2)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	_, err = txn.DropDatabase("db2")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	_, err = db.DropRelationByName(schema2.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	seg, err = rel.GetSegment(blk1Meta.GetSegment().ID)
	assert.Nil(t, err)
	err = seg.SoftDeleteBlock(blk1Meta.ID)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	ts := txn.GetCommitTS()

	txn = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	seg, err = rel.CreateSegment()
	assert.Nil(t, err)
	_, err = seg.CreateBlock()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	err = tae.Catalog.Checkpoint(ts)
	assert.Nil(t, err)
	err = tae.Catalog.Checkpoint(tae.Scheduler.GetSafeTS())
	assert.Nil(t, err)
	tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	defer tae2.Close()

	c := tae2.Catalog
	defer c.Close()

	t.Log(c.SimplePPString(common.PPL1))
	t.Logf("GetCatalogCheckpointed: %v", tae.Catalog.GetCheckpointed())
	t.Logf("GetCatalogCheckpointed2: %v", c.GetCheckpointed())
	assert.Equal(t, tae.Catalog.GetCheckpointed(), c.GetCheckpointed())
}

func TestReplayCatalog3(t *testing.T) {
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2)
	schema2 := catalog.MockSchema(2)
	txn := tae.StartTxn(nil)
	_, err := txn.CreateDatabase("db2")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	seg, err := rel.CreateSegment()
	assert.Nil(t, err)
	blk1, err := seg.CreateBlock()
	assert.Nil(t, err)
	blk1Meta := blk1.GetMeta().(*catalog.BlockEntry)
	_, err = seg.CreateBlock()
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema2)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	_, err = txn.DropDatabase("db2")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	_, err = db.DropRelationByName(schema2.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	seg, err = rel.GetSegment(blk1Meta.GetSegment().ID)
	assert.Nil(t, err)
	err = seg.SoftDeleteBlock(blk1Meta.ID)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	seg, err = rel.CreateSegment()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	err = rel.SoftDeleteSegment(seg.GetID())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	defer tae2.Close()

	c := tae2.Catalog
	defer c.Close()

	t.Log(c.SimplePPString(common.PPL1))
	t.Logf("GetCatalogCheckpointed: %v", tae.Catalog.GetCheckpointed())
	t.Logf("GetCatalogCheckpointed2: %v", c.GetCheckpointed())
	assert.Equal(t, tae.Catalog.GetCheckpointed(), c.GetCheckpointed())
}

func TestReplayCatalog4(t *testing.T) {
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	txn := tae.StartTxn(nil)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	seg, err := rel.CreateSegment()
	assert.Nil(t, err)
	_, err = seg.CreateBlock()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	logutil.Infof("%d,%d", txn.GetStartTS(), txn.GetCommitTS())

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	c := tae2.Catalog
	t.Log(c.SimplePPString(common.PPL1))

	bat := compute.MockBatch(schema.Types(), 10000, int(schema.PrimaryKey), nil)
	// bats := compute.SplitBatch(bat, 2)
	txn = tae2.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	rel.Append(bat)
	assert.Nil(t, txn.Commit())

	t.Log(c.SimplePPString(common.PPL1))
	c.Close()
	tae2.Close()

	tae3, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	c3 := tae3.Catalog
	t.Log(c3.SimplePPString(common.PPL1))
}
