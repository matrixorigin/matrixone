package txnimpl

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/updates"
	"github.com/stretchr/testify/assert"
)

func TestBlockUpdate1(t *testing.T) {
	dir := initTestPath(t)
	c, mgr, driver := initTestContext(t, dir)
	defer driver.Close()
	defer mgr.Stop()
	defer c.Close()

	blkCnt := 100
	chains := make([]*updates.BlockUpdateChain, 0, blkCnt)

	schema := catalog.MockSchema(1)
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		rel, _ := db.CreateRelation(schema)
		rel.CreateSegment()
		err := txn.Commit()
		assert.Nil(t, err)
		t.Log(c.SimplePPString(common.PPL1))
	}

	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		it := rel.MakeSegmentIt()
		seg := it.GetSegment()
		for i := 0; i < blkCnt; i++ {
			blk, err := seg.CreateBlock()
			assert.Nil(t, err)
			chain := updates.NewUpdateChain(nil, blk.GetMeta().(*catalog.BlockEntry))
			chains = append(chains, chain)
		}
		err := txn.Commit()
		assert.Nil(t, err)
		t.Log(c.SimplePPString(common.PPL1))
	}
}
