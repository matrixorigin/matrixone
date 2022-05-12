// Copyright 2021 Matrix Origin
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

package txnimpl

import (
	"testing"
)

func TestBlockUpdate1(t *testing.T) {
	// dir := initTestPath(t)
	// c, mgr, driver := initTestContext(t, dir)
	// defer driver.Close()
	// defer mgr.Stop()
	// defer c.Close()

	// blkCnt := 100
	// chains := make([]*updates.BlockUpdateChain, 0, blkCnt)

	// schema := catalog.MockSchema(1)
	// {
	// 	txn := mgr.StartTxn(nil)
	// 	db, _ := txn.CreateDatabase("db")
	// 	rel, _ := db.CreateRelation(schema)
	// 	rel.CreateSegment()
	// 	err := txn.Commit()
	// 	assert.Nil(t, err)
	// 	t.Log(c.SimplePPString(common.PPL1))
	// }

	// {
	// 	txn := mgr.StartTxn(nil)
	// 	db, _ := txn.GetDatabase("db")
	// 	rel, _ := db.GetRelationByName(schema.Name)
	// 	it := rel.MakeSegmentIt()
	// 	seg := it.GetSegment()
	// 	for i := 0; i < blkCnt; i++ {
	// 		blk, err := seg.CreateBlock()
	// 		assert.Nil(t, err)
	// 		chain := updates.NewUpdateChain(nil, blk.GetMeta().(*catalog.BlockEntry))
	// 		chains = append(chains, chain)
	// 	}
	// 	err := txn.Commit()
	// 	assert.Nil(t, err)
	// 	t.Log(c.SimplePPString(common.PPL1))
	// }
}
