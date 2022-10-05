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

package updates

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAEUPDATES"
)

func mockTxn() *txnbase.Txn {
	txn := new(txnbase.Txn)
	txn.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(), types.NextGlobalTsForTest(), nil)
	return txn
}

func commitTxn(txn *txnbase.Txn) {
	txn.CommitTS = types.NextGlobalTsForTest()
}

func TestColumnChain1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	schema := catalog.MockSchema(1, 0)
	dir := testutils.InitTestEnv(ModuleName, t)
	c := catalog.MockCatalog(dir, "mock", nil, nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)
	controller := NewMVCCHandle(blk)

	// uncommitted := new(common.Link)
	chain := NewColumnChain(nil, 0, controller)
	cnt1 := 2
	cnt2 := 3
	cnt3 := 4
	cnt4 := 5
	for i := 0; i < cnt1+cnt2+cnt3+cnt4; i++ {
		txn := new(txnbase.Txn)
		txn.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(),
			types.NextGlobalTsForTest(), nil)
		n := chain.AddNode(txn)
		if (i >= cnt1 && i < cnt1+cnt2) || (i >= cnt1+cnt2+cnt3) {
			txn.CommitTS = types.NextGlobalTsForTest()
			_ = n.PrepareCommit()
			_ = n.ApplyCommit(nil)
		}
	}
	t.Log(chain.StringLocked())
	assert.Equal(t, cnt1+cnt2+cnt3+cnt4, chain.DepthLocked())
	t.Log(chain.GetHead().GetPayload().StringLocked())
}

func TestColumnChain2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	schema := catalog.MockSchema(1, 0)
	dir := testutils.InitTestEnv(ModuleName, t)
	c := catalog.MockCatalog(dir, "mock", nil, nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)

	controller := NewMVCCHandle(blk)
	chain := NewColumnChain(nil, 0, controller)
	txn1 := new(txnbase.Txn)
	txn1.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(), types.NextGlobalTsForTest(), nil)
	n1 := chain.AddNode(txn1)

	err := chain.TryUpdateNodeLocked(1, int32(11), n1)
	assert.Nil(t, err)
	err = chain.TryUpdateNodeLocked(2, int32(22), n1)
	assert.Nil(t, err)
	err = chain.TryUpdateNodeLocked(3, int32(33), n1)
	assert.Nil(t, err)
	err = chain.TryUpdateNodeLocked(1, int32(111), n1)
	assert.Nil(t, err)
	assert.Equal(t, 3, chain.view.RowCnt())

	txn2 := new(txnbase.Txn)
	txn2.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(),
		types.NextGlobalTsForTest(), nil)
	n2 := chain.AddNode(txn2)
	err = chain.TryUpdateNodeLocked(2, int32(222), n2)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
	err = chain.TryUpdateNodeLocked(4, int32(44), n2)
	assert.Nil(t, err)
	assert.Equal(t, 4, chain.view.RowCnt())

	txn1.CommitTS = types.NextGlobalTsForTest()
	_ = n1.PrepareCommit()
	_ = n1.ApplyCommit(nil)

	err = chain.TryUpdateNodeLocked(2, int32(222), n2)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))

	assert.Equal(t, 1, chain.view.links[1].Depth())
	assert.Equal(t, 1, chain.view.links[2].Depth())
	assert.Equal(t, 1, chain.view.links[3].Depth())
	assert.Equal(t, 1, chain.view.links[4].Depth())

	txn3 := new(txnbase.Txn)
	txn3.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(),
		types.NextGlobalTsForTest(), nil)
	n3 := chain.AddNode(txn3)
	err = chain.TryUpdateNodeLocked(2, int32(2222), n3)
	assert.Nil(t, err)
	assert.Equal(t, 2, chain.view.links[2].Depth())

	var wg sync.WaitGroup
	doUpdate := func(i int) func() {
		return func() {
			defer wg.Done()
			txn := new(txnbase.Txn)
			txn.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(),
				types.NextGlobalTsForTest(), nil)
			n := chain.AddNode(txn)
			for j := 0; j < 4; j++ {
				n.GetChain().Lock()
				err := chain.TryUpdateNodeLocked(uint32(i*2000+j), int32(i*20000+j), n)
				assert.Nil(t, err)
				n.GetChain().Unlock()
			}
		}
	}

	p, _ := ants.NewPool(10)
	now := time.Now()
	for i := 1; i < 30; i++ {
		wg.Add(1)
		_ = p.Submit(doUpdate(i))
	}
	wg.Wait()
	t.Log(time.Since(now))

	t.Log(chain.Depth())

	v, err := chain.view.GetValue(1, txn1.GetStartTS())
	assert.Equal(t, int32(111), v)
	assert.Nil(t, err)
	v, err = chain.view.GetValue(2, txn1.GetStartTS())
	assert.Equal(t, int32(22), v)
	assert.Nil(t, err)
	_, err = chain.view.GetValue(2, txn2.GetStartTS())
	assert.NotNil(t, err)
	v, err = chain.view.GetValue(2, txn3.GetStartTS())
	assert.Equal(t, int32(2222), v)
	assert.Nil(t, err)
	v, err = chain.view.GetValue(2, types.NextGlobalTsForTest())
	assert.Equal(t, int32(22), v)
	assert.Nil(t, err)
	_, err = chain.view.GetValue(2000, types.NextGlobalTsForTest())
	assert.NotNil(t, err)

	mask, vals, err := chain.view.CollectUpdates(txn1.GetStartTS())
	assert.NoError(t, err)
	assert.True(t, mask.Contains(1))
	assert.True(t, mask.Contains(2))
	assert.True(t, mask.Contains(3))
	assert.Equal(t, int32(111), vals[uint32(1)])
	assert.Equal(t, int32(22), vals[uint32(2)])
	assert.Equal(t, int32(33), vals[uint32(3)])
	t.Log(mask.String())
	t.Log(vals)

	// t.Log(chain.view.StringLocked())
	// t.Log(chain.StringLocked())
	// t.Log(chain.GetHead().GetPayload().(*ColumnUpdateNode).StringLocked())
}

func TestColumnChain3(t *testing.T) {
	testutils.EnsureNoLeak(t)
	ncnt := 100
	schema := catalog.MockSchema(1, 0)
	dir := testutils.InitTestEnv(ModuleName, t)
	c := catalog.MockCatalog(dir, "mock", nil, nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)

	controller := NewMVCCHandle(blk)
	chain := NewColumnChain(nil, 0, controller)

	start := time.Now()
	ecnt := 10
	for i := 0; i < ncnt; i++ {
		txn := mockTxn()
		node := chain.AddNode(txn)
		for j := i * ecnt; j < i*ecnt+ecnt; j++ {
			_ = chain.TryUpdateNodeLocked(uint32(j), int32(j), node)
		}
		commitTxn(txn)
		_ = node.PrepareCommit()
		_ = node.ApplyCommit(nil)
	}
	t.Log(time.Since(start))
	start = time.Now()
	t.Log(time.Since(start))
	// t.Log(chain.StringLocked())
	assert.Equal(t, ncnt, chain.DepthLocked())

	node := chain.GetHead().GetPayload()
	cmd, err := node.MakeCommand(1)
	assert.Nil(t, err)
	defer cmd.Close()

	var w bytes.Buffer
	_, err = cmd.WriteTo(&w)
	assert.Nil(t, err)
	buf := w.Bytes()
	r := bytes.NewBuffer(buf)

	cmd2, _, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	defer cmd2.Close()
	updateCmd := cmd2.(*UpdateCmd)
	assert.Equal(t, txnbase.CmdUpdate, updateCmd.GetType())
	assert.Equal(t, *node.id, *updateCmd.update.id)
	assert.True(t, node.mask.Equals(updateCmd.update.mask))
	// t.Log(updateCmd.update.StringLocked())
	// assert.Equal(t, node.vals, updateCmd.update.vals)
	// t.Log(updateCmd.update.id.BlockString())
	// t.Log(updateCmd.update.mask.String())
}

func TestColumnChain4(t *testing.T) {
	testutils.EnsureNoLeak(t)
	schema := catalog.MockSchema(1, 0)
	dir := testutils.InitTestEnv(ModuleName, t)
	c := catalog.MockCatalog(dir, "mock", nil, nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)

	controller := NewMVCCHandle(blk)
	chain := NewColumnChain(nil, 0, controller)
	var ts1 types.TS
	var ts2 types.TS
	var ts3 types.TS
	{
		txn := mockTxn()
		node := chain.AddNode(txn)
		_ = chain.TryUpdateNodeLocked(uint32(5), int32(5), node)
	}
	{
		txn := mockTxn()
		node := chain.AddNode(txn)
		_ = chain.TryUpdateNodeLocked(uint32(10), int32(10), node)
		commitTxn(txn)
		_ = node.PrepareCommit()
		_ = node.ApplyCommit(nil)
		ts1 = txn.GetCommitTS()
	}
	{
		txn := mockTxn()
		node := chain.AddNode(txn)
		_ = chain.TryUpdateNodeLocked(uint32(20), int32(20), node)
		commitTxn(txn)
		_ = node.PrepareCommit()
		_ = node.ApplyCommit(nil)
		ts2 = txn.GetCommitTS()
	}
	{
		txn := mockTxn()
		node := chain.AddNode(txn)
		_ = chain.TryUpdateNodeLocked(uint32(30), int32(30), node)
	}
	{
		txn := mockTxn()
		node := chain.AddNode(txn)
		_ = chain.TryUpdateNodeLocked(uint32(40), int32(40), node)
		commitTxn(txn)
		_ = node.PrepareCommit()
		_ = node.ApplyCommit(nil)
		ts3 = txn.GetCommitTS()
	}
	var startTs types.TS
	mask, vals, _, err := chain.CollectCommittedInRangeLocked(startTs, types.NextGlobalTsForTest())
	assert.NoError(t, err)
	assert.True(t, mask.Contains(10))
	assert.True(t, mask.Contains(20))
	assert.True(t, mask.Contains(40))
	assert.Equal(t, uint64(3), mask.GetCardinality())
	assert.Equal(t, int32(10), vals[10])
	assert.Equal(t, int32(20), vals[20])
	assert.Equal(t, int32(40), vals[40])

	mask, vals, _, err = chain.CollectCommittedInRangeLocked(ts1, types.NextGlobalTsForTest())
	assert.NoError(t, err)
	assert.True(t, mask.Contains(10))
	assert.True(t, mask.Contains(20))
	assert.True(t, mask.Contains(40))
	assert.Equal(t, uint64(3), mask.GetCardinality())
	assert.Equal(t, int32(10), vals[10])
	assert.Equal(t, int32(20), vals[20])
	assert.Equal(t, int32(40), vals[40])

	mask, vals, _, err = chain.CollectCommittedInRangeLocked(ts2, types.NextGlobalTsForTest())
	assert.NoError(t, err)
	assert.True(t, mask.Contains(20))
	assert.True(t, mask.Contains(40))
	assert.Equal(t, uint64(2), mask.GetCardinality())
	assert.Equal(t, int32(20), vals[20])
	assert.Equal(t, int32(40), vals[40])

	mask, vals, _, err = chain.CollectCommittedInRangeLocked(ts3, types.NextGlobalTsForTest())
	assert.NoError(t, err)
	assert.True(t, mask.Contains(40))
	assert.Equal(t, uint64(1), mask.GetCardinality())
	assert.Equal(t, int32(40), vals[40])

	mask, _, _, err = chain.CollectCommittedInRangeLocked(ts3.Next(), types.NextGlobalTsForTest())
	assert.NoError(t, err)
	assert.Nil(t, mask)

	mask, vals, _, err = chain.CollectCommittedInRangeLocked(ts1, ts3)
	assert.NoError(t, err)
	assert.True(t, mask.Contains(10))
	assert.True(t, mask.Contains(20))
	assert.Equal(t, uint64(2), mask.GetCardinality())
	assert.Equal(t, int32(10), vals[10])
	assert.Equal(t, int32(20), vals[20])

	t.Log(chain.StringLocked())
}

func TestDeleteChain1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	schema := catalog.MockSchema(1, 0)
	dir := testutils.InitTestEnv(ModuleName, t)
	c := catalog.MockCatalog(dir, "mock", nil, nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)

	controller := NewMVCCHandle(blk)
	chain := NewDeleteChain(nil, controller)
	txn1 := new(txnbase.Txn)
	txn1.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(), types.NextGlobalTsForTest(), nil)
	n1 := chain.AddNodeLocked(txn1, handle.DeleteType(handle.DT_Normal)).(*DeleteNode)
	assert.Equal(t, 1, chain.Depth())

	// 1. Txn1 delete from 1 to 10 -- PASS
	err := chain.PrepareRangeDelete(1, 10, txn1.GetStartTS())
	assert.Nil(t, err)
	n1.RangeDeleteLocked(1, 10)
	assert.Equal(t, uint32(10), n1.GetCardinalityLocked())
	t.Log(n1.mask.String())

	// 2. Txn1 delete 10 -- FAIL
	err = chain.PrepareRangeDelete(10, 10, txn1.GetStartTS())
	assert.NotNil(t, err)

	txn2 := mockTxn()
	// 3. Txn2 delete 2 -- FAIL
	err = chain.PrepareRangeDelete(2, 2, txn2.GetStartTS())
	assert.NotNil(t, err)

	// 4. Txn2 delete from 21 to 30 -- PASS
	err = chain.PrepareRangeDelete(20, 30, txn2.GetStartTS())
	assert.Nil(t, err)
	n2 := chain.AddNodeLocked(txn2, handle.DeleteType(handle.DT_Normal)).(*DeleteNode)
	n2.RangeDeleteLocked(20, 30)
	assert.Equal(t, uint32(11), n2.GetCardinalityLocked())
	t.Log(n2.mask.String())

	merged := chain.AddMergeNode().(*DeleteNode)
	assert.Nil(t, merged)
	assert.Equal(t, 2, chain.Depth())

	collected, err := chain.CollectDeletesLocked(txn1.GetStartTS(), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint32(10), collected.GetCardinalityLocked())
	collected, err = chain.CollectDeletesLocked(txn2.GetStartTS(), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint32(11), collected.GetCardinalityLocked())

	var startTs1 types.TS
	collected, err = chain.CollectDeletesLocked(startTs1, false, nil)
	assert.NoError(t, err)
	assert.Nil(t, collected)
	collected, err = chain.CollectDeletesLocked(types.NextGlobalTsForTest(), false, nil)
	assert.NoError(t, err)
	assert.Nil(t, collected)

	commitTxn(txn1)
	assert.Nil(t, n1.PrepareCommit())
	assert.Nil(t, n1.ApplyCommit(nil))
	t.Log(chain.StringLocked())

	var startTs2 types.TS
	collected, err = chain.CollectDeletesLocked(startTs2, false, nil)
	assert.NoError(t, err)
	assert.Nil(t, collected)
	collected, err = chain.CollectDeletesLocked(types.NextGlobalTsForTest(), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint32(10), collected.GetCardinalityLocked())
	collected, err = chain.CollectDeletesLocked(txn2.GetStartTS(), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint32(11), collected.GetCardinalityLocked())

	txn3 := mockTxn()
	err = chain.PrepareRangeDelete(5, 5, txn3.GetStartTS())
	assert.NotNil(t, err)
	err = chain.PrepareRangeDelete(31, 33, txn3.GetStartTS())
	assert.Nil(t, err)
	n3 := chain.AddNodeLocked(txn3, handle.DeleteType(handle.DT_Normal))
	n3.RangeDeleteLocked(31, 33)

	collected, err = chain.CollectDeletesLocked(txn3.GetStartTS(), false, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint32(13), collected.GetCardinalityLocked())
	t.Log(chain.StringLocked())

	merged = chain.AddMergeNode().(*DeleteNode)
	assert.NotNil(t, merged)
	t.Log(chain.StringLocked())
	assert.Equal(t, 4, chain.DepthLocked())

	cmd, err := merged.MakeCommand(1)
	assert.Nil(t, err)
	assert.NotNil(t, cmd)

	var w bytes.Buffer
	_, err = cmd.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)

	cmd2, _, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	assert.Equal(t, txnbase.CmdDelete, cmd2.GetType())
	assert.Equal(t, txnbase.CmdDelete, cmd2.(*UpdateCmd).cmdType)
	assert.True(t, cmd2.(*UpdateCmd).delete.mask.Equals(merged.mask))
}

func TestDeleteChain2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	controller := NewMVCCHandle(nil)
	chain := NewDeleteChain(nil, controller)

	txn1 := mockTxn()
	n1 := chain.AddNodeLocked(txn1, handle.DeleteType(handle.DT_Normal)).(*DeleteNode)
	err := chain.PrepareRangeDelete(1, 4, txn1.GetStartTS())
	assert.Nil(t, err)
	n1.RangeDeleteLocked(1, 4)
	commitTxn(txn1)
	err = n1.PrepareCommit()
	assert.Nil(t, err)
	err = n1.ApplyCommit(nil)
	assert.Nil(t, err)
	t.Log(chain.StringLocked())

	txn2 := mockTxn()
	n2 := chain.AddNodeLocked(txn2, handle.DeleteType(handle.DT_Normal)).(*DeleteNode)
	err = chain.PrepareRangeDelete(5, 8, txn2.GetStartTS())
	assert.Nil(t, err)
	n2.RangeDeleteLocked(5, 8)
	t.Log(chain.StringLocked())

	txn3 := mockTxn()
	n3 := chain.AddNodeLocked(txn3, handle.DeleteType(handle.DT_Normal)).(*DeleteNode)
	err = chain.PrepareRangeDelete(9, 12, txn3.GetStartTS())
	assert.Nil(t, err)
	n3.RangeDeleteLocked(9, 12)
	commitTxn(txn3)
	err = n3.PrepareCommit()
	assert.Nil(t, err)
	err = n3.ApplyCommit(nil)
	assert.Nil(t, err)
	t.Log(chain.StringLocked())

	m, err := chain.CollectDeletesLocked(types.NextGlobalTsForTest(), false, nil)
	assert.NoError(t, err)
	mask := m.(*DeleteNode).mask
	assert.Equal(t, uint64(8), mask.GetCardinality())
	m, err = chain.CollectDeletesLocked(txn3.GetCommitTS(), false, nil)
	assert.NoError(t, err)
	mask = m.(*DeleteNode).mask
	assert.Equal(t, uint64(8), mask.GetCardinality())
	m, err = chain.CollectDeletesLocked(txn1.GetCommitTS(), false, nil)
	assert.NoError(t, err)
	mask = m.(*DeleteNode).mask
	assert.Equal(t, uint64(4), mask.GetCardinality())
	m, err = chain.CollectDeletesLocked(txn1.GetCommitTS().Prev(), false, nil)
	assert.NoError(t, err)
	assert.Nil(t, m)

	var startTs1 types.TS
	mask, _, err = chain.CollectDeletesInRange(startTs1, txn3.GetCommitTS(), nil)
	assert.NoError(t, err)
	t.Log(mask.String())
	assert.Equal(t, uint64(8), mask.GetCardinality())

	var startTs2 types.TS
	mask, _, err = chain.CollectDeletesInRange(startTs2, txn3.GetCommitTS().Next(), nil)
	assert.NoError(t, err)
	t.Log(mask.String())
	assert.Equal(t, uint64(8), mask.GetCardinality())

	mask, _, err = chain.CollectDeletesInRange(txn1.GetCommitTS(), txn3.GetCommitTS().Next(), nil)
	assert.NoError(t, err)
	t.Log(mask.String())
	assert.Equal(t, uint64(4), mask.GetCardinality())
}
