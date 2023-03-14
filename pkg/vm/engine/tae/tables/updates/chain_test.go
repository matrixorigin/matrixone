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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAEUPDATES"
)

func mockTxn() *txnbase.Txn {
	txn := new(txnbase.Txn)
	txn.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(), types.NextGlobalTsForTest(), types.TS{})
	return txn
}

func commitTxn(txn *txnbase.Txn) {
	txn.CommitTS = types.NextGlobalTsForTest()
}

func TestDeleteChain1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)

	controller := NewMVCCHandle(blk)
	chain := NewDeleteChain(nil, controller)
	txn1 := new(txnbase.Txn)
	txn1.TxnCtx = txnbase.NewTxnCtx(common.NewTxnIDAllocator().Alloc(), types.NextGlobalTsForTest(), types.TS{})
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
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	controller := NewMVCCHandle(catalog.NewStandaloneBlock(nil, 0, types.TS{}))
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
