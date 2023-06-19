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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

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

func commitTxn(txn *txnbase.Txn) {
	txn.CommitTS = types.NextGlobalTsForTest()
}

func TestDeleteChain1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog()
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil, nil)

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

	collected, err := chain.CollectDeletesLocked(txn1, nil)
	assert.NoError(t, err)
	assert.Equal(t, 10, collected.GetCardinality())
	collected, err = chain.CollectDeletesLocked(txn2, nil)
	assert.NoError(t, err)
	assert.Equal(t, 11, collected.GetCardinality())

	var startTs1 types.TS
	collected, err = chain.CollectDeletesLocked(MockTxnWithStartTS(startTs1), nil)
	assert.NoError(t, err)
	assert.True(t, collected.IsEmpty())
	collected, err = chain.CollectDeletesLocked(MockTxnWithStartTS(types.NextGlobalTsForTest()), nil)
	assert.NoError(t, err)
	assert.True(t, collected.IsEmpty())

	commitTxn(txn1)
	assert.Nil(t, n1.PrepareCommit())
	assert.Nil(t, n1.ApplyCommit())
	t.Log(chain.StringLocked())

	var startTs2 types.TS
	collected, err = chain.CollectDeletesLocked(MockTxnWithStartTS(startTs2), nil)
	assert.NoError(t, err)
	assert.True(t, collected.IsEmpty())
	collected, err = chain.CollectDeletesLocked(MockTxnWithStartTS(types.NextGlobalTsForTest()), nil)
	assert.NoError(t, err)
	assert.Equal(t, 10, collected.GetCardinality())
	collected, err = chain.CollectDeletesLocked(txn2, nil)
	assert.NoError(t, err)
	assert.Equal(t, 11, collected.GetCardinality())

	txn3 := mockTxn()
	err = chain.PrepareRangeDelete(5, 5, txn3.GetStartTS())
	assert.NotNil(t, err)
	err = chain.PrepareRangeDelete(31, 33, txn3.GetStartTS())
	assert.Nil(t, err)
	n3 := chain.AddNodeLocked(txn3, handle.DeleteType(handle.DT_Normal))
	n3.RangeDeleteLocked(31, 33)

	collected, err = chain.CollectDeletesLocked(txn3, nil)
	assert.NoError(t, err)
	assert.Equal(t, 13, collected.GetCardinality())
	t.Log(chain.StringLocked())

	merged = chain.AddMergeNode().(*DeleteNode)
	assert.NotNil(t, merged)
	t.Log(chain.StringLocked())
	assert.Equal(t, 4, chain.DepthLocked())

	cmd, err := merged.MakeCommand(1)
	assert.Nil(t, err)
	assert.NotNil(t, cmd)

	buf, err := cmd.MarshalBinary()
	assert.Nil(t, err)

	vcmd, err := txnbase.BuildCommandFrom(buf)
	assert.Nil(t, err)
	cmd2 := vcmd.(*UpdateCmd)
	assert.Equal(t, IOET_WALTxnCommand_DeleteNode, cmd2.GetType())
	assert.Equal(t, IOET_WALTxnCommand_DeleteNode, cmd2.cmdType)
	assert.True(t, cmd2.delete.mask.Equals(merged.mask))
}

func TestDeleteChain2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	seg := objectio.NewSegmentid()
	controller := NewMVCCHandle(catalog.NewStandaloneBlock(nil, objectio.NewBlockid(seg, 0, 0), types.TS{}))
	chain := NewDeleteChain(nil, controller)

	txn1 := mockTxn()
	n1 := chain.AddNodeLocked(txn1, handle.DeleteType(handle.DT_Normal)).(*DeleteNode)
	err := chain.PrepareRangeDelete(1, 4, txn1.GetStartTS())
	assert.Nil(t, err)
	n1.RangeDeleteLocked(1, 4)
	commitTxn(txn1)
	err = n1.PrepareCommit()
	assert.Nil(t, err)
	err = n1.ApplyCommit()
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
	err = n3.ApplyCommit()
	assert.Nil(t, err)
	t.Log(chain.StringLocked())

	mask, err := chain.CollectDeletesLocked(MockTxnWithStartTS(types.NextGlobalTsForTest()), nil)
	assert.NoError(t, err)
	assert.Equal(t, 8, mask.GetCardinality())
	mask, err = chain.CollectDeletesLocked(MockTxnWithStartTS(txn3.GetCommitTS()), nil)
	assert.NoError(t, err)
	assert.Equal(t, 8, mask.GetCardinality())
	mask, err = chain.CollectDeletesLocked(MockTxnWithStartTS(txn1.GetCommitTS()), nil)
	assert.NoError(t, err)
	assert.Equal(t, 4, mask.GetCardinality())
	mask, err = chain.CollectDeletesLocked(MockTxnWithStartTS(txn1.GetCommitTS().Prev()), nil)
	assert.NoError(t, err)
	assert.True(t, mask.IsEmpty())

	var startTs1 types.TS
	mask, err = chain.CollectDeletesInRange(startTs1, txn3.GetCommitTS(), nil)
	assert.NoError(t, err)
	t.Log(mask.GetBitmap().String())
	assert.Equal(t, 8, mask.GetCardinality())

	var startTs2 types.TS
	mask, err = chain.CollectDeletesInRange(startTs2, txn3.GetCommitTS().Next(), nil)
	assert.NoError(t, err)
	t.Log(mask.GetBitmap().String())
	assert.Equal(t, 8, mask.GetCardinality())

	mask, err = chain.CollectDeletesInRange(txn1.GetCommitTS(), txn3.GetCommitTS().Next(), nil)
	assert.NoError(t, err)
	t.Log(mask.GetBitmap().String())
	assert.Equal(t, 4, mask.GetCardinality())
}
