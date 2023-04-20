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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestCompactBlockCmd(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil, nil)

	controller := NewMVCCHandle(blk)

	ts := types.NextGlobalTsForTest()
	//node := MockAppendNode(341, 0, 2515, controller)
	node := MockAppendNode(ts, 0, 2515, controller)
	cmd := NewAppendCmd(1, node)

	buf, err := cmd.MarshalBinary()
	assert.Nil(t, err)

	cmd2, err := txnbase.BuildCommandFrom(buf)
	assert.Nil(t, err)
	checkAppendCmdIsEqual(t, cmd, cmd2.(*UpdateCmd))
}

func checkAppendCmdIsEqual(t *testing.T, cmd1, cmd2 *UpdateCmd) {
	assert.Equal(t, IOET_WALTxnCommand_AppendNode, cmd1.GetType())
	assert.Equal(t, IOET_WALTxnCommand_AppendNode, cmd2.GetType())
	assert.Equal(t, cmd1.append.maxRow, cmd2.append.maxRow)
}

func TestDeleteNodeCmd(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil, nil)

	controller := NewMVCCHandle(blk)

	node := NewDeleteNode(nil, handle.DT_Normal)
	node.mask = roaring.NewBitmap()
	node.mask.Add(35)
	node.chain = controller.deletes
	cmd, err := node.MakeCommand(1)
	assert.Nil(t, err)

	buf, err := cmd.MarshalBinary()
	assert.Nil(t, err)

	cmd2, err := txnbase.BuildCommandFrom(buf)
	assert.Nil(t, err)
	checkDeleteCmdIsEqual(t, cmd.(*UpdateCmd), cmd2.(*UpdateCmd))
}

func checkDeleteCmdIsEqual(t *testing.T, cmd1, cmd2 *UpdateCmd) {
	assert.Equal(t, IOET_WALTxnCommand_DeleteNode, cmd1.GetType())
	assert.Equal(t, IOET_WALTxnCommand_DeleteNode, cmd2.GetType())
	assert.Equal(t, cmd1.delete.GetCardinalityLocked(), cmd2.delete.GetCardinalityLocked())
	mask1 := cmd1.delete.mask
	if mask1 != nil && !mask1.IsEmpty() {
		it := mask1.Iterator()
		for it.HasNext() {
			row := it.Next()
			assert.True(t, cmd2.delete.mask.Contains(row))
		}
	}
}
