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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestComposedCmd(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	composed := txnbase.NewComposedCmd(0)
	defer composed.Close()

	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog()
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	dbCmd, err := db.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(dbCmd)

	table, _ := db.CreateTableEntry(schema, nil, nil)
	tblCmd, err := table.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(tblCmd)

	stats := objectio.NewObjectStatsWithObjectID(objectio.NewObjectid(), true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	objCmd, err := obj.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(objCmd)

	// TODO

	// objMvcc := updates.NewObjectMVCCHandle(obj)

	// controller := updates.NewMVCCHandle(objMvcc, 0)
	// ts := types.NextGlobalTsForTest()

	// appenderMvcc := updates.NewAppendMVCCHandle(obj)

	// node := updates.MockAppendNode(ts, 0, 2515, appenderMvcc)
	// cmd := updates.NewAppendCmd(1, node)

	// composed.AddCmd(cmd)

	// del := updates.NewDeleteNode(nil, handle.DT_Normal,
	// 	updates.IOET_WALTxnCommand_DeleteNode_V2)
	// del.AttachTo(controller.GetDeleteChain())
	// cmd2, err := del.MakeCommand(1)
	// assert.Nil(t, err)
	// composed.AddCmd(cmd2)

	// buf, err := composed.MarshalBinary()
	// assert.Nil(t, err)
	// _, err = txnbase.BuildCommandFrom(buf)
	// assert.Nil(t, err)
}

func TestComposedCmdMaxSize(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	composed := txnbase.NewComposedCmd(1280 + txnbase.CmdBufReserved)
	defer composed.Close()

	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog()
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	dbCmd, err := db.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(dbCmd)

	table, _ := db.CreateTableEntry(schema, nil, nil)
	for i := 2; i <= 10; i++ {
		cmd, err := table.MakeCommand(uint32(i))
		assert.Nil(t, err)
		composed.AddCmd(cmd)
	}

	buf, err := composed.MarshalBinary()
	assert.Nil(t, err)
	assert.Equal(t, true, composed.MoreCmds())
	cmd, err := txnbase.BuildCommandFrom(buf)
	assert.Nil(t, err)
	c1, ok := cmd.(*txnbase.ComposedCmd)
	assert.True(t, ok)
	assert.NotNil(t, c1)
	assert.Equal(t, composed.LastPos, len(c1.Cmds))

	buf, err = composed.MarshalBinary()
	assert.Nil(t, err)
	assert.Equal(t, true, composed.MoreCmds())
	cmd, err = txnbase.BuildCommandFrom(buf)
	assert.Nil(t, err)
	c2, ok := cmd.(*txnbase.ComposedCmd)
	assert.True(t, ok)
	assert.NotNil(t, c2)
	assert.Equal(t, composed.LastPos, len(c1.Cmds)+len(c2.Cmds))

	// Remove the left commands, because it has different result in different platforms.
}

func TestAppendCmd_Compatibility(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()
	testutils.EnsureNoLeak(t)
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver := initTestContext(ctx, t, dir)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(3, 2)

	txn, _ := mgr.StartTxn(nil)
	db, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	rel, _ := db.CreateRelation(schema)
	bat := catalog.MockBatch(schema, int(mpool.KB)*100)
	defer bat.Close()
	bats := bat.Split(100)
	for _, data := range bats {
		err := rel.Append(context.Background(), data)
		assert.Nil(t, err)
	}
	tDB, _ := txn.GetStore().(*txnStore).getOrSetDB(db.GetID())
	tbl, _ := tDB.getOrSetTable(rel.ID())

	cmd, err := tbl.dataTable.tableSpace.node.MakeCommand(1)
	assert.Nil(t, err)

	appendCmd := cmd.(*AppendCmd)
	data, err := appendCmd.MarshalBinaryV2()
	assert.Nil(t, err)

	cmd2 := NewEmptyAppendCmd()
	err = cmd2.UnmarshalBinaryV2(data[4:])
	assert.Nil(t, err)

	require.Equal(t, appendCmd.Data.Length(), cmd2.Data.Length())
}
