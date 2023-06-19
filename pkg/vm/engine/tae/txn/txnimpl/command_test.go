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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
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

	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil, nil)
	segCmd, err := seg.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(segCmd)

	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil, nil)
	blkCmd, err := blk.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(blkCmd)

	controller := updates.NewMVCCHandle(blk)
	ts := types.NextGlobalTsForTest()

	node := updates.MockAppendNode(ts, 0, 2515, controller)
	cmd := updates.NewAppendCmd(1, node)

	composed.AddCmd(cmd)

	del := updates.NewDeleteNode(nil, handle.DT_Normal)
	del.AttachTo(controller.GetDeleteChain())
	cmd2, err := del.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(cmd2)

	buf, err := composed.MarshalBinary()
	assert.Nil(t, err)
	_, err = txnbase.BuildCommandFrom(buf)
	assert.Nil(t, err)
}

func TestComposedCmdMaxSize(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	composed := txnbase.NewComposedCmd(2048)
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

	buf, err = composed.MarshalBinary()
	assert.Nil(t, err)
	assert.Equal(t, false, composed.MoreCmds())
	cmd, err = txnbase.BuildCommandFrom(buf)
	assert.Nil(t, err)
	c3, ok := cmd.(*txnbase.ComposedCmd)
	assert.True(t, ok)
	assert.NotNil(t, c3)
	assert.Equal(t, 10, len(c1.Cmds)+len(c2.Cmds)+len(c3.Cmds))
}
