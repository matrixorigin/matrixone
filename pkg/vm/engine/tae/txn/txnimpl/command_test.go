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

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestComposedCmd(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	composed := txnbase.NewComposedCmd()
	defer composed.Close()

	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	dbCmd, err := db.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(dbCmd)

	table, _ := db.CreateTableEntry(schema, nil, nil)
	tblCmd, err := table.MakeCommand(1)
	assert.Nil(t, err)
	composed.AddCmd(tblCmd)

	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
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
