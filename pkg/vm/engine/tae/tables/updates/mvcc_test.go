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
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestMutationControllerAppend(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog()
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	stats := objectio.NewObjectStatsWithObjectID(objectio.NewObjectid(), true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	mc := NewAppendMVCCHandle(obj)

	nodeCnt := 10000
	rowsPerNode := uint32(5)
	//ts := uint64(2)
	//ts = 4

	ts := types.NextGlobalTsForTest()
	ts = ts.Next()
	ts = ts.Next()
	//queries := make([]uint64, 0)
	//queries = append(queries, ts-1)
	queries := make([]types.TS, 0)
	queries = append(queries, ts.Prev())

	for i := 0; i < nodeCnt; i++ {
		txn := mockTxn()
		txn.CommitTS = ts
		txn.PrepareTS = ts
		node, _ := mc.AddAppendNodeLocked(txn, rowsPerNode*uint32(i), rowsPerNode*(uint32(i)+1))
		err := node.ApplyCommit(txn.ID)
		assert.Nil(t, err)
		//queries = append(queries, ts+1)
		queries = append(queries, ts.Next())
		//ts += 2
		ts = ts.Next()
		ts = ts.Next()
	}

	st := time.Now()
	for i, qts := range queries {
		row, ok, _, _ := mc.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(qts))
		if i == 0 {
			assert.False(t, ok)
		} else {
			assert.True(t, ok)
			assert.Equal(t, uint32(i)*rowsPerNode, row)
		}
	}
	t.Logf("%s -- %d ops", time.Since(st), len(queries))
}

// AppendNode Start Prepare End Aborted
// a1 1,1,1 false
// a2 1,3,5 false
// a3 1,4,4 false
// a4 1,5,5 true
func TestGetVisibleRow(t *testing.T) {
	defer testutils.AfterTest(t)()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog()
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	stats := objectio.NewObjectStatsWithObjectID(objectio.NewObjectid(), true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	n := NewAppendMVCCHandle(obj)
	an1, _ := n.AddAppendNodeLocked(nil, 0, 1)
	an1.Start = types.BuildTS(1, 0)
	an1.Prepare = types.BuildTS(1, 0)
	an1.End = types.BuildTS(1, 0)
	an2, _ := n.AddAppendNodeLocked(nil, 1, 2)
	an2.Start = types.BuildTS(1, 0)
	an2.Prepare = types.BuildTS(3, 0)
	an2.End = types.BuildTS(5, 0)
	an3, _ := n.AddAppendNodeLocked(nil, 2, 3)
	an3.Start = types.BuildTS(1, 0)
	an3.Prepare = types.BuildTS(4, 0)
	an3.End = types.BuildTS(4, 0)
	an4, _ := n.AddAppendNodeLocked(nil, 3, 4)
	an4.Start = types.BuildTS(1, 0)
	an4.Prepare = types.BuildTS(5, 0)
	an4.End = types.BuildTS(5, 0)
	an4.Aborted = true

	// ts=1 maxrow=1, holes={}
	maxrow, visible, holes, err := n.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(types.BuildTS(1, 0)))
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), maxrow)
	assert.True(t, visible)
	assert.Equal(t, 0, holes.GetCardinality())

	// ts=4 maxrow=3, holes={1}
	maxrow, visible, holes, err = n.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(types.BuildTS(4, 0)))
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), maxrow)
	assert.True(t, visible)
	assert.Equal(t, 1, holes.GetCardinality())
	assert.True(t, holes.Contains(1))

	// ts=5 maxrow=3, holes={}
	maxrow, visible, holes, err = n.GetVisibleRowLocked(context.TODO(), MockTxnWithStartTS(types.BuildTS(5, 0)))
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), maxrow)
	assert.True(t, visible)
	assert.Equal(t, 0, holes.GetCardinality())

}
