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

package tables

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestGetActiveRow(t *testing.T) {
	defer testutils.AfterTest(t)()
	ts1 := types.BuildTS(1, 0)
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog()
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	stats := objectio.NewObjectStatsWithObjectID(objectio.NewObjectid(), true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	mvcc := updates.NewAppendMVCCHandle(obj)
	// blk := &dataBlock{
	// 	mvcc: mvcc,
	// }
	b := &baseObject{
		RWMutex:    mvcc.RWMutex,
		appendMVCC: mvcc,
	}
	b.meta.Store(obj)
	mnode := &memoryNode{
		object: b,
	}
	blk := &aobject{baseObject: b}

	mnode.Ref()
	n := NewNode(mnode)
	blk.node.Store(n)

	// appendnode1 [0,1)
	an1, _ := mvcc.AddAppendNodeLocked(nil, 0, 1)
	an1.Start = ts1
	an1.Prepare = ts1
	an1.End = ts1

	// appendnode1 [1,2)
	an2, _ := mvcc.AddAppendNodeLocked(nil, 1, 2)
	an2.Start = ts1
	an2.Prepare = ts1
	an2.End = ts1

	// index uint8(1)-0,1
	vec := containers.MakeVector(types.T_int8.ToType(), common.DefaultAllocator)
	vec.Append(int8(1), false)
	vec.Append(int8(1), false)
	idx := indexwrapper.NewMutIndex(types.T_int8.ToType())
	err := idx.BatchUpsert(vec.GetDownstreamVector(), 0)
	assert.NoError(t, err)
	blk.node.Load().MustMNode().pkIndex = idx
}
