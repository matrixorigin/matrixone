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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	api "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidatePersistedCommitTSVectors(t *testing.T) {
	mp := mpool.MustNewZero()

	valid := containers.MakeVector(types.T_TS.ToType(), mp)
	valid.Append(types.BuildTS(1, 0), false)
	got, err := validatePersistedCommitTSVectors(
		"valid",
		[]containers.Vector{valid},
	)
	require.NoError(t, err)
	require.Same(t, valid, got)
	got.Close()
	require.Zero(t, mp.CurrNB())

	for _, test := range []struct {
		name    string
		vectors func() []containers.Vector
	}{
		{
			name: "empty",
			vectors: func() []containers.Vector {
				return nil
			},
		},
		{
			name: "nil vector",
			vectors: func() []containers.Vector {
				return []containers.Vector{nil}
			},
		},
		{
			name: "wrong type is released",
			vectors: func() []containers.Vector {
				vec := containers.MakeVector(types.T_int64.ToType(), mp)
				vec.Append(int64(1), false)
				return []containers.Vector{vec}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := validatePersistedCommitTSVectors(
				"bad",
				test.vectors(),
			)
			require.ErrorContains(t, err, "bad commits layout")
			require.Nil(t, got)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestGetActiveRow(t *testing.T) {
	defer testutils.AfterTest(t)()
	ts1 := types.BuildTS(1, 0)
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
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

func TestApplyAppendLockedPadsMissingColumnsForUpgradedSchema(t *testing.T) {
	defer testutils.AfterTest(t)()

	oldSchema := catalog.MockSchema(2, 0)
	newSchema := oldSchema.Clone()
	require.NoError(t, newSchema.ApplyAlterTable(
		api.NewAddColumnReq(0, 0, "added_flag", types.NewProtoType(types.T_int8), 2),
	))

	c := catalog.MockCatalog(nil)
	defer c.Close()

	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(oldSchema, nil, nil)
	require.NoError(t, err)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, err := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)
	require.NoError(t, err)

	mvcc := updates.NewAppendMVCCHandle(obj)
	b := &baseObject{
		RWMutex:    mvcc.RWMutex,
		appendMVCC: mvcc,
	}
	b.meta.Store(obj)

	mnode := &memoryNode{
		object:      b,
		writeSchema: newSchema,
	}

	bat := containers.BuildBatch(
		oldSchema.AllNames(),
		oldSchema.AllTypes(),
		containers.Options{Allocator: common.DefaultAllocator},
	)
	defer bat.Close()
	for _, vec := range bat.Vecs {
		vec.Append(nil, true)
	}

	from, err := mnode.ApplyAppendLocked(bat)
	require.NoError(t, err)
	require.Equal(t, 0, from)

	for _, vec := range mnode.data.Vecs {
		require.Equal(t, bat.Length(), vec.Length())
	}
	addedVec := mnode.data.GetVectorByName("added_flag")
	require.Equal(t, bat.Length(), addedVec.Length())
	require.True(t, addedVec.IsNull(0))

	pool := containers.NewVectorPool("upgrade-compat", 4, containers.WithMPool(common.DefaultAllocator))
	require.NotPanics(t, func() {
		win := mnode.data.CloneWindowWithPool(0, bat.Length(), pool)
		win.Close()
	})
}

func TestMemoryNodeRollbackHoleVisibilityAndWriteLayout(t *testing.T) {
	defer testutils.AfterTest(t)()
	rt := moruntime.ServiceRuntime("")
	originalVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion10)
	defer func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, originalVersion)
		}
	}()
	schema := catalog.MockSchema(1, 0)
	c := catalog.MockCatalog(nil)
	defer c.Close()
	db, _ := c.CreateDBEntry("db", "", "", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	noid := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&noid, true, false, false)
	obj, _ := table.CreateObject(nil, &objectio.CreateObjOpt{Stats: stats}, nil)

	mvcc := updates.NewAppendMVCCHandle(obj)
	base := &baseObject{
		RWMutex:    mvcc.RWMutex,
		appendMVCC: mvcc,
		rt:         dbutils.NewRuntime(),
	}
	base.meta.Store(obj)
	mnode := &memoryNode{object: base, writeSchema: schema}

	input := containers.BuildBatch(
		schema.AllNames(),
		schema.AllTypes(),
		containers.Options{Allocator: common.DefaultAllocator},
	)
	defer input.Close()
	for _, vec := range input.Vecs {
		for range 3 {
			vec.Append(nil, true)
		}
	}
	_, err := mnode.ApplyAppendLocked(input)
	require.NoError(t, err)
	defer mnode.data.Close()

	for row := uint32(0); row < 3; row++ {
		appendNode, _ := mvcc.AddAppendNodeLocked(nil, row, row+1)
		ts := types.BuildTS(int64(row+1), 0)
		appendNode.Start, appendNode.Prepare, appendNode.End = ts, ts, ts
		if row == 1 {
			appendNode.Aborted = true
		}
	}

	flushBatches := make(map[uint32]*containers.BatchWithVersion)
	err = mnode.getDataWindowOnWriteSchema(
		context.Background(),
		flushBatches,
		types.TS{},
		types.BuildTS(3, 0),
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	flushBatch := flushBatches[schema.Version]
	require.NotNil(t, flushBatch)
	defer flushBatch.Close()
	require.Contains(t, flushBatch.Seqnums, uint16(objectio.SEQNUM_ABORT))
	aborts := vector.MustFixedColWithTypeCheck[bool](
		flushBatch.GetVectorByName(objectio.TombstoneAttr_Abort_Attr).GetDownstreamVector(),
	)
	require.Equal(t, []bool{false, true, false}, aborts)

	// During a rolling upgrade, the TN keeps the legacy layout and physically
	// removes rollback holes so an old CN cannot expose them as user rows.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion9)
	legacyBatches := make(map[uint32]*containers.BatchWithVersion)
	err = mnode.getDataWindowOnWriteSchema(
		context.Background(),
		legacyBatches,
		types.TS{},
		types.BuildTS(3, 0),
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	legacyBatch := legacyBatches[schema.Version]
	require.NotNil(t, legacyBatch)
	defer legacyBatch.Close()
	require.NotContains(t, legacyBatch.Seqnums, uint16(objectio.SEQNUM_ABORT))
	require.NotContains(t, legacyBatch.Nameidx, objectio.TombstoneAttr_Abort_Attr)
	require.Contains(t, legacyBatch.Seqnums, uint16(objectio.SEQNUM_COMMITTS))
	require.Equal(t, 2, legacyBatch.Length())

	var output *containers.Batch
	err = mnode.Scan(
		context.Background(),
		&output,
		updates.MockTxnWithStartTS(types.BuildTS(3, 0)),
		schema,
		0,
		[]int{0},
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	require.NotNil(t, output)
	defer output.Close()
	require.Equal(t, 3, output.Length())
	require.True(t, output.IsDeleted(1))
	require.False(t, output.IsDeleted(0))
	require.False(t, output.IsDeleted(2))
}
