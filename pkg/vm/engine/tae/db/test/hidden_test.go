// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

// Testing Steps
// 1. Mock schema with PK.
// 2. Append data (append rows less than a block) and commit. Scan hidden column and check.
// 3. Append data and the total rows is more than a block. Commit and then compact the full block.
// 4. Scan hidden column and check.
// 5. Append data and the total rows is more than one Object. Commit and then merge sort the full Object.
// 6. Scan hidden column and check.
func TestHiddenWithPK1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*4))
	defer bat.Close()
	bats := bat.Split(10)

	txn, _, rel := testutil.CreateRelationNoCommit(t, tae, testutil.DefaultTestDB, schema, true)
	err := rel.Append(context.Background(), bats[0])
	{
		offsets := make([]uint32, 0)
		it := rel.MakeObjectIt()
		for it.Valid() {
			blk := it.GetObject()
			view, err := blk.GetColumnDataById(context.Background(), 0, schema.PhyAddrKey.Idx, common.DefaultAllocator)
			assert.NoError(t, err)
			defer view.Close()
			fp := blk.Fingerprint()
			_ = view.GetData().Foreach(func(v any, _ bool, _ int) (err error) {
				rid := v.(types.Rowid)
				bid, offset := rid.Decode()
				t.Logf("bid=%s,offset=%d", bid.String(), offset)
				assert.Equal(t, fp.BlockID, bid)
				offsets = append(offsets, offset)
				return
			}, nil)
			it.Next()
		}
		// sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		// assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
	}
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	{
		blk := testutil.GetOneObject(rel)
		view, err := blk.GetColumnDataByName(context.Background(), 0, catalog.PhyAddrColumnName, common.DefaultAllocator)
		assert.NoError(t, err)
		defer view.Close()
		offsets := make([]uint32, 0)
		fp := blk.Fingerprint()
		t.Log(fp.String())
		_ = view.GetData().Foreach(func(v any, _ bool, _ int) (err error) {
			rid := v.(types.Rowid)
			bid, offset := rid.Decode()
			t.Logf(",bid=%s,offset=%d", bid, offset)
			assert.Equal(t, fp.BlockID, bid)
			offsets = append(offsets, offset)
			return
		}, nil)
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
	}

	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[3])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[4])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[5])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	testutil.CompactBlocks(t, 0, tae, "db", schema, false)

	t.Log(tae.Catalog.SimplePPString(3))
	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	{
		it := rel.MakeObjectIt()
		for it.Valid() {
			blk := it.GetObject()
			for j := 0; j < blk.BlkCnt(); j++ {
				view, err := blk.GetColumnDataByName(context.Background(), uint16(j), catalog.PhyAddrColumnName, common.DefaultAllocator)
				assert.NoError(t, err)
				defer view.Close()
				offsets := make([]uint32, 0)
				meta := blk.GetMeta().(*catalog.ObjectEntry)
				t.Logf("%v, blk %d", meta.String(), j)
				_ = view.GetData().Foreach(func(v any, _ bool, _ int) (err error) {
					rid := v.(types.Rowid)
					bid, offset := rid.Decode()
					// t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
					expectedBid := objectio.NewBlockidWithObjectID(&meta.ID, uint16(j))
					assert.Equal(t, *expectedBid, bid, "expect %v, get %v", expectedBid.String(), bid.String())
					offsets = append(offsets, offset)
					return
				}, nil)
				sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
				if meta.IsAppendable() {
					assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
				} else {
					if j != 2 {
						assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, offsets)
					} else {
						assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
					}
				}
			}
			it.Next()
		}
	}

	assert.NoError(t, txn.Commit(context.Background()))
	{
		testutil.CompactBlocks(t, 0, tae, "db", schema, false)
		testutil.MergeBlocks(t, 0, tae, "db", schema, false)
	}

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	{
		it := rel.MakeObjectIt()
		objIdx := -1
		for it.Valid() {
			blk := it.GetObject()
			objIdx++
			for j := 0; j < blk.BlkCnt(); j++ {
				view, err := blk.GetColumnDataByName(context.Background(), uint16(j), catalog.PhyAddrColumnName, common.DefaultAllocator)
				assert.NoError(t, err)
				defer view.Close()
				offsets := make([]uint32, 0)
				meta := blk.GetMeta().(*catalog.ObjectEntry)
				t.Log(meta.String())
				t.Log(meta.String())
				_ = view.GetData().Foreach(func(v any, _ bool, _ int) (err error) {
					rid := v.(types.Rowid)
					bid, offset := rid.Decode()
					// t.Logf("sid=%d,bid=%d,offset=%d", sid, bid, offset)
					assert.Equal(t, *objectio.NewBlockidWithObjectID(&meta.ID, uint16(j)), bid)
					offsets = append(offsets, offset)
					return
				}, nil)
				sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
				if meta.IsAppendable() {
					assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
				} else {
					if objIdx != 1 {
						assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, offsets)
					} else {
						assert.Equal(t, []uint32{0, 1, 2, 3}, offsets)
					}
				}

			}
			it.Next()
		}
	}

	assert.NoError(t, txn.Commit(context.Background()))
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

// Testing Steps
// 1. Mock schema w/o primary key
// 2. Append data (append rows less than a block)
func TestHidden2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, -1)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*4))
	defer bat.Close()
	bats := bat.Split(10)

	txn, _, rel := testutil.CreateRelationNoCommit(t, tae, testutil.DefaultTestDB, schema, true)
	err := rel.Append(context.Background(), bats[0])
	{
		blk := testutil.GetOneObject(rel)
		var hidden *containers.ColumnView
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(context.Background(), 0, def.Idx, common.DefaultAllocator)
			assert.NoError(t, err)
			defer view.Close()
			assert.Equal(t, bats[0].Length(), view.Length())
			if def.IsPhyAddr() {
				hidden = view
			}
		}
		_ = hidden.GetData().Foreach(func(key any, _ bool, _ int) (err error) {
			rid := key.(types.Rowid)
			bid, offset := rid.Decode()
			t.Logf(",bid=%s,offset=%d", bid, offset)
			v, _, err := rel.GetValueByPhyAddrKey(key, schema.PhyAddrKey.Idx)
			assert.NoError(t, err)
			assert.Equal(t, key, v)
			if offset == 1 {
				err = rel.DeleteByPhyAddrKey(key)
				assert.NoError(t, err)
			}
			return
		}, nil)
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(context.Background(), 0, def.Idx, common.DefaultAllocator)
			assert.NoError(t, err)
			defer view.Close()
			view.ApplyDeletes()
			assert.Equal(t, bats[0].Length()-1, view.Length())
		}
	}
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	{
		blk := testutil.GetOneObject(rel)
		var hidden *containers.ColumnView
		for _, def := range schema.ColDefs {
			view, err := blk.GetColumnDataById(context.Background(), 0, def.Idx, common.DefaultAllocator)
			assert.NoError(t, err)
			defer view.Close()
			assert.Equal(t, bats[0].Length()-1, view.Length())
			if def.IsPhyAddr() {
				hidden = view
			}
		}
		_ = hidden.GetData().Foreach(func(key any, _ bool, _ int) (err error) {
			rid := key.(types.Rowid)
			bid, offset := rid.Decode()
			t.Logf(",bid=%s,offset=%d", bid, offset)
			v, _, err := rel.GetValueByPhyAddrKey(key, schema.PhyAddrKey.Idx)
			assert.NoError(t, err)
			assert.Equal(t, key, v)
			if offset == 1 {
				err = rel.DeleteByPhyAddrKey(key)
				assert.NoError(t, err)
			}
			return
		}, nil)
	}
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	testutil.CompactBlocks(t, 0, tae, "db", schema, false)
	testutil.MergeBlocks(t, 0, tae, "db", schema, false)

	t.Log(tae.Catalog.SimplePPString(common.PPL1))

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	{
		it := rel.MakeObjectIt()
		rows := 0
		for it.Valid() {
			blk := it.GetObject()
			for j := 0; j < blk.BlkCnt(); j++ {
				hidden, err := blk.GetColumnDataById(context.Background(), uint16(j), schema.PhyAddrKey.Idx, common.DefaultAllocator)
				assert.NoError(t, err)
				defer hidden.Close()
				hidden.ApplyDeletes()
				rows += hidden.Length()
			}
			it.Next()
		}
		assert.Equal(t, 26, rows)
	}
	assert.NoError(t, txn.Commit(context.Background()))
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}
