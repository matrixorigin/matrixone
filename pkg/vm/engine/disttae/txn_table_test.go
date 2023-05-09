// Copyright 2023 Matrix Origin
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

package disttae

import (
	"context"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/assert"
)

func newTxnTableForTest(
	mp *mpool.MPool,
) *txnTable {
	engine := &Engine{
		packerPool: fileservice.NewPool(
			128,
			func() *types.Packer {
				return types.NewPacker(mp)
			},
			func(packer *types.Packer) {
				packer.Reset()
			},
			func(packer *types.Packer) {
				packer.FreeMem()
			},
		),
		dnMap: make(map[string]int),
	}
	engine.dnMap["fakeDNStore"] = 0
	engine.partitions = make(map[[2]uint64]logtailreplay.Partitions)
	var dnStore DNStore
	txn := &Transaction{
		engine:    engine,
		dnStores:  []DNStore{dnStore},
		createMap: new(sync.Map),
	}
	db := &txnDatabase{
		txn: txn,
	}
	table := &txnTable{
		db:         db,
		primaryIdx: 0,
	}
	return table
}

func makeBatchForTest(
	mp *mpool.MPool,
	ints ...int64,
) *batch.Batch {
	bat := batch.New(false, []string{"a"})
	vec := vector.NewVec(types.T_int64.ToType())
	for _, n := range ints {
		vector.AppendFixed(vec, n, false, mp)
	}
	bat.SetVector(0, vec)
	bat.SetZs(len(ints), mp)
	return bat
}

func TestPrimaryKeyCheck(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	getRowIDsBatch := func(table *txnTable) *batch.Batch {
		bat := batch.New(false, []string{catalog.Row_ID})
		vec := vector.NewVec(types.T_Rowid.ToType())
		iter := table.localState.NewRowsIter(
			types.TimestampToTS(table.nextLocalTS()),
			nil,
			false,
		)
		l := 0
		for iter.Next() {
			entry := iter.Entry()
			vector.AppendFixed(vec, entry.RowID, false, mp)
			l++
		}
		iter.Close()
		bat.SetVector(0, vec)
		bat.SetZs(l, mp)
		return bat
	}

	table := newTxnTableForTest(mp)

	// insert
	err := table.Write(
		ctx,
		makeBatchForTest(mp, 1),
	)
	assert.Nil(t, err)

	// insert duplicated
	err = table.Write(
		ctx,
		makeBatchForTest(mp, 1),
	)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

	// insert no duplicated
	err = table.Write(
		ctx,
		makeBatchForTest(mp, 2, 3),
	)
	assert.Nil(t, err)

	// duplicated in same batch
	err = table.Write(
		ctx,
		makeBatchForTest(mp, 4, 4),
	)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

	table = newTxnTableForTest(mp)

	// insert, delete then insert
	err = table.Write(
		ctx,
		makeBatchForTest(mp, 1),
	)
	assert.Nil(t, err)
	err = table.Delete(
		ctx,
		getRowIDsBatch(table),
		catalog.Row_ID,
	)
	assert.Nil(t, err)
	err = table.Write(
		ctx,
		makeBatchForTest(mp, 5),
	)
	assert.Nil(t, err)

}

func BenchmarkTxnTableInsert(b *testing.B) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	table := newTxnTableForTest(mp)
	for i, max := int64(0), int64(b.N); i < max; i++ {
		err := table.Write(
			ctx,
			makeBatchForTest(mp, i),
		)
		assert.Nil(b, err)
	}
}

func BenchmarkLargeBlocksBtree(b *testing.B) {
	pool := mpool.MustNewZero()
	table := newTxnTableForTest(pool)
	// add 1000000 block as non-append blocks
	blockNums := 1000000
	table.blockInfos = make([][]catalog.BlockInfo, 1)
	state := logtailreplay.NewPartitionState(false)
	sid := objectio.NewSegmentid()
	table.blockInfos[0] = make([]catalog.BlockInfo, 0, blockNums)
	// add blockInfo
	for i := 0; i < blockNums; i++ {
		table.blockInfos[0] = append(table.blockInfos[0], catalog.BlockInfo{
			BlockID: *objectio.NewBlockid(sid, 0, uint16(i)),
		})
	}

	// add 10000 blocks in dn_delete_metaLoc_batch
	mp2 := make(map[types.Blockid][]*batch.Batch)
	table.db.txn.blockId_dn_delete_metaLoc_batch = mp2
	for i := 0; i < 10000; i++ {
		mp2[*objectio.NewBlockid(sid, 0, uint16(i))] = nil
	}

	// add non-append blocks into btree state
	for _, info := range table.blockInfos[0] {
		state.AddBlockId(info)
	}

	for i, max := int64(0), int64(b.N); i < max; i++ {
		for blkId := range table.db.txn.blockId_dn_delete_metaLoc_batch {
			if !state.BlockVisible(blkId, types.TimestampToTS(table.db.txn.meta.SnapshotTS)) {
				// load dn memory data deletes
				table.LoadDeletesForBlock(&blkId, nil, nil)
			}
		}
	}
}

func BenchmarkLargeBlocksMap(b *testing.B) {
	pool := mpool.MustNewZero()
	table := newTxnTableForTest(pool)
	// add 1000000 block as non-append blocks
	blockNums := 1000000
	table.blockInfos = make([][]catalog.BlockInfo, 1)
	state := logtailreplay.NewPartitionState(false)
	sid := objectio.NewSegmentid()
	table.blockInfos[0] = make([]catalog.BlockInfo, 0, blockNums)
	// add blockInfo
	for i := 0; i < blockNums; i++ {
		table.blockInfos[0] = append(table.blockInfos[0], catalog.BlockInfo{
			BlockID: *objectio.NewBlockid(sid, 0, uint16(i)),
		})
	}

	// add 10000 blocks in dn_delete_metaLoc_batch
	mp2 := make(map[types.Blockid][]*batch.Batch)
	table.db.txn.blockId_dn_delete_metaLoc_batch = mp2
	for i := 0; i < 10000; i++ {
		mp2[*objectio.NewBlockid(sid, 0, uint16(i))] = nil
	}

	// add non-append blocks into btree state
	for _, info := range table.blockInfos[0] {
		state.AddBlockId(info)
	}

	for i, max := int64(0), int64(b.N); i < max; i++ {
		// model each map
		meta_blocks := make(map[types.Blockid]bool)
		for _, info := range table.blockInfos[0] {
			meta_blocks[info.BlockID] = true
		}
		for blkId := range table.db.txn.blockId_dn_delete_metaLoc_batch {
			if !meta_blocks[blkId] {
				// load dn memory data deletes
				table.LoadDeletesForBlock(&blkId, nil, nil)
			}
		}
	}
}
