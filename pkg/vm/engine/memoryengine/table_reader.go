// Copyright 2022 Matrix Origin
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

package memoryengine

import (
	"context"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type TableReader struct {
	ctx         context.Context
	engine      *Engine
	txnOperator client.TxnOperator
	iterInfos   []IterInfo
}

type IterInfo struct {
	Shard  Shard
	IterID ID
}

func (t *Table) BuildReaders(
	ctx context.Context,
	_ any,
	expr *plan.Expr,
	relData engine.RelData,
	parallel int,
	_ int,
	_ bool,
	_ engine.TombstoneApplyPolicy) (readers []engine.Reader, err error) {

	readers = make([]engine.Reader, parallel)
	var shardIDs = relData.GetShardIDList()

	var shards []Shard
	if len(shardIDs) == 0 {
		switch t.id {

		case catalog.MO_DATABASE_ID,
			catalog.MO_TABLES_ID,
			catalog.MO_COLUMNS_ID:
			// sys table
			var err error
			shards, err = t.engine.anyShard()
			if err != nil {
				return nil, err
			}

		default:
			// all
			var err error
			shards, err = t.engine.allShards()
			if err != nil {
				return nil, err
			}
		}

	} else {
		// some
		idSet := make(map[uint64]bool)
		for i := 0; i < len(shardIDs); i++ {
			idSet[shardIDs[i]] = true
		}
		for _, store := range getTNServices(t.engine.cluster) {
			for _, shard := range store.Shards {
				if !idSet[shard.ShardID] {
					continue
				}
				shards = append(shards, Shard{
					TNShardRecord: metadata.TNShardRecord{
						ShardID: shard.ShardID,
					},
					ReplicaID: shard.ReplicaID,
					Address:   store.TxnServiceAddress,
				})
			}
		}
	}

	resps, err := DoTxnRequest[NewTableIterResp](
		ctx,
		t.txnOperator,
		true,
		theseShards(shards),
		OpNewTableIter,
		&NewTableIterReq{
			TableID: t.id,
			Expr:    expr,
		},
	)
	if err != nil {
		return nil, err
	}

	iterInfoSets := make([][]IterInfo, parallel)
	for i, resp := range resps {
		if resp.IterID == emptyID {
			continue
		}
		iterInfo := IterInfo{
			Shard:  shards[i],
			IterID: resp.IterID,
		}
		iterInfoSets[i%parallel] = append(iterInfoSets[i%parallel], iterInfo)
	}

	for i, set := range iterInfoSets {
		if len(set) == 0 {
			readers[i] = new(TableReader)
			continue
		}
		reader := &TableReader{
			engine:      t.engine,
			txnOperator: t.txnOperator,
			ctx:         ctx,
			iterInfos:   set,
		}
		readers[i] = reader
	}

	return
}

var _ engine.Reader = new(TableReader)

func (t *TableReader) Read(
	ctx context.Context,
	colNames []string,
	plan *plan.Expr,
	mp *mpool.MPool,
	bat *batch.Batch) (bool, error) {
	if t == nil {
		return true, nil
	}

	for {

		if len(t.iterInfos) == 0 {
			return true, nil
		}

		resps, err := DoTxnRequest[ReadResp](
			t.ctx,
			t.txnOperator,
			true,
			thisShard(t.iterInfos[0].Shard),
			OpRead,
			&ReadReq{
				IterID:   t.iterInfos[0].IterID,
				ColNames: colNames,
			},
		)
		if err != nil {
			return true, err
		}

		resp := resps[0]

		if resp.Batch == nil {
			// no more
			t.iterInfos = t.iterInfos[1:]
			continue
		}

		logutil.Debug(testutil.OperatorCatchBatch("table reader", resp.Batch))
		_, err = bat.Append(t.ctx, mp, resp.Batch)
		resp.Batch.Clean(mp)
		if err != nil {
			return true, err
		}
		return false, nil
	}

}

func (t *TableReader) SetFilterZM(objectio.ZoneMap) {
}

func (t *TableReader) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (t *TableReader) SetOrderBy([]*plan.OrderBySpec) {
}

func (t *TableReader) Close() error {
	if t == nil {
		return nil
	}
	for _, info := range t.iterInfos {
		_, err := DoTxnRequest[CloseTableIterResp](
			t.ctx,
			t.txnOperator,
			true,
			thisShard(info.Shard),
			OpCloseTableIter,
			&CloseTableIterReq{
				IterID: info.IterID,
			},
		)
		_ = err // ignore error
	}
	return nil
}

func (t *Table) GetEngineType() engine.EngineType {
	return engine.Memory
}

func (t *Table) Ranges(_ context.Context, _ []*plan.Expr, _ int) (engine.RelData, error) {
	rd := &MemRelationData{}
	nodes := getTNServices(t.engine.cluster)
	shards := make(ShardIdSlice, 0, len(nodes)*8)
	for _, node := range nodes {
		for _, shard := range node.Shards {
			id := make([]byte, 8)
			binary.LittleEndian.PutUint64(id, shard.ShardID)
			shards = append(shards, id...)
		}
	}
	rd.Shards = shards
	return rd, nil
}

func (t *Table) CollectTombstones(
	_ context.Context,
	_ int,
	_ engine.TombstoneCollectPolicy,
) (engine.Tombstoner, error) {
	panic("implement me")
}

// for memory engine.
type MemRelationData struct {
	Shards ShardIdSlice
}

func (rd *MemRelationData) String() string {
	return "RelData[M]"
}

func (rd *MemRelationData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	panic("not supported")
}

func (rd *MemRelationData) GetBlockInfo(i int) objectio.BlockInfo {
	panic("not supported")
}

func (rd *MemRelationData) SetBlockInfo(i int, blk objectio.BlockInfo) {
	panic("not supported")
}

func (rd *MemRelationData) AppendBlockInfo(blk objectio.BlockInfo) {
	panic("not supported")
}

func (rd *MemRelationData) GetShardIDList() []uint64 {
	ids := make([]uint64, rd.Shards.Len())
	idsLen := rd.Shards.Len()

	for idx := range idsLen {
		ids[idx] = rd.Shards.Get(idx)
	}

	return ids
}

func (rd *MemRelationData) GetShardID(i int) uint64 {
	return rd.Shards.Get(i)
}

func (rd *MemRelationData) SetShardID(i int, id uint64) {
	rd.Shards.Set(i, id)
}

func (rd *MemRelationData) AppendShardID(id uint64) {
	bb := make([]byte, 8)
	binary.LittleEndian.PutUint64(bb, id)
	rd.Shards.Append(bb)
}

func (rd *MemRelationData) MarshalBinary() ([]byte, error) {
	panic("Not Support")
}

func (rd *MemRelationData) GetType() engine.RelDataType {
	return engine.RelDataShardIDList
}

func (rd MemRelationData) UnmarshalBinary(buf []byte) error {
	panic("Not Support")
}

func (rd *MemRelationData) AttachTombstones(tombstones engine.Tombstoner) error {
	panic("Not Support")
}

func (rd *MemRelationData) GetTombstones() engine.Tombstoner {
	panic("Not Support")
}

func (rd *MemRelationData) DataSlice(i, j int) engine.RelData {
	panic("Not Support")
}

// GroupByPartitionNum TODO::remove it after refactor of partition table.
func (rd *MemRelationData) GroupByPartitionNum() map[int16]engine.RelData {
	panic("Not Support")
}

func (rd *MemRelationData) BuildEmptyRelData() engine.RelData {
	return &MemRelationData{}
}

func (rd *MemRelationData) DataCnt() int {
	return rd.Shards.Len()
}

type ShardIdSlice []byte

var _ engine.Ranges = (*ShardIdSlice)(nil)

func (s *ShardIdSlice) Set(i int, id uint64) {
	buf := (*s)[i*8:]
	binary.LittleEndian.PutUint64(buf, id)
}

func (s *ShardIdSlice) GetBytes(i int) []byte {
	return (*s)[i*8 : (i+1)*8]
}

func (s *ShardIdSlice) Len() int {
	return len(*s) / 8
}

func (s *ShardIdSlice) Append(bs []byte) {
	*s = append(*s, bs...)
}

func (s *ShardIdSlice) Size() int {
	return len(*s)
}

func (s *ShardIdSlice) SetBytes(bs []byte) {
	*s = bs
}

func (s *ShardIdSlice) GetAllBytes() []byte {
	return *s
}

func (s *ShardIdSlice) Slice(i, j int) []byte {
	return (*s)[i*8 : j*8]
}

func (s *ShardIdSlice) Get(i int) uint64 {
	return binary.LittleEndian.Uint64(s.GetBytes(i))
}
