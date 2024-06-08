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

func (t *Table) NewReader(ctx context.Context, parallel int, expr *plan.Expr, bytes []byte, _ bool, txnOffset int) (readers []engine.Reader, err error) {
	readers = make([]engine.Reader, parallel)
	shardIDs := ShardIdSlice(bytes)

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
		for i := 0; i < shardIDs.Len(); i++ {
			idSet[shardIDs.Get(i)] = true
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

func (t *TableReader) Read(ctx context.Context, colNames []string, plan *plan.Expr, mp *mpool.MPool, _ engine.VectorPool) (*batch.Batch, error) {
	if t == nil {
		return nil, nil
	}

	for {

		if len(t.iterInfos) == 0 {
			return nil, nil
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
			return nil, err
		}

		resp := resps[0]

		if resp.Batch == nil {
			// no more
			t.iterInfos = t.iterInfos[1:]
			continue
		}

		logutil.Debug(testutil.OperatorCatchBatch("table reader", resp.Batch))
		return resp.Batch, nil
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

func (t *Table) Ranges(_ context.Context, _ []*plan.Expr, _ int) (engine.Ranges, error) {
	// return encoded shard ids
	nodes := getTNServices(t.engine.cluster)
	shards := make(ShardIdSlice, 0, len(nodes)*8)
	for _, node := range nodes {
		for _, shard := range node.Shards {
			id := make([]byte, 8)
			binary.LittleEndian.PutUint64(id, shard.ShardID)
			shards = append(shards, id...)
		}
	}
	return &shards, nil
}

type ShardIdSlice []byte

var _ engine.Ranges = (*ShardIdSlice)(nil)

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
