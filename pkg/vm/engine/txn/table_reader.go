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

package txnengine

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type TableReader struct {
	ctx         context.Context
	engine      *Engine
	txnOperator client.TxnOperator
	iterInfos   []IterInfo
}

type IterInfo struct {
	Shard  Shard
	IterID string
}

func (t *Table) NewReader(
	ctx context.Context,
	parallel int,
	expr *plan.Expr,
	shards [][]byte,
) (
	readers []engine.Reader,
	err error,
) {

	clusterDetails, err := t.engine.getClusterDetails()
	if err != nil {
		return nil, err
	}

	readers = make([]engine.Reader, parallel)
	nodes := clusterDetails.DNNodes

	if len(shards) > 0 {
		uuidSet := make(map[string]bool)
		for _, shard := range shards {
			uuidSet[string(shard)] = true
		}
		filteredNodes := nodes[:0]
		for _, node := range nodes {
			if uuidSet[node.UUID] {
				filteredNodes = append(filteredNodes, node)
			}
		}
		nodes = filteredNodes
	}
	dnShards, err := t.engine.shardPolicy.Nodes(nodes)
	if err != nil {
		return nil, err
	}

	resps, err := doTxnRequest[NewTableIterResp](
		ctx,
		t.engine,
		t.txnOperator.Read,
		theseShards(dnShards),
		OpNewTableIter,
		NewTableIterReq{
			TableID: t.id,
			Expr:    expr,
			Shards:  shards,
		},
	)
	if err != nil {
		return nil, err
	}

	iterIDSets := make([][]string, parallel)
	i := 0
	for _, resp := range resps {
		if resp.IterID != "" {
			iterIDSets[i] = append(iterIDSets[i], resp.IterID)
			i++
			if i >= parallel {
				// round
				i = 0
			}
		}
	}

	for i, idSet := range iterIDSets {
		if len(idSet) == 0 {
			readers[i] = new(TableReader)
			continue
		}
		reader := &TableReader{
			engine:      t.engine,
			txnOperator: t.txnOperator,
			ctx:         ctx,
		}
		for _, iterID := range idSet {
			reader.iterInfos = append(reader.iterInfos, IterInfo{
				Shard:  dnShards[i],
				IterID: iterID,
			})
		}
		readers[i] = reader
	}

	return
}

var _ engine.Reader = new(TableReader)

func (t *TableReader) Read(colNames []string, plan *plan.Expr, mh *mheap.Mheap) (*batch.Batch, error) {
	if t == nil {
		return nil, nil
	}

	for {

		if len(t.iterInfos) == 0 {
			return nil, nil
		}

		resps, err := doTxnRequest[ReadResp](
			t.ctx,
			t.engine,
			t.txnOperator.Read,
			thisShard(t.iterInfos[0].Shard),
			OpRead,
			ReadReq{
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

		return resp.Batch, nil
	}

}

func (t *TableReader) Close() error {
	if t == nil {
		return nil
	}
	for _, info := range t.iterInfos {
		_, err := doTxnRequest[CloseTableIterResp](
			t.ctx,
			t.engine,
			t.txnOperator.Read,
			thisShard(info.Shard),
			OpCloseTableIter,
			CloseTableIterReq{
				IterID: info.IterID,
			},
		)
		_ = err // ignore error
	}
	return nil
}
