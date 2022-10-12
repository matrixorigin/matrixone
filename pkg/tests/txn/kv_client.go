// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"context"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"go.uber.org/zap"
)

// kvClient the kvClient use the pkg/txn/storage/mem as the txn storage. Sends transaction requests directly to the DN,
// bypassing the CN and assuming some of the CN's responsibilities, such as routing, etc.
type kvClient struct {
	env    service.Cluster
	client client.TxnClient

	mu struct {
		sync.RWMutex
		dnshards []metadata.DNShard
	}
}

func newKVClient(env service.Cluster,
	clock clock.Clock,
	logger *zap.Logger) (Client, error) {
	sender, err := rpc.NewSender(clock, logger)
	if err != nil {
		return nil, err
	}
	c := &kvClient{
		env:    env,
		client: client.NewTxnClient(sender),
	}
	c.refreshDNShards()
	return c, nil
}

func (c *kvClient) NewTxn(options ...client.TxnOption) (Txn, error) {
	op, err := c.client.New(options...)
	if err != nil {
		return nil, err
	}

	return &kvTxn{op: op,
		env:           c.env,
		router:        c.getTargetDN,
		refreshRouter: c.refreshDNShards}, nil
}

func (c *kvClient) getTargetDN(key string) metadata.DNShard {
	h := fnv.New32()
	if _, err := h.Write([]byte(key)); err != nil {
		panic(err)
	}
	v := h.Sum32()

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.mu.dnshards[v%uint32(len(c.mu.dnshards))]
}

func (c *kvClient) refreshDNShards() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshDNShardsLocked()
}

func (c *kvClient) refreshDNShardsLocked() {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	for {
		state, err := c.env.GetClusterState(ctx)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		c.mu.dnshards = c.mu.dnshards[:0]
		for _, store := range state.DNState.Stores {
			for _, shard := range store.Shards {
				c.mu.dnshards = append(c.mu.dnshards, metadata.DNShard{
					DNShardRecord: metadata.DNShardRecord{
						ShardID: shard.ShardID,
					},
					ReplicaID: shard.ReplicaID,
					Address:   store.ServiceAddress,
				})
			}
		}
		sort.Slice(c.mu.dnshards, func(i, j int) bool {
			return c.mu.dnshards[i].ShardID < c.mu.dnshards[j].ShardID
		})
		break
	}
}

type kvTxn struct {
	op            client.TxnOperator
	env           service.Cluster
	router        func(key string) metadata.DNShard
	refreshRouter func()

	mu struct {
		sync.Mutex
		closed bool
	}
}

func (kop *kvTxn) Commit() error {
	kop.mu.Lock()
	defer kop.mu.Unlock()
	kop.mu.closed = true

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	return kop.op.Commit(ctx)
}

func (kop *kvTxn) Rollback() error {
	kop.mu.Lock()
	defer kop.mu.Unlock()
	if kop.mu.closed {
		return nil
	}

	kop.mu.closed = true

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	return kop.op.Rollback(ctx)
}

func (kop *kvTxn) Read(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	for {
		req := mem.NewGetTxnRequest([][]byte{[]byte(key)})
		req.CNRequest.Target = kop.router(key)
		result, err := kop.op.Read(ctx, []txn.TxnRequest{req})
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrDNShardNotFound) {
				kop.refreshRouter()
				continue
			}
			return "", err
		}
		defer result.Release()

		values := mem.MustParseGetPayload(result.Responses[0].CNOpResponse.Payload)
		if len(values) != 1 {
			panic("invalid read responses")
		}
		if values[0] == nil {
			return "", nil
		}
		return string(values[0]), nil
	}
}

func (kop *kvTxn) Write(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	for {
		req := mem.NewSetTxnRequest([][]byte{[]byte(key)}, [][]byte{[]byte(value)})
		req.CNRequest.Target = kop.router(key)

		result, err := kop.op.Write(ctx,
			[]txn.TxnRequest{req})
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrDNShardNotFound) {
				kop.refreshRouter()
				continue
			}
			return err
		}
		result.Release()
		return nil
	}

}
