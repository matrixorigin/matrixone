// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

type TxnClient interface {
	getLatestCheckpoint(ctx context.Context) (*api.CheckpointResp, error)
	close()
}

type HAKeeperClientFactory func(
	ctx context.Context,
	sid string,
	cfg logservice.HAKeeperClientConfig,
) logservice.ClusterHAKeeperClient

type txnClient struct {
	common

	// mc is the instance of MOCluster which is the memory cache of HAKeeper.
	mc clusterservice.MOCluster

	// haKeeperClientFactory is the factory to create logservice.LogHAKeeperClient.
	haKeeperClientFactory HAKeeperClientFactory

	// haKeeperClient is the client to communicate with HAKeeper server.
	haKeeperClient logservice.ClusterHAKeeperClient

	// client is the client to communicate wit transaction server.
	client rpc.TxnSender
}

// newTxnClient creates a txnClient instance.
func newTxnClient(common common, factory HAKeeperClientFactory) *txnClient {
	c := &txnClient{
		common:                common,
		haKeeperClientFactory: factory,
	}
	if c.haKeeperClientFactory == nil {
		c.haKeeperClientFactory = logservice.NewLogHAKeeperClientWithRetry
	}
	client, err := rpc.NewSender(common.rpcConfig, common.rt)
	if err != nil {
		panic(fmt.Sprintf("failed to create txn client: %v", err))
	}
	c.client = client
	return c
}

func (c *txnClient) close() {
	if c.mc != nil {
		c.mc.Close()
	}
	if c.haKeeperClient != nil {
		_ = c.haKeeperClient.Close()
	}
	if c.client != nil {
		_ = c.client.Close()
	}
}

func (c *txnClient) prepare(ctx context.Context) {
	if c.mc == nil {
		if c.haKeeperClient == nil {
			c.haKeeperClient = c.haKeeperClientFactory(ctx, c.sid, c.haKeeperConfig)
		}
		c.mc = clusterservice.NewMOCluster(
			c.sid,
			c.haKeeperClient,
			time.Second*3,
		)
	}

	timer := time.NewTimer(time.Minute * 10)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			panic("wait for tn start timeout")

		default:
			var num int
			c.mc.GetTNService(clusterservice.NewSelector(), func(service metadata.TNService) bool {
				num = len(service.Shards)
				return true
			})
			if num == 0 {
				time.Sleep(time.Second)
			} else {
				return
			}
		}
	}
}

func (c *txnClient) getLatestCheckpoint(ctx context.Context) (*api.CheckpointResp, error) {
	c.prepare(ctx)
	var cnOpRequest *txn.CNOpRequest
	c.mc.GetTNService(clusterservice.NewSelector(), func(service metadata.TNService) bool {
		for _, shard := range service.Shards {
			cnOpRequest = &txn.CNOpRequest{
				OpCode: uint32(api.OpCode_OpGetLatestCheckpoint),
				Target: metadata.TNShard{
					TNShardRecord: metadata.TNShardRecord{
						ShardID: shard.ShardID,
					},
					ReplicaID: shard.ReplicaID,
					Address:   service.TxnServiceAddress,
				},
			}
			break
		}
		return false
	})
	if cnOpRequest == nil {
		return nil, moerr.NewInternalError(ctx, "cannot get request")
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()
	result, err := c.client.Send(ctx, []txn.TxnRequest{
		{
			Method:    txn.TxnMethod_DEBUG,
			CNRequest: cnOpRequest,
		},
	})
	if err != nil {
		return nil, err
	}
	defer result.Release()

	if len(result.Responses) != 1 {
		panic(fmt.Sprintf("should return only one response, but got %d", len(result.Responses)))
	}
	if result.Responses[0].CNOpResponse == nil {
		return nil, moerr.NewInternalError(ctx, "response is nil")
	}
	resp := &api.CheckpointResp{}
	if err := resp.Unmarshal(result.Responses[0].CNOpResponse.Payload); err != nil {
		return nil, err
	}
	return resp, nil
}
