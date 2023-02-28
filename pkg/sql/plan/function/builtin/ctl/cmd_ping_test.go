// Copyright 2021 - 2022 Matrix Origin
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

package ctl

import (
	"context"
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdPingDNWithEmptyDN(t *testing.T) {
	ctx := context.Background()
	initTestRuntime()
	proc := process.New(ctx, nil, nil, nil, nil)
	result, err := handlePing()(proc,
		dn,
		"",
		func(ctx context.Context, cr []txn.CNOpRequest) ([]txn.CNOpResponse, error) {
			return nil, nil
		})
	require.NoError(t, err)
	assert.Equal(t, pb.CtlResult{Method: pb.CmdMethod_Ping.String(), Data: make([]interface{}, 0)},
		result)
}

func TestCmdPingDNWithSingleDN(t *testing.T) {
	initTestRuntime(1)

	shardID := uint64(1)
	ctx := context.Background()
	proc := process.New(ctx, nil, nil, nil, nil)
	result, err := handlePing()(proc,
		dn,
		"",
		func(ctx context.Context, cr []txn.CNOpRequest) ([]txn.CNOpResponse, error) {
			return []txn.CNOpResponse{
				{
					Payload: protoc.MustMarshal(&pb.DNPingResponse{ShardID: shardID}),
				},
			}, nil
		})
	require.NoError(t, err)
	assert.Equal(t, pb.CtlResult{
		Method: pb.CmdMethod_Ping.String(),
		Data:   []interface{}{pb.DNPingResponse{ShardID: shardID}},
	}, result)
}

func TestCmdPingDNWithMultiDN(t *testing.T) {
	ctx := context.Background()
	initTestRuntime(1, 2)
	proc := process.New(ctx, nil, nil, nil, nil)
	result, err := handlePing()(proc,
		dn,
		"",
		func(ctx context.Context, cr []txn.CNOpRequest) ([]txn.CNOpResponse, error) {
			return []txn.CNOpResponse{
				{
					Payload: protoc.MustMarshal(&pb.DNPingResponse{ShardID: 1}),
				},
				{
					Payload: protoc.MustMarshal(&pb.DNPingResponse{ShardID: 2}),
				},
			}, nil
		})
	require.NoError(t, err)
	assert.Equal(t, pb.CtlResult{
		Method: pb.CmdMethod_Ping.String(),
		Data:   []interface{}{pb.DNPingResponse{ShardID: 1}, pb.DNPingResponse{ShardID: 2}},
	}, result)
}

func TestCmdPingDNWithParameter(t *testing.T) {
	ctx := context.Background()
	initTestRuntime(1, 2)
	proc := process.New(ctx, nil, nil, nil, nil)
	result, err := handlePing()(proc,
		dn,
		"1",
		func(ctx context.Context, cr []txn.CNOpRequest) ([]txn.CNOpResponse, error) {
			return []txn.CNOpResponse{
				{
					Payload: protoc.MustMarshal(&pb.DNPingResponse{ShardID: 1}),
				},
			}, nil
		})
	require.NoError(t, err)
	assert.Equal(t, pb.CtlResult{
		Method: pb.CmdMethod_Ping.String(),
		Data:   []interface{}{pb.DNPingResponse{ShardID: 1}},
	}, result)
}

func initTestRuntime(shardIDs ...uint64) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	var shards []metadata.DNShard
	for _, id := range shardIDs {
		shards = append(shards, metadata.DNShard{
			DNShardRecord: metadata.DNShardRecord{ShardID: id},
		})
	}

	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(nil, []metadata.DNService{
			{
				Shards: shards,
			},
		}))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)
}
