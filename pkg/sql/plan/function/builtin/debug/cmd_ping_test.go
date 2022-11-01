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

package debug

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"testing"

	"github.com/fagongzi/util/protoc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/debug"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdPingDNWithEmptyDN(t *testing.T) {
	ctx := context.Background()
	clusterDetails := func() (logservice.ClusterDetails, error) {
		return logservice.ClusterDetails{}, nil
	}
	proc := process.New(ctx, nil, nil, nil, nil, clusterDetails)
	result, err := handlePing()(proc,
		dn,
		"",
		func(ctx context.Context, cr []txn.CNOpRequest) ([]txn.CNOpResponse, error) {
			return nil, nil
		})
	require.NoError(t, err)
	assert.Equal(t, pb.DebugResult{Method: pb.CmdMethod_Ping.String(), Data: make([]interface{}, 0)},
		result)
}

func TestCmdPingDNWithSingleDN(t *testing.T) {
	shardID := uint64(1)
	ctx := context.Background()
	clusterDetails := func() (logservice.ClusterDetails, error) {
		return logservice.ClusterDetails{
			DNStores: []logservice.DNStore{
				{
					Shards: []logservice.DNShardInfo{
						{
							ShardID: 1,
						},
					},
				},
			},
		}, nil
	}
	proc := process.New(ctx, nil, nil, nil, nil, clusterDetails)
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
	assert.Equal(t, pb.DebugResult{
		Method: pb.CmdMethod_Ping.String(),
		Data:   []interface{}{pb.DNPingResponse{ShardID: shardID}},
	}, result)
}

func TestCmdPingDNWithMultiDN(t *testing.T) {
	ctx := context.Background()
	clusterDetails := func() (logservice.ClusterDetails, error) {
		return logservice.ClusterDetails{
			DNStores: []logservice.DNStore{
				{
					Shards: []logservice.DNShardInfo{
						{
							ShardID: 1,
						},
						{
							ShardID: 2,
						},
					},
				},
			},
		}, nil
	}
	proc := process.New(ctx, nil, nil, nil, nil, clusterDetails)
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
	assert.Equal(t, pb.DebugResult{
		Method: pb.CmdMethod_Ping.String(),
		Data:   []interface{}{pb.DNPingResponse{ShardID: 1}, pb.DNPingResponse{ShardID: 2}},
	}, result)
}

func TestCmdPingDNWithParameter(t *testing.T) {
	ctx := context.Background()
	clusterDetails := func() (logservice.ClusterDetails, error) {
		return logservice.ClusterDetails{
			DNStores: []logservice.DNStore{
				{
					Shards: []logservice.DNShardInfo{
						{
							ShardID: 1,
						},
						{
							ShardID: 2,
						},
					},
				},
			},
		}, nil
	}
	proc := process.New(ctx, nil, nil, nil, nil, clusterDetails)
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
	assert.Equal(t, pb.DebugResult{
		Method: pb.CmdMethod_Ping.String(),
		Data:   []interface{}{pb.DNPingResponse{ShardID: 1}},
	}, result)
}
