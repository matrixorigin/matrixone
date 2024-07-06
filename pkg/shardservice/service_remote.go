// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func (s *service) initRemote() {
	s.remote.cluster = clusterservice.GetMOCluster()
	s.remote.pool = morpc.NewMessagePool(
		func() *pb.Request {
			return &pb.Request{}
		},
		func() *pb.Response {
			return &pb.Response{}
		},
	)

	s.initRemoteClient()

	svr, err := morpc.NewMessageHandler(
		"shard-service",
		s.cfg.ListenAddress,
		s.cfg.RPC,
		s.remote.pool,
	)
	if err != nil {
		panic(err)
	}
	s.remote.server = svr

	s.remote.server.RegisterMethod(
		uint32(pb.Method_ShardRead),
		s.handleRemoteRead,
		true,
	)

	if err := s.remote.server.Start(); err != nil {
		panic(err)
	}
}

func (s *service) initRemoteClient() {
	c, err := morpc.NewMethodBasedClient(
		"shard-service",
		s.cfg.RPC,
		s.remote.pool,
	)
	if err != nil {
		panic(err)
	}
	s.remote.client = c

	// register rpc method
	s.remote.client.RegisterMethod(
		uint32(pb.Method_Heartbeat),
		func(r *pb.Request) (string, error) {
			return getTNAddress(s.remote.cluster), nil
		},
	)
	s.remote.client.RegisterMethod(
		uint32(pb.Method_CreateShards),
		func(r *pb.Request) (string, error) {
			return getTNAddress(s.remote.cluster), nil
		},
	)
	s.remote.client.RegisterMethod(
		uint32(pb.Method_DeleteShards),
		func(r *pb.Request) (string, error) {
			return getTNAddress(s.remote.cluster), nil
		},
	)
	s.remote.client.RegisterMethod(
		uint32(pb.Method_GetShards),
		func(r *pb.Request) (string, error) {
			return getTNAddress(s.remote.cluster), nil
		},
	)

	s.remote.client.RegisterMethod(
		uint32(pb.Method_ShardRead),
		func(r *pb.Request) (string, error) {
			return getCNAddress(r.ShardRead.CN, s.remote.cluster), nil
		},
	)
}

func (s *service) handleRemoteRead(
	ctx context.Context,
	req *pb.Request,
	resp *pb.Response,
	buffer *morpc.Buffer,
) error {
	value, err := s.doRead(
		ctx,
		req.ShardRead.Shard,
		req.ShardRead.ReadAt,
		int(req.ShardRead.Method),
		req.ShardRead.Param,
		buffer,
	)
	if err != nil {
		return err
	}
	resp.ShardRead.Payload = value
	return nil
}

func (s *service) validReplica(
	shard pb.TableShard,
	replica pb.ShardReplica,
) error {
	current, ok := s.getAllocatedShard(shard.TableID, shard.ShardID)
	if !ok {
		return moerr.NewReplicaNotFound(replica.String())
	}
	if !shard.Same(current) {
		return moerr.NewReplicaNotMatch(current.String(), shard.String())
	}

	for _, r := range current.Replicas {
		if r.ReplicaID != replica.ReplicaID {
			continue
		}

		if r.Same(replica) {
			return nil
		}

		return moerr.NewReplicaNotMatch(r.String(), shard.String())
	}

	return moerr.NewReplicaNotFound(replica.String())
}

func (s *service) send(
	req *pb.Request,
) (*pb.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	return s.unwrapError(
		s.remote.client.Send(ctx, req),
	)
}

func (s *service) unwrapError(
	resp *pb.Response,
	err error,
) (*pb.Response, error) {
	if err != nil {
		return nil, err
	}
	if err := resp.UnwrapError(); err != nil {
		s.remote.pool.ReleaseResponse(resp)
		return nil, err
	}
	return resp, nil
}

func (s *service) newReadRequest(
	shard pb.TableShard,
	method int,
	param pb.ReadParam,
	readAt timestamp.Timestamp,
) *pb.Request {
	req := s.remote.pool.AcquireRequest()
	req.RPCMethod = pb.Method_ShardRead
	req.ShardRead.Shard = shard
	req.ShardRead.Method = uint32(method)
	req.ShardRead.Param = param
	req.ShardRead.CN = shard.Replicas[0].CN
	req.ShardRead.ReadAt = readAt
	return req
}
