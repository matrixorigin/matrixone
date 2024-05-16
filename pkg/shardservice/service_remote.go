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
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
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
}
