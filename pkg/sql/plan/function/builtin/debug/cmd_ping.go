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

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/debug"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func handlePing(ctx context.Context,
	serviceType serviceType,
	parameter string,
	sender requestSender,
	clusterDetailsGetter engine.GetClusterDetailsFunc) (pb.DebugResult, error) {

	shardID := uint64(0)
	if len(parameter) > 0 {
		id, err := format.ParseStringUint64(parameter)
		if err != nil {
			return pb.DebugResult{}, moerr.ConvertGoError(err)
		}
		shardID = id
	}

	switch serviceType {
	case dn:
		detail, err := clusterDetailsGetter()
		if err != nil {
			return pb.DebugResult{}, err
		}

		var requests []txn.CNOpRequest
		for _, store := range detail.GetDNStores() {
			for _, shard := range store.Shards {
				if shardID == 0 || shard.ShardID == shardID {
					requests = append(requests, txn.CNOpRequest{
						OpCode: uint32(pb.CmdMethod_Ping),
						Target: metadata.DNShard{
							DNShardRecord: metadata.DNShardRecord{
								ShardID: shard.ShardID,
							},
							ReplicaID: shard.ReplicaID,
							Address:   store.ServiceAddress,
						},
						Payload: protoc.MustMarshal(&pb.DNPingRequest{Parameter: parameter}),
					})
				}
			}
		}
		results := make([]pb.DNPingResponse, 0, len(requests))
		if len(requests) > 0 {
			responses, err := sender(ctx, requests)
			if err != nil {
				return pb.DebugResult{}, err
			}
			if len(responses) != len(requests) {
				panic("requests and response not match")
			}

			for _, resp := range responses {
				pong := pb.DNPingResponse{}
				protoc.MustUnmarshal(&pong, resp.Payload)
				results = append(results, pong)
			}
		}
		return pb.DebugResult{Method: pb.CmdMethod_Ping.String(), Data: results}, nil
	default:
		return pb.DebugResult{}, moerr.NewNotSupported("ping service %s not supported", serviceType)
	}
}
