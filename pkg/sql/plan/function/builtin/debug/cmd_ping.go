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

func handlePing() handleFunc {
	return getDNHandlerFunc(
		pb.CmdMethod_Ping,
		func(parameter string) ([]uint64, error) {
			if len(parameter) > 0 {
				id, err := format.ParseStringUint64(parameter)
				if err != nil {
					return nil, err
				}
				return []uint64{id}, nil
			}
			return nil, nil
		},
		func(dnShardID uint64, parameter string) []byte {
			return protoc.MustMarshal(&pb.DNPingRequest{Parameter: parameter})
		})
}

// getDNHandlerFunc used to handle dn's debug command handle func.
// method: debug command type.
// whichDN: used to decide which DNs to send the debug request to, nil returned means send all dns.
// payload: used to get debug command request payload
func getDNHandlerFunc(method pb.CmdMethod,
	whichDN func(parameter string) ([]uint64, error),
	payload func(dnShardID uint64, parameter string) []byte) handleFunc {
	return func(ctx context.Context,
		service serviceType,
		parameter string,
		sender requestSender,
		clusterDetailsGetter engine.GetClusterDetailsFunc) (pb.DebugResult, error) {
		if service != dn {
			return pb.DebugResult{}, moerr.NewNotSupported("ping service %s not supported", service)
		}
		targetDNs, err := whichDN(parameter)
		if err != nil {
			return pb.DebugResult{}, moerr.ConvertGoError(err)
		}

		containsDN := func(id uint64) bool {
			for _, v := range targetDNs {
				if v == id {
					return true
				}
			}
			return false
		}

		detail, err := clusterDetailsGetter()
		if err != nil {
			return pb.DebugResult{}, err
		}
		var requests []txn.CNOpRequest
		for _, store := range detail.GetDNStores() {
			for _, shard := range store.Shards {
				if len(targetDNs) == 0 || containsDN(shard.ShardID) {
					requests = append(requests, txn.CNOpRequest{
						OpCode: uint32(method),
						Target: metadata.DNShard{
							DNShardRecord: metadata.DNShardRecord{
								ShardID: shard.ShardID,
							},
							ReplicaID: shard.ReplicaID,
							Address:   store.ServiceAddress,
						},
						Payload: payload(shard.ShardID, parameter),
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
		return pb.DebugResult{Method: method.String(), Data: results}, nil
	}
}
