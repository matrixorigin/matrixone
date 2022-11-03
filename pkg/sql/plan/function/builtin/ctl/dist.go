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

package ctl

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// getDNHandlerFunc used to handle dn's debug command handle func.
// method: debug command type.
// whichDN: used to decide which DNs to send the debug request to, nil returned means send all dns.
// payload: used to get debug command request payload
// repsonseUnmarshaler: used to unmarshal response
func getDNHandlerFunc(method pb.CmdMethod,
	whichDN func(parameter string) ([]uint64, error),
	payload func(dnShardID uint64, parameter string, proc *process.Process) ([]byte, error),
	repsonseUnmarshaler func([]byte) (interface{}, error)) handleFunc {
	return func(proc *process.Process,
		service serviceType,
		parameter string,
		sender requestSender) (pb.CtlResult, error) {
		if service != dn {
			return pb.CtlResult{}, moerr.NewNotSupported("service %s not supported", service)
		}
		targetDNs, err := whichDN(parameter)
		if err != nil {
			return pb.CtlResult{}, moerr.ConvertGoError(err)
		}

		containsDN := func(id uint64) bool {
			for _, v := range targetDNs {
				if v == id {
					return true
				}
			}
			return false
		}

		detail, err := proc.GetClusterDetails()
		if err != nil {
			return pb.CtlResult{}, err
		}
		var requests []txn.CNOpRequest
		for _, store := range detail.GetDNStores() {
			for _, shard := range store.Shards {
				if len(targetDNs) == 0 || containsDN(shard.ShardID) {
					payLoad, err := payload(shard.ShardID, parameter, proc)
					if err != nil {
						return pb.CtlResult{}, err
					}
					requests = append(requests, txn.CNOpRequest{
						OpCode: uint32(method),
						Target: metadata.DNShard{
							DNShardRecord: metadata.DNShardRecord{
								ShardID: shard.ShardID,
							},
							ReplicaID: shard.ReplicaID,
							Address:   store.ServiceAddress,
						},
						Payload: payLoad,
					})
				}
			}
		}
		results := make([]interface{}, 0, len(requests))
		if len(requests) > 0 {
			responses, err := sender(proc.Ctx, requests)
			if err != nil {
				return pb.CtlResult{}, err
			}
			if len(responses) != len(requests) {
				panic("requests and response not match")
			}

			for _, resp := range responses {
				r, err := repsonseUnmarshaler(resp.Payload)
				if err != nil {
					return pb.CtlResult{}, err
				}
				results = append(results, r)
			}
		}
		return pb.CtlResult{Method: method.String(), Data: results}, nil
	}
}
