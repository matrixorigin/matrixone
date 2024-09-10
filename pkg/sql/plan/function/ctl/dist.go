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
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// GetTNHandlerFunc used to handle dn's debug command handle func.
// method: debug command type.
// whichDN: used to decide which DNs to send the debug request to, nil returned means send all dns.
// payload: used to get debug command request payload
// repsonseUnmarshaler: used to unmarshal response
func GetTNHandlerFunc(method api.OpCode,
	whichTN func(parameter string) ([]uint64, error),
	payload func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error),
	repsonseUnmarshaler func([]byte) (any, error)) handleFunc {
	return func(proc *process.Process,
		service serviceType,
		parameter string,
		sender requestSender) (Result, error) {
		if service != tn {
			return Result{}, moerr.NewNotSupportedf(proc.Ctx, "service %s not supported", service)
		}
		targetTNs, err := whichTN(parameter)
		if err != nil {
			return Result{}, moerr.ConvertGoError(proc.Ctx, err)
		}

		containsTN := func(id uint64) bool {
			for _, v := range targetTNs {
				if v == id {
					return true
				}
			}
			return false
		}

		cluster := clusterservice.GetMOCluster()
		var requests []txn.CNOpRequest
		cluster.GetTNService(clusterservice.NewSelector(),
			func(store metadata.TNService) bool {
				for _, shard := range store.Shards {
					if len(targetTNs) == 0 || containsTN(shard.ShardID) {
						payLoad, e := payload(shard.ShardID, parameter, proc)
						if e != nil {
							err = e
							return false
						}
						requests = append(requests, txn.CNOpRequest{
							OpCode: uint32(method),
							Target: metadata.TNShard{
								TNShardRecord: metadata.TNShardRecord{
									ShardID: shard.ShardID,
								},
								ReplicaID: shard.ReplicaID,
								Address:   store.TxnServiceAddress,
							},
							Payload: payLoad,
						})
					}
				}
				return true
			})
		if err != nil {
			return Result{}, err
		}

		results := make([]any, 0, len(requests))
		if len(requests) > 0 {
			responses, err := sender(proc.Ctx, proc, requests)
			if err != nil {
				return Result{}, err
			}
			if len(responses) != len(requests) {
				panic("requests and response not match")
			}

			for _, resp := range responses {
				r, err := repsonseUnmarshaler(resp.Payload)
				if err != nil {
					return Result{}, err
				}
				results = append(results, r)
			}
		}
		// remove "Op" prefix
		methodName, ok := api.OpMethodName[method]
		if !ok {
			return Result{Method: method.String(), Data: results}, nil
		}
		return Result{Method: methodName, Data: results}, nil
	}
}
