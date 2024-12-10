// Copyright 2023 Matrix Origin
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

package fj

import (
	"strings"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/util/json"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type PodResponse struct {
	PodType   serviceType `json:"pod_type,omitempty"`
	PodID     string      `json:"pod_id,omitempty"`
	ReturnStr string      `json:"return_str,omitempty"`
	ErrorStr  string      `json:"error_str,omitempty"`

	ReturnList []fault.Point `json:"return_list,omitempty"`
}

func FaultInject(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	_ = rs
	args0 := vector.GenerateFunctionStrParameter(ivecs[0])
	args1 := vector.GenerateFunctionStrParameter(ivecs[1])
	args2 := vector.GenerateFunctionStrParameter(ivecs[2])

	if length != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "fault inject can only be called with const args")
	}

	arg0, _ := args0.GetStrValue(0)
	arg1, _ := args1.GetStrValue(0)
	arg2, _ := args2.GetStrValue(0)

	args := strings.Split(functionUtil.QuickBytesToStr(arg0), ".")
	service, pods := serviceType(strings.ToUpper(args[0])), strings.Split(args[1], ",")
	if pods[0] == "" {
		pods = []string{}
	}
	command := strings.ToUpper(functionUtil.QuickBytesToStr(arg1))
	parameter := functionUtil.QuickBytesToStr(arg2)

	if service != tn && service != cn && service != all {
		return moerr.NewNotSupportedf(proc.Ctx, "service %s not supported", service)
	}

	var res []PodResponse

	if service == tn || service == all {
		res = append(res, TNFaultInject(pods, command, parameter, proc)...)
	}

	if service == cn || service == all {
		res = append(res, CNFaultInject(pods, command, parameter, proc)...)
	}

	return rs.AppendBytes(json.Pretty(res), false)
}

func CNFaultInject(pods []string, command string, parameter string, proc *process.Process) []PodResponse {
	cnRes := make([]PodResponse, len(pods))

	if len(pods) == 0 {
		clusterservice.GetMOCluster(proc.GetService()).GetCNService(clusterservice.Selector{}, func(cn metadata.CNService) bool {
			pods = append(pods, cn.ServiceID)
			return true
		})
	}

	for idx := range pods {
		res := PodResponse{
			PodType: cn,
			PodID:   pods[idx],
		}
		// the current cn also need to process this span cmd
		if pods[idx] == proc.GetQueryClient().ServiceID() {
			str := fault.HandleFaultInject(proc.Ctx, command, parameter)
			unmarshalResp(command, []byte(str), &res)
		} else {
			request := proc.GetQueryClient().NewRequest(query.CmdMethod_FaultInject)
			request.FaultInjectRequest = &query.FaultInjectRequest{
				Method:     command,
				Parameters: parameter,
			}
			// transfer query to another cn and receive its response
			resp, err := ctl.TransferRequest2OtherCNs(proc, pods[idx], request)
			if err != nil {
				res.ErrorStr = err.Error()
			} else {
				unmarshalResp(command, []byte(resp.FaultInjectResponse.Resp), &res)
			}
		}
		cnRes = append(cnRes, res)
	}

	return cnRes
}

func TNFaultInject(pods []string, command string, parameter string, proc *process.Process) []PodResponse {
	tnRes := make([]PodResponse, len(pods))
	cluster := clusterservice.GetMOCluster(proc.GetService())

	if len(pods) == 0 {
		cluster.GetTNService(clusterservice.NewSelector(), func(store metadata.TNService) bool {
			pods = append(pods, store.ServiceID)
			return true
		})
	}

	containsTN := func(id string) bool {
		if len(pods) == 0 {
			return true
		}
		for _, v := range pods {
			if v == id {
				return true
			}
		}
		return false
	}

	unmarshaler := func(data []byte) any {
		resp := api.TNStringResponse{}
		protoc.MustUnmarshal(&resp, data)
		return resp
	}

	excuteRequest := func(id string) PodResponse {
		res := PodResponse{
			PodType: tn,
			PodID:   id,
		}
		var requests []txn.CNOpRequest
		cluster.GetTNService(clusterservice.NewServiceIDSelector(id),
			func(store metadata.TNService) bool {
				if !containsTN(store.ServiceID) {
					return true
				}
				for _, shard := range store.Shards {
					payLoad, e := types.Encode(&cmd_util.FaultInjectReq{
						Method:    command,
						Parameter: parameter,
					})
					if e != nil {
						return false
					}
					requests = append(requests, txn.CNOpRequest{
						OpCode: uint32(api.OpCode_OpFaultInject),
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
				return true
			})

		if len(requests) > 0 {
			responses, err := ctl.MoCtlTNCmdSender(proc.Ctx, proc, requests)
			if err != nil {
				res.ErrorStr = err.Error()
				return res
			}
			if len(responses) != len(requests) {
				panic("requests and response not match")
			}
			for _, resp := range responses {
				rs := unmarshaler(resp.Payload).(api.TNStringResponse)
				unmarshalResp(command, []byte(rs.ReturnStr), &res)
			}
		}
		return res
	}

	for idx := range pods {
		tnRes = append(tnRes, excuteRequest(pods[idx]))
	}

	return tnRes
}
