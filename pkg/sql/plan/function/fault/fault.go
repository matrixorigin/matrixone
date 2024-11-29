package fj

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type PodResponse struct {
	PodID     string `json:"cn_id,omitempty"`
	ReturnStr string `json:"return_str,omitempty"`
	ErrorStr  string `json:"error_str,omitempty"`

	ReturnList []fault.Point `json:"return_list,omitempty"`
}

func FaultInject(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	_ = rs
	args0 := vector.GenerateFunctionStrParameter(ivecs[0])
	args1 := vector.GenerateFunctionStrParameter(ivecs[1])
	args2 := vector.GenerateFunctionStrParameter(ivecs[2])

	if length != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "mo_ctl can only be called with const args")
	}

	arg0, _ := args0.GetStrValue(0)
	arg1, _ := args1.GetStrValue(0)
	arg2, _ := args2.GetStrValue(0)

	args := strings.Split(functionUtil.QuickBytesToStr(arg0), ".")
	service, pods := serviceType(args[0]), args[1:]
	command := strings.ToUpper(functionUtil.QuickBytesToStr(arg1))
	parameter := functionUtil.QuickBytesToStr(arg2)

	var res []PodResponse

	if service == tn || service == all {
		res = append(res, FaultInjectTn(pods, command, parameter, proc)...)
	}

	if service == cn || service == all {
		res = append(res, FaultInjectCn(pods, command, parameter, proc)...)
	}

	return nil
}

func FaultInjectCn(pods []string, command string, parameter string, proc *process.Process) []PodResponse {
	var cnRes []PodResponse

	if len(pods) == 0 {
		pods = make([]string, 0)
		clusterservice.GetMOCluster(proc.GetService()).GetCNService(clusterservice.Selector{}, func(cn metadata.CNService) bool {
			pods = append(pods, cn.ServiceID)
			return true
		})
	}

	for idx := range pods {
		res := PodResponse{
			PodID: pods[idx],
		}
		// the current cn also need to process this span cmd
		if pods[idx] == proc.GetQueryClient().ServiceID() {
			str := fault.HandleFaultInject(proc.Ctx, command, parameter)
			res.ReturnStr = str
		} else {
			request := proc.GetQueryClient().NewRequest(query.CmdMethod_FaultInject)
			request.FaultInjectionRequest = &query.FaultInjectRequest{
				Method:     command,
				Parameters: parameter,
			}
			// transfer query to another cn and receive its response
			resp, err := ctl.TransferRequest2OtherCNs(proc, pods[idx], request)
			if err != nil {
				res.ErrorStr = err.Error()
			} else if command == fault.ListFault {
				if err = jsoniter.Unmarshal([]byte(resp.FaultInjectionResponse.Resp), &res.ReturnList); err != nil {
					res.ErrorStr = err.Error()
				}
			} else {
				res.ReturnStr = resp.FaultInjectionResponse.Resp
			}
		}
		cnRes = append(cnRes, res)
	}

	return cnRes
}

func FaultInjectTn(pods []string, command string, parameter string, proc *process.Process) []PodResponse {
	var tnRes []PodResponse
	cluster := clusterservice.GetMOCluster(proc.GetService())

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

	var requests []txn.CNOpRequest
	cluster.GetTNService(clusterservice.NewSelector(),
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
			return nil
		}
		if len(responses) != len(requests) {
			panic("requests and response not match")
		}

	}

	return tnRes
}
