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
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strconv"
	"strings"
)

func handleTNAddFaultPoint() handleFunc {
	return GetTNHandlerFunc(
		api.OpCode_OpAddFaultPoint,
		func(string) ([]uint64, error) { return nil, nil },
		func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			// parameter like "name.freq.action.iarg.sarg"
			parameters := strings.Split(parameter, ".")
			if len(parameters) == 1 {
				payload, err := types.Encode(&cmd_util.FaultPoint{
					Name: parameters[0],
				})
				if err != nil {
					return nil, moerr.NewInternalError(proc.Ctx, "payload encode err")
				}
				return payload, nil
			}
			if len(parameters) != 5 {
				return nil, moerr.NewInternalError(proc.Ctx, "handleAddFaultPoint: invalid argument!")
			}
			name := parameters[0]
			if name == "" {
				return nil, moerr.NewInternalError(proc.Ctx, "handleAddFaultPoint: fault point's name should not be empty!")
			}
			freq := parameters[1]
			action := parameters[2]
			iarg, err := strconv.Atoi(parameters[3])
			if err != nil {
				return nil, moerr.NewInternalErrorf(proc.Ctx, "handleAddFaultPoint: %s", err.Error())
			}
			sarg := parameters[4]
			payload, err := types.Encode(&cmd_util.FaultPoint{
				Name:   name,
				Freq:   freq,
				Action: action,
				Iarg:   int64(iarg),
				Sarg:   sarg,
			})
			if err != nil {
				return nil, moerr.NewInternalError(proc.Ctx, "payload encode err")
			}
			return payload, nil
		},
		func(data []byte) (any, error) {
			resp := api.TNStringResponse{}
			protoc.MustUnmarshal(&resp, data)
			return resp, nil
		})
}

type CNResponse struct {
	CNid      string `json:"cn_id,omitempty"`
	ReturnStr string `json:"return_str,omitempty"`
	ErrorStr  string `json:"error_str,omitempty"`
}

func handleAddFaultPoint(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	if service != cn && service != tn {
		return Result{}, moerr.NewWrongServiceNoCtx("CN or DN", string(service))
	}

	if service == tn {
		return handleTNAddFaultPoint()(proc, service, parameter, sender)
	}

	// parameter like "cnid.name.freq.action.iarg.sarg"
	name, freq, action, iarg, sarg, cns, err := getInputs(proc.Ctx, parameter)
	if err != nil {
		return Result{}, err
	}

	if len(cns) == 1 && strings.ToLower(cns[0]) == "all" {
		cns = make([]string, 0)
		clusterservice.GetMOCluster(proc.GetService()).GetCNService(clusterservice.Selector{}, func(cn metadata.CNService) bool {
			cns = append(cns, cn.ServiceID)
			return true
		})
	}

	cnRes := make([]CNResponse, 0)

	for idx := range cns {
		res := CNResponse{
			CNid: cns[idx],
		}
		// the current cn also need to process this span cmd
		if cns[idx] == proc.GetQueryClient().ServiceID() {
			res.ReturnStr = HandleCnFaultInjection(proc.Ctx, name, freq, action, iarg, sarg)
		} else {
			request := proc.GetQueryClient().NewRequest(query.CmdMethod_FaultInjection)
			request.FaultInjectionRequest = &query.FaultInjectionRequest{
				Name:   name,
				Freq:   freq,
				Action: action,
				Iarg:   iarg,
				Sarg:   sarg,
			}
			// transfer query to another cn and receive its response
			resp, err := transferRequest2OtherCNs(proc, cns[idx], request)
			if err != nil {
				res.ErrorStr = err.Error()
			} else {
				res.ReturnStr = resp.TraceSpanResponse.Resp
			}
		}
		cnRes = append(cnRes, res)
	}

	return Result{
		Method: AddFaultPointMethod,
		Data:   cnRes,
	}, nil
}

func getInputs(ctx context.Context, input string) (
	name string,
	freq string,
	action string,
	iarg int64,
	sarg string,
	cns []string,
	err error,
) {
	args := strings.Split(input, ".")
	// cn uuid
	if len(args) < 2 {
		err = moerr.NewInternalError(ctx, "invalid argument! should be like \"cnid.name.freq.action.iarg.sarg\"")
		return
	}
	cns = strings.Split(args[0], ",")
	name = args[1]
	if name == "" {
		err = moerr.NewInternalError(ctx, "fault point's name should not be empty!")
		return
	}
	if len(args) == 2 {
		return
	}
	if len(args) != 6 {
		err = moerr.NewInternalError(ctx, "invalid argument! should be like \"cnid.name.freq.action.iarg.sarg\"")
		return
	}
	freq = args[2]
	action = args[3]
	i, err := strconv.Atoi(args[4])
	iarg = int64(i)
	if err != nil {
		return
	}
	sarg = args[5]

	return
}

const (
	enable  = "enable_fault_injection"
	disable = "disable_fault_injection"
	status  = "status_fault_injection"
)

func HandleCnFaultInjection(
	ctx context.Context,
	name string,
	freq string,
	action string,
	iarg int64,
	sarg string,
) (res string) {
	switch name {
	case enable:
		res = "fault injection enabled, previous status: "
		if fault.Enable() {
			res += "disabled"
		} else {
			res += "enabled"
		}
	case disable:
		res = "fault injection disabled, previous status: "
		if fault.Disable() {
			res += "enabled"
		} else {
			res += "disabled"
		}
	case status:
		if fault.Status() {
			res = "fault injection is enabled"
		} else {
			res = "fault injection is disabled"
		}
	default:
		if err := fault.AddFaultPoint(ctx, name, freq, action, iarg, sarg); err != nil {
			return err.Error()
		}
		res = "OK"
	}
	return
}
