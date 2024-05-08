// Copyright 2023 Matrix Origin
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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// handleEnableSpan enable or disable spans for some specified operation in TN service
// or/and CN service, this operation last exceeds than the threshold (ms), it will record
// this operation. threshold means not to consider time threshold.
//
// the cmd format for CN service:
// 		mo_ctl("cn", "TranceSpan" "uuids of cn:enable/disable:kinds of span:time threshold")
// examples as below:
// 		mo_ctl("cn", "TraceSpan", "cn_uuid1:enable:s3:0")
// 		mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:enable:s3,local,...:1")
// 		mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:enable:all:1")
// 		mo_ctl("cn", "TraceSpan", "all:enable:all:1)
//
// the cmd format for TN service:
// 		mo_ctl("dn", "TraceSpan", "enable/disable:kinds of span:time threshold")
// (because there only exist one dn service, so we don't need to specify the uuid,
// 		but, the uuid will be ignored and will not check its validation even though it is specified.)
// examples as below:
// mo_ctl("dn", "TraceSpan", "disable:s3:10")
// mo_ctl("dn", "TraceSpan", "disable:local:1000")
// mo_ctl("dn", "TraceSpan", "disable:s3, local,...:0")
// mo_ctl("dn", "TraceSpan", "enable:all:0")

var cmd2State = map[string]bool{
	"enable":  true,
	"disable": false,
}

func checkParameter(param string, ignoreUUID bool) (args []string, threshold int64, err error) {
	param = strings.ToLower(param)
	// [uuids], enable/disable, spans, time threshold
	args = strings.Split(param, ":")

	// cmd for tn will ignore the uuid
	if (ignoreUUID && len(args) < 3) ||
		(!ignoreUUID && len(args) != 4) {
		return nil, 0, moerr.NewInternalErrorNoCtx("parameter invalid")
	}

	cmdIdx := 0
	if !ignoreUUID { // contains uuids
		cmdIdx = 1
	}
	_, ok := cmd2State[args[cmdIdx]]
	if !ok {
		return nil, 0, moerr.NewInternalErrorNoCtx("cmd invalid, expected enable or disable")
	}

	threshold, err = strconv.ParseInt(args[len(args)-1], 10, 64)
	if err != nil {
		return nil, 0, moerr.NewInvalidArgNoCtx("threshold", "convert to int failed")
	}

	return args, threshold, nil
}

func handleTraceSpan(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	if service != cn && service != tn {
		return Result{}, moerr.NewWrongServiceNoCtx("CN or DN", string(service))
	}

	if service == tn {
		return send2TNAndWaitResp(proc, service, parameter, sender)
	}

	args, threshold, err := checkParameter(parameter, false)
	if err != nil {
		return Result{}, err
	}

	// the uuids of cn
	cns := strings.Split(args[0], ",")

	if len(cns) == 1 && strings.ToLower(cns[0]) == "all" {
		cns = make([]string, 0)
		clusterservice.GetMOCluster().GetCNService(clusterservice.Selector{}, func(cn metadata.CNService) bool {
			cns = append(cns, cn.ServiceID)
			return true
		})
	}

	info := map[string]string{}
	for idx := range cns {
		// the current cn also need to process this span cmd
		if cns[idx] == proc.QueryClient.ServiceID() {
			info[cns[idx]] = SelfProcess(args[1], args[2], threshold)
		} else {
			// transfer query to another cn and receive its response
			resp, _ := transferRequest(proc, cns[idx], args[1], args[2], threshold)
			if resp == nil {
				// no such cn service
				info[cns[idx]] = "no such cn service"
			} else {
				info[cns[idx]] = resp.TraceSpanResponse.Resp
			}
		}
	}

	data := ""
	for k, v := range info {
		data += fmt.Sprintf("%s:%s; ", k, v)
	}

	return Result{
		Method: TraceSpanMethod,
		Data:   data,
	}, nil
}

func SelfProcess(cmd string, spans string, threshold int64) string {
	var succeed, failed []string
	ss := strings.Split(spans, ",")
	for _, t := range ss {
		if trace.SetMoCtledSpanState(t, cmd2State[cmd], threshold) {
			succeed = append(succeed, t)
		} else {
			failed = append(failed, t)
		}
	}

	return fmt.Sprintf("%v %sd, %v failed", succeed, cmd, failed)
}

func transferRequest(proc *process.Process, uuid string, cmd string, spans string, threshold int64) (resp *query.Response, err error) {
	clusterservice.GetMOCluster().GetCNService(clusterservice.NewServiceIDSelector(uuid),
		func(cn metadata.CNService) bool {
			request := proc.QueryClient.NewRequest(query.CmdMethod_TraceSpan)
			request.TraceSpanRequest = &query.TraceSpanRequest{
				Cmd:       cmd,
				Spans:     spans,
				Threshold: threshold,
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			resp, err = proc.QueryClient.SendMessage(ctx, cn.QueryAddress, request)
			return true
		})
	return
}

func send2TNAndWaitResp(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {

	whichTN := func(string) ([]uint64, error) { return nil, nil }
	payloadFn := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
		args, threshold, err := checkParameter(parameter, true)
		if err != nil {
			return nil, err
		}

		if len(args) == 4 {
			args = args[1:]
		}

		req := db.TraceSpan{
			Cmd:       args[0],
			Spans:     args[1],
			Threshold: threshold,
		}

		return req.MarshalBinary()
	}

	repsonseUnmarshaler := func(b []byte) (interface{}, error) {
		return string(b[:]), nil
	}

	return GetTNHandlerFunc(
		api.OpCode_OpTraceSpan, whichTN, payloadFn, repsonseUnmarshaler,
	)(proc, service, parameter, sender)

}
