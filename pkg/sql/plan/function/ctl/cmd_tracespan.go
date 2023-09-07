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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// handleEnableSpan enable or disable spans for some specified operation in TN service
// or/and CN service.
//
// the cmd format for CN service:
// 		mo_ctl("cn", "TranceSpan" "uuids of cn:enable/disable:kinds of span")
// examples as below:
// 		mo_ctl("cn", "TraceSpan", "cn_uuid1:enable:s3")
// 		mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:enable:s3,local,...")
// 		mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:enable:all")
// 		mo_ctl("cn", "TraceSpan", "all:enable:all)
//
// the cmd format for TN service:
// 		mo_ctl("dn", "TraceSpan", "enable/disable:kinds of span")
// (because there only exist one dn service, so we don't need to specify the uuid,
// 		but, the uuid will be ignored and will not check its validation even through it is specified.)
// examples as below:
// mo_ctl("dn", "TraceSpan", "[uuid]:disable:s3")
// mo_ctl("dn", "TraceSpan", "[uuid]:disable:local")
// mo_ctl("dn", "TraceSpan", "[uuid]:disable:s3, local,...")
// mo_ctl("dn", "TraceSpan", "[uuid]:enable:all")

var SupportedSpans = map[string]func(state bool){
	// enable or disable s3 file service read and write span
	"s3": func(s bool) { trace.MOCtledSpanEnableConfig.EnableS3FSSpan.Store(s) },
	// enable or disable local file service read and write span
	"local": func(s bool) { trace.MOCtledSpanEnableConfig.EnableLocalFSSpan.Store(s) },
	// enable or disable all span
	"all": func(s bool) {
		trace.MOCtledSpanEnableConfig.EnableS3FSSpan.Store(s)
		trace.MOCtledSpanEnableConfig.EnableLocalFSSpan.Store(s)
	},
}

var cmd2State = map[string]bool{
	"enable":  true,
	"disable": false,
}

func checkParameter(param string, ignoreUUID bool) (args []string, err error) {
	param = strings.ToLower(param)
	// [uuids], enable/disable, spans
	args = strings.Split(param, ":")

	if (ignoreUUID && len(args) < 2) ||
		(!ignoreUUID && len(args) != 3) {
		return nil, moerr.NewInternalErrorNoCtx("parameter invalid")
	}

	cmdIdx := 0
	if !ignoreUUID { // contains uuids
		cmdIdx = 1
	}
	_, ok := cmd2State[args[cmdIdx]]
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("cmd invalid, expected enable or disable")
	}

	return args, nil
}

func handleTraceSpan(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (pb.CtlResult, error) {
	if service != cn && service != tn {
		return pb.CtlResult{}, moerr.NewWrongServiceNoCtx("CN or DN", string(service))
	}

	if service == tn {
		return send2TNAndWaitResp(proc, service, parameter, sender)
	}

	args, err := checkParameter(parameter, false)
	if err != nil {
		return pb.CtlResult{}, err
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
		if cns[idx] == proc.QueryService.ServiceID() {
			info[cns[idx]] = SelfProcess(args[1], args[2])
		} else {
			// transfer query to another cn and receive its response
			resp, _ := transferRequest(proc, cns[idx], args[1], args[2])
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

	return pb.CtlResult{
		Method: pb.CmdMethod_TraceSpan.String(),
		Data:   data,
	}, nil
}

func SelfProcess(cmd string, spans string) string {
	var succeed, failed []string
	ss := strings.Split(spans, ",")
	for _, t := range ss {
		if fn, ok2 := SupportedSpans[t]; ok2 {
			fn(cmd2State[cmd])
			succeed = append(succeed, t)
		} else {
			failed = append(failed, t)
		}
	}

	return fmt.Sprintf("%v %sd, %v failed", succeed, cmd, failed)
}

func transferRequest(proc *process.Process, uuid string, cmd string, spans string) (resp *query.Response, err error) {
	clusterservice.GetMOCluster().GetCNService(clusterservice.NewServiceIDSelector(uuid),
		func(cn metadata.CNService) bool {
			request := proc.QueryService.NewRequest(query.CmdMethod_TraceSpan)
			request.TraceSpanRequest = &query.TraceSpanRequest{
				Cmd:   cmd,
				Spans: spans,
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			resp, err = proc.QueryService.SendMessage(ctx, cn.QueryAddress, request)
			return true
		})
	return
}

func send2TNAndWaitResp(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (pb.CtlResult, error) {

	whichTN := func(string) ([]uint64, error) { return nil, nil }
	payloadFn := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
		args, err := checkParameter(parameter, true)
		if err != nil {
			return nil, err
		}

		if len(args) == 3 {
			args = args[1:]
		}

		req := db.TraceSpan{
			Cmd:   args[0],
			Spans: strings.Split(args[1], ","),
		}

		return req.MarshalBinary()
	}

	repsonseUnmarshaler := func(b []byte) (interface{}, error) {
		return string(b[:]) + ": succeed", nil
	}

	return getTNHandlerFunc(
		pb.CmdMethod_TraceSpan, whichTN, payloadFn, repsonseUnmarshaler,
	)(proc, service, parameter, sender)

}
