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

// handleEnableSpan enable or disable spans for some  specified operation.
// the cmd format:
// mo_ctl("cn", "TranceSpan" "uuids of cn:enable/disable:kinds of span")
//
// examples as below:
// mo_ctl("cn", "TraceSpan", "cn_uuid1:enable:s3")
// mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:disable:s3")
// mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:enable:local")
// mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:disable:local")
// mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:enable:s3,local,...")
// mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:disable:s3, local,...")
// mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:enable:all")
// mo_ctl("cn", "TraceSpan", "cn_uuid1,cn_uuid2,...:disable:all")

var supportedSpans = map[string]func(state bool){
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

func handleTraceSpan(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (pb.CtlResult, error) {
	if service != cn {
		return pb.CtlResult{}, moerr.NewWrongServiceNoCtx("CN", string(service))
	}

	parameter = strings.ToLower(parameter)
	// uuids, enable/disable, spans
	args := strings.Split(parameter, ":")

	_, ok := cmd2State[args[1]]
	if !ok {
		return pb.CtlResult{}, moerr.NewInternalErrorNoCtx("cmd invalid, expected enable or disable")
	}

	// the uuids of cn
	cns := strings.Split(args[0], ",")

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
		if fn, ok2 := supportedSpans[t]; ok2 {
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
