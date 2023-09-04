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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// handleEnableSpan enable or disable spans for some  specified operation.
// the cmd format examples as below:
// mo_ctl("cn", "TraceSpan", "enable:s3")
// mo_ctl("cn", "TraceSpan", "disable:s3")
// mo_ctl("cn", "TraceSpan", "enable:local")
// mo_ctl("cn", "TraceSpan", "disable:local")
// mo_ctl("cn", "TraceSpan", "enable:s3,local,...")
// mo_ctl("cn", "TraceSpan", "disable:s3, local,...")
// mo_ctl("cn", "TraceSpan", "enable:all")
// mo_ctl("cn", "TraceSpan", "disable:all")

var supportedSpans = map[string]func(state bool){
	// enable or disable s3 file service read and write span
	"s3": func(s bool) { trace.MOCtledSpanEnableConfig.EnableS3FSSpan = s },
	// enable or disable local file service read and write span
	"local": func(s bool) { trace.MOCtledSpanEnableConfig.EnableLocalFSSpan = s },
	// enable or disable all span
	"all": func(s bool) {
		trace.MOCtledSpanEnableConfig.EnableS3FSSpan = s
		trace.MOCtledSpanEnableConfig.EnableLocalFSSpan = s
	},
}

func handleTraceSpan(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (pb.CtlResult, error) {
	if service != cn {
		return pb.CtlResult{}, moerr.NewWrongServiceNoCtx("CN", string(service))
	}

	cmd2State := map[string]bool{
		"enable":  true,
		"disable": false,
	}

	var succeed, failed []string
	parameter = strings.ToLower(parameter)
	args := strings.Split(parameter, ":")
	if state, ok := cmd2State[args[0]]; ok {
		tars := strings.Split(args[1], ",")
		for _, t := range tars {
			if fn, ok2 := supportedSpans[t]; ok2 {
				fn(state)
				succeed = append(succeed, t)
			} else {
				failed = append(failed, t)
			}
		}

	} else {
		return pb.CtlResult{},
			moerr.NewInvalidArgNoCtx("parameter", "expected enable or disable")
	}

	return pb.CtlResult{
		Method: pb.CmdMethod_TraceSpan.String(),
		Data:   fmt.Sprintf("%v %sd, %v failed", succeed, args[0], failed),
	}, nil
}
