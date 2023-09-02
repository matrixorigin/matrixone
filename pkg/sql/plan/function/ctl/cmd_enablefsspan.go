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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleEnableFSSpan(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (pb.CtlResult, error) {
	if service != cn {
		return pb.CtlResult{}, moerr.NewWrongServiceNoCtx("CN", string(service))
	}
	parameter = strings.ToUpper(parameter)
	ret := ""
	if parameter == "DISABLE" {
		trace.MOCtledSpanEnableConfig.EnableS3FSSpan = false
		trace.MOCtledSpanEnableConfig.EnableLocalFSSpan = false
		ret = "S3FS and LocalFS are all disabled"
	} else if parameter == "ENABLE" {
		trace.MOCtledSpanEnableConfig.EnableS3FSSpan = true
		trace.MOCtledSpanEnableConfig.EnableLocalFSSpan = true
		ret = "S3FS and LocalFS are all enabled"
	} else {
		return pb.CtlResult{},
			moerr.NewInvalidArgNoCtx("parameter", "expected enable or disable")
	}

	return pb.CtlResult{
		Method: pb.CmdMethod_EnableFSSpan.String(),
		Data:   ret,
	}, nil
}
