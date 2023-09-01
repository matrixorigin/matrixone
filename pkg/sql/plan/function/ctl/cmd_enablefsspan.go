package ctl

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
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
