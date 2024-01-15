package ctl

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strconv"
	"strings"
	"time"
)

func handleRunTask(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	qs := proc.QueryService
	target, taskCode, err := checkRunTaskParameter(parameter)
	if err != nil {
		return Result{}, err
	}
	if len(target) == 0 {
		return Result{}, moerr.NewInternalErrorNoCtx("cmd invalid, no target")
	}

	resp, err := transferTaskToCN(qs, target, taskCode)

	return Result{
		Method: RunTaskMethod,
		Data:   resp,
	}, nil
}

func checkRunTaskParameter(param string) (string, int64, error) {
	param = strings.ToLower(param)
	// uuid:taskId
	args := strings.Split(param, ":")
	if len(args) > 2 {
		return "", 0, moerr.NewInternalErrorNoCtx("cmd invalid, too many ':'")
	}
	taskCode, err := strconv.ParseInt(args[len(args)-1], 10, 64)
	if err != nil {
		return "", 0, moerr.NewInternalErrorNoCtx("cmd invalid, expected task code")
	}

	if len(args) == 2 {
		return args[0], taskCode, nil
	}

	return "", taskCode, nil
}

func transferTaskToCN(qs queryservice.QueryService, target string, taskCode int64) (resp *querypb.Response, err error) {
	clusterservice.GetMOCluster().GetCNService(
		clusterservice.NewServiceIDSelector(target),
		func(cn metadata.CNService) bool {
			req := qs.NewRequest(querypb.CmdMethod_RunTask)
			req.RunTask = &querypb.RunTaskRequest{
				TaskCode: int32(taskCode),
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			resp, err = qs.SendMessage(ctx, cn.QueryAddress, req)
			return true
		})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
