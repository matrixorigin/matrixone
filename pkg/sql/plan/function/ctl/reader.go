package ctl

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// mo_ctl("cn", "reader", "enable:force_remote_datasource");
// mo_ctl("cn", "reader", "disable:force_remote_datasource");
// mo_ctl("cn", "reader", "enable:force_shuffle");

func handleCtlReader(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewWrongServiceNoCtx("only cn supported", string(service))
	}

	parameter = strings.ToLower(parameter)
	args := strings.Split(parameter, ":")

	cns := make([]string, 0)
	clusterservice.GetMOCluster(proc.GetService()).GetCNService(clusterservice.Selector{}, func(cn metadata.CNService) bool {
		cns = append(cns, cn.ServiceID)
		return true
	})

	info := map[string]string{}
	for idx := range cns {
		// the current cn also need to process this span cmd
		if cns[idx] == proc.GetQueryClient().ServiceID() {
			info[cns[idx]] = UpdateCurrentCNReader(args[0], args[1])
		} else {
			// transfer query to another cn and receive its response
			request := proc.GetQueryClient().NewRequest(query.CmdMethod_CtlReader)
			request.CtlReaderRequest = &query.CtlReaderRequest{
				Cmd: args[1],
				Cfg: args[2],
			}
			resp, _ := transferRequest2OtherCNs(proc, cns[idx], request)
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

func UpdateCurrentCNReader(cmd string, cfg string) string {
	cc := false
	if cmd == "enable" {
		cc = true
	} else if cmd == "disable" {
		cc = false
	} else {
		return fmt.Sprintf("not support cmd: %s", cmd)
	}

	if cfg == "force_remote_datasource" {
		engine.SetForceBuildRemoteDS(cc)
	} else if cfg == "force_shuffle" {
		engine.SetForceShuffleReader(cc)
	} else {
		return fmt.Sprintf("not support cfg: %s", cfg)
	}

	return fmt.Sprintf("successed cmd: %s, cfg: %s", cmd, cfg)
}
