package ctl

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// force build remote datasource
// mo_ctl("cn", "reader", "enable:force_build_remote_ds:tid1,tid2");
// mo_ctl("cn", "reader", "disable:force_build_remote_ds:tid1,tid2");
//
// force shuffle specified number blocks and table
// mo_ctl("cn", "reader", "enable:force_shuffle:tid1,tid2:blk_cnt");
// mo_ctl("cn", "reader", "disable:force_shuffle:tid1,tid2:blk_cnt");

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
			info[cns[idx]] = UpdateCurrentCNReader(args[0], args[1:]...)
		} else {
			// transfer query to another cn and receive its response
			request := proc.GetQueryClient().NewRequest(query.CmdMethod_CtlReader)
			extra := args[2]
			if len(args) == 4 {
				extra += fmt.Sprintf(":%s", args[3])
			}

			request.CtlReaderRequest = &query.CtlReaderRequest{
				Cmd:   args[0],
				Cfg:   args[1],
				Extra: types.EncodeStringSlice([]string{extra}),
			}
			resp, err := transferRequest2OtherCNs(proc, cns[idx], request)
			if resp == nil || err != nil {
				// no such cn service
				info[cns[idx]] = fmt.Sprintf("transfer to %s failed, err: %v", cns[idx], err)
			} else if resp.CtlReaderResponse != nil {
				info[cns[idx]] = resp.CtlReaderResponse.Resp
			}
		}
	}

	data := ""
	for k, v := range info {
		data += fmt.Sprintf("%s:%s; ", k, v)
	}

	return Result{
		Method: CtlReaderMethod,
		Data:   data,
	}, nil
}

func UpdateCurrentCNReader(cmd string, cfgs ...string) string {
	cc := false
	if cmd == "enable" {
		cc = true
	} else if cmd == "disable" {
		cc = false
	} else {
		return fmt.Sprintf("not support cmd: %s", cmd)
	}

	if len(cfgs) == 2 && cfgs[0] == "force_build_remote_ds" {
		tbls := strings.Split(cfgs[1], ",")
		engine.SetForceBuildRemoteDS(cc, tbls)

	} else if len(cfgs) == 3 && cfgs[0] == "force_shuffle" {
		tbls := strings.Split(cfgs[1], ",")
		blkCnt, err := strconv.Atoi(cfgs[2])
		if err != nil {
			return fmt.Sprintf("parametrer err: %v", err)
		}

		engine.SetForceShuffleReader(cc, tbls, blkCnt)
	} else {
		return fmt.Sprintf("not support cfg: %v", cfgs)
	}

	return fmt.Sprintf("successed cmd: %s, cfg: %v", cmd, cfgs)
}
