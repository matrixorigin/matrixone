// Copyright 2021 - 2024 Matrix Origin
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
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func getTargets(param string) []string {
	cns := strings.Split(param, ",")
	if len(cns) == 1 && strings.ToLower(cns[0]) == "all" {
		cns = make([]string, 0)
		clusterservice.GetMOCluster().GetCNService(clusterservice.Selector{}, func(cn metadata.CNService) bool {
			cns = append(cns, cn.ServiceID)
			return true
		})
	}
	return cns
}

// handleUnsubscribeTable unsubscribes a table from logtail client.
func handleUnsubscribeTable(proc *process.Process,
	service serviceType,
	parameter string,
	_ requestSender) (Result, error) {
	if service != cn {
		return Result{}, moerr.NewWrongServiceNoCtx("CN", string(service))
	}
	args := strings.Split(parameter, ":")
	if len(args) != 3 {
		return Result{}, moerr.NewInternalErrorNoCtx("parameter invalid")
	}
	dbID, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return Result{}, moerr.NewInternalErrorNoCtx("wrong database ID: %s", args[1])
	}
	tbID, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		return Result{}, moerr.NewInternalErrorNoCtx("wrong table ID: %s", args[2])
	}
	targets := getTargets(args[0])
	for _, c := range targets {
		if err := doUnsubscribeTable(proc, c, dbID, tbID); err != nil {
			return Result{}, err
		}
	}
	return Result{
		Method: UnsubscribeTable,
		Data:   "OK",
	}, nil
}

func doUnsubscribeTable(proc *process.Process, uuid string, dbID, tbID uint64) error {
	var err error
	clusterservice.GetMOCluster().GetCNService(clusterservice.NewServiceIDSelector(uuid),
		func(cn metadata.CNService) bool {
			request := proc.QueryClient.NewRequest(query.CmdMethod_UnsubscribeTable)
			request.UnsubscribeTable = &query.UnsubscribeTableRequest{
				DatabaseID: dbID,
				TableID:    tbID,
			}
			ctx, cancel := context.WithTimeout(proc.Ctx, time.Second*3)
			defer cancel()
			resp, err := proc.QueryClient.SendMessage(ctx, cn.QueryAddress, request)
			if err != nil {
				return false
			}
			if resp != nil {
				proc.QueryClient.Release(resp)
			}
			return true
		})
	return err
}
