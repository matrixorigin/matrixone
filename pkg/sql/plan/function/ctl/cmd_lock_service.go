// Copyright 2024 Matrix Origin
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

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleRemoveRemoteLockTable(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	infos := strings.Split(parameter, "-")
	if len(infos) != 2 {
		return Result{}, moerr.NewInvalidInputNoCtx("invalid parameter %s, use 'table_id' + '-' + bind_version'", parameter)
	}

	tableID, err := format.ParseStringUint64(infos[0])
	if err != nil {
		return Result{}, moerr.NewInvalidInputNoCtx("invalid parameter %s, use 'table_id' + '-' + bind_version'", parameter)
	}

	version, err := format.ParseStringUint64(infos[1])
	if err != nil {
		return Result{}, moerr.NewInvalidInputNoCtx("invalid parameter %s, use 'table_id' + '-' + bind_version'", parameter)
	}

	qs := proc.QueryService
	mc := clusterservice.GetMOCluster()
	var addrs []string
	mc.GetCNServiceWithoutWorkingState(
		clusterservice.NewSelector(),
		func(c metadata.CNService) bool {
			addrs = append(addrs, c.QueryAddress)
			return true
		})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	total := 0
	for _, addr := range addrs {
		req := qs.NewRequest(querypb.CmdMethod_RemoveRemoteLockTable)
		req.RemoveRemoteLockTable = &querypb.RemoveRemoteLockTableRequest{
			TableID: tableID,
			Version: version,
		}

		resp, err := qs.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		total += int(resp.RemoveRemoteLockTable.Count)
		qs.Release(resp)
	}

	return Result{
		Method: RemoveRemoteLockTable,
		Data:   fmt.Sprintf("remote %d in all cn", total),
	}, nil
}

func handleGetLatestBind(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	tableID, err := format.ParseStringUint64(parameter)
	if err != nil {
		return Result{}, moerr.NewInvalidInputNoCtx("invalid group_id + '-' + table id: %s", parameter)
	}

	qs := proc.QueryService
	mc := clusterservice.GetMOCluster()
	var addr string
	mc.GetTNService(
		clusterservice.NewSelector(),
		func(t metadata.TNService) bool {
			addr = t.QueryAddress
			return true
		})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	req := qs.NewRequest(querypb.CmdMethod_GetLatestBind)
	req.GetLatestBind = &querypb.GetLatestBindRequest{
		TableID: tableID,
	}

	resp, err := qs.SendMessage(ctx, addr, req)
	if err != nil {
		return Result{}, err
	}
	defer qs.Release(resp)

	return Result{
		Method: GetLatestBind,
		Data:   resp.GetLatestBind.Bind,
	}, nil
}
