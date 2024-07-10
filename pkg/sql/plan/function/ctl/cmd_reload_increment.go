// Copyright 2021-2024 Matrix Origin
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
	"time"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// select mo_ctl('cn', 'reload-auto-increment-cache', 'table id')
func handleReloadAutoIncrementCache(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	tableID, err := format.ParseStringUint64(parameter)
	if err != nil {
		return Result{}, err
	}

	qt := proc.GetQueryClient()
	mc := clusterservice.GetMOCluster()
	var addrs []string
	mc.GetCNService(
		clusterservice.NewSelector(),
		func(c metadata.CNService) bool {
			addrs = append(addrs, c.QueryAddress)
			return true
		})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, addr := range addrs {
		req := qt.NewRequest(querypb.CmdMethod_ReloadAutoIncrementCache)
		req.ReloadAutoIncrementCache = &querypb.ReloadAutoIncrementCacheRequest{TableID: tableID}
		resp, err := qt.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		qt.Release(resp)
	}

	return Result{
		Method: ReloadAutoIncrementCache,
		Data:   "OK",
	}, nil
}
