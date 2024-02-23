// Copyright 2021 - 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleCoreDump(proc *process.Process, service serviceType,
	param string, sender requestSender) (Result, error) {
	var addrs []string

	qt := proc.QueryClient
	mc := clusterservice.GetMOCluster()
	if service == cn {
		mc.GetCNService(
			clusterservice.NewSelector(),
			func(c metadata.CNService) bool {
				addrs = append(addrs, c.QueryAddress)
				return true
			})
	}
	if service == tn {
		mc.GetTNService(
			clusterservice.NewSelector(),
			func(d metadata.TNService) bool {
				if d.QueryAddress != "" {
					addrs = append(addrs, d.QueryAddress)
				}
				return true
			})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, addr := range addrs {
		req := qt.NewRequest(querypb.CmdMethod_CoreDumpConfig)
		req.CoreDumpConfig = &querypb.CoreDumpConfigRequest{Action: param}
		resp, err := qt.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		qt.Release(resp)
	}
	return Result{
		Method: CoreDumpMethod,
		Data:   "OK",
	}, nil
}
