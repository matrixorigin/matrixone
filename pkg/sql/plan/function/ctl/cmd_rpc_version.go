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
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleGetProtocolVersion(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	qs := proc.QueryService
	mc := clusterservice.GetMOCluster()
	var addrs []string
	mc.GetCNService(
		clusterservice.NewSelector(),
		func(c metadata.CNService) bool {
			addrs = append(addrs, c.QueryAddress)
			return true
		})
	mc.GetTNService(
		clusterservice.NewSelector(),
		func(d metadata.TNService) bool {
			if d.QueryAddress != "" {
				addrs = append(addrs, d.QueryAddress)
			}
			return true
		})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	versions := make(map[string]string, len(addrs))
	for _, addr := range addrs {
		req := qs.NewRequest(querypb.CmdMethod_GetProtocolVersion)
		req.GetProtocolVersion = &querypb.GetProtocolVersionRequest{}
		resp, err := qs.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		versions[addr] = resp.GetProtocolVersion.Version
		qs.Release(resp)
	}

	bytes, err := json.Marshal(versions)
	if err != nil {
		return Result{}, err
	}

	return Result{
		Method: GetProtocolVersionMethod,
		Data:   string(bytes),
	}, nil
}

// handleSetProtocolVersion sets the version of mo components' protocol versions
//
// the cmd format for CN service:
//
//	mo_ctl("cn", "SetProtocolVersion" "uuids of cn:protocol version")
//
// examples as below:
//
//	mo_ctl("cn", "SetProtocolVersion", "cn_uuid1:1")
//	mo_ctl("cn", "SetProtocolVersion", "cn_uuid1,cn_uuid2,...:2")
//
// the cmd format for TN service:
//
//	mo_ctl("dn", "SetProtocolVersion", "protocol version")
//
// (because there only exist one dn service, so we don't need to specify the uuid,
//
//	but, the uuid will be ignored and will not check its validation even though it is specified.)
//
// examples as below:
// mo_ctl("dn", "SetProtocolVersion", "1")
func handleSetProtocolVersion(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	qs := proc.QueryService
	mc := clusterservice.GetMOCluster()
	var addrs []string
	mc.GetCNService(
		clusterservice.NewSelector(),
		func(c metadata.CNService) bool {
			addrs = append(addrs, c.QueryAddress)
			return true
		})
	mc.GetTNService(
		clusterservice.NewSelector(),
		func(d metadata.TNService) bool {
			if d.QueryAddress != "" {
				addrs = append(addrs, d.QueryAddress)
			}
			return true
		})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	versions := make(map[string]string, len(addrs))
	for _, addr := range addrs {
		req := qs.NewRequest(querypb.CmdMethod_SetProtocolVersion)
		req.SetProtocolVersion = &querypb.SetProtocolVersionRequest{
			Version: parameter,
		}

		resp, err := qs.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		versions[addr] = resp.SetProtocolVersion.Version
		qs.Release(resp)
	}

	bytes, err := json.Marshal(versions)
	if err != nil {
		return Result{}, err
	}

	return Result{
		Method: SetProtocolVersionMethod,
		Data:   string(bytes),
	}, nil
}

func checkProtocolParameter(param string) ([]string, int64, error) {
	param = strings.ToLower(param)
	// [uuids]:version
	args := strings.Split(param, ":")
	version, err := strconv.ParseInt(args[len(args)-1], 10, 64)
	if err != nil {
		return nil, 0, moerr.NewInternalErrorNoCtx("cmd invalid, expected version number")
	}

	if len(args) == 2 {
		arg := args[0]
		targets := strings.Split(arg, ",")
		return targets, version, nil

	}

	return nil, version, nil
}
