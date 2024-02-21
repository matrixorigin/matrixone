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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleGetProtocolVersion(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	qt := proc.QueryClient
	mc := clusterservice.GetMOCluster()
	var addrs []string
	var nodeIds []string
	mc.GetCNService(
		clusterservice.NewSelector(),
		func(c metadata.CNService) bool {
			addrs = append(addrs, c.QueryAddress)
			nodeIds = append(nodeIds, c.ServiceID)
			return true
		})
	mc.GetTNService(
		clusterservice.NewSelector(),
		func(d metadata.TNService) bool {
			if d.QueryAddress != "" {
				addrs = append(addrs, d.QueryAddress)
				nodeIds = append(nodeIds, d.ServiceID)
			}
			return true
		})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	versions := make([]string, 0, len(addrs))
	for i, addr := range addrs {
		req := qt.NewRequest(querypb.CmdMethod_GetProtocolVersion)
		req.GetProtocolVersion = &querypb.GetProtocolVersionRequest{}
		resp, err := qt.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		versions = append(versions, fmt.Sprintf("%s:%d", nodeIds[i], resp.GetProtocolVersion.Version))
		qt.Release(resp)
	}

	return Result{
		Method: GetProtocolVersionMethod,
		Data:   strings.Join(versions, ", "),
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
	qt := proc.QueryClient
	targets, version, err := checkProtocolParameter(parameter)
	if err != nil {
		return Result{}, err
	}
	if service == tn && targets == nil {
		// set protocol version for tn node
		// there only exist one tn node, so we don't need to specify the uuid
		return transferToTN(qt, version)
	}

	if service == cn && targets != nil {
		versions := make([]string, 0, len(targets))
		for _, target := range targets {
			resp, err := transferToCN(qt, target, version)
			if err != nil {
				return Result{}, err
			}
			if resp == nil {
				return Result{}, moerr.NewInternalErrorNoCtx("no such cn service")
			}
			versions = append(versions, fmt.Sprintf("%s:%d", target, resp.SetProtocolVersion.Version))
		}

		return Result{
			Method: SetProtocolVersionMethod,
			Data:   strings.Join(versions, ", "),
		}, nil
	}

	return Result{}, moerr.NewInternalError(proc.Ctx, "unsupported cmd")
}

func checkProtocolParameter(param string) ([]string, int64, error) {
	param = strings.ToLower(param)
	// [uuids]:version
	args := strings.Split(param, ":")
	if len(args) > 2 {
		return nil, 0, moerr.NewInternalErrorNoCtx("cmd invalid, too many ':'")
	}
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

func transferToTN(qt qclient.QueryClient, version int64) (Result, error) {
	var addr string
	var resp *querypb.Response
	var err error
	clusterservice.GetMOCluster().GetTNService(
		clusterservice.NewSelector(),
		func(t metadata.TNService) bool {
			if t.QueryAddress == "" {
				return true
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := qt.NewRequest(querypb.CmdMethod_SetProtocolVersion)
			req.SetProtocolVersion = &querypb.SetProtocolVersionRequest{
				Version: version,
			}
			resp, err = qt.SendMessage(ctx, addr, req)
			return true
		})
	if err != nil {
		return Result{}, err
	}
	if resp == nil {
		return Result{}, moerr.NewInternalErrorNoCtx("no such tn service")
	}
	defer qt.Release(resp)
	return Result{
		Method: SetProtocolVersionMethod,
		Data:   strconv.FormatInt(resp.SetProtocolVersion.Version, 10),
	}, nil
}

func transferToCN(qt qclient.QueryClient, target string, version int64) (resp *querypb.Response, err error) {
	clusterservice.GetMOCluster().GetCNService(
		clusterservice.NewServiceIDSelector(target),
		func(cn metadata.CNService) bool {
			req := qt.NewRequest(querypb.CmdMethod_SetProtocolVersion)
			req.SetProtocolVersion = &querypb.SetProtocolVersionRequest{
				Version: version,
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			resp, err = qt.SendMessage(ctx, cn.QueryAddress, req)
			return true
		})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
