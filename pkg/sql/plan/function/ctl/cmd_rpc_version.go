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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleGetProtocolVersion(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	qt := proc.GetQueryClient()
	mc := clusterservice.GetMOCluster(proc.GetService())
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
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second*10, moerr.CauseHandleGetProtocolVersion)
	defer cancel()

	versions := make([]string, 0, len(addrs))
	for i, addr := range addrs {
		req := qt.NewRequest(querypb.CmdMethod_GetProtocolVersion)
		req.GetProtocolVersion = &querypb.GetProtocolVersionRequest{}
		resp, err := qt.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, moerr.AttachCause(ctx, err)
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
	qt := proc.GetQueryClient()
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
		if version >= defines.MORPCVersion40 {
			const maxDDLVisibilityActivationTargets = 1024
			if len(targets) == 0 || len(targets) > maxDDLVisibilityActivationTargets {
				return Result{}, moerr.NewInternalErrorNoCtxf(
					"DDL visibility activation target count must be between 1 and %d",
					maxDDLVisibilityActivationTargets)
			}
			seen := make(map[string]struct{}, len(targets))
			for _, target := range targets {
				if target == "" {
					return Result{}, moerr.NewInternalErrorNoCtx("DDL visibility activation target is empty")
				}
				if _, ok := seen[target]; ok {
					return Result{}, moerr.NewInternalErrorNoCtxf(
						"DDL visibility activation target %s is duplicated", target)
				}
				seen[target] = struct{}{}
			}
		}
		if version >= defines.MORPCVersion40 {
			if err := validateDDLVisibilityActivationMembership(qt, targets); err != nil {
				return Result{}, err
			}
		}
		if version < defines.MORPCVersion40 {
			versions := make([]string, 0, len(targets))
			for _, target := range targets {
				resp, sendErr := transferToCN(qt, target, version, nil)
				if sendErr != nil {
					return Result{}, sendErr
				}
				if resp == nil || resp.SetProtocolVersion == nil {
					if resp != nil {
						qt.Release(resp)
					}
					return Result{}, moerr.NewInternalErrorNoCtx("no such cn service")
				}
				versions = append(versions, fmt.Sprintf("%s:%d", target, resp.SetProtocolVersion.Version))
				qt.Release(resp)
			}
			return Result{Method: SetProtocolVersionMethod, Data: strings.Join(versions, ", ")}, nil
		}

		// Live v40 activation is a distributed barrier. Dispatch every target
		// concurrently so each CN can block local DDL producers before any CN
		// waits for the complete Prepared set.
		type targetResult struct {
			index int
			resp  *querypb.Response
			err   error
		}
		results := make(chan targetResult, len(targets))
		for i, target := range targets {
			go func(index int, serviceID string) {
				resp, sendErr := transferToCN(qt, serviceID, version, targets)
				results <- targetResult{index: index, resp: resp, err: sendErr}
			}(i, target)
		}

		versions := make([]string, len(targets))
		var resultErr error
		for range targets {
			result := <-results
			if result.err != nil {
				if result.resp != nil {
					qt.Release(result.resp)
				}
				resultErr = errors.Join(resultErr, result.err)
				continue
			}
			if result.resp == nil || result.resp.SetProtocolVersion == nil {
				if result.resp != nil {
					qt.Release(result.resp)
				}
				resultErr = errors.Join(resultErr, moerr.NewInternalErrorNoCtx("no such cn service"))
				continue
			}
			versions[result.index] = fmt.Sprintf(
				"%s:%d", targets[result.index], result.resp.SetProtocolVersion.Version)
			qt.Release(result.resp)
		}
		if resultErr != nil {
			return Result{}, resultErr
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

func validateDDLVisibilityActivationMembership(qt qclient.QueryClient, targets []string) error {
	cluster := clusterservice.GetMOCluster(qt.ServiceID())
	refresher, ok := cluster.(clusterservice.AuthoritativeRefresher)
	if !ok {
		return moerr.NewInternalErrorNoCtx(
			"CN cluster service does not support authoritative DDL activation inventory")
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Minute, moerr.CauseTransferToCN)
	defer cancel()
	if err := refresher.Refresh(ctx); err != nil {
		return moerr.AttachCause(ctx, err)
	}
	authoritative := make(map[string]metadata.CNService)
	if err := clusterservice.GetCNServiceRawWithContext(
		ctx, cluster, clusterservice.NewSelector(), func(cn metadata.CNService) bool {
			if cn.QueryAddress != "" && cn.ViewMetadataAdmissionGeneration != 0 {
				authoritative[cn.ServiceID] = cn
			}
			return true
		}); err != nil {
		return moerr.AttachCause(ctx, err)
	}
	requested := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		requested[target] = struct{}{}
	}
	if len(requested) != len(authoritative) {
		return moerr.NewInvalidStateNoCtx(fmt.Sprintf(
			"DDL visibility activation target set does not match authoritative CN membership: requested=%d authoritative=%d",
			len(requested), len(authoritative)))
	}
	for serviceID, cn := range authoritative {
		if _, ok := requested[serviceID]; !ok {
			return moerr.NewInvalidStateNoCtx(fmt.Sprintf(
				"DDL visibility activation omits authoritative CN %s", serviceID))
		}
		if !cn.DDLVisibilityBarrierReady {
			return moerr.NewInvalidStateNoCtx(fmt.Sprintf(
				"authoritative CN %s does not support the DDL visibility activation receiver", serviceID))
		}
	}
	return nil
}

func transferToTN(qt qclient.QueryClient, version int64) (Result, error) {
	var addr string
	var resp *querypb.Response
	var err error
	clusterservice.GetMOCluster(qt.ServiceID()).GetTNService(
		clusterservice.NewSelector(),
		func(t metadata.TNService) bool {
			if t.QueryAddress == "" {
				return true
			}
			addr = t.QueryAddress
			ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second*10, moerr.CauseTransferToTN)
			defer cancel()
			req := qt.NewRequest(querypb.CmdMethod_SetProtocolVersion)
			req.SetProtocolVersion = &querypb.SetProtocolVersionRequest{
				Version: version,
			}
			resp, err = qt.SendMessage(ctx, addr, req)
			err = moerr.AttachCause(ctx, err)
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

func transferToCN(
	qt qclient.QueryClient,
	target string,
	version int64,
	activationTargets []string,
) (resp *querypb.Response, err error) {
	cluster := clusterservice.GetMOCluster(qt.ServiceID())
	var selected metadata.CNService
	if version >= defines.MORPCVersion40 {
		refresher, refreshOK := cluster.(clusterservice.AuthoritativeRefresher)
		if !refreshOK {
			return nil, moerr.NewInternalErrorNoCtx(
				"CN cluster service does not support authoritative activation recovery")
		}
		ctx, cancel := context.WithTimeoutCause(
			context.Background(), time.Minute, moerr.CauseTransferToCN)
		defer cancel()
		if err := refresher.Refresh(ctx); err != nil {
			return nil, moerr.AttachCause(ctx, err)
		}
		if err := clusterservice.GetCNServiceRawWithContext(
			ctx, cluster, clusterservice.NewServiceIDSelector(target), func(cn metadata.CNService) bool {
				selected = cn
				return false
			}); err != nil {
			return nil, moerr.AttachCause(ctx, err)
		}
		if selected.ServiceID != target || selected.ViewMetadataAdmissionGeneration == 0 ||
			selected.QueryAddress == "" {
			return nil, moerr.NewInternalErrorNoCtxf(
				"no authoritative CN activation target %s with valid generation and query address", target)
		}
	} else {
		cluster.GetCNService(clusterservice.NewServiceIDSelector(target), func(cn metadata.CNService) bool {
			selected = cn
			return false
		})
	}
	if selected.QueryAddress == "" {
		return nil, nil
	}

	req := qt.NewRequest(querypb.CmdMethod_SetProtocolVersion)
	req.SetProtocolVersion = &querypb.SetProtocolVersionRequest{
		Version:                         version,
		DDLVisibilityActivationTargets:  append([]string(nil), activationTargets...),
		DDLVisibilityTargetGeneration:   selected.ViewMetadataAdmissionGeneration,
		DDLVisibilityTargetQueryAddress: selected.QueryAddress,
	}
	// Live protocol activation may withdraw CN ingress and wait for a bounded
	// logtail frontier fence before acknowledging the transition.
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Minute, moerr.CauseTransferToCN)
	defer cancel()
	resp, err = qt.SendMessage(ctx, selected.QueryAddress, req)
	return resp, moerr.AttachCause(ctx, err)
}
