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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_CanHandleCnFaultInjection(t *testing.T) {
	var a1, a2, a3, a4 struct {
		proc      *process.Process
		service   serviceType
		parameter string
		sender    requestSender
	}
	id := uuid.New().String()
	addr := "127.0.0.1:7777"
	initRuntime([]string{id}, []string{addr})

	runtime.SetupServiceBasedRuntime(id, runtime.ServiceRuntime(""))

	cli, err := qclient.NewQueryClient(id, morpc.Config{})
	require.Nil(t, err)

	proc := new(process.Process)
	proc.Base = &process.BaseProcess{}
	proc.Base.QueryClient = cli

	// testing query with wrong serviceType
	a1.proc = proc
	a1.service = "log"
	ret, err := handleAddFaultPoint(a1.proc, a1.service, a1.parameter, a1.sender)
	require.Equal(t, ret, Result{})
	require.Equal(t, err, moerr.NewWrongServiceNoCtx("CN or DN", string(a1.service)))

	// testing query with tn cmd
	a2.proc = proc
	a2.service = tn
	a2.parameter = "test.:::.echo.0."
	_, err = handleAddFaultPoint(a2.proc, a2.service, a2.parameter, a2.sender)
	require.NoError(t, err)

	// testing query with cn cmd
	a3.proc = proc
	a3.service = cn
	a3.parameter = "all.enable_fault_injection"
	ret, err = handleAddFaultPoint(a3.proc, a3.service, a3.parameter, a3.sender)
	require.NoError(t, err)
	res := CNResponse{
		CNid:      id,
		ReturnStr: "fault injection enabled, previous status: disabled",
	}

	require.Equal(t, ret, Result{
		Method: AddFaultPointMethod,
		Data:   []CNResponse{res},
	})

	a4.proc = proc
	a4.service = cn
	a4.parameter = "all.test.:::.echo.0."
	ret, err = handleAddFaultPoint(a4.proc, a4.service, a4.parameter, a4.sender)
	require.NoError(t, err)

	res.ReturnStr = "OK"

	require.Equal(t, ret, Result{
		Method: AddFaultPointMethod,
		Data:   []CNResponse{res},
	})

}

func Test_CanTransferCnFaultInjection(t *testing.T) {
	var a1 struct {
		proc      *process.Process
		service   serviceType
		parameter string
		sender    requestSender
	}

	uuids := []string{
		uuid.New().String(),
		uuid.New().String(),
	}
	addrs := []string{
		"127.0.0.1:7777",
		"127.0.0.1:5555",
	}

	a1.proc = new(process.Process)
	a1.proc.Base = &process.BaseProcess{}
	a1.service = cn
	a1.parameter = "all.test.:::.echo.0."

	initRuntime(uuids, addrs)
	trace.InitMOCtledSpan()

	qs1, err := queryservice.NewQueryService(uuids[0], addrs[0], morpc.Config{})
	require.Nil(t, err)
	qs2, err := queryservice.NewQueryService(uuids[1], addrs[1], morpc.Config{})
	require.Nil(t, err)
	qt1, err := qclient.NewQueryClient(uuids[1], morpc.Config{})
	require.Nil(t, err)

	qs1.AddHandleFunc(query.CmdMethod_FaultInjection, mockHandleFaultInjection, false)
	qs2.AddHandleFunc(query.CmdMethod_FaultInjection, mockHandleFaultInjection, false)

	a1.proc.Base.QueryClient = qt1

	err = qs1.Start()
	require.Nil(t, err)
	err = qs2.Start()
	require.Nil(t, err)

	defer func() {
		qs1.Close()
		qs2.Close()
	}()

	_, err = handleAddFaultPoint(a1.proc, a1.service, "all.status_fault_injection", a1.sender)
	require.NoError(t, err)

	_, err = handleAddFaultPoint(a1.proc, a1.service, "all.enable_fault_injection", a1.sender)
	require.NoError(t, err)

	_, err = handleAddFaultPoint(a1.proc, a1.service, "all.enable_fault_injection", a1.sender)
	require.NoError(t, err)

	_, err = handleAddFaultPoint(a1.proc, a1.service, "all.status_fault_injection", a1.sender)
	require.NoError(t, err)

	_, err = handleAddFaultPoint(a1.proc, a1.service, a1.parameter, a1.sender)
	require.Nil(t, err)

	_, err = handleAddFaultPoint(a1.proc, a1.service, "all.disable_fault_injection", a1.sender)
	require.NoError(t, err)

	_, err = handleAddFaultPoint(a1.proc, a1.service, "all.disable_fault_injection", a1.sender)
	require.NoError(t, err)
}

func mockHandleFaultInjection(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.TraceSpanResponse = new(query.TraceSpanResponse)
	resp.TraceSpanResponse.Resp = HandleCnFaultInjection(
		ctx,
		req.FaultInjectionRequest.Name,
		req.FaultInjectionRequest.Freq, req.FaultInjectionRequest.Action,
		req.FaultInjectionRequest.Iarg, req.FaultInjectionRequest.Sarg,
	)
	return nil
}
