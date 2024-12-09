// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fj

import (
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func Test_CanHandleFaultInjection(t *testing.T) {
	id := uuid.New().String()
	addr := "127.0.0.1:7777"
	initRuntime([]string{id}, []string{addr})

	runtime.SetupServiceBasedRuntime(id, runtime.ServiceRuntime(""))

	cli, err := qclient.NewQueryClient(id, morpc.Config{})
	require.Nil(t, err)

	proc := new(process.Process)
	proc.Base = &process.BaseProcess{}
	proc.Base.QueryClient = cli

	// query with wrong command
	res := []PodResponse{{
		PodType:   cn,
		PodID:     id,
		ReturnStr: "unknown fault injection command",
	}}
	ret := CNFaultInject([]string{}, "enable", "", proc)
	require.Equal(t, res, ret)

	// enable fault injection
	res = []PodResponse{{
		PodType:   cn,
		PodID:     id,
		ReturnStr: "Fault injection enabled. Previous status: disabled",
	}}
	ret = CNFaultInject([]string{}, "ENABLE_FAULT_INJECTION", "", proc)
	require.Equal(t, res, ret)

	// status fault point
	res = []PodResponse{{
		PodType:   cn,
		PodID:     id,
		ReturnStr: "Fault injection is enabled",
	}}
	ret = CNFaultInject([]string{}, "STATUS_FAULT_POINT", "", proc)
	require.Equal(t, res, ret)

	// add fault point
	res = []PodResponse{{
		PodType:   cn,
		PodID:     id,
		ReturnStr: "OK",
	}}
	ret = CNFaultInject([]string{}, "ADD_FAULT_POINT", "test.:::.echo.0..true", proc)
	require.Equal(t, res, ret)

	// modify constant fault point
	res = []PodResponse{{
		PodType:  cn,
		PodID:    id,
		ErrorStr: "internal error: failed to add fault point; it may already exist and be constant.",
	}}
	ret = CNFaultInject([]string{}, "ADD_FAULT_POINT", "test.:::.echo.0..true", proc)
	require.Equal(t, res, ret)

	// list fault point
	res = []PodResponse{{
		PodType: cn,
		PodID:   id,
		ReturnList: []fault.Point{{
			Name:     "test",
			Iarg:     0,
			Sarg:     "",
			Constant: true,
		}},
	}}
	ret = CNFaultInject([]string{}, "LIST_FAULT_POINT", "", proc)
	require.Equal(t, res, ret)

	// remove fault point
	res = []PodResponse{{
		PodType:   cn,
		PodID:     id,
		ReturnStr: "Fault point 'test' successfully removed. Previously existed: true",
	}}
	ret = CNFaultInject([]string{}, "REMOVE_FAULT_POINT", "test", proc)
	require.Equal(t, res, ret)

	// remove non-existent fault point
	res = []PodResponse{{
		PodType:   cn,
		PodID:     id,
		ReturnStr: "Fault point 'test' successfully removed. Previously existed: false",
	}}
	ret = CNFaultInject([]string{}, "REMOVE_FAULT_POINT", "test", proc)
	require.Equal(t, res, ret)

	// disable fault injection
	res = []PodResponse{{
		PodType:   cn,
		PodID:     id,
		ReturnStr: "Fault injection disabled. Previous status: enabled",
	}}
	ret = CNFaultInject([]string{}, "DISABLE_FAULT_INJECTION", "", proc)
	require.Equal(t, res, ret)
}

func Test_CanTransferCnFaultInject(t *testing.T) {
	var a1 struct {
		proc *process.Process
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

	initRuntime(uuids, addrs)
	trace.InitMOCtledSpan()

	qs1, err := queryservice.NewQueryService(uuids[0], addrs[0], morpc.Config{})
	require.Nil(t, err)
	qs2, err := queryservice.NewQueryService(uuids[1], addrs[1], morpc.Config{})
	require.Nil(t, err)
	qt1, err := qclient.NewQueryClient(uuids[1], morpc.Config{})
	require.Nil(t, err)

	qs1.AddHandleFunc(query.CmdMethod_FaultInject, mockHandleFaultInject, false)
	qs2.AddHandleFunc(query.CmdMethod_FaultInject, mockHandleFaultInject, false)

	a1.proc.Base.QueryClient = qt1

	err = qs1.Start()
	require.Nil(t, err)
	err = qs2.Start()
	require.Nil(t, err)

	defer func() {
		qs1.Close()
		qs2.Close()
	}()

	// enable fault injection
	res := []PodResponse{
		{
			PodType:   cn,
			PodID:     uuids[0],
			ReturnStr: "Fault injection enabled. Previous status: disabled",
		},
		{
			PodType:   cn,
			PodID:     uuids[1],
			ReturnStr: "Fault injection enabled. Previous status: enabled",
		},
	}
	ret := CNFaultInject([]string{}, "ENABLE_FAULT_INJECTION", "", a1.proc)
	require.Equal(t, res, ret)
}
