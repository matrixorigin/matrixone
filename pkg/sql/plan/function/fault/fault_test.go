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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/mohae/deepcopy"
	"github.com/stretchr/testify/require"
)

func Test_CanHandleFaultInjection(t *testing.T) {
	id := uuid.New().String()
	addr := "127.0.0.1:7775"
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
		ReturnStr: "OK, Fault injection enabled. Previous status: disabled",
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
	ret = CNFaultInject([]string{}, "ADD_FAULT_POINT", "test#:::#echo#0##true", proc)
	require.Equal(t, res, ret)

	// modify constant fault point
	res = []PodResponse{{
		PodType:  cn,
		PodID:    id,
		ErrorStr: "internal error: failed to add fault point; it may already exist and be constant.",
	}}
	ret = CNFaultInject([]string{}, "ADD_FAULT_POINT", "test#:::#echo#0##true", proc)
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
		ReturnStr: "OK, Fault injection disabled. Previous status: enabled",
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
		"127.0.0.1:7775",
		"127.0.0.1:5557",
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
			ReturnStr: "OK, Fault injection enabled. Previous status: disabled",
		},
		{
			PodType:   cn,
			PodID:     uuids[1],
			ReturnStr: "OK, Fault injection enabled. Previous status: enabled",
		},
	}
	ret := CNFaultInject([]string{}, "ENABLE_FAULT_INJECTION", "", a1.proc)
	require.Equal(t, res, ret)
}

func mockHandleFaultInject(ctx context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
	resp.FaultInjectResponse = new(query.FaultInjectResponse)
	resp.FaultInjectResponse.Resp = fault.HandleFaultInject(
		ctx, req.FaultInjectRequest.Method, req.FaultInjectRequest.Parameters,
	)
	return nil
}

func initRuntime(uuids []string, queryAddress []string) {
	runtime.RunTest(
		"",
		func(rt runtime.Runtime) {
			cns := make([]metadata.CNService, len(uuids))
			for idx := range uuids {
				cns[idx] = metadata.CNService{
					ServiceID:    uuids[idx],
					QueryAddress: queryAddress[idx],
				}
				runtime.SetupServiceBasedRuntime(uuids[idx], rt)
			}

			moCluster := clusterservice.NewMOCluster(
				"",
				new(testHAKeeperClient),
				time.Duration(time.Second),
				clusterservice.WithDisableRefresh(),
				clusterservice.WithServices(cns, nil))
			rt.SetGlobalVariables(runtime.ClusterService, moCluster)
			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		},
	)

}

type testHAKeeperClient struct {
	sync.RWMutex
	value logpb.ClusterDetails
	err   error
}

func (c *testHAKeeperClient) GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error) {
	c.RLock()
	defer c.RUnlock()
	// deep copy the cluster details to avoid data race.
	copied := deepcopy.Copy(c.value)
	return copied.(logpb.ClusterDetails), c.err
}

// The local CN keeps the fault point named in the arguments after the
// argument vectors are freed, so FaultInject must not keep views of them.
func Test_FaultInjectCopiesArguments(t *testing.T) {
	id := uuid.New().String()
	initRuntime([]string{id}, []string{"127.0.0.1:7776"})
	runtime.SetupServiceBasedRuntime(id, runtime.ServiceRuntime(""))
	cli, err := qclient.NewQueryClient(id, morpc.Config{})
	require.NoError(t, err)

	proc := testutil.NewProcess(t)
	proc.Base.QueryClient = cli
	mp := proc.Mp()

	call := func(service, command, parameter string) []byte {
		var args []*vector.Vector
		for _, s := range []string{service, command, parameter} {
			v, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(s), 1, mp)
			require.NoError(t, err)
			args = append(args, v)
		}
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
		require.NoError(t, result.PreExtendAndReset(1))
		require.NoError(t, FaultInject(args, result, proc, 1))
		out := result.GetResultVector()
		ret := out.CloneBytesAt(0)
		// Overwrite the argument memory as a later allocation would.
		for _, v := range args {
			b := v.GetBytesAt(0)
			for i := range b {
				b[i] = 'x'
			}
			v.Free(mp)
		}
		result.Free()
		return ret
	}

	call("cn.", "enable_fault_injection", "")
	defer call("cn.", "disable_fault_injection", "")
	call("cn.", "add_fault_point", "copied_name#:::#echo#7#copied_sarg#false")
	iv, sv, ok := fault.TriggerFault("copied_name")
	require.True(t, ok, "the fault point keeps its name")
	require.Equal(t, int64(7), iv)
	require.Equal(t, "copied_sarg", sv)
	call("cn.", "remove_fault_point", "copied_name")
}
