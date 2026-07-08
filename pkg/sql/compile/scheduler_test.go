// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type schedulerTestEngine struct {
	engine.Engine
	nodes      engine.Nodes
	err        error
	calls      int
	isInternal bool
	tenant     string
	uid        string
	cnLabel    map[string]string
}

func (e *schedulerTestEngine) Nodes(
	isInternal bool,
	tenant string,
	uid string,
	cnLabel map[string]string,
) (engine.Nodes, error) {
	e.calls++
	e.isInternal = isInternal
	e.tenant = tenant
	e.uid = uid
	e.cnLabel = cnLabel
	return e.nodes, e.err
}

func TestScheduleQueryWorkersKeepsLocalExecTypesLocal(t *testing.T) {
	for _, tt := range []struct {
		name     string
		execType plan2.ExecType
		mcpu     int
	}{
		{name: "tp", execType: plan2.ExecTypeTP, mcpu: 1},
		{name: "ap-one-cn", execType: plan2.ExecTypeAP_ONECN, mcpu: 6},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := NewMockCompile(t)
			c.addr = "local:6001"
			c.ncpu = 6
			c.execType = tt.execType
			e := &schedulerTestEngine{err: errors.New("candidate lookup should not run")}
			c.e = e

			nodes, err := c.scheduleQueryWorkers()
			require.NoError(t, err)
			require.Equal(t, engine.Nodes{{Addr: "local:6001", Mcpu: tt.mcpu}}, nodes)
			require.Zero(t, e.calls)
		})
	}
}

func TestScheduleQueryWorkersAllowsLocalExecWithoutAddress(t *testing.T) {
	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeTP
	c.e = &schedulerTestEngine{err: errors.New("candidate lookup should not run")}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{Mcpu: 1}}, nodes)
}

func TestScheduleQueryWorkersSortsMultiCNCandidates(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Id: "cn-z", Addr: "z:6001", Mcpu: 8},
			{Id: "cn-a", Addr: "a:6001", Mcpu: 4},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, []string{"a:6001", "z:6001"}, []string{nodes[0].Addr, nodes[1].Addr})
}

func TestScheduleQueryWorkersForwardsCandidateFilters(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.isInternal = true
	c.tenant = "sys"
	c.uid = "root"
	c.cnLabel = map[string]string{"role": "ap"}
	e := &schedulerTestEngine{
		nodes: engine.Nodes{{Id: "remote", Addr: "remote:6001", Mcpu: 4}},
	}
	c.e = e

	_, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, 1, e.calls)
	require.True(t, e.isInternal)
	require.Equal(t, "sys", e.tenant)
	require.Equal(t, "root", e.uid)
	require.Equal(t, map[string]string{"role": "ap"}, e.cnLabel)
}

func TestScheduleQueryWorkersRequiredLocalExecWithoutAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const localID = "local-cn"
	for _, tt := range []struct {
		execType plan2.ExecType
		mcpu     int
	}{
		{execType: plan2.ExecTypeTP, mcpu: 1},
		{execType: plan2.ExecTypeAP_ONECN, mcpu: 6},
	} {
		c := NewMockCompile(t)
		c.ncpu = 6
		c.execType = tt.execType
		c.proc.Base.QueryClient = fakeQueryClient{}
		lockSvc := mock_lock.NewMockLockService(ctrl)
		lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: localID}).AnyTimes()
		c.proc.Base.LockService = lockSvc

		nodes, err := c.scheduleQueryWorkers()
		require.NoError(t, err)
		require.Equal(t, engine.Nodes{{Id: localID, Mcpu: tt.mcpu}}, nodes)
	}
}

func TestScheduleQueryWorkersFallsBackToLocalWhenNoCandidate(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerTestEngine{}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{Addr: "local:6001", Mcpu: 6}}, nodes)
}

func TestScheduleQueryWorkersNormalizesInvalidMcpu(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Id: "cn-zero", Addr: "zero:6001"},
			{Id: "cn-negative", Addr: "negative:6001", Mcpu: -4},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{
		{Id: "cn-negative", Addr: "negative:6001", Mcpu: 1},
		{Id: "cn-zero", Addr: "zero:6001", Mcpu: 1},
	}, nodes)
}

func TestScheduleQueryWorkersDropsUnroutableCandidates(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Id: "missing-addr", Mcpu: 8},
			{Id: "remote", Addr: "remote:6001", Mcpu: 4},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{Id: "remote", Addr: "remote:6001", Mcpu: 4}}, nodes)
}

func TestScheduleQueryWorkersFallsBackToLocalWhenCandidatesUnroutable(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{{Id: "missing-addr", Mcpu: 8}},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{Addr: "local:6001", Mcpu: 6}}, nodes)

	decision, err := c.decideQueryPlacement()
	require.NoError(t, err)
	require.Equal(t, schedule.ReasonNoCandidateCN, decision.Reason)
}

func TestScheduleQueryWorkersIncludesLocalWhenQueryClientExists(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{{Id: "remote", Addr: "remote:6001", Mcpu: 4}},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, []string{"local:6001", "remote:6001"}, []string{nodes[0].Addr, nodes[1].Addr})
}

func TestScheduleQueryWorkersRejectsRequiredCurrentCNWithoutAddressForMultiCN(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const localID = "local-cn"
	c := NewMockCompile(t)
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: localID}).AnyTimes()
	c.proc.Base.LockService = lockSvc
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{{Id: "remote", Addr: "remote:6001", Mcpu: 4}},
	}

	_, err := c.scheduleQueryWorkers()
	require.ErrorContains(t, err, "without address for multi-CN execution")
}

func TestScheduleQueryWorkersAllowsRequiredCurrentCNWithoutAddressForLocalFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const localID = "local-cn"
	c := NewMockCompile(t)
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: localID}).AnyTimes()
	c.proc.Base.LockService = lockSvc
	c.e = &schedulerTestEngine{}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{Id: localID, Mcpu: 6}}, nodes)
}

func TestScheduleQueryWorkersReturnsErrorWhenRequiredCurrentCNMissingIdentity(t *testing.T) {
	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{{Id: "remote", Addr: "remote:6001", Mcpu: 4}},
	}

	_, err := c.scheduleQueryWorkers()
	require.ErrorContains(t, err, schedule.ReasonCurrentCNMissingIdentity)
}

func TestScheduleQueryWorkersDeduplicatesRequiredLocalByAddress(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Id: "remote", Addr: "remote:6001", Mcpu: 4},
			{Addr: "local:6001", Mcpu: 6},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	addrs := []string{nodes[0].Addr, nodes[1].Addr}
	sort.Strings(addrs)
	require.Equal(t, []string{"local:6001", "remote:6001"}, addrs)

	decision, err := c.decideQueryPlacement()
	require.NoError(t, err)
	require.Equal(t, schedule.ReasonRequiredCurrentCN, decision.Reason)
	require.Equal(t, 2, len(decision.Workers))
	require.Equal(t, "local:6001", decision.Workers[0].Addr)
	require.Equal(t, "remote:6001", decision.Workers[1].Addr)
}

func TestScheduleQueryWorkersDeduplicatesRequiredLocalByServiceID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const localID = "local-cn"
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: localID}).AnyTimes()
	c.proc.Base.LockService = lockSvc
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Id: "remote", Addr: "remote:6001", Mcpu: 4},
			{Id: localID, Addr: "local:6001", Mcpu: 6},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{
		{Id: localID, Addr: "local:6001", Mcpu: 6},
		{Id: "remote", Addr: "remote:6001", Mcpu: 4},
	}, nodes)

	decision, err := c.decideQueryPlacement()
	require.NoError(t, err)
	require.Equal(t, schedule.ReasonRequiredCurrentCN, decision.Reason)
	require.Equal(t, 2, len(decision.Workers))
	require.Equal(t, localID, decision.Workers[0].ID)
}

func TestScheduleQueryWorkersDeduplicatesRequiredLocalByAddressWhenServiceIDDifferent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const localID = "local-cn"
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: localID}).AnyTimes()
	c.proc.Base.LockService = lockSvc
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Id: "remote", Addr: "remote:6001", Mcpu: 4},
			{Id: "stale-local", Addr: "local:6001", Mcpu: 6},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{
		{Id: "stale-local", Addr: "local:6001", Mcpu: 6},
		{Id: "remote", Addr: "remote:6001", Mcpu: 4},
	}, nodes)

	decision, err := c.decideQueryPlacement()
	require.NoError(t, err)
	require.Equal(t, schedule.ReasonRequiredCurrentCN, decision.Reason)
	require.Equal(t, 2, len(decision.Workers))
	require.Equal(t, "stale-local", decision.Workers[0].ID)
	require.Equal(t, "local:6001", decision.Workers[0].Addr)
}

func TestScheduleQueryWorkersReturnsCandidateError(t *testing.T) {
	expected := errors.New("nodes failed")
	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerTestEngine{err: expected}

	_, err := c.scheduleQueryWorkers()
	require.ErrorIs(t, err, expected)
}

func TestScheduleQueryWorkersReturnsErrorWhenEngineMissing(t *testing.T) {
	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeAP_MULTICN

	_, err := c.scheduleQueryWorkers()
	require.ErrorContains(t, err, "compile engine is not initialized")
}

type fakeQueryClient struct{}

var _ qclient.QueryClient = fakeQueryClient{}

func (fakeQueryClient) ServiceID() string {
	return "fake-query-client"
}

func (fakeQueryClient) SendMessage(context.Context, string, *query.Request) (*query.Response, error) {
	return nil, nil
}

func (fakeQueryClient) NewRequest(query.CmdMethod) *query.Request {
	return &query.Request{}
}

func (fakeQueryClient) Release(*query.Response) {}

func (fakeQueryClient) Close() error {
	return nil
}
