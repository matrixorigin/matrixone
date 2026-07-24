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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	motestutil "github.com/matrixorigin/matrixone/pkg/testutil"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

type schedulerProviderTestEngine struct {
	*schedulerTestEngine
	candidates       engine.QueryCandidates
	resolvedNodes    engine.Nodes
	discoveryErr     error
	resolutionErr    error
	discoveryCalls   int
	resolutionCalls  int
	resolvedSnapshot engine.QueryCandidates
	poolRequest      engine.QueryCandidatePoolRequest
	mutateLabels     bool
}

type schedulerCurrentProviderTestEngine struct {
	*schedulerProviderTestEngine
	currentCandidates engine.QueryCandidates
	currentErr        error
	currentCalls      int
	currentServiceID  string
}

func (e *schedulerCurrentProviderTestEngine) DiscoverCurrentQueryCandidate(
	ctx context.Context,
	serviceID string,
) (engine.QueryCandidates, error) {
	e.currentCalls++
	e.currentServiceID = serviceID
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.currentCandidates, e.currentErr
}

func (e *schedulerProviderTestEngine) DiscoverQueryCandidates(ctx context.Context) (engine.QueryCandidates, error) {
	e.discoveryCalls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.candidates, e.discoveryErr
}

func (e *schedulerProviderTestEngine) ResolveQueryCandidatePool(
	ctx context.Context,
	candidates engine.QueryCandidates,
	request engine.QueryCandidatePoolRequest,
) (engine.ResolvedQueryPool, error) {
	e.resolutionCalls++
	e.resolvedSnapshot = candidates
	e.poolRequest = request
	e.poolRequest.CNLabel = cloneCNLabels(request.CNLabel)
	e.poolRequest.TargetLabels = cloneCNLabels(request.TargetLabels)
	if e.mutateLabels {
		if request.TargetLabels != nil {
			request.TargetLabels["mutated"] = "true"
		} else {
			request.CNLabel["mutated"] = "true"
		}
	}
	if err := ctx.Err(); err != nil {
		return engine.ResolvedQueryPool{}, err
	}
	return engine.ResolvedQueryPool{
		Nodes:             e.resolvedNodes,
		RequestedIdentity: request.RequestedPool,
		Identity:          request.RequestedPool,
		Resolution:        engine.QueryPoolResolutionExactLabels,
	}, e.resolutionErr
}

type schedulerDiscoverOnlyEngine struct {
	*schedulerTestEngine
}

func (*schedulerDiscoverOnlyEngine) DiscoverQueryCandidates(context.Context) (engine.QueryCandidates, error) {
	return nil, nil
}

type schedulerResolveOnlyEngine struct {
	*schedulerTestEngine
}

func (*schedulerResolveOnlyEngine) ResolveQueryCandidatePool(
	context.Context,
	engine.QueryCandidates,
	engine.QueryCandidatePoolRequest,
) (engine.ResolvedQueryPool, error) {
	return engine.ResolvedQueryPool{}, nil
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

type schedulerTestCluster struct {
	clusterservice.MOCluster
	cns []metadata.CNService
}

func (c schedulerTestCluster) GetCNServiceWithoutWorkingState(
	_ clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	for _, cn := range c.cns {
		if !apply(cn) {
			return
		}
	}
}

type panicWorkStateCluster struct {
	clusterservice.MOCluster
}

func (panicWorkStateCluster) GetCNServiceWithoutWorkingState(
	clusterservice.Selector,
	func(metadata.CNService) bool,
) {
	panic("local execution should not read current CN work state")
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

func TestScheduleQueryWorkersKeepsLocalExecTypesFromRuntimeStateLookup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const localID = "local-cn-local-exec"
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.ClusterService, panicWorkStateCluster{})
	moruntime.SetupServiceBasedRuntime(localID, rt)

	for _, tt := range []struct {
		execType plan2.ExecType
		mcpu     int
	}{
		{execType: plan2.ExecTypeTP, mcpu: 1},
		{execType: plan2.ExecTypeAP_ONECN, mcpu: 6},
	} {
		c := NewMockCompile(t)
		c.addr = "local:6001"
		c.ncpu = 6
		c.execType = tt.execType
		lockSvc := mock_lock.NewMockLockService(ctrl)
		lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: localID}).AnyTimes()
		c.proc.Base.LockService = lockSvc

		nodes, err := c.scheduleQueryWorkers()
		require.NoError(t, err)
		require.Equal(t, engine.Nodes{{Id: localID, Addr: "local:6001", Mcpu: tt.mcpu}}, nodes)
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

func TestScheduleQueryWorkersKeepsIvfCurrentParticipantAtOrdinalZero(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{
			NodeType: plan.Node_FUNCTION_SCAN,
			TableDef: &plan.TableDef{
				TblFunc: &plan.TableFunction{Name: ivfflatplan.IVFFLATSearchFuncName},
			},
			IndexReaderParam: &plan.IndexReaderParam{
				OrigFuncName: "l2_distance",
			},
		}},
	}}}
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Addr: "local:6001", Mcpu: 6},
			{Id: "cn-1", Addr: "one:6001", Mcpu: 4},
			{Id: "cn-2", Addr: "two:6001", Mcpu: 4},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	require.Equal(t, []string{"local:6001", "one:6001", "two:6001"}, []string{nodes[0].Addr, nodes[1].Addr, nodes[2].Addr})
}

func TestScheduleQueryWorkersDoesNotUseClusterWideMixedCommitProxyForIvf(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{
			NodeType: plan.Node_FUNCTION_SCAN,
			TableDef: &plan.TableDef{
				TblFunc: &plan.TableFunction{Name: ivfflatplan.IVFFLATSearchFuncName},
			},
			IndexReaderParam: &plan.IndexReaderParam{OrigFuncName: "l2_distance"},
		}},
	}}}
	c.e = &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{{
			Service: metadata.CNService{ServiceID: "pool-excluded", CommitID: "old-version"},
		}},
		resolvedNodes: engine.Nodes{
			{Addr: "local:6001", Mcpu: 6},
			{Id: "cn-1", Addr: "one:6001", Mcpu: 4},
			{Id: "cn-2", Addr: "two:6001", Mcpu: 4},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	require.Len(t, nodes, 3)
}

func TestScheduleQueryWorkersKeepsCurrentCNFirstForIvfEntriesScan(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "z-local:6001"
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{
			NodeType: plan.Node_FUNCTION_SCAN,
			TableDef: &plan.TableDef{
				TblFunc: &plan.TableFunction{Name: ivfflatplan.IVFFLATSearchFuncName},
			},
			IndexReaderParam: &plan.IndexReaderParam{OrigFuncName: "l2_distance"},
		}},
	}}}
	c.e = &schedulerTestEngine{nodes: engine.Nodes{
		{Id: "remote", Addr: "a-remote:6001", Mcpu: 4},
		{Id: "local", Addr: "z-local:6001", Mcpu: 6},
	}}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, []string{"z-local:6001", "a-remote:6001"}, []string{nodes[0].Addr, nodes[1].Addr})
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
	require.Equal(t, schedule.CandidateSourceEngineNodes, c.queryPlacement.CandidateResolution.DiscoverySource)
	require.Equal(t, schedule.PoolResolutionLegacyEngineNodes, c.queryPlacement.CandidateResolution.PoolResolution)
	require.Equal(t, 1, c.queryPlacement.CandidateResolution.DiscoveredCount)
	require.Equal(t, 1, c.queryPlacement.ResolvedCandidateCount)
}

func TestScheduleQueryWorkersUsesIndependentCandidateProviders(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.isInternal = true
	c.tenant = "sys"
	c.uid = "root"
	c.cnLabel = map[string]string{"role": "ap"}
	legacy := &schedulerTestEngine{err: errors.New("legacy Nodes must not run")}
	provider := &schedulerProviderTestEngine{
		schedulerTestEngine: legacy,
		candidates: engine.QueryCandidates{
			{Service: metadata.CNService{ServiceID: "ap-1", PipelineServiceAddress: "ap-1:6001"}, Mcpu: 4},
			{Service: metadata.CNService{ServiceID: "ap-2", PipelineServiceAddress: "ap-2:6001"}, Mcpu: 8},
			{Service: metadata.CNService{ServiceID: "tp-1", PipelineServiceAddress: "tp-1:6001"}, Mcpu: 2},
		},
		resolvedNodes: engine.Nodes{
			{Id: "ap-2", Addr: "ap-2:6001", Mcpu: 8},
			{Id: "ap-1", Addr: "ap-1:6001", Mcpu: 4},
		},
		mutateLabels: true,
	}
	c.e = provider

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, []string{"ap-1:6001", "ap-2:6001"}, []string{nodes[0].Addr, nodes[1].Addr})
	require.Zero(t, legacy.calls)
	require.Equal(t, 1, provider.discoveryCalls)
	require.Equal(t, 1, provider.resolutionCalls)
	require.Equal(t, provider.candidates, provider.resolvedSnapshot)
	require.Equal(t, engine.QueryCandidatePoolRequest{
		IsInternal:    true,
		Tenant:        "sys",
		Username:      "root",
		CNLabel:       map[string]string{"role": "ap"},
		RequestedPool: "tenant:3:sys|4:role=2:ap",
	}, provider.poolRequest)
	require.Equal(t, map[string]string{"role": "ap"}, c.cnLabel)
	require.Equal(t, schedule.CandidateSourceClusterInventory, c.queryPlacement.CandidateResolution.DiscoverySource)
	require.Equal(t, schedule.PoolResolutionTenantLabels, c.queryPlacement.CandidateResolution.PoolResolution)
	require.Equal(t, 3, c.queryPlacement.CandidateResolution.DiscoveredCount)
	require.Equal(t, 2, c.queryPlacement.ResolvedCandidateCount)
}

func TestScheduleQueryWorkersUsesWorkloadTargetPoolInsteadOfIngressPool(t *testing.T) {
	policySet, err := schedule.ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"ap": {
				"pool": "tenant-ap",
				"labels": {"role": "ap"},
				"current_cn": "excluded"
			}
		}
	}`)
	require.NoError(t, err)

	c := NewMockCompile(t)
	c.addr = "tp-local:6001"
	c.execType = plan2.ExecTypeAP_ONECN
	c.tenant = "tenant-a"
	c.cnLabel = map[string]string{"account": "tenant-a", "role": "tp"}
	provider := &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{
			{Service: metadata.CNService{
				ServiceID: "tp-local", PipelineServiceAddress: "tp-local:6001",
				WorkState: metadata.WorkState_Working,
			}, Mcpu: 4},
			{Service: metadata.CNService{
				ServiceID: "ap-remote", PipelineServiceAddress: "ap-remote:6001",
				WorkState: metadata.WorkState_Working,
			}, Mcpu: 8},
		},
		resolvedNodes: engine.Nodes{{
			Id: "ap-remote", Addr: "ap-remote:6001", Mcpu: 8,
			WorkState: metadata.WorkState_Working,
		}},
		mutateLabels: true,
	}
	c.e = provider
	c.SetWorkloadPolicy(policySet, "")

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{
		Id: "ap-remote", Addr: "ap-remote:6001", Mcpu: 8,
		WorkState: metadata.WorkState_Working,
	}}, nodes)
	require.Equal(t, map[string]string{
		"account": "tenant-a",
		"role":    "ap",
	}, provider.poolRequest.TargetLabels)
	require.Equal(t, map[string]string{
		"account": "tenant-a",
		"role":    "tp",
	}, provider.poolRequest.CNLabel)
	require.Equal(t, "tenant-ap", provider.poolRequest.RequestedPool)
	require.Equal(t, engine.QueryPoolFallbackStrict, provider.poolRequest.FallbackPolicy)
	require.Equal(t, schedule.WorkloadAP, c.queryPlacement.WorkloadPolicy.WorkloadClass)
	require.Equal(t, schedule.WorkloadRoutingSingle, c.queryPlacement.WorkloadPolicy.Routing)
	require.Equal(t, schedule.CurrentCNExcluded, c.queryPlacement.CurrentCNPolicy)
	require.Equal(t, schedule.WorkerSetMax, c.queryPlacement.Intent.WorkerSet.Mode)
	require.Equal(t, 1, c.queryPlacement.Intent.WorkerSet.MaxWorkers)
	require.Equal(t, "tp-local:6001", c.queryPlacement.CurrentCN.Addr)
	require.Equal(t, "tp-local:6001", c.currentCNWorker().Addr)
	c.cnList = nodes
	require.Equal(t, "ap-remote:6001", getEngineNode(c).Addr)
	require.Equal(t, "tp", c.cnLabel["role"])
}

func TestScheduleQueryWorkersClassifiesAndRoutesLoadIndependentlyOfExecType(t *testing.T) {
	policySet, err := schedule.ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"load": {
				"pool": "tenant-etl",
				"labels": {"role": "etl"},
				"current_cn": "excluded"
			}
		}
	}`)
	require.NoError(t, err)

	c := NewMockCompile(t)
	c.addr = "tp-local:6001"
	c.execType = plan2.ExecTypeTP
	c.stmt = &tree.Load{}
	c.tenant = "tenant-a"
	c.cnLabel = map[string]string{"account": "tenant-a", "role": "tp"}
	c.e = &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{{
			Service: metadata.CNService{
				ServiceID: "etl-remote", PipelineServiceAddress: "etl-remote:6001",
				WorkState: metadata.WorkState_Working,
			},
			Mcpu: 6,
		}},
		resolvedNodes: engine.Nodes{{
			Id: "etl-remote", Addr: "etl-remote:6001", Mcpu: 6,
			WorkState: metadata.WorkState_Working,
		}},
	}
	c.SetWorkloadPolicy(policySet, "")

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, "etl-remote:6001", nodes[0].Addr)
	require.Equal(t, schedule.WorkloadLoad, c.queryPlacement.WorkloadPolicy.WorkloadClass)
	require.Equal(t, schedule.WorkloadRoutingSingle, c.queryPlacement.WorkloadPolicy.Routing)
}

func TestScheduleQueryWorkersTPPolicyVerifiesCurrentPoolWithoutRelocation(t *testing.T) {
	policySet, err := schedule.ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"tp": {
				"pool": "tenant-tp",
				"labels": {"role": "tp"},
				"current_cn": "required"
			}
		}
	}`)
	require.NoError(t, err)

	c := NewMockCompile(t)
	c.addr = "tp-local:6001"
	c.execType = plan2.ExecTypeTP
	c.tenant = "tenant-a"
	c.cnLabel = map[string]string{"account": "tenant-a", "role": "tp"}
	c.e = &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{{
			Service: metadata.CNService{
				ServiceID: "tp-local", PipelineServiceAddress: "tp-local:6001",
				WorkState: metadata.WorkState_Working,
			},
			Mcpu: 4,
		}},
		resolvedNodes: engine.Nodes{{
			Id: "tp-local", Addr: "tp-local:6001", Mcpu: 4,
			WorkState: metadata.WorkState_Working,
		}},
	}
	c.SetWorkloadPolicy(policySet, "")

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, "tp-local:6001", nodes[0].Addr)
	require.Equal(t, schedule.WorkloadRoutingLocal, c.queryPlacement.WorkloadPolicy.Routing)
	require.Equal(t, schedule.ReasonRequiredCurrentCN, c.queryPlacement.Reason)
}

func TestScheduleQueryWorkersTPPolicyDiscoversOnlyCurrentCN(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: "tp-local"}).AnyTimes()

	policySet, err := schedule.ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"tp": {
				"pool": "tenant-tp",
				"labels": {"role": "tp"},
				"current_cn": "required"
			}
		}
	}`)
	require.NoError(t, err)

	base := &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		resolvedNodes: engine.Nodes{{
			Id: "tp-local", Addr: "tp-local:6001", Mcpu: 4,
			WorkState: metadata.WorkState_Working,
		}},
	}
	provider := &schedulerCurrentProviderTestEngine{
		schedulerProviderTestEngine: base,
		currentCandidates: engine.QueryCandidates{{
			Service: metadata.CNService{
				ServiceID: "tp-local", PipelineServiceAddress: "tp-local:6001",
				WorkState: metadata.WorkState_Working,
			},
			Mcpu: 4,
		}},
	}
	c := NewMockCompile(t)
	c.proc.Base.LockService = lockSvc
	c.addr = "tp-local:6001"
	c.execType = plan2.ExecTypeTP
	c.tenant = "tenant-a"
	c.cnLabel = map[string]string{"account": "tenant-a", "role": "tp"}
	c.e = provider
	c.SetWorkloadPolicy(policySet, "")

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, "tp-local", provider.currentServiceID)
	require.Equal(t, 1, provider.currentCalls)
	require.Zero(t, provider.discoveryCalls)
	require.Len(t, provider.resolvedSnapshot, 1)
	require.Equal(t, "tp-local:6001", nodes[0].Addr)
}

func TestPreparedWorkloadPolicyRefreshIsClassScoped(t *testing.T) {
	apOnly := schedule.WorkloadPolicySet{
		Rules: map[schedule.WorkloadClass]schedule.WorkloadPolicyRule{
			schedule.WorkloadAP: {PoolIdentity: "ap"},
		},
	}
	tpOnly := schedule.WorkloadPolicySet{
		Rules: map[schedule.WorkloadClass]schedule.WorkloadPolicyRule{
			schedule.WorkloadTP: {
				PoolIdentity: "tp",
				Labels:       map[string]string{"role": "tp"},
			},
		},
	}
	c := &Compile{execType: plan2.ExecTypeTP}

	require.False(t, c.NeedsPreparedWorkloadPolicyRefresh(apOnly))
	require.True(t, c.NeedsPreparedWorkloadPolicyRefresh(tpOnly))

	c.workloadPolicySet = tpOnly.Clone()
	require.False(t, c.NeedsPreparedWorkloadPolicyRefresh(tpOnly.Clone()))
	require.True(t, c.NeedsPreparedWorkloadPolicyRefresh(schedule.WorkloadPolicySet{
		Rules: map[schedule.WorkloadClass]schedule.WorkloadPolicyRule{
			schedule.WorkloadTP: {
				PoolIdentity: "tp-new",
				Labels:       map[string]string{"role": "tp"},
			},
		},
	}))
	require.True(t, c.NeedsPreparedWorkloadPolicyRefresh(schedule.WorkloadPolicySet{}))
	c.workloadPolicySet = schedule.WorkloadPolicySet{}
	require.True(t, c.NeedsPreparedWorkloadPolicyRefresh(
		schedule.WorkloadPolicySet{InvalidReason: "corrupt catalog value"},
	))
}

func TestInternalDDLRemainsMaintenanceWorkload(t *testing.T) {
	c := &Compile{
		isInternal: true,
		execType:   plan2.ExecTypeTP,
		stmt:       &tree.CreateTable{},
	}
	require.Equal(t, schedule.WorkloadMaintenance, c.queryWorkloadClass())

	c.stmt = &tree.Select{}
	require.Equal(t, schedule.WorkloadInternal, c.queryWorkloadClass())
}

func TestScheduleQueryWorkersTPPolicyRejectsRelocationOutsideTargetPool(t *testing.T) {
	policySet, err := schedule.ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"tp": {
				"pool": "tenant-tp",
				"labels": {"role": "tp"},
				"current_cn": "required"
			}
		}
	}`)
	require.NoError(t, err)

	c := NewMockCompile(t)
	c.addr = "ingress:6001"
	c.execType = plan2.ExecTypeTP
	c.tenant = "tenant-a"
	c.e = &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{{
			Service: metadata.CNService{
				ServiceID: "other-tp", PipelineServiceAddress: "other-tp:6001",
				WorkState: metadata.WorkState_Working,
			},
			Mcpu: 4,
		}},
		resolvedNodes: engine.Nodes{{
			Id: "other-tp", Addr: "other-tp:6001", Mcpu: 4,
			WorkState: metadata.WorkState_Working,
		}},
	}
	c.SetWorkloadPolicy(policySet, "")

	_, err = c.scheduleQueryWorkers()
	require.ErrorContains(t, err, schedule.ReasonRequiredCurrentOutsidePool)
	require.Equal(t, schedule.ReasonRequiredCurrentOutsidePool, c.queryPlacement.Reason)
	require.Empty(t, c.queryPlacement.Workers)
}

func TestScheduleQueryWorkersMultiCNPolicyStaysInsideStrictTargetPool(t *testing.T) {
	policySet, err := schedule.ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"ap": {
				"pool": "tenant-ap",
				"labels": {"role": "ap"},
				"current_cn": "excluded",
				"max_workers": 2
			}
		}
	}`)
	require.NoError(t, err)

	c := NewMockCompile(t)
	c.addr = "ingress:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.tenant = "tenant-a"
	c.cnLabel = map[string]string{"account": "tenant-a", "role": "tp"}
	provider := &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{
			{Service: metadata.CNService{
				ServiceID: "ingress", PipelineServiceAddress: "ingress:6001",
				WorkState: metadata.WorkState_Working,
			}, Mcpu: 4},
			{Service: metadata.CNService{
				ServiceID: "ap-1", PipelineServiceAddress: "ap-1:6001",
				WorkState: metadata.WorkState_Working,
			}, Mcpu: 8},
			{Service: metadata.CNService{
				ServiceID: "ap-2", PipelineServiceAddress: "ap-2:6001",
				WorkState: metadata.WorkState_Working,
			}, Mcpu: 12},
		},
		resolvedNodes: engine.Nodes{
			{Id: "ap-1", Addr: "ap-1:6001", Mcpu: 8, WorkState: metadata.WorkState_Working},
			{Id: "ap-2", Addr: "ap-2:6001", Mcpu: 12, WorkState: metadata.WorkState_Working},
		},
	}
	c.e = provider
	c.SetWorkloadPolicy(policySet, "")

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.ElementsMatch(t, []string{"ap-1:6001", "ap-2:6001"}, []string{
		nodes[0].Addr,
		nodes[1].Addr,
	})
	require.Equal(t, engine.QueryPoolFallbackStrict, provider.poolRequest.FallbackPolicy)
	require.Equal(t, map[string]string{
		"account": "tenant-a",
		"role":    "ap",
	}, provider.poolRequest.TargetLabels)
}

func TestScheduleQueryWorkersStrictWorkloadPoolNeverFallsBackToIngress(t *testing.T) {
	policySet, err := schedule.ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"ap": {
				"pool": "tenant-ap",
				"labels": {"role": "ap"}
			}
		}
	}`)
	require.NoError(t, err)

	c := NewMockCompile(t)
	c.addr = "ingress:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.tenant = "tenant-a"
	c.e = &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		resolvedNodes:       engine.Nodes{},
	}
	c.SetWorkloadPolicy(policySet, "")

	_, err = c.scheduleQueryWorkers()
	require.ErrorContains(t, err, schedule.ReasonNoCandidateCN)
	require.Equal(t, schedule.ReasonNoCandidateCN, c.queryPlacement.Reason)
	require.Empty(t, c.queryPlacement.Workers)
}

func TestScheduleQueryWorkersRejectsUnroutableSingleWorkloadTarget(t *testing.T) {
	policySet, err := schedule.ParseWorkloadPolicyConfig(`{
		"version": 1,
		"policies": {
			"ap": {
				"pool": "tenant-ap",
				"labels": {"role": "ap"},
				"current_cn": "excluded"
			}
		}
	}`)
	require.NoError(t, err)

	c := NewMockCompile(t)
	c.addr = "tp-local:6001"
	c.execType = plan2.ExecTypeAP_ONECN
	c.tenant = "tenant-a"
	c.cnLabel = map[string]string{"account": "tenant-a", "role": "tp"}
	c.e = &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{{
			Service: metadata.CNService{
				ServiceID: "ap-unroutable",
				WorkState: metadata.WorkState_Working,
			},
			Mcpu: 4,
		}},
		resolvedNodes: engine.Nodes{{
			Id: "ap-unroutable", Mcpu: 4,
			WorkState: metadata.WorkState_Working,
		}},
	}
	c.SetWorkloadPolicy(policySet, "")

	_, err = c.scheduleQueryWorkers()
	require.ErrorContains(t, err, "cannot satisfy placement")
	require.Equal(t, schedule.ReasonExcludedCurrentCN, c.queryPlacement.Reason)
	require.Equal(t, schedule.ReasonDroppedUnroutableCN, c.queryPlacement.Dropped[0].Reason)
}

func TestScheduleQueryWorkersFallsBackWhenResolvedPoolIsEmpty(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{
			{Service: metadata.CNService{ServiceID: "tp-1", PipelineServiceAddress: "tp-1:6001"}, Mcpu: 4},
			{Service: metadata.CNService{ServiceID: "tp-2", PipelineServiceAddress: "tp-2:6001"}, Mcpu: 4},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{Addr: "local:6001", Mcpu: 6}}, nodes)
	require.Equal(t, schedule.ReasonNoCandidateCN, c.queryPlacement.Reason)
	require.Equal(t, 2, c.queryPlacement.CandidateResolution.DiscoveredCount)
	require.Zero(t, c.queryPlacement.ResolvedCandidateCount)
}

func TestIndependentDiscoveryUsesSameSnapshotForCurrentCNState(t *testing.T) {
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
	c.e = &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{
			{
				Service: metadata.CNService{
					ServiceID:              localID,
					PipelineServiceAddress: "local:6001",
					WorkState:              metadata.WorkState_Draining,
				},
				Mcpu: 6,
			},
			{
				Service: metadata.CNService{
					ServiceID:              "remote-cn",
					PipelineServiceAddress: "remote:6001",
					WorkState:              metadata.WorkState_Working,
				},
				Mcpu: 4,
			},
		},
		resolvedNodes: engine.Nodes{{
			Id:        "remote-cn",
			Addr:      "remote:6001",
			Mcpu:      4,
			WorkState: metadata.WorkState_Working,
		}},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{Id: "remote-cn", Addr: "remote:6001", Mcpu: 4, WorkState: metadata.WorkState_Working}}, nodes)
	require.Equal(t, schedule.WorkerStateDraining, c.queryPlacement.CurrentCN.State)
}

func TestQueryCandidatePipelineRejectsPartialProvider(t *testing.T) {
	for _, provider := range []engine.Engine{
		&schedulerDiscoverOnlyEngine{schedulerTestEngine: &schedulerTestEngine{}},
		&schedulerResolveOnlyEngine{schedulerTestEngine: &schedulerTestEngine{}},
	} {
		_, _, err := queryCandidatePipeline(
			provider,
			queryCandidatePoolRequest{},
			queryCandidateModeExecution,
		)
		require.ErrorContains(t, err, "must implement both")
	}
}

func TestScheduleQueryWorkersRecordsPartialProviderFailure(t *testing.T) {
	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerDiscoverOnlyEngine{schedulerTestEngine: &schedulerTestEngine{}}
	recorder := new(schedule.TraceRecorder)
	c.SetSchedulingTraceRecorder(recorder)
	c.beginSchedulingTraceAttempt()

	_, err := c.scheduleQueryWorkers()
	require.ErrorContains(t, err, "must implement both")
	trace := recorder.Snapshot()
	require.Equal(t, scheduleFailureCandidateProvider, trace.Attempts[0].Failures[0].Category)
}

func TestQueryCandidatePipelineUnwrapsEntireEngine(t *testing.T) {
	provider := &schedulerProviderTestEngine{schedulerTestEngine: &schedulerTestEngine{}}
	wrapper := &engine.EntireEngine{Engine: &engine.EntireEngine{Engine: provider}}

	discoverer, resolver, err := queryCandidatePipeline(
		wrapper,
		queryCandidatePoolRequest{},
		queryCandidateModeExecution,
	)
	require.NoError(t, err)
	require.IsType(t, engineQueryCandidateDiscoverer{}, discoverer)
	require.IsType(t, engineQueryCandidatePoolResolver{}, resolver)
}

func TestScheduleQueryWorkersRecordsPoolResolutionFailure(t *testing.T) {
	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{
			{Service: metadata.CNService{ServiceID: "cn", PipelineServiceAddress: "cn:6001"}, Mcpu: 4},
		},
		resolutionErr: errors.New("pool resolution failed"),
	}
	recorder := new(schedule.TraceRecorder)
	c.SetSchedulingTraceRecorder(recorder)
	c.beginSchedulingTraceAttempt()

	_, err := c.scheduleQueryWorkers()
	require.ErrorContains(t, err, "pool resolution failed")
	trace := recorder.Snapshot()
	require.Equal(t, scheduleFailurePoolResolution, trace.Attempts[0].Failures[0].Category)
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
	require.Equal(t, schedule.DroppedWorkers{
		{Worker: schedule.Worker{ID: "missing-addr", Mcpu: 8, Route: schedule.WorkerRouteRemote}, Reason: schedule.ReasonDroppedUnroutableCN},
	}, c.queryPlacement.Dropped)
}

func TestScheduleQueryWorkersDropsRuntimeIneligibleCandidates(t *testing.T) {
	decisionCounter := metricv2.QueryScheduleDecisionCounter.WithLabelValues(
		"ap-multi-cn", "allowed", schedule.ReasonMultiCN, "satisfied")
	drainingCounter := metricv2.QueryScheduleDroppedWorkerCounter.WithLabelValues(schedule.ReasonDroppedDrainingCN)
	drainedCounter := metricv2.QueryScheduleDroppedWorkerCounter.WithLabelValues(schedule.ReasonDroppedDrainedCN)
	decisionBefore := testutil.ToFloat64(decisionCounter)
	drainingBefore := testutil.ToFloat64(drainingCounter)
	drainedBefore := testutil.ToFloat64(drainedCounter)

	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Id: "working", Addr: "z:6001", Mcpu: 4, WorkState: metadata.WorkState_Working},
			{Id: "draining", Addr: "a:6001", Mcpu: 4, WorkState: metadata.WorkState_Draining},
			{Id: "unknown", Addr: "b:6001", Mcpu: 4, WorkState: metadata.WorkState_Unknown},
			{Id: "drained", Addr: "c:6001", Mcpu: 4, WorkState: metadata.WorkState_Drained},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{
		{Id: "unknown", Addr: "b:6001", Mcpu: 4},
		{Id: "working", Addr: "z:6001", Mcpu: 4, WorkState: metadata.WorkState_Working},
	}, nodes)
	require.Equal(t, schedule.DroppedWorkers{
		{
			Worker: schedule.Worker{
				ID:    "draining",
				Addr:  "a:6001",
				Mcpu:  4,
				State: schedule.WorkerStateDraining,
				Route: schedule.WorkerRouteRemote,
			},
			Reason: schedule.ReasonDroppedDrainingCN,
		},
		{
			Worker: schedule.Worker{
				ID:    "drained",
				Addr:  "c:6001",
				Mcpu:  4,
				State: schedule.WorkerStateDrained,
				Route: schedule.WorkerRouteRemote,
			},
			Reason: schedule.ReasonDroppedDrainedCN,
		},
	}, c.queryPlacement.Dropped)

	require.Equal(t, decisionBefore+1, testutil.ToFloat64(decisionCounter))
	require.Equal(t, drainingBefore+1, testutil.ToFloat64(drainingCounter))
	require.Equal(t, drainedBefore+1, testutil.ToFloat64(drainedCounter))
}

func TestScheduleQueryWorkersRecordsStructuredTrace(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Id: "working", Addr: "working:6001", Mcpu: 8, WorkState: metadata.WorkState_Working},
			{Id: "draining", Addr: "draining:6001", Mcpu: 4, WorkState: metadata.WorkState_Draining},
		},
	}
	recorder := new(schedule.TraceRecorder)
	c.SetSchedulingTraceRecorder(recorder)
	c.beginSchedulingTraceAttempt()

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	trace := recorder.Snapshot()
	require.Equal(t, 1, trace.AttemptCount)
	require.Len(t, trace.Attempts, 1)
	query := trace.Attempts[0].Query
	require.NotNil(t, query)
	require.Equal(t, "ap-multi-cn", query.ExecKind)
	require.Equal(t, string(schedule.CandidateSourceEngineNodes), query.CandidateSource)
	require.Equal(t, string(schedule.PoolResolutionLegacyEngineNodes), query.PoolResolution)
	require.Equal(t, 2, query.DiscoveredCount)
	require.Equal(t, 2, query.ResolvedCount)
	require.Equal(t, 1, query.SelectedCount)
	require.Equal(t, "working", query.Selected[0].ID)
	require.Equal(t, 1, query.DroppedCount)
	require.Equal(t, []schedule.ReasonCount{{
		Reason: schedule.ReasonDroppedDrainingCN,
		Count:  1,
	}}, query.Dropped)
}

func TestScheduleQueryWorkersRecordsCandidateDiscoveryFailure(t *testing.T) {
	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerTestEngine{err: errors.New("discovery failed")}
	recorder := new(schedule.TraceRecorder)
	c.SetSchedulingTraceRecorder(recorder)
	c.beginSchedulingTraceAttempt()

	_, err := c.scheduleQueryWorkers()
	require.ErrorContains(t, err, "discovery failed")

	trace := recorder.Snapshot()
	require.Len(t, trace.Attempts, 1)
	require.Nil(t, trace.Attempts[0].Query)
	require.Equal(t, 1, trace.Attempts[0].FailureCount)
	require.Equal(t, scheduleFailureCandidateDiscovery, trace.Attempts[0].Failures[0].Category)
	require.True(t, trace.PersistStandalone())
}

func TestPreparedCompileReleaseDetachesStatementTrace(t *testing.T) {
	c := NewMockCompile(t)
	c.isPrepare = true
	recorder := new(schedule.TraceRecorder)
	c.SetSchedulingTraceRecorder(recorder)
	c.beginSchedulingTraceAttempt()

	c.Release()

	require.Nil(t, c.schedulingTrace)
	require.Zero(t, c.schedulingAttempt)
	require.NotNil(t, c.proc)
}

func TestValidateScheduledQueryRoutesRecordsFailure(t *testing.T) {
	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeAP_MULTICN
	recorder := new(schedule.TraceRecorder)
	c.SetSchedulingTraceRecorder(recorder)
	c.beginSchedulingTraceAttempt()
	placement := schedule.QueryDecision{
		ExecKind:        schedule.QueryExecAPMultiCN,
		Reason:          schedule.ReasonMultiCN,
		CurrentCNPolicy: schedule.CurrentCNAllowed,
		Satisfied:       true,
	}

	err := c.validateScheduledQueryRoutes(engine.Nodes{{
		Id:        "draining",
		Addr:      "draining:6001",
		Mcpu:      4,
		WorkState: metadata.WorkState_Draining,
	}}, placement)
	require.ErrorContains(t, err, "draining")

	trace := recorder.Snapshot()
	require.Equal(t, scheduleFailureRuntimeIneligibleSelectedWorker, trace.Attempts[0].Failures[0].Category)
	require.NotNil(t, trace.Attempts[0].Failures[0].Worker)
	require.Equal(t, "draining", trace.Attempts[0].Failures[0].Worker.ID)
}

func TestScheduleQueryWorkersFallsBackWhenCandidatesRuntimeIneligible(t *testing.T) {
	fallbackCounter := metricv2.QueryScheduleDecisionCounter.WithLabelValues(
		"ap-multi-cn", "allowed", schedule.ReasonNoCandidateCN, "satisfied")
	fallbackBefore := testutil.ToFloat64(fallbackCounter)

	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Id: "draining", Addr: "draining:6001", Mcpu: 4, WorkState: metadata.WorkState_Draining},
			{Id: "drained", Addr: "drained:6001", Mcpu: 4, WorkState: metadata.WorkState_Drained},
		},
	}

	nodes, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{Addr: "local:6001", Mcpu: 6}}, nodes)
	require.Equal(t, schedule.ReasonNoCandidateCN, c.queryPlacement.Reason)
	require.Equal(t, 2, len(c.queryPlacement.Dropped))
	require.Equal(t, fallbackBefore+1, testutil.ToFloat64(fallbackCounter))
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

func TestScheduleQueryWorkersDoesNotTreatQueryClientAsCurrentCNConstraint(t *testing.T) {
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
	require.Equal(t, engine.Nodes{{Id: "remote", Addr: "remote:6001", Mcpu: 4}}, nodes)
}

func TestScheduleQueryWorkersRejectsRequiredCurrentCNWithoutAddressForMultiCN(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const localID = "local-cn"
	c := NewMockCompile(t)
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	c.SetQuerySchedulingIntent(schedule.SchedulingIntent{CurrentCNPolicy: schedule.CurrentCNRequired})
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: localID}).AnyTimes()
	c.proc.Base.LockService = lockSvc
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{
			{Id: localID, Mcpu: 6},
			{Id: "remote", Addr: "remote:6001", Mcpu: 4},
		},
	}

	_, err := c.scheduleQueryWorkers()
	require.ErrorContains(t, err, "without address for multi-CN execution")
}

func TestValidateScheduledQueryRoutesRejectsRuntimeIneligibleSelectedWorker(t *testing.T) {
	failureCounter := metricv2.QueryScheduleSelectedWorkerFailureCounter.WithLabelValues(
		scheduleFailureRuntimeIneligibleSelectedWorker)
	failureBefore := testutil.ToFloat64(failureCounter)

	c := NewMockCompile(t)
	c.execType = plan2.ExecTypeAP_MULTICN

	err := c.validateScheduledQueryRoutes(engine.Nodes{
		{Id: "draining", Addr: "draining:6001", Mcpu: 4, WorkState: metadata.WorkState_Draining},
	}, schedule.QueryDecision{Reason: schedule.ReasonMultiCN})
	require.ErrorContains(t, err, "runtime state Draining")
	require.Equal(t, failureBefore+1, testutil.ToFloat64(failureCounter))
}

func TestCompileResetRecordsOnePreparedReuseSchedulingAttempt(t *testing.T) {
	c := NewCompile(
		"local:6001",
		"",
		"execute p1",
		"",
		"",
		nil,
		motestutil.NewProcess(t),
		nil,
		false,
		nil,
		time.Now(),
	)
	c.anal = newAnalyzeModule()
	c.anal.qry = &plan.Query{}
	defer c.Release()
	c.queryPlacement = schedule.QueryDecision{
		ExecKind:  schedule.QueryExecAPMultiCN,
		Reason:    "reused-placement",
		Satisfied: true,
	}

	var lastRecorder *schedule.TraceRecorder
	for execution := 0; execution < 10; execution++ {
		recorder := new(schedule.TraceRecorder)
		lastRecorder = recorder
		c.SetSchedulingTraceRecorder(recorder)
		c.Reset(c.proc, time.Now(), nil, "execute p1")
		trace := recorder.Snapshot()
		require.Equal(t, 1, trace.AttemptCount)
		require.Len(t, trace.Attempts, 1)
		require.Equal(t, "reused-placement", trace.Attempts[0].Query.Reason)
	}

	c.beginSchedulingTraceAttempt()
	c.recordQuerySchedulingTrace(schedule.QueryDecision{
		ExecKind:  schedule.QueryExecAPMultiCN,
		Reason:    "retry-placement",
		Satisfied: true,
	})
	retryTrace := lastRecorder.Snapshot()
	require.Equal(t, 2, retryTrace.AttemptCount)
	require.Equal(t, "reused-placement", retryTrace.Attempts[0].Query.Reason)
	require.Equal(t, "retry-placement", retryTrace.Attempts[1].Query.Reason)
}

func TestRecordScanSchedulingMetricsRecordsEveryScan(t *testing.T) {
	c := NewMockCompile(t)
	c.cnList = engine.Nodes{
		{Id: "cn1", Addr: "cn1:6001", Mcpu: 4},
		{Id: "cn2", Addr: "cn2:6001", Mcpu: 4},
	}
	c.queryPlacement = schedule.QueryDecision{Reason: schedule.ReasonMultiCN}
	stats := &schedule.ScanStats{
		BlockNum:   42,
		Dop:        8,
		ForceOneCN: true,
	}

	decisionCounter := metricv2.ScanScheduleDecisionCounter.WithLabelValues(
		schedule.ReasonScanForceOneCN, schedule.ReasonMultiCN, "true", "true", "true", "true")
	decisionBefore := testutil.ToFloat64(decisionCounter)
	decision := schedule.ScanDecision{
		Workers:   schedule.Workers{{ID: "cn1", Addr: "cn1:6001", Mcpu: 4}},
		LocalOnly: true,
		Reason:    schedule.ReasonScanForceOneCN,
	}
	c.recordScanSchedulingMetrics(decision, stats, true)
	c.recordScanSchedulingMetrics(decision, stats, true)

	require.Equal(t, decisionBefore+2, testutil.ToFloat64(decisionCounter))
}

func TestScheduleQueryWorkersRejectsDrainingRequiredCurrentCN(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const localID = "local-cn-draining"
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	c.SetQuerySchedulingIntent(schedule.SchedulingIntent{CurrentCNPolicy: schedule.CurrentCNRequired})
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: localID}).AnyTimes()
	c.proc.Base.LockService = lockSvc
	c.e = &schedulerTestEngine{
		nodes: engine.Nodes{{Id: "remote", Addr: "remote:6001", Mcpu: 4, WorkState: metadata.WorkState_Working}},
	}

	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.ClusterService, schedulerTestCluster{
		cns: []metadata.CNService{{ServiceID: localID, WorkState: metadata.WorkState_Draining}},
	})
	moruntime.SetupServiceBasedRuntime(localID, rt)

	_, err := c.scheduleQueryWorkers()
	require.ErrorContains(t, err, schedule.ReasonCurrentCNDraining)
	require.Equal(t, schedule.ReasonCurrentCNDraining, c.queryPlacement.Reason)
	require.False(t, c.queryPlacement.Satisfied)
}

func TestScheduleQueryWorkersAllowsRequiredCurrentCNWithoutAddressForLocalFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const localID = "local-cn"
	c := NewMockCompile(t)
	c.ncpu = 6
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}
	c.SetQuerySchedulingIntent(schedule.SchedulingIntent{CurrentCNPolicy: schedule.CurrentCNRequired})
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
	c.SetQuerySchedulingIntent(schedule.SchedulingIntent{CurrentCNPolicy: schedule.CurrentCNRequired})
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
	c.SetQuerySchedulingIntent(schedule.SchedulingIntent{CurrentCNPolicy: schedule.CurrentCNRequired})
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
	c.SetQuerySchedulingIntent(schedule.SchedulingIntent{CurrentCNPolicy: schedule.CurrentCNRequired})
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
	c.SetQuerySchedulingIntent(schedule.SchedulingIntent{CurrentCNPolicy: schedule.CurrentCNRequired})
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
		{Id: "remote", Addr: "remote:6001", Mcpu: 4},
		{Id: "stale-local", Addr: "local:6001", Mcpu: 6},
	}, nodes)

	decision, err := c.decideQueryPlacement()
	require.NoError(t, err)
	require.Equal(t, schedule.ReasonRequiredCurrentCN, decision.Reason)
	require.Equal(t, 2, len(decision.Workers))
	require.Equal(t, "stale-local", decision.Workers[1].ID)
	require.Equal(t, "local:6001", decision.Workers[1].Addr)
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

func TestScheduleQueryWorkersSelectsStableStatementSubset(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.SetStmtProfile(process.NewStmtProfile(uuid.Nil, uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")))
	provider := &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		resolvedNodes: engine.Nodes{
			{Id: "cn-4", Addr: "4:6001", Mcpu: 4},
			{Id: "cn-2", Addr: "2:6001", Mcpu: 2},
			{Id: "cn-1", Addr: "1:6001", Mcpu: 1},
			{Id: "cn-3", Addr: "3:6001", Mcpu: 3},
		},
	}
	c.e = provider
	c.SetQuerySchedulingIntent(schedule.SchedulingIntent{
		Explicit:          true,
		EmptyWorkerPolicy: schedule.EmptyWorkerFail,
		WorkerSet: schedule.WorkerSetPolicy{
			Mode: schedule.WorkerSetMax, MaxWorkers: 2,
		},
	})

	first, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Len(t, first, 2)
	provider.resolvedNodes = engine.Nodes{
		provider.resolvedNodes[2], provider.resolvedNodes[0], provider.resolvedNodes[3], provider.resolvedNodes[1],
	}
	second, err := c.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, schedule.WorkerSelectionAlgorithmV1, c.queryPlacement.Intent.WorkerSet.AlgorithmVersion)
	require.Equal(t, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", c.queryPlacement.Intent.WorkerSet.SelectionKey)
}

func TestScheduleQueryWorkersStrictIntentUnhappyPaths(t *testing.T) {
	t.Run("invalid intent fails before candidate discovery", func(t *testing.T) {
		for _, tt := range []struct {
			name   string
			intent schedule.SchedulingIntent
			reason string
		}{
			{
				name: "pool policy",
				intent: schedule.SchedulingIntent{
					Explicit: true, PoolFallback: schedule.PoolFallbackPolicy(255),
					EmptyWorkerPolicy: schedule.EmptyWorkerFail,
					WorkerSet:         schedule.WorkerSetPolicy{Mode: schedule.WorkerSetAll},
				},
				reason: schedule.ReasonInvalidSchedulingIntent,
			},
			{
				name: "current CN policy",
				intent: schedule.SchedulingIntent{
					CurrentCNPolicy: schedule.CurrentCNPolicy(255),
					WorkerSet:       schedule.WorkerSetPolicy{Mode: schedule.WorkerSetAll},
				},
				reason: schedule.ReasonInvalidCurrentCNPolicy,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				provider := &schedulerProviderTestEngine{schedulerTestEngine: &schedulerTestEngine{}}
				c := NewMockCompile(t)
				c.execType = plan2.ExecTypeAP_MULTICN
				c.e = provider
				c.SetQuerySchedulingIntent(tt.intent)

				_, err := c.scheduleQueryWorkers()
				require.ErrorContains(t, err, tt.reason)
				require.Zero(t, provider.discoveryCalls)
				require.Zero(t, provider.resolutionCalls)
			})
		}
	})

	t.Run("legacy provider cannot prove strict pool", func(t *testing.T) {
		c := NewMockCompile(t)
		c.execType = plan2.ExecTypeAP_MULTICN
		c.e = &schedulerTestEngine{}
		c.SetQuerySchedulingIntent(schedule.SchedulingIntent{
			Explicit: true, PoolFallback: schedule.PoolFallbackStrict, EmptyWorkerPolicy: schedule.EmptyWorkerFail,
		})

		_, err := c.scheduleQueryWorkers()
		require.ErrorContains(t, err, "strict query pool intent requires explicit")
	})

	t.Run("empty resolved pool never silently runs local", func(t *testing.T) {
		c := NewMockCompile(t)
		c.addr = "local:6001"
		c.execType = plan2.ExecTypeAP_MULTICN
		c.e = &schedulerProviderTestEngine{schedulerTestEngine: &schedulerTestEngine{}}
		c.SetQuerySchedulingIntent(schedule.SchedulingIntent{
			Explicit: true, PoolFallback: schedule.PoolFallbackStrict, EmptyWorkerPolicy: schedule.EmptyWorkerFail,
		})

		_, err := c.scheduleQueryWorkers()
		require.ErrorContains(t, err, schedule.ReasonNoCandidateCN)
	})

	t.Run("local exec kind satisfies explicit upper bound", func(t *testing.T) {
		c := NewMockCompile(t)
		c.addr = "local:6001"
		c.execType = plan2.ExecTypeAP_ONECN
		c.SetQuerySchedulingIntent(schedule.SchedulingIntent{
			Explicit: true, PoolFallback: schedule.PoolFallbackStrict,
			WorkerSet: schedule.WorkerSetPolicy{
				Mode:       schedule.WorkerSetMax,
				MaxWorkers: 1,
			},
		})

		workers, err := c.scheduleQueryWorkers()
		require.NoError(t, err)
		require.Len(t, workers, 1)
		require.Equal(t, "local:6001", workers[0].Addr)
	})
}

func TestQuerySchedulingSelectionKeyHasDeterministicSQLFallback(t *testing.T) {
	c := &Compile{originSQL: "select 1"}
	first := c.querySchedulingSelectionKey()
	require.NotEmpty(t, first)
	require.Equal(t, first, c.querySchedulingSelectionKey())
	require.NotEqual(t, first, (&Compile{originSQL: "select 2"}).querySchedulingSelectionKey())
	require.LessOrEqual(t, len(first), 64)
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
