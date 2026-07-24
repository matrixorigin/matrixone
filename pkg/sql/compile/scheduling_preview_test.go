// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPreviewQuerySchedulingRejectsLegacyCandidateProvider(t *testing.T) {
	e := &schedulerTestEngine{}
	trace := PreviewQueryScheduling(SchedulingPreviewRequest{
		Query: &plan.Query{
			Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN}},
		},
		Engine: e,
	})

	require.Equal(t, schedule.TraceModePreview, trace.Mode)
	require.Zero(t, e.calls)
	require.Nil(t, trace.Attempts[0].Query)
	require.Equal(t, scheduleFailureCandidateProvider, trace.Attempts[0].Failures[0].Category)
}

func TestPreviewQuerySchedulingDoesNotDiscoverCandidatesForLocalQuery(t *testing.T) {
	e := &schedulerTestEngine{err: assertUnexpectedCandidateDiscovery{}}
	trace := PreviewQueryScheduling(SchedulingPreviewRequest{
		Query:   &plan.Query{},
		Engine:  e,
		Address: "local:6001",
	})

	require.Zero(t, e.calls)
	query := trace.Attempts[0].Query
	require.NotNil(t, query)
	require.Equal(t, schedule.QueryExecTP.String(), query.ExecKind)
	require.Equal(t, string(schedule.CandidateSourceNotRequired), query.CandidateSource)
	require.Equal(t, string(schedule.PoolResolutionNotRequired), query.PoolResolution)
}

func TestPreviewQuerySchedulingUsesSameWorkloadPolicyBoundaryAsExecution(t *testing.T) {
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
	provider := &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		candidates: engine.QueryCandidates{{
			Service: metadata.CNService{
				ServiceID: "etl", PipelineServiceAddress: "etl:6001",
				WorkState: metadata.WorkState_Working,
			},
			Mcpu: 8,
		}},
		resolvedNodes: engine.Nodes{{
			Id: "etl", Addr: "etl:6001", Mcpu: 8,
			WorkState: metadata.WorkState_Working,
		}},
	}

	trace := PreviewQueryScheduling(SchedulingPreviewRequest{
		Query: &plan.Query{
			Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN}},
		},
		Engine:   provider,
		Address:  "tp:6001",
		Tenant:   "tenant-a",
		CNLabel:  map[string]string{"account": "tenant-a", "role": "tp"},
		Intent:   schedule.SchedulingIntent{WorkerSet: schedule.WorkerSetPolicy{Mode: schedule.WorkerSetAll}},
		Policy:   policySet,
		Workload: schedule.WorkloadLoad,
	})

	require.Len(t, trace.Attempts, 1)
	query := trace.Attempts[0].Query
	require.NotNil(t, query)
	require.Equal(t, "load", query.WorkloadClass)
	require.Equal(t, "account-global", query.WorkloadPolicySource)
	require.Equal(t, policySet.Generation, query.WorkloadPolicyGeneration)
	require.Equal(t, "tenant-etl", query.RequestedPool)
	require.Equal(t, "etl", query.Selected[0].ID)
	require.Equal(t, map[string]string{
		"account": "tenant-a",
		"role":    "etl",
	}, provider.poolRequest.TargetLabels)
	require.Equal(t, map[string]string{
		"account": "tenant-a",
		"role":    "tp",
	}, provider.poolRequest.CNLabel)
}

func TestPreviewQuerySchedulingRejectsInvalidPolicyBeforeDiscovery(t *testing.T) {
	provider := &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
	}
	trace := PreviewQueryScheduling(SchedulingPreviewRequest{
		Query: &plan.Query{
			Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN}},
		},
		Engine: provider,
		Policy: schedule.WorkloadPolicySet{
			InvalidReason: "corrupt account policy",
		},
	})

	require.Zero(t, provider.discoveryCalls)
	require.Zero(t, provider.resolutionCalls)
	require.NotNil(t, trace.Attempts[0].Query)
	require.False(t, trace.Attempts[0].Query.Satisfied)
	require.Equal(t, schedule.ReasonInvalidSchedulingIntent, trace.Attempts[0].Query.Reason)
}

func TestPreviewQuerySchedulingRecordsUnhappyPathsWithoutReturningError(t *testing.T) {
	t.Run("nil query", func(t *testing.T) {
		trace := PreviewQueryScheduling(SchedulingPreviewRequest{})
		require.Equal(t, schedule.TraceModePreview, trace.Mode)
		require.Nil(t, trace.Attempts[0].Query)
		require.Equal(t, scheduleFailureInvalidQuery, trace.Attempts[0].Failures[0].Category)
	})

	t.Run("missing engine", func(t *testing.T) {
		trace := PreviewQueryScheduling(SchedulingPreviewRequest{
			Query: &plan.Query{
				Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN}},
			},
		})
		require.Nil(t, trace.Attempts[0].Query)
		require.Equal(t, scheduleFailureCandidateProvider, trace.Attempts[0].Failures[0].Category)
	})

	t.Run("typed nil engine", func(t *testing.T) {
		var e *engine.EntireEngine
		trace := PreviewQueryScheduling(SchedulingPreviewRequest{
			Query: &plan.Query{
				Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN}},
			},
			Engine: e,
		})
		require.Nil(t, trace.Attempts[0].Query)
		require.Equal(t, scheduleFailureCandidateProvider, trace.Attempts[0].Failures[0].Category)
	})

	t.Run("empty entire engine wrapper", func(t *testing.T) {
		trace := PreviewQueryScheduling(SchedulingPreviewRequest{
			Query: &plan.Query{
				Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN}},
			},
			Engine: &engine.EntireEngine{},
		})
		require.Nil(t, trace.Attempts[0].Query)
		require.Equal(t, scheduleFailureCandidateProvider, trace.Attempts[0].Failures[0].Category)
	})

	t.Run("cyclic entire engine wrapper", func(t *testing.T) {
		e := &engine.EntireEngine{}
		e.Engine = e
		trace := PreviewQueryScheduling(SchedulingPreviewRequest{
			Query: &plan.Query{
				Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN}},
			},
			Engine: e,
		})
		require.Nil(t, trace.Attempts[0].Query)
		require.Equal(t, scheduleFailureCandidateProvider, trace.Attempts[0].Failures[0].Category)
	})

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		e := &schedulerTestEngine{nodes: engine.Nodes{{Id: "cn", Addr: "cn:6001"}}}
		trace := PreviewQueryScheduling(SchedulingPreviewRequest{
			Context: ctx,
			Query: &plan.Query{
				Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN}},
			},
			Engine: e,
		})
		require.Zero(t, e.calls)
		require.Nil(t, trace.Attempts[0].Query)
		require.Equal(t, scheduleFailureCandidateDiscovery, trace.Attempts[0].Failures[0].Category)
	})
}

type assertUnexpectedCandidateDiscovery struct{}

func (assertUnexpectedCandidateDiscovery) Error() string {
	return "candidate discovery should not run"
}

func TestLegacyEngineNodesPoolResolverPreservesCandidates(t *testing.T) {
	nodes := engine.Nodes{
		{Id: "cn-a", Addr: "a:6001", Mcpu: 0},
		{Id: "cn-b", Addr: "b:6001", Mcpu: 8, WorkState: metadata.WorkState_Draining},
	}
	resolved, err := (legacyEngineNodesPoolResolver{}).resolve(
		context.Background(),
		discoveredQueryCandidates{
			legacyNodes: nodes,
			source:      schedule.CandidateSourceEngineNodes,
		},
		queryCandidatePoolRequest{Tenant: "tenant", CNLabel: map[string]string{"role": "ap"}},
	)

	require.NoError(t, err)
	require.Equal(t, 2, resolved.resolution.DiscoveredCount)
	require.Equal(t, schedule.CandidateSourceEngineNodes, resolved.resolution.DiscoverySource)
	require.Equal(t, schedule.PoolResolutionLegacyEngineNodes, resolved.resolution.PoolResolution)
	require.Equal(t, schedule.Workers{
		{ID: "cn-a", Addr: "a:6001", Mcpu: 1, Route: schedule.WorkerRouteRemote},
		{ID: "cn-b", Addr: "b:6001", Mcpu: 8, State: schedule.WorkerStateDraining, Route: schedule.WorkerRouteRemote},
	}, resolved.pool.Workers)
	require.Equal(t, 0, nodes[0].Mcpu)
}

func TestCandidateBoundariesHonorCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := &schedulerTestEngine{nodes: engine.Nodes{{Id: "cn", Addr: "cn:6001"}}}
	discoverer := legacyEngineNodesCandidateDiscoverer{engine: e}

	_, err := discoverer.discover(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, e.calls)

	_, err = (legacyEngineNodesPoolResolver{}).resolve(
		ctx,
		discoveredQueryCandidates{},
		queryCandidatePoolRequest{},
	)
	require.ErrorIs(t, err, context.Canceled)
}
