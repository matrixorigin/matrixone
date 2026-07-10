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
		{ID: "cn-a", Addr: "a:6001", Mcpu: 1},
		{ID: "cn-b", Addr: "b:6001", Mcpu: 8, State: schedule.WorkerStateDraining},
	}, resolved.workers)
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
