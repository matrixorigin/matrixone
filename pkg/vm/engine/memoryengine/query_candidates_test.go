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

package memoryengine

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type queryCandidateTestCluster struct {
	clusterservice.MOCluster
	candidates []metadata.CNService
}

func (c queryCandidateTestCluster) GetCNServiceWithoutWorkingState(
	_ clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	for _, candidate := range c.candidates {
		if !apply(candidate) {
			return
		}
	}
}

func TestQueryCandidateProviders(t *testing.T) {
	appLabels := map[string]metadata.LabelList{
		"account": {Labels: []string{"app"}},
	}
	e := &Engine{cluster: queryCandidateTestCluster{candidates: []metadata.CNService{
		{ServiceID: "app-working", PipelineServiceAddress: "app-working:6001", Labels: appLabels, WorkState: metadata.WorkState_Working},
		{ServiceID: "app-draining", PipelineServiceAddress: "app-draining:6001", Labels: appLabels, WorkState: metadata.WorkState_Draining},
		{ServiceID: "unlabeled", PipelineServiceAddress: "unlabeled:6001", WorkState: metadata.WorkState_Working},
	}}}

	candidates, err := e.DiscoverQueryCandidates(context.Background())
	require.NoError(t, err)
	require.Len(t, candidates, 3)

	nodes, err := e.ResolveQueryCandidatePool(context.Background(), candidates, engine.QueryCandidatePoolRequest{
		Tenant:  "app",
		CNLabel: map[string]string{"account": "app"},
	})
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{
		{Mcpu: 1, Id: "app-working", Addr: "app-working:6001", WorkState: metadata.WorkState_Working},
		{Mcpu: 1, Id: "unlabeled", Addr: "unlabeled:6001", WorkState: metadata.WorkState_Working},
		{Mcpu: 1, Id: "app-draining", Addr: "app-draining:6001", WorkState: metadata.WorkState_Draining},
	}, nodes)

	nodes, err = e.ResolveQueryCandidatePool(context.Background(), candidates, engine.QueryCandidatePoolRequest{
		IsInternal: true,
	})
	require.NoError(t, err)
	require.Len(t, nodes, 3)
}

func TestQueryCandidateProvidersHonorCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := &Engine{cluster: queryCandidateTestCluster{}}

	_, err := e.DiscoverQueryCandidates(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = e.ResolveQueryCandidatePool(ctx, nil, engine.QueryCandidatePoolRequest{})
	require.ErrorIs(t, err, context.Canceled)
}
