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

package schedule

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecideSingleWorkerStagePlacementUsesSingleQueryWorker(t *testing.T) {
	queryWorkers := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 8}}
	decision := DecideSingleWorkerStagePlacement(StageRequest{
		QueryWorkers: queryWorkers,
		CurrentCN:    Worker{ID: "local", Addr: "local:6001", Mcpu: 4},
	})

	require.Equal(t, ReasonStageSingleQueryWorker, decision.Reason)
	require.Equal(t, queryWorkers[0], decision.Worker)

	decision.Worker.Mcpu = 1
	require.Equal(t, 8, queryWorkers[0].Mcpu)
}

func TestDecideSingleWorkerStagePlacementKeepsLocalIdentityWithoutRoute(t *testing.T) {
	decision := DecideSingleWorkerStagePlacement(StageRequest{
		QueryWorkers: Workers{{ID: "local", Mcpu: 4}},
		CurrentCN:    Worker{ID: "local", Addr: "local:6001", Mcpu: 4},
	})

	require.Equal(t, ReasonStageSingleQueryWorker, decision.Reason)
	require.Equal(t, Worker{ID: "local", Mcpu: 4}, decision.Worker)
}

func TestDecideSingleWorkerStagePlacementUsesCurrentCNCoordinator(t *testing.T) {
	current := Worker{ID: "local", Addr: "local:6001", Mcpu: 4}
	for _, workers := range []Workers{
		nil,
		{
			{ID: "remote-a", Addr: "remote-a:6001", Mcpu: 8},
			{ID: "remote-b", Addr: "remote-b:6001", Mcpu: 8},
		},
	} {
		decision := DecideSingleWorkerStagePlacement(StageRequest{
			QueryWorkers: workers,
			CurrentCN:    current,
		})

		require.Equal(t, ReasonStageCurrentCoordinator, decision.Reason)
		require.Equal(t, current, decision.Worker)
	}
}

func TestDecideQueryWorkerStagePlacementUsesQueryWorkers(t *testing.T) {
	queryWorkers := Workers{
		{ID: "local", Mcpu: 4},
		{ID: "remote", Addr: "remote:6001", Mcpu: 8},
	}
	decision := DecideQueryWorkerStagePlacement(StageRequest{
		QueryWorkers: queryWorkers,
		CurrentCN:    Worker{ID: "current", Addr: "current:6001", Mcpu: 2},
	})

	require.Equal(t, ReasonStageQueryWorkers, decision.Reason)
	require.Equal(t, queryWorkers, decision.Workers)

	decision.Workers[0].Mcpu = 1
	require.Equal(t, 4, queryWorkers[0].Mcpu)
}

func TestDecideQueryWorkerStagePlacementKeepsNoWorkerExplicit(t *testing.T) {
	decision := DecideQueryWorkerStagePlacement(StageRequest{
		CurrentCN: Worker{ID: "current", Addr: "current:6001", Mcpu: 2},
	})

	require.Equal(t, ReasonStageNoQueryWorkers, decision.Reason)
	require.Nil(t, decision.Workers)
}
