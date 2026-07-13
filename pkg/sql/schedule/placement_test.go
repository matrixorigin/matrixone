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

func TestDecideQueryPlacementKeepsLocalExecTypesLocal(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	for _, execKind := range []QueryExecKind{QueryExecTP, QueryExecAPOneCN} {
		decision := DecideQueryPlacement(QueryRequest{
			ExecKind:   execKind,
			CurrentCN:  local,
			Candidates: candidates,
		})

		require.Equal(t, execKind, decision.ExecKind)
		require.Equal(t, ReasonLocalExecType, decision.Reason)
		require.Equal(t, Workers{local}, decision.Workers)
		require.True(t, decision.Satisfied)
	}
}

func TestDecideQueryPlacementRejectsInvalidCurrentCNPolicy(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}

	for _, execKind := range []QueryExecKind{QueryExecTP, QueryExecAPOneCN, QueryExecAPMultiCN} {
		decision := DecideQueryPlacement(QueryRequest{
			ExecKind:        execKind,
			CurrentCN:       local,
			Candidates:      Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}},
			CurrentCNPolicy: CurrentCNPolicy(99),
		})

		require.Equal(t, execKind, decision.ExecKind)
		require.Equal(t, ReasonInvalidCurrentCNPolicy, decision.Reason)
		require.Empty(t, decision.Workers)
		require.False(t, decision.Satisfied)
		require.Equal(t, "unknown", decision.CurrentCNPolicy.String())
	}
}

func TestDecideQueryPlacementRejectsExcludedCurrentCNForLocalExecTypes(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}

	for _, execKind := range []QueryExecKind{QueryExecTP, QueryExecAPOneCN} {
		decision := DecideQueryPlacement(QueryRequest{
			ExecKind:        execKind,
			CurrentCN:       local,
			CurrentCNPolicy: CurrentCNExcluded,
		})

		require.Equal(t, execKind, decision.ExecKind)
		require.Equal(t, ReasonExcludedCurrentCN, decision.Reason)
		require.Empty(t, decision.Workers)
		require.False(t, decision.Satisfied)
	}
}

func TestDecideQueryPlacementKeepsRequiredAndPreferredLocalExecTypesLocal(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}

	for _, policy := range []CurrentCNPolicy{CurrentCNRequired, CurrentCNPreferred} {
		for _, execKind := range []QueryExecKind{QueryExecTP, QueryExecAPOneCN} {
			decision := DecideQueryPlacement(QueryRequest{
				ExecKind:        execKind,
				CurrentCN:       local,
				CurrentCNPolicy: policy,
			})

			require.Equal(t, execKind, decision.ExecKind)
			require.Equal(t, ReasonLocalExecType, decision.Reason)
			require.Equal(t, Workers{local}, decision.Workers)
			require.True(t, decision.Satisfied)
		}
	}
}

func TestDecideQueryPlacementAllowsLocalExecTypeWithoutRoute(t *testing.T) {
	local := Worker{Mcpu: 1}

	for _, policy := range []CurrentCNPolicy{CurrentCNAllowed, CurrentCNPreferred} {
		for _, execKind := range []QueryExecKind{QueryExecTP, QueryExecAPOneCN} {
			decision := DecideQueryPlacement(QueryRequest{
				ExecKind:        execKind,
				CurrentCN:       local,
				CurrentCNPolicy: policy,
			})

			require.Equal(t, execKind, decision.ExecKind)
			require.Equal(t, ReasonLocalExecType, decision.Reason)
			require.Equal(t, Workers{local}, decision.Workers)
			require.True(t, decision.Satisfied)
		}
	}
}

func TestDecideQueryPlacementAllowsRequiredLocalExecTypeWithoutRoute(t *testing.T) {
	local := Worker{ID: "local", Mcpu: 8}

	for _, execKind := range []QueryExecKind{QueryExecTP, QueryExecAPOneCN} {
		decision := DecideQueryPlacement(QueryRequest{
			ExecKind:        execKind,
			CurrentCN:       local,
			CurrentCNPolicy: CurrentCNRequired,
		})

		require.Equal(t, execKind, decision.ExecKind)
		require.Equal(t, ReasonLocalExecType, decision.Reason)
		require.Equal(t, Workers{local}, decision.Workers)
		require.True(t, decision.Satisfied)
	}
}

func TestDecideQueryPlacementRejectsRequiredLocalExecTypeWithoutIdentity(t *testing.T) {
	for _, execKind := range []QueryExecKind{QueryExecTP, QueryExecAPOneCN} {
		decision := DecideQueryPlacement(QueryRequest{
			ExecKind:        execKind,
			CurrentCNPolicy: CurrentCNRequired,
		})

		require.Equal(t, execKind, decision.ExecKind)
		require.Equal(t, ReasonCurrentCNMissingIdentity, decision.Reason)
		require.Empty(t, decision.Workers)
		require.False(t, decision.Satisfied)
	}
}

func TestDecideQueryPlacementUsesCandidatesForMultiCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{
		{ID: "cn2", Addr: "cn2:6001", Mcpu: 16},
		{ID: "cn1", Addr: "cn1:6001", Mcpu: 12},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:   QueryExecAPMultiCN,
		CurrentCN:  local,
		Candidates: candidates,
		CandidateResolution: CandidateResolution{
			DiscoverySource: CandidateSourceEngineNodes,
			PoolResolution:  PoolResolutionLegacyEngineNodes,
			DiscoveredCount: 2,
		},
	})

	require.Equal(t, QueryExecAPMultiCN, decision.ExecKind)
	require.Equal(t, ReasonMultiCN, decision.Reason)
	require.Equal(t, Workers{candidates[1], candidates[0]}, decision.Workers)
	require.Equal(t, CurrentCNAllowed, decision.CurrentCNPolicy)
	require.Equal(t, local, decision.CurrentCN)
	require.Equal(t, CandidateSourceEngineNodes, decision.CandidateResolution.DiscoverySource)
	require.Equal(t, PoolResolutionLegacyEngineNodes, decision.CandidateResolution.PoolResolution)
	require.Equal(t, 2, decision.ResolvedCandidateCount)
	require.True(t, decision.Satisfied)

	decision.Workers[0].Mcpu = 1
	require.Equal(t, 12, candidates[1].Mcpu)
	require.Equal(t, 16, candidates[0].Mcpu)
}

func TestDecideQueryPlacementDropsUnroutableCandidates(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{
		{ID: "missing-addr", Mcpu: 12},
		{ID: "remote", Addr: "remote:6001", Mcpu: 16},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:   QueryExecAPMultiCN,
		CurrentCN:  local,
		Candidates: candidates,
	})

	require.Equal(t, Workers{candidates[1]}, decision.Workers)
	require.Equal(t, ReasonMultiCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementDropsRuntimeIneligibleCandidates(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	working := Worker{ID: "working", Addr: "z:6001", Mcpu: 16, State: WorkerStateWorking}
	unknown := Worker{ID: "unknown", Addr: "a:6001", Mcpu: 12}
	draining := Worker{ID: "draining", Addr: "b:6001", Mcpu: 8, State: WorkerStateDraining}
	drained := Worker{ID: "drained", Addr: "c:6001", Mcpu: 8, State: WorkerStateDrained}
	unroutable := Worker{ID: "missing-addr", Mcpu: 8}
	duplicate := Worker{ID: "working", Addr: "stale:6001", Mcpu: 4, State: WorkerStateWorking}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: local,
		Candidates: Workers{
			working,
			draining,
			unknown,
			drained,
			unroutable,
			duplicate,
		},
	})

	require.Equal(t, Workers{unknown, working}, decision.Workers)
	require.Equal(t, ReasonMultiCN, decision.Reason)
	require.True(t, decision.Satisfied)
	require.Equal(t, DroppedWorkers{
		{Worker: draining, Reason: ReasonDroppedDrainingCN},
		{Worker: drained, Reason: ReasonDroppedDrainedCN},
		{Worker: unroutable, Reason: ReasonDroppedUnroutableCN},
		{Worker: duplicate, Reason: ReasonDroppedDuplicateCN},
	}, decision.Dropped)
}

func TestDecideQueryPlacementFallsBackWhenCandidatesAreRuntimeIneligible(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	draining := Worker{ID: "draining", Addr: "draining:6001", Mcpu: 8, State: WorkerStateDraining}
	drained := Worker{ID: "drained", Addr: "drained:6001", Mcpu: 8, State: WorkerStateDrained}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:   QueryExecAPMultiCN,
		CurrentCN:  local,
		Candidates: Workers{draining, drained},
	})

	require.Equal(t, Workers{local}, decision.Workers)
	require.Equal(t, ReasonNoCandidateCN, decision.Reason)
	require.True(t, decision.Satisfied)
	require.Equal(t, DroppedWorkers{
		{Worker: draining, Reason: ReasonDroppedDrainingCN},
		{Worker: drained, Reason: ReasonDroppedDrainedCN},
	}, decision.Dropped)
}

func TestDecideQueryPlacementDoesNotPreferDrainingCurrentCandidate(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	drainingLocal := Worker{ID: "local", Addr: "local:6001", Mcpu: 8, State: WorkerStateDraining}
	remote := Worker{ID: "remote", Addr: "remote:6001", Mcpu: 16, State: WorkerStateWorking}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      Workers{drainingLocal, remote},
		CurrentCNPolicy: CurrentCNPreferred,
	})

	require.Equal(t, Workers{remote}, decision.Workers)
	require.Equal(t, ReasonMultiCN, decision.Reason)
	require.True(t, decision.Satisfied)
	require.Equal(t, DroppedWorkers{{Worker: drainingLocal, Reason: ReasonDroppedDrainingCN}}, decision.Dropped)
}

func TestDecideQueryPlacementFallsBackWhenAllCandidatesUnroutable(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{ID: "missing-addr", Mcpu: 12}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:   QueryExecAPMultiCN,
		CurrentCN:  local,
		Candidates: candidates,
	})

	require.Equal(t, Workers{local}, decision.Workers)
	require.Equal(t, ReasonNoCandidateCN, decision.Reason)
	require.True(t, decision.Satisfied)
	require.Equal(t, DroppedWorkers{
		{Worker: candidates[0], Reason: ReasonDroppedUnroutableCN},
	}, decision.Dropped)
}

func TestDecideQueryPlacementCanRequireCurrentCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, Workers{local, candidates[0]}, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementFallsBackToLocalWhenCandidatesEmpty(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: local,
	})

	require.Equal(t, QueryExecAPMultiCN, decision.ExecKind)
	require.Equal(t, ReasonNoCandidateCN, decision.Reason)
	require.Equal(t, Workers{local}, decision.Workers)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementFallsBackToCurrentCNWithoutRoute(t *testing.T) {
	local := Worker{ID: "local", Mcpu: 8}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: local,
	})

	require.Equal(t, ReasonNoCandidateCN, decision.Reason)
	require.Equal(t, Workers{local}, decision.Workers)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementDoesNotDuplicateRequiredCurrentCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{
		{ID: "remote", Addr: "remote:6001", Mcpu: 16},
		{ID: "local", Addr: "other-addr:6001", Mcpu: 12},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, Workers{candidates[1], candidates[0]}, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementUsesRoutableCandidateForRequiredCurrentCNByServiceID(t *testing.T) {
	local := Worker{ID: "local", Mcpu: 8}
	candidates := Workers{
		{ID: "remote", Addr: "remote:6001", Mcpu: 16},
		{ID: "local", Addr: "local:6001", Mcpu: 12},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, Workers{candidates[1], candidates[0]}, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementDeduplicatesRequiredCurrentCNByAddressWhenIDMissing(t *testing.T) {
	local := Worker{Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{Addr: "local:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, candidates, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementDeduplicatesRequiredCurrentCNByAddressWhenIDDifferent(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{ID: "stale-local", Addr: "local:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, candidates, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementAppendsRequiredCurrentCNWhenIDMissingAndAddressDiffers(t *testing.T) {
	local := Worker{Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, Workers{local, candidates[0]}, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementFallsBackToLocalWhenRequiredCandidatesEmpty(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, Workers{local}, decision.Workers)
	require.Equal(t, ReasonNoCandidateCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementRejectsRequiredFallbackWithoutIdentity(t *testing.T) {
	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Empty(t, decision.Workers)
	require.Equal(t, ReasonCurrentCNMissingIdentity, decision.Reason)
	require.False(t, decision.Satisfied)
}

func TestDecideQueryPlacementFallsBackToRequiredCurrentCNWithoutRoute(t *testing.T) {
	local := Worker{ID: "local", Mcpu: 8}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, Workers{local}, decision.Workers)
	require.Equal(t, ReasonNoCandidateCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementAppendsRequiredCurrentCNWithoutRoute(t *testing.T) {
	local := Worker{ID: "local", Mcpu: 8}
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, Workers{local, candidates[0]}, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementDeduplicatesCandidateWorkers(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{
		{ID: "cn1", Addr: "cn1:6001", Mcpu: 12},
		{ID: "cn1", Addr: "stale-cn1:6001", Mcpu: 4},
		{ID: "stale-cn2", Addr: "cn2:6001", Mcpu: 6},
		{ID: "cn2", Addr: "cn2:6001", Mcpu: 16},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:   QueryExecAPMultiCN,
		CurrentCN:  local,
		Candidates: candidates,
	})

	require.Equal(t, Workers{candidates[0], candidates[2]}, decision.Workers)
	require.Equal(t, ReasonMultiCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementPrefersCurrentCNWhenCandidateAllowed(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{
		{ID: "remote", Addr: "remote:6001", Mcpu: 16},
		{ID: "local", Addr: "local:6001", Mcpu: 8},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNPreferred,
	})

	require.Equal(t, Workers{candidates[1], candidates[0]}, decision.Workers)
	require.Equal(t, ReasonPreferredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementKeepsPreferredCurrentCNFirstAndSortsTail(t *testing.T) {
	local := Worker{ID: "local", Addr: "m:6001", Mcpu: 8}
	candidates := Workers{
		{ID: "local", Addr: "m:6001", Mcpu: 8},
		{ID: "remote-z", Addr: "z:6001", Mcpu: 16},
		{ID: "remote-a", Addr: "a:6001", Mcpu: 12},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNPreferred,
	})

	require.Equal(t, Workers{candidates[0], candidates[2], candidates[1]}, decision.Workers)
	require.Equal(t, ReasonPreferredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementDoesNotForcePreferredCurrentCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNPreferred,
	})

	require.Equal(t, candidates, decision.Workers)
	require.Equal(t, ReasonMultiCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementExcludesCurrentCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{
		{ID: "local", Addr: "local:6001", Mcpu: 8},
		{ID: "remote", Addr: "remote:6001", Mcpu: 16},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNExcluded,
	})

	require.Equal(t, Workers{candidates[1]}, decision.Workers)
	require.Equal(t, ReasonExcludedCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideQueryPlacementRejectsExcludedCurrentCNWhenNoOtherCandidate(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      Workers{local},
		CurrentCNPolicy: CurrentCNExcluded,
	})

	require.Empty(t, decision.Workers)
	require.Equal(t, ReasonExcludedCurrentCN, decision.Reason)
	require.False(t, decision.Satisfied)
}

func TestDecideQueryPlacementRejectsExcludedCurrentCNWithoutIdentity(t *testing.T) {
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNExcluded,
	})

	require.Equal(t, candidates, decision.Workers)
	require.Equal(t, ReasonCurrentCNMissingIdentity, decision.Reason)
	require.False(t, decision.Satisfied)
}

func TestDecideQueryPlacementRejectsRequiredCurrentCNWithoutIdentity(t *testing.T) {
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, candidates, decision.Workers)
	require.Equal(t, ReasonCurrentCNMissingIdentity, decision.Reason)
	require.False(t, decision.Satisfied)
}

func TestDecideQueryPlacementRejectsRequiredDrainingCurrentCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8, State: WorkerStateDraining}
	candidates := Workers{
		local,
		{ID: "remote", Addr: "remote:6001", Mcpu: 16, State: WorkerStateWorking},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, Workers{candidates[1]}, decision.Workers)
	require.Equal(t, ReasonCurrentCNDraining, decision.Reason)
	require.False(t, decision.Satisfied)
}

func TestDecideQueryPlacementDoesNotFallbackToDrainedCurrentCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8, State: WorkerStateDrained}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: local,
	})

	require.Empty(t, decision.Workers)
	require.Equal(t, ReasonCurrentCNDrained, decision.Reason)
	require.False(t, decision.Satisfied)
}

func TestDecideQueryPlacementDoesNotLeaveWorkerWhenOnlyCurrentCandidateIsDraining(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8, State: WorkerStateDraining}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      Workers{local},
		CurrentCNPolicy: CurrentCNAllowed,
	})

	require.Empty(t, decision.Workers)
	require.Equal(t, ReasonCurrentCNDraining, decision.Reason)
	require.False(t, decision.Satisfied)
	require.Equal(t, DroppedWorkers{{Worker: local, Reason: ReasonDroppedDrainingCN}}, decision.Dropped)
}

func TestDecideQueryPlacementRequiresCurrentCNByIdentityWithoutRoute(t *testing.T) {
	local := Worker{ID: "local", Mcpu: 8}
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Equal(t, Workers{local, candidates[0]}, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}
