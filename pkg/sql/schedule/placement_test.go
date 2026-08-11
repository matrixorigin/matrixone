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

func TestDecideQueryPlacementDoesNotWidenResolvedPoolForRequiredCurrentCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Empty(t, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentOutsidePool, decision.Reason)
	require.False(t, decision.Satisfied)
}

func TestDecideQueryPlacementOrdersRequiredCurrentCNFirstWhenRequested(t *testing.T) {
	local := Worker{ID: "z-local", Addr: "z-local:6001", Mcpu: 8}
	remote := Worker{ID: "a-remote", Addr: "a-remote:6001", Mcpu: 16}

	req := QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      Workers{remote, local},
		CurrentCNPolicy: CurrentCNRequired,
	}
	// Required membership alone does not pin an execution ordinal.
	require.Equal(t, Workers{remote, local}, DecideQueryPlacement(req).Workers)

	req.CurrentCNOrdinalZero = true
	decision := DecideQueryPlacement(req)

	require.Equal(t, Workers{local, remote}, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentCN, decision.Reason)
	require.True(t, decision.Satisfied)
}

func TestDecideLocalQueryPlacementPreservesRequestedPoolIdentity(t *testing.T) {
	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:  QueryExecTP,
		CurrentCN: Worker{ID: "local", Route: WorkerRouteLocal},
		Intent: SchedulingIntent{
			RequestedPool:     "tenant:app",
			PoolFallback:      PoolFallbackStrict,
			EmptyWorkerPolicy: EmptyWorkerFail,
			WorkerSet:         WorkerSetPolicy{Mode: WorkerSetAll},
		},
	})

	require.True(t, decision.Satisfied)
	require.Equal(t, "tenant:app", decision.ResolvedPool.RequestedIdentity)
	require.Equal(t, PoolResolutionUnspecified, decision.ResolvedPool.Resolution)
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

func TestDecideQueryPlacementRejectsRequiredCurrentCNOutsidePoolByAddress(t *testing.T) {
	local := Worker{Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Empty(t, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentOutsidePool, decision.Reason)
	require.False(t, decision.Satisfied)
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

func TestDecideQueryPlacementRejectsRequiredCurrentCNOutsidePoolWithoutRoute(t *testing.T) {
	local := Worker{ID: "local", Mcpu: 8}
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Empty(t, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentOutsidePool, decision.Reason)
	require.False(t, decision.Satisfied)
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

func TestDecideQueryPlacementDoesNotInjectRequiredIdentityWithoutRoute(t *testing.T) {
	local := Worker{ID: "local", Mcpu: 8}
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:        QueryExecAPMultiCN,
		CurrentCN:       local,
		Candidates:      candidates,
		CurrentCNPolicy: CurrentCNRequired,
	})

	require.Empty(t, decision.Workers)
	require.Equal(t, ReasonRequiredCurrentOutsidePool, decision.Reason)
	require.False(t, decision.Satisfied)
}

func TestDecideQueryPlacementSelectsDeterministicMaxWorkerSubset(t *testing.T) {
	workers := Workers{
		{ID: "cn-4", Addr: "4:6001", Route: WorkerRouteRemote},
		{ID: "cn-2", Addr: "2:6001", Route: WorkerRouteRemote},
		{ID: "cn-1", Addr: "1:6001", Route: WorkerRouteRemote},
		{ID: "cn-3", Addr: "3:6001", Route: WorkerRouteRemote},
	}
	intent := SchedulingIntent{WorkerSet: WorkerSetPolicy{
		Mode: WorkerSetMax, MaxWorkers: 2, SelectionKey: "stmt-018f",
	}}
	decide := func(candidates Workers) QueryDecision {
		return DecideQueryPlacement(QueryRequest{
			ExecKind: QueryExecAPMultiCN,
			Intent:   intent,
			ResolvedPool: ResolvedPool{
				Identity: "tenant-label:account=a", Workers: candidates,
			},
		})
	}

	first := decide(workers)
	reordered := decide(Workers{workers[2], workers[0], workers[3], workers[1]})
	require.True(t, first.Satisfied)
	require.Len(t, first.Workers, 2)
	require.Equal(t, first.Workers, reordered.Workers)
	require.Equal(t, 4, first.EligibleCount)
	require.Equal(t, "tenant-label:account=a", first.ResolvedPool.Identity)
}

func TestStableHRWScoreGoldenV1(t *testing.T) {
	// This value locks the byte-level cross-release contract: three
	// length-delimited fields, little-endian uint64 lengths, then FNV-1a.
	require.Equal(t, uint64(6827858094303544662),
		stableHRWScore("stmt-018f", "id:cn-1"))
	require.Equal(t, stableHRWScore("stmt-018f", "id:cn-1"),
		stableHRWWorkerScore("stmt-018f", Worker{ID: "cn-1"}))
}

func TestDecideQueryPlacementDropsIdentitylessLocalCandidate(t *testing.T) {
	identityless := Worker{Route: WorkerRouteLocal, State: WorkerStateWorking}
	decision := DecideQueryPlacement(QueryRequest{
		ExecKind: QueryExecAPMultiCN,
		Intent: SchedulingIntent{
			EmptyWorkerPolicy: EmptyWorkerFail,
		},
		Candidates: Workers{identityless},
	})

	require.False(t, decision.Satisfied)
	require.Equal(t, ReasonNoCandidateCN, decision.Reason)
	require.Zero(t, decision.EligibleCount)
	require.Equal(t, DroppedWorkers{{
		Worker: identityless,
		Reason: ReasonDroppedUnroutableCN,
	}}, decision.Dropped)
}

func TestDecideQueryPlacementReportsEligibleCountOnSelectionFailure(t *testing.T) {
	decision := DecideQueryPlacement(QueryRequest{
		ExecKind: QueryExecAPMultiCN,
		Intent: SchedulingIntent{WorkerSet: WorkerSetPolicy{
			Mode: WorkerSetMax, MaxWorkers: 1,
		}},
		Candidates: Workers{
			{ID: "cn-1", Addr: "1:6001"},
			{ID: "cn-2", Addr: "2:6001"},
		},
	})

	require.False(t, decision.Satisfied)
	require.Equal(t, ReasonMissingSelectionKey, decision.Reason)
	require.Equal(t, 2, decision.EligibleCount)
}

func TestDecideQueryPlacementHRWHasMinimalMembershipChurn(t *testing.T) {
	base := Workers{
		{ID: "cn-1", Addr: "1:6001"}, {ID: "cn-2", Addr: "2:6001"},
		{ID: "cn-3", Addr: "3:6001"}, {ID: "cn-4", Addr: "4:6001"},
	}
	request := QueryRequest{
		ExecKind: QueryExecAPMultiCN,
		Intent: SchedulingIntent{WorkerSet: WorkerSetPolicy{
			Mode: WorkerSetMax, MaxWorkers: 3, SelectionKey: "stable-statement",
		}},
		Candidates: base,
	}
	before := DecideQueryPlacement(request)
	request.Candidates = append(cloneWorkers(base), Worker{ID: "cn-5", Addr: "5:6001"})
	after := DecideQueryPlacement(request)

	common := 0
	for _, worker := range before.Workers {
		if containsWorker(after.Workers, worker) {
			common++
		}
	}
	require.GreaterOrEqual(t, common, 2)
}

func TestDecideQueryPlacementWorkerSetPolicyUnhappyPaths(t *testing.T) {
	worker := Worker{ID: "cn-1", Addr: "1:6001"}
	tests := []struct {
		name   string
		policy WorkerSetPolicy
		reason string
	}{
		{name: "zero max", policy: WorkerSetPolicy{Mode: WorkerSetMax, SelectionKey: "stmt"}, reason: ReasonInvalidWorkerSetPolicy},
		{name: "negative max", policy: WorkerSetPolicy{Mode: WorkerSetMax, MaxWorkers: -1, SelectionKey: "stmt"}, reason: ReasonInvalidWorkerSetPolicy},
		{name: "missing key", policy: WorkerSetPolicy{Mode: WorkerSetMax, MaxWorkers: 1}, reason: ReasonMissingSelectionKey},
		{name: "unknown algorithm", policy: WorkerSetPolicy{Mode: WorkerSetMax, MaxWorkers: 1, SelectionKey: "stmt", AlgorithmVersion: "hrw-v99"}, reason: ReasonInvalidWorkerSetPolicy},
		{name: "invalid mode", policy: WorkerSetPolicy{Mode: WorkerSetMode(99), MaxWorkers: 1, SelectionKey: "stmt"}, reason: ReasonInvalidWorkerSetPolicy},
		{name: "ambiguous all", policy: WorkerSetPolicy{Mode: WorkerSetAll, MaxWorkers: 1}, reason: ReasonInvalidWorkerSetPolicy},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := DecideQueryPlacement(QueryRequest{
				ExecKind:   QueryExecAPMultiCN,
				Candidates: Workers{worker},
				Intent:     SchedulingIntent{WorkerSet: test.policy},
			})
			require.False(t, decision.Satisfied)
			require.Equal(t, test.reason, decision.Reason)
			require.Empty(t, decision.Workers)
		})
	}
}

func TestDecideQueryPlacementRejectsInvalidOrForbiddenPoolPolicies(t *testing.T) {
	worker := Worker{ID: "cn-1", Addr: "1:6001"}
	for _, test := range []struct {
		name   string
		intent SchedulingIntent
		pool   ResolvedPool
		reason string
	}{
		{name: "invalid fallback", intent: SchedulingIntent{PoolFallback: PoolFallbackPolicy(99)}, reason: ReasonInvalidSchedulingIntent},
		{name: "invalid empty policy", intent: SchedulingIntent{EmptyWorkerPolicy: EmptyWorkerPolicy(99)}, reason: ReasonInvalidSchedulingIntent},
		{
			name:   "strict resolver defense",
			intent: SchedulingIntent{PoolFallback: PoolFallbackStrict},
			pool:   ResolvedPool{Fallback: true, FallbackReason: "shared-unlabeled", Workers: Workers{worker}},
			reason: ReasonStrictPoolFallback,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision := DecideQueryPlacement(QueryRequest{
				ExecKind: QueryExecAPMultiCN, Candidates: Workers{worker}, Intent: test.intent, ResolvedPool: test.pool,
			})
			require.False(t, decision.Satisfied)
			require.Equal(t, test.reason, decision.Reason)
			require.Empty(t, decision.Workers)
		})
	}
}

func TestDecideQueryPlacementSeparatesPoolAndEmptyWorkerFallback(t *testing.T) {
	local := Worker{ID: "local", Route: WorkerRouteLocal}
	request := QueryRequest{
		ExecKind:  QueryExecAPMultiCN,
		CurrentCN: local,
		Intent: SchedulingIntent{
			PoolFallback:      PoolFallbackStrict,
			EmptyWorkerPolicy: EmptyWorkerFail,
		},
		ResolvedPool: ResolvedPool{
			RequestedIdentity: "tenant-label:account=a",
			Identity:          "tenant-label:account=a",
			Resolution:        PoolResolutionTenantLabels,
		},
	}

	decision := DecideQueryPlacement(request)
	require.False(t, decision.Satisfied)
	require.Equal(t, ReasonNoCandidateCN, decision.Reason)
	require.Empty(t, decision.Workers)

	request.Intent.EmptyWorkerPolicy = EmptyWorkerLocalFallback
	decision = DecideQueryPlacement(request)
	require.True(t, decision.Satisfied)
	require.Equal(t, Workers{local}, decision.Workers)
}

func TestDecideQueryPlacementLocalExecKindSatisfiesExplicitUpperBound(t *testing.T) {
	local := Worker{ID: "local", Route: WorkerRouteLocal}
	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:  QueryExecAPOneCN,
		CurrentCN: local,
		Intent: SchedulingIntent{
			Explicit:     true,
			PoolFallback: PoolFallbackStrict,
			WorkerSet: WorkerSetPolicy{
				Mode:       WorkerSetMax,
				MaxWorkers: 1,
			},
		},
	})

	require.True(t, decision.Satisfied)
	require.Equal(t, ReasonLocalExecType, decision.Reason)
	require.Equal(t, Workers{local}, decision.Workers)
}

func TestDecideQueryPlacementLocalExecKindRejectsInvalidWorkerPolicy(t *testing.T) {
	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:  QueryExecTP,
		CurrentCN: Worker{ID: "local", Route: WorkerRouteLocal},
		Intent: SchedulingIntent{
			WorkerSet: WorkerSetPolicy{Mode: WorkerSetMax},
		},
	})

	require.False(t, decision.Satisfied)
	require.Equal(t, ReasonInvalidWorkerSetPolicy, decision.Reason)
	require.Empty(t, decision.Workers)
}

func TestDecideQueryPlacementUsesExplicitWorkerRoute(t *testing.T) {
	local := Worker{ID: "local", Route: WorkerRouteLocal}
	remoteWithoutAddress := Worker{ID: "remote", Route: WorkerRouteRemote}
	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:   QueryExecAPMultiCN,
		Candidates: Workers{remoteWithoutAddress, local},
	})

	require.Equal(t, Workers{local}, decision.Workers)
	require.Equal(t, DroppedWorkers{{Worker: remoteWithoutAddress, Reason: ReasonDroppedUnroutableCN}}, decision.Dropped)
}

func TestDecideQueryPlacementPinsRequiredCurrentWithinMaxSubset(t *testing.T) {
	current := Worker{ID: "current", Addr: "current:6001", Route: WorkerRouteLocal}
	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:             QueryExecAPMultiCN,
		CurrentCN:            current,
		CurrentCNPolicy:      CurrentCNRequired,
		CurrentCNOrdinalZero: true,
		Candidates: Workers{
			{ID: "cn-1", Addr: "1:6001"}, current, {ID: "cn-2", Addr: "2:6001"},
		},
		Intent: SchedulingIntent{WorkerSet: WorkerSetPolicy{
			Mode: WorkerSetMax, MaxWorkers: 1, SelectionKey: "stmt",
		}},
	})

	require.True(t, decision.Satisfied)
	require.Equal(t, Workers{current}, decision.Workers)
}

func BenchmarkDecideQueryPlacementMaxWorkers(b *testing.B) {
	workers := make(Workers, 64)
	for i := range workers {
		identity := string(rune(i + 1))
		workers[i] = Worker{ID: identity, Addr: identity}
	}
	request := QueryRequest{
		ExecKind:   QueryExecAPMultiCN,
		Candidates: workers,
		Intent: SchedulingIntent{WorkerSet: WorkerSetPolicy{
			Mode: WorkerSetMax, MaxWorkers: 8, SelectionKey: "benchmark-statement",
		}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = DecideQueryPlacement(request)
	}
}

func BenchmarkDecideQueryPlacementAllWorkers(b *testing.B) {
	workers := make(Workers, 64)
	for i := range workers {
		identity := string(rune(i + 1))
		workers[i] = Worker{ID: identity, Addr: identity}
	}
	request := QueryRequest{ExecKind: QueryExecAPMultiCN, Candidates: workers}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = DecideQueryPlacement(request)
	}
}
