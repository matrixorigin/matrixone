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
			ExecKind:    execKind,
			LocalWorker: local,
			Candidates:  candidates,
		})

		require.Equal(t, execKind, decision.ExecKind)
		require.Equal(t, ReasonLocalExecType, decision.Reason)
		require.Equal(t, Workers{local}, decision.Workers)
	}
}

func TestDecideQueryPlacementUsesCandidatesForMultiCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{
		{ID: "cn2", Addr: "cn2:6001", Mcpu: 16},
		{ID: "cn1", Addr: "cn1:6001", Mcpu: 12},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:    QueryExecAPMultiCN,
		LocalWorker: local,
		Candidates:  candidates,
	})

	require.Equal(t, QueryExecAPMultiCN, decision.ExecKind)
	require.Equal(t, ReasonMultiCN, decision.Reason)
	require.Equal(t, candidates, decision.Workers)

	decision.Workers[0].Mcpu = 1
	require.Equal(t, 16, candidates[0].Mcpu)
}

func TestDecideQueryPlacementCanRequireCurrentCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{ID: "remote", Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:     QueryExecAPMultiCN,
		LocalWorker:  local,
		Candidates:   candidates,
		RequireLocal: true,
	})

	require.Equal(t, Workers{candidates[0], local}, decision.Workers)
}

func TestDecideQueryPlacementFallsBackToLocalWhenCandidatesEmpty(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:    QueryExecAPMultiCN,
		LocalWorker: local,
	})

	require.Equal(t, QueryExecAPMultiCN, decision.ExecKind)
	require.Equal(t, ReasonMultiCN, decision.Reason)
	require.Equal(t, Workers{local}, decision.Workers)
}

func TestDecideQueryPlacementDoesNotDuplicateRequiredCurrentCN(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}
	candidates := Workers{
		{ID: "remote", Addr: "remote:6001", Mcpu: 16},
		{ID: "local", Addr: "other-addr:6001", Mcpu: 12},
	}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:     QueryExecAPMultiCN,
		LocalWorker:  local,
		Candidates:   candidates,
		RequireLocal: true,
	})

	require.Equal(t, candidates, decision.Workers)
}

func TestDecideQueryPlacementDeduplicatesRequiredCurrentCNByAddressWhenIDMissing(t *testing.T) {
	local := Worker{Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{Addr: "local:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:     QueryExecAPMultiCN,
		LocalWorker:  local,
		Candidates:   candidates,
		RequireLocal: true,
	})

	require.Equal(t, candidates, decision.Workers)
}

func TestDecideQueryPlacementAppendsRequiredCurrentCNWhenIDMissingAndAddressDiffers(t *testing.T) {
	local := Worker{Addr: "local:6001", Mcpu: 8}
	candidates := Workers{{Addr: "remote:6001", Mcpu: 16}}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:     QueryExecAPMultiCN,
		LocalWorker:  local,
		Candidates:   candidates,
		RequireLocal: true,
	})

	require.Equal(t, Workers{candidates[0], local}, decision.Workers)
}

func TestDecideQueryPlacementFallsBackToLocalWhenRequiredCandidatesEmpty(t *testing.T) {
	local := Worker{ID: "local", Addr: "local:6001", Mcpu: 8}

	decision := DecideQueryPlacement(QueryRequest{
		ExecKind:     QueryExecAPMultiCN,
		LocalWorker:  local,
		RequireLocal: true,
	})

	require.Equal(t, Workers{local}, decision.Workers)
}
