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

func TestDecideScanPlacementKeepsSingleWorkerLocal(t *testing.T) {
	decision := DecideScanPlacement(ScanRequest{
		QueryWorkers: Workers{{ID: "local", Addr: "local:6001"}},
		Stats:        scanStats(100, 4, false),
	})

	require.True(t, decision.LocalOnly)
	require.Equal(t, ReasonScanSingleWorker, decision.Reason)
	require.Nil(t, decision.Workers)
}

func TestDecideScanPlacementKeepsMissingStatsLocal(t *testing.T) {
	decision := DecideScanPlacement(ScanRequest{
		QueryWorkers: Workers{
			{ID: "local", Addr: "local:6001"},
			{ID: "remote", Addr: "remote:6001"},
		},
	})

	require.True(t, decision.LocalOnly)
	require.Equal(t, ReasonScanMissingStats, decision.Reason)
}

func TestDecideScanPlacementKeepsForcedScanLocal(t *testing.T) {
	workers := Workers{
		{ID: "local", Addr: "local:6001"},
		{ID: "remote", Addr: "remote:6001"},
	}

	decision := DecideScanPlacement(ScanRequest{
		QueryWorkers: workers,
		Stats:        scanStats(100, 4, true),
	})

	require.True(t, decision.LocalOnly)
	require.Equal(t, ReasonScanForceOneCN, decision.Reason)

	decision = DecideScanPlacement(ScanRequest{
		QueryWorkers: workers,
		Stats:        scanStats(100, 4, false),
		ForceSingle:  true,
	})

	require.True(t, decision.LocalOnly)
	require.Equal(t, ReasonScanForceSingle, decision.Reason)
}

func TestDecideScanPlacementKeepsSmallScansLocal(t *testing.T) {
	decision := DecideScanPlacement(ScanRequest{
		QueryWorkers: Workers{
			{ID: "local", Addr: "local:6001"},
			{ID: "remote", Addr: "remote:6001"},
		},
		Stats:               scanStats(10, 4, false),
		OneCNBlockThreshold: 10,
	})

	require.True(t, decision.LocalOnly)
	require.Equal(t, ReasonScanSmallBlocks, decision.Reason)
}

func TestDecideScanPlacementCanForceSmallScansToMultiCN(t *testing.T) {
	workers := Workers{
		{ID: "local", Addr: "local:6001", Mcpu: 8},
		{ID: "remote", Addr: "remote:6001", Mcpu: 16},
	}

	decision := DecideScanPlacement(ScanRequest{
		QueryWorkers:        workers,
		Stats:               scanStats(10, 4, false),
		ForceMultiCN:        true,
		OneCNBlockThreshold: 10,
	})

	require.False(t, decision.LocalOnly)
	require.Equal(t, ReasonScanMultiCN, decision.Reason)
	require.Equal(t, workers, decision.Workers)

	decision.Workers[0].Mcpu = 1
	require.Equal(t, 8, workers[0].Mcpu)
}

func TestDecideScanPlacementUsesMultiCNForLargeScans(t *testing.T) {
	workers := Workers{
		{ID: "local", Addr: "local:6001", Mcpu: 8},
		{ID: "remote", Addr: "remote:6001", Mcpu: 16},
	}

	decision := DecideScanPlacement(ScanRequest{
		QueryWorkers:        workers,
		Stats:               scanStats(11, 4, false),
		OneCNBlockThreshold: 10,
	})

	require.False(t, decision.LocalOnly)
	require.Equal(t, ReasonScanMultiCN, decision.Reason)
	require.Equal(t, workers, decision.Workers)
}

func TestDecideScanPlacementKeepsLargeScanEmptyWhenNoWorkers(t *testing.T) {
	decision := DecideScanPlacement(ScanRequest{
		Stats:               scanStats(11, 4, false),
		OneCNBlockThreshold: 10,
	})

	require.False(t, decision.LocalOnly)
	require.Equal(t, ReasonScanNoWorkers, decision.Reason)
	require.Nil(t, decision.Workers)
}

func scanStats(blockNum int32, dop int32, forceOneCN bool) *ScanStats {
	return &ScanStats{
		BlockNum:   blockNum,
		Dop:        dop,
		ForceOneCN: forceOneCN,
	}
}
