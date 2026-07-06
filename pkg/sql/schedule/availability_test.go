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

func TestFilterAvailableWorkersKeepsLocalWithoutChecking(t *testing.T) {
	workers := Workers{
		{ID: "local", Addr: "local:6001", Mcpu: 8},
		{ID: "remote", Addr: "remote:6001", Mcpu: 16},
	}
	checked := make([]string, 0, 1)

	filtered := FilterAvailableWorkers(AvailabilityRequest{
		Workers:   workers,
		LocalAddr: "local:6001",
		SameCN: func(addr string, currentAddr string) bool {
			return addr == currentAddr
		},
		IsAvailable: func(addr string) bool {
			checked = append(checked, addr)
			return false
		},
	})

	require.Equal(t, Workers{workers[0]}, filtered)
	require.Equal(t, []string{"remote:6001"}, checked)
}

func TestFilterAvailableWorkersKeepsOnlyAvailableRemoteWorkers(t *testing.T) {
	workers := Workers{
		{ID: "local", Addr: "local:6001", Mcpu: 8},
		{ID: "remote-1", Addr: "remote-1:6001", Mcpu: 16},
		{ID: "remote-2", Addr: "remote-2:6001", Mcpu: 12},
	}

	filtered := FilterAvailableWorkers(AvailabilityRequest{
		Workers:   workers,
		LocalAddr: "local:6001",
		IsAvailable: func(addr string) bool {
			return addr == "remote-2:6001"
		},
	})

	require.Equal(t, Workers{workers[0], workers[2]}, filtered)
}

func TestFilterAvailableWorkersClonesWorkersWhenCheckerMissing(t *testing.T) {
	workers := Workers{
		{ID: "local", Addr: "local:6001", Mcpu: 8},
		{ID: "remote", Addr: "remote:6001", Mcpu: 16},
	}

	filtered := FilterAvailableWorkers(AvailabilityRequest{Workers: workers})

	require.Equal(t, workers, filtered)
	filtered[0].Mcpu = 1
	require.Equal(t, 8, workers[0].Mcpu)
}

func TestFilterAvailableWorkersDoesNotTreatEmptyAddressAsLocalByDefault(t *testing.T) {
	workers := Workers{{ID: "unknown", Mcpu: 8}}
	checked := make([]string, 0, 1)

	filtered := FilterAvailableWorkers(AvailabilityRequest{
		Workers: workers,
		IsAvailable: func(addr string) bool {
			checked = append(checked, addr)
			return false
		},
	})

	require.Empty(t, filtered)
	require.Equal(t, []string{""}, checked)
}

func TestFilterAvailableWorkersDoesNotMutateInput(t *testing.T) {
	workers := Workers{
		{ID: "remote-1", Addr: "remote-1:6001", Mcpu: 16},
		{ID: "remote-2", Addr: "remote-2:6001", Mcpu: 12},
		{ID: "remote-3", Addr: "remote-3:6001", Mcpu: 10},
	}
	original := cloneWorkers(workers)
	expected := Workers{workers[0], workers[2]}

	filtered := FilterAvailableWorkers(AvailabilityRequest{
		Workers: workers,
		IsAvailable: func(addr string) bool {
			return addr != "remote-2:6001"
		},
	})

	require.Equal(t, expected, filtered)
	require.Equal(t, original, workers)
}
