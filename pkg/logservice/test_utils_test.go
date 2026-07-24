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

package logservice

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllocatedPortsRejectsInvalidRequest(t *testing.T) {
	testCases := []struct {
		name  string
		count int
		start int
	}{
		{name: "zero count", count: 0, start: 0},
		{name: "count exceeds range", count: testPortCount + 1, start: 0},
		{name: "negative start", count: 1, start: -1},
		{name: "start exceeds range", count: 1, start: testPortCount},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			allocator := allocatedPorts{}

			ports, err := allocator.allocate(testCase.count, nil, testCase.start)

			require.Nil(t, ports)
			require.ErrorIs(t, err, errNoAvailableTestPort)
			require.Empty(t, allocator.ports)
		})
	}
}

func TestAllocatedPortsSkipsOccupiedPorts(t *testing.T) {
	allocator := allocatedPorts{}
	occupied := map[uint16]struct{}{
		uint16(testPortMin): {},
	}

	ports, err := allocator.allocate(3, occupied, 0)

	require.NoError(t, err)
	require.Equal(t, []int{testPortMin + 1, testPortMin + 2, testPortMin + 3}, ports)
	require.Len(t, allocator.ports, 3)
}

func TestAllocatedPortsConcurrentReservationsAreUnique(t *testing.T) {
	const (
		workers        = 64
		portsPerWorker = 3
	)
	allocator := allocatedPorts{}
	type result struct {
		ports []int
		err   error
	}
	results := make(chan result, workers)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ports, err := allocator.allocate(portsPerWorker, nil, 0)
			results <- result{ports: ports, err: err}
		}()
	}
	wg.Wait()
	close(results)

	seen := make(map[int]struct{}, workers*portsPerWorker)
	for result := range results {
		require.NoError(t, result.err)
		require.Len(t, result.ports, portsPerWorker)
		for _, port := range result.ports {
			_, exists := seen[port]
			require.False(t, exists)
			seen[port] = struct{}{}
		}
	}
	require.Len(t, seen, workers*portsPerWorker)
	require.Len(t, allocator.ports, workers*portsPerWorker)
}

func TestAllocatedPortsReturnsErrorWhenExhausted(t *testing.T) {
	allocator := allocatedPorts{
		ports: make(map[int]struct{}, testPortCount),
	}
	for port := testPortMin; port < testPortLimit; port++ {
		allocator.ports[port] = struct{}{}
	}

	ports, err := allocator.allocate(1, nil, 0)

	require.Nil(t, ports)
	require.ErrorIs(t, err, errNoAvailableTestPort)
	require.Len(t, allocator.ports, testPortCount)
}

func TestAllocatedPortsDoesNotReservePartialResult(t *testing.T) {
	allocator := allocatedPorts{
		ports: make(map[int]struct{}, testPortCount-2),
	}
	for port := testPortMin; port < testPortLimit-2; port++ {
		allocator.ports[port] = struct{}{}
	}
	before := len(allocator.ports)

	ports, err := allocator.allocate(3, nil, 0)

	require.Nil(t, ports)
	require.ErrorIs(t, err, errNoAvailableTestPort)
	require.Len(t, allocator.ports, before)
}
