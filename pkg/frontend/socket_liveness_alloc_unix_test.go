// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build (darwin || linux) && !race

package frontend

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Allocation measurement is separate from the race-tested functional cases.
func TestConnectionProbeSteadyStateAllocations(t *testing.T) {
	server, _ := newDisconnectTestTCPPair(t)
	conn := &Conn{}
	conn.UseConn(server)
	routine := &Routine{}
	routine.setInProcessRequest(true)
	rm := &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}
	rm.cancelDisconnectedRequests(connectionPeerClosed)
	allocations := testing.AllocsPerRun(100, func() {
		rm.cancelDisconnectedRequests(connectionPeerClosed)
	})
	require.Zero(t, allocations)
	require.False(t, routine.isCancelled())
}

// Shared-fd scans isolate monitor bookkeeping/syscall cost; this is not a
// claim about real distinct-socket capacity or server throughput.
func BenchmarkConnectionLivenessScan(b *testing.B) {
	server, _ := newDisconnectTestTCPPair(b)
	for _, count := range []int{1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			rm := &RoutineManager{clients: make(map[*Conn]*Routine, count)}
			for range count {
				conn := &Conn{}
				conn.UseConn(server)
				routine := &Routine{}
				routine.setInProcessRequest(true)
				rm.clients[conn] = routine
			}
			rm.cancelDisconnectedRequests(connectionPeerClosed)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				rm.cancelDisconnectedRequests(connectionPeerClosed)
			}
		})
	}
}
