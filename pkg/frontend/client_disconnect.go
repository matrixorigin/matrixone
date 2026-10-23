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

package frontend

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

const clientDisconnectProbeInterval = time.Second

type activeClientRequest struct {
	conn    *Conn
	routine *Routine
}

// The request handler cannot read the next packet while executing a statement.
// One server-owned monitor observes socket closure without consuming any bytes.
func (mo *MOServer) startConnectionLivenessMonitor() {
	if mo == nil || mo.rm == nil || mo.rm.ctx == nil {
		return
	}
	mo.wg.Add(1)
	go func() {
		defer mo.wg.Done()
		ticker := time.NewTicker(clientDisconnectProbeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-mo.rm.ctx.Done():
				return
			case <-ticker.C:
				mo.rm.cancelDisconnectedRequests(connectionPeerClosed)
			}
		}
	}()
}

func (rm *RoutineManager) appendActiveRequests(requests []activeClientRequest) []activeClientRequest {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	for conn, routine := range rm.clients {
		if conn != nil && routine != nil && routine.inProcessRequest.Load() && !routine.isCancelled() {
			requests = append(requests, activeClientRequest{conn: conn, routine: routine})
		}
	}
	return requests
}

func (rm *RoutineManager) cancelDisconnectedRequests(probe func(*Conn) (bool, error)) {
	if probe == nil {
		return
	}
	rm.disconnectProbeMu.Lock()
	defer rm.disconnectProbeMu.Unlock()
	requests := rm.appendActiveRequests(rm.disconnectProbeScratch[:0])
	defer func() {
		clear(requests)
		rm.disconnectProbeScratch = requests[:0]
	}()
	// Bound diagnostics to one inconclusive probe per monitor pass.
	reportedError := false
	for _, request := range requests {
		closed, err := probe(request.conn)
		if err != nil {
			if !reportedError {
				logutil.Debugf("failed to probe active client connection: %v", err)
				reportedError = true
			}
			continue
		}
		if closed {
			request.routine.cancelOnDisconnect()
		}
	}
}
