// Copyright 2021 - 2026 Matrix Origin
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

package hakeeper

import (
	"bytes"
	"encoding/base64"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestLegacyTNStartSnapshotReplayConverges(t *testing.T) {
	// Frozen snapshots produced by c21a102e74 bracket consumption of an ordinary
	// TN start. Replay must agree with the historical post-heartbeat snapshot.
	snapshots := []string{
		"CAEoATAEQjMKCXRuLXJldmlldxImEiQKCXRuLXJldmlldxgBIhUKEQoJdG4tcmV2aWV3EAIYCigBEAJSAFoAYhYKFAgAEhASDggBEgpsb2ctcmV2aWV3agByAHoAkgEA",
		"CAIoATAEUgBaEQoPCgl0bi1yZXZpZXcSAmgBYhYKFAgAEhASDggBEgpsb2ctcmV2aWV3agByAHoAkgEA",
	}
	heartbeat, err := (&pb.TNStoreHeartbeat{UUID: "tn-review", CommandDeliveryAckSupported: true}).Marshal()
	require.NoError(t, err)
	var recovered [2]*stateMachine
	for i, encoded := range snapshots {
		data, err := base64.StdEncoding.DecodeString(encoded)
		require.NoError(t, err)
		r := NewStateMachine(0, uint64(i+1)).(*stateMachine)
		require.NoError(t, r.RecoverFromSnapshot(bytes.NewReader(data), nil, nil))
		if r.state.Index < 2 {
			_, err = r.Update(sm.Entry{Index: 2, Cmd: GetTNStoreHeartbeatCmd(heartbeat)})
			require.NoError(t, err)
		}
		require.Empty(t, r.state.ScheduleCommands)
		recovered[i] = r
	}
	require.Equal(t, recovered[0].state, recovered[1].state)
	for _, r := range recovered {
		for i, cmd := range [][]byte{GetEnableCommandDeliveryCmd(), GetTickCmd(), GetTNStoreHeartbeatCmd(heartbeat)} {
			_, err := r.Update(sm.Entry{Index: uint64(i + 3), Cmd: cmd})
			require.NoError(t, err)
		}
	}
	require.Equal(t, recovered[0].state, recovered[1].state, "ACK activation must preserve replay convergence")
}
