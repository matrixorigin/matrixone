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

package logservice

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestUniqueKeyCodecMetadataRoundTripsThroughHeartbeatAndChecker(t *testing.T) {
	capability := &UniqueKeyCodecCapability{
		ReadableVersions:   1 << 2,
		WritableVersions:   1 << 2,
		RegistryVersion:    1,
		RegistryDigest:     []byte{1, 2, 3},
		MaxEncodedKeyBytes: 64 << 20,
	}
	activation := &UniqueKeyCodecActivation{
		RequestedVersion: 2,
		RegistryVersion:  1,
		RegistryDigest:   []byte{1, 2, 3},
		Generation:       19,
		Phase:            ENABLED,
		CnTargets:        map[string]uint64{"cn-1": 7},
		TnTargets:        map[string]uint64{"tn-1": 8},
	}
	heartbeat := &CNStoreHeartbeat{UniqueKeyCodecCapability: capability}
	encoded, err := proto.Marshal(heartbeat)
	require.NoError(t, err)
	decoded := &CNStoreHeartbeat{}
	require.NoError(t, proto.Unmarshal(encoded, decoded))
	require.Equal(t, capability, decoded.UniqueKeyCodecCapability)

	state := &CheckerState{UniqueKeyCodecActivation: activation}
	encoded, err = proto.Marshal(state)
	require.NoError(t, err)
	decodedState := &CheckerState{}
	require.NoError(t, proto.Unmarshal(encoded, decodedState))
	require.Equal(t, activation, decodedState.UniqueKeyCodecActivation)
}

func TestUniqueKeyCodecActivationRoundTripsThroughHAKeeperRSMState(t *testing.T) {
	activation := &UniqueKeyCodecActivation{
		RequestedVersion: 2,
		RegistryVersion:  1,
		RegistryDigest:   []byte{9, 8, 7},
		Generation:       23,
		Phase:            PREPARING,
		CnTargets:        map[string]uint64{"cn-1": 11},
		TnTargets:        map[string]uint64{"tn-1": 12},
	}
	want := &HAKeeperRSMState{Index: 42, UniqueKeyCodecActivation: activation}
	encoded, err := proto.Marshal(want)
	require.NoError(t, err)
	got := &HAKeeperRSMState{}
	require.NoError(t, proto.Unmarshal(encoded, got))
	require.Equal(t, want, got)
}
