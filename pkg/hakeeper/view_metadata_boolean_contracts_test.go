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

package hakeeper

import (
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// TestPersistedFloorExpiryReleasesWithoutShutdownEvidence pins design class
// (vi) of docs/design/20261027-implicit-boolean-semantics-enum-set-bit-json.md
// (revision 7): heartbeat expiry is control-plane-only tick arithmetic that
// carries no process-exit, session-drain, or listener evidence, yet it is the
// only thing standing between a fresh below-floor CN store and cluster-wide
// authoring activation. The fresh store blocks; pure tick expiry releases the
// block; a post-completion reconnect re-registers the old binary NotReady
// without re-closing the completed admission for other CNs.
func TestPersistedFloorExpiryReleasesWithoutShutdownEvidence(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.ViewMetadataAdmissionEnabled = true
	rsm.state.ViewMetadataAdmissionEpoch = 2
	rsm.state.ViewMetadataRevalidationRequired = true
	rsm.state.ViewMetadataCatalogFencedEpoch = 2
	rsm.state.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
		Replicas: map[uint64]string{1: "log-1"},
	}
	updateViewMetadataLogWithProtocol(t, rsm, "log-1", true)
	// Fresh heartbeat state for both binaries (the upstream convention: the
	// seeded store Tick equals the current state tick, so the store is fresh
	// by commandDeliveryStoreExpired arithmetic): the below-floor binary is
	// live, ingress-ready, acknowledged at the current admission epoch and
	// routable. Only its stored protocol version is below the floor about to
	// be raised.
	rsm.state.CNState.Stores["cn-fixed"] = pb.CNStoreInfo{
		Tick:                               0,
		ViewMetadataAdmissionSupported:     true,
		ViewMetadataAdmissionGeneration:    1,
		ViewMetadataObservedEpoch:          2,
		ViewMetadataCatalogFencedEpoch:     2,
		ViewMetadataAdmissionReady:         true,
		ViewMetadataIngressReady:           true,
		PersistedExpressionProtocolVersion: uint64(defines.MORPCLatestVersion),
	}
	rsm.state.CNState.Stores["cn-old"] = pb.CNStoreInfo{
		Tick:                            0,
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 1,
		ViewMetadataObservedEpoch:       2,
		ViewMetadataCatalogFencedEpoch:  2,
		ViewMetadataAdmissionReady:      true,
		ViewMetadataIngressReady:        true,
		// PersistedExpressionProtocolVersion stays zero: a pre-132 binary
		// whose fresh store must block the activation.
	}
	require.True(t, rsm.state.CNState.Stores["cn-old"].ViewMetadataAdmissionReady,
		"before the floor raise the live below-floor CN is routable")
	cfg := Config{TickPerSecond: 1, CNStoreTimeout: 10 * time.Second, ProxyStoreTimeout: 10 * time.Second}

	// (1) The FRESH below-floor store blocks the activation and opens the
	// decoder floor + pending-authoring closure exactly as at present.
	result, err := rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmdForConfigWithProtocol(cfg, uint64(defines.MORPCLatestVersion)),
	})
	require.NoError(t, err)
	require.Zero(t, result.Value)
	require.Equal(t, uint64(defines.MORPCLatestVersion), rsm.state.PersistedExpressionRequiredProtocolVersion)
	require.True(t, rsm.state.PersistedExpressionProtocolActivationPending)
	require.Equal(t, uint64(2), rsm.state.ViewMetadataAdmissionEpoch)
	require.False(t, rsm.state.CNState.Stores["cn-old"].ViewMetadataAdmissionReady,
		"the fresh below-floor heartbeat must withdraw routing and hold the closure")

	// (2) Pure tick arithmetic expires the below-floor store. Nothing in the
	// replication state records a shutdown: the store entry is byte-identical
	// apart from the withdrawn routing bit, no field of system state carries
	// process-exit, session-drain, or listener evidence, and the only change
	// anywhere is state.Tick.
	timeoutTicks := uint64(cfg.CNStoreTimeout/time.Second) * uint64(cfg.TickPerSecond)
	oldStore := rsm.state.CNState.Stores["cn-old"]
	require.False(t, commandDeliveryStoreExpired(oldStore.Tick, rsm.state.Tick, timeoutTicks))
	tickCount := uint64(0)
	for tickCount < 22 {
		_, err = rsm.Update(sm.Entry{Index: rsm.state.Index + 1, Cmd: GetTickCmd()})
		require.NoError(t, err)
		tickCount++
	}
	require.Equal(t, uint64(22), rsm.state.Tick)
	expiredStore, expired := rsm.state.CNState.Stores["cn-old"]
	require.True(t, expired)
	require.True(t, commandDeliveryStoreExpired(expiredStore.Tick, rsm.state.Tick, timeoutTicks),
		"expiry is current>last && current-last>timeout over ticks alone")
	require.Equal(t, oldStore, expiredStore,
		"expiry mutated no aspect of the store: no shutdown evidence exists in the state machine")

	// The same command that the fresh store blocked now passes with ZERO
	// shutdown evidence anywhere: persistedExpressionProtocolReady skips the
	// expired below-floor store purely on tick arithmetic, and the epoch raise
	// proceeds for the cluster.
	result, err = rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmdForConfigWithProtocol(cfg, uint64(defines.MORPCLatestVersion)),
	})
	require.NoError(t, err)
	require.Zero(t, result.Value)
	require.False(t, rsm.state.PersistedExpressionProtocolActivationPending,
		"tick expiry alone released the pending activation")
	require.Equal(t, uint64(3), rsm.state.ViewMetadataAdmissionEpoch)
	require.True(t, rsm.state.ViewMetadataRevalidationRequired)
	require.Zero(t, rsm.state.ViewMetadataCatalogFencedEpoch)
	require.False(t, rsm.state.CNState.Stores["cn-old"].ViewMetadataAdmissionReady)

	// (3) Phase two completes through the admitted CN only.
	updateViewMetadataLogWithProtocol(t, rsm, "log-1", true)
	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                               "cn-fixed",
		ViewMetadataAdmissionSupported:     true,
		ViewMetadataAdmissionGeneration:    1,
		ViewMetadataObservedEpoch:          3,
		ViewMetadataCatalogFencedEpoch:     3,
		ViewMetadataIngressReady:           true,
		PersistedExpressionProtocolVersion: uint64(defines.MORPCLatestVersion),
	})
	result, err = rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmdForConfig(cfg),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Value)
	require.True(t, rsm.state.ViewMetadataAdmissionEnabled)

	// (4) The stale CN reconnects after completion: re-registration lands
	// NotReady (re-poison) and marks the next epoch pending, but it cannot
	// re-close the completed admission for other CNs, and the durable floor
	// recorded above keeps its value.
	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-old",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 1,
		ViewMetadataObservedEpoch:       3,
		ViewMetadataCatalogFencedEpoch:  3,
		ViewMetadataIngressReady:        true,
	})
	staleEntry := rsm.state.CNState.Stores["cn-old"]
	require.False(t, staleEntry.ViewMetadataAdmissionReady,
		"a below-floor re-registration must never be routable")
	require.True(t, rsm.state.ViewMetadataAdmissionPending,
		"the re-observed below-floor heartbeat defers the next admission epoch")
	require.Equal(t, uint64(defines.MORPCLatestVersion), rsm.state.PersistedExpressionRequiredProtocolVersion)
	require.True(t, rsm.state.ViewMetadataAdmissionEnabled)

	fixedSnap := rsm.viewMetadataAdmissionSnapshot("cn-fixed", false)
	require.True(t, fixedSnap.Ready)
	require.True(t, fixedSnap.Admitted,
		"the reconnection must not re-close another CN's published authoring/admission state")
	fixedEntry := rsm.state.CNState.Stores["cn-fixed"]
	require.True(t, fixedEntry.ViewMetadataAdmissionReady)

	oldSnap := rsm.viewMetadataAdmissionSnapshot("cn-old", false)
	require.False(t, oldSnap.Ready)
	require.False(t, oldSnap.Admitted)
	require.Equal(t, uint64(defines.MORPCLatestVersion), oldSnap.PersistedExpressionRequiredProtocolVersion)
}
