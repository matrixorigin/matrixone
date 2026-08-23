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
	"bytes"
	"fmt"
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func updateViewMetadataCN(
	t *testing.T,
	rsm *stateMachine,
	hb pb.CNStoreHeartbeat,
) pb.CommandBatch {
	t.Helper()
	data, err := hb.Marshal()
	require.NoError(t, err)
	result, err := rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetCNStoreHeartbeatCmd(data),
	})
	require.NoError(t, err)
	var batch pb.CommandBatch
	require.NoError(t, batch.Unmarshal(result.Data))
	return batch
}

func updateViewMetadataProxy(
	t *testing.T,
	rsm *stateMachine,
	hb pb.ProxyHeartbeat,
) pb.CommandBatch {
	t.Helper()
	data, err := hb.Marshal()
	require.NoError(t, err)
	result, err := rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetProxyHeartbeatCmd(data),
	})
	require.NoError(t, err)
	var batch pb.CommandBatch
	require.NoError(t, batch.Unmarshal(result.Data))
	return batch
}

func updateViewMetadataLog(t *testing.T, rsm *stateMachine, uuid string) {
	t.Helper()
	data, err := (&pb.LogStoreHeartbeat{
		UUID:                           uuid,
		ViewMetadataAdmissionSupported: true,
	}).Marshal()
	require.NoError(t, err)
	_, err = rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetLogStoreHeartbeatCmd(data),
	})
	require.NoError(t, err)
}

func TestViewMetadataAdmissionActivationUsesPostBarrierAcks(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
		Replicas: map[uint64]string{1: "log-1"},
	}
	updateViewMetadataLog(t, rsm, "log-1")
	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 10,
		ViewMetadataIngressReady:        true,
	})
	updateViewMetadataProxy(t, rsm, pb.ProxyHeartbeat{
		UUID:                            "proxy-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 20,
	})

	result, err := rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmd(),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), result.Value)
	require.True(t, rsm.state.ViewMetadataAdmissionPreparing)
	require.Equal(t, uint64(1), rsm.state.ViewMetadataAdmissionEpoch)
	require.True(t, rsm.state.ViewMetadataRevalidationRequired)
	require.Empty(t, rsm.state.ViewMetadataAdmissionLogReady)
	require.Empty(t, rsm.state.ViewMetadataAdmissionCNReady)
	require.Empty(t, rsm.state.ViewMetadataAdmissionProxyReady)
	require.True(t, rsm.state.CNState.Stores["cn-1"].ViewMetadataAdmissionReady,
		"pre-barrier services remain active during preparation")

	// Pre-barrier observations were discarded. Phase two cannot commit until
	// every participant acknowledges epoch 1 after the replicated barrier and
	// one CN confirms the durable catalog fence.
	result, err = rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmdForConfig(Config{}),
	})
	require.NoError(t, err)
	require.Zero(t, result.Value)

	updateViewMetadataLog(t, rsm, "log-1")
	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 10,
		ViewMetadataObservedEpoch:       1,
		ViewMetadataCatalogFencedEpoch:  1,
		ViewMetadataIngressReady:        true,
	})
	updateViewMetadataProxy(t, rsm, pb.ProxyHeartbeat{
		UUID:                            "proxy-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 20,
		ViewMetadataObservedEpoch:       1,
	})
	result, err = rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmdForConfig(Config{}),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Value)
	require.True(t, rsm.state.ViewMetadataAdmissionEnabled)
	require.False(t, rsm.state.ViewMetadataAdmissionPreparing)
	require.True(t, rsm.state.CNState.Stores["cn-1"].ViewMetadataAdmissionReady)
	require.True(t, rsm.state.ProxyState.Stores["proxy-1"].ViewMetadataAdmissionReady)
}

func TestViewMetadataAdmissionPreparingKeepsLegacyAndHidesPendingCN(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
		Replicas: map[uint64]string{1: "log-1"},
	}
	updateViewMetadataLog(t, rsm, "log-1")
	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:           "legacy-cn",
		ServiceAddress: "legacy-pipeline",
	})
	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "pending-cn",
		ServiceAddress:                  "pending-pipeline",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 2,
	})

	result, err := rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmd(),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), result.Value)
	require.True(t, rsm.state.CNState.Stores["legacy-cn"].ViewMetadataAdmissionReady)
	require.False(t, rsm.state.CNState.Stores["pending-cn"].ViewMetadataAdmissionReady)

	details := rsm.handleClusterDetailsQuery(Config{})
	require.Len(t, details.CNStores, 1)
	require.Equal(t, "legacy-cn", details.CNStores[0].UUID)
}

func TestViewMetadataAdmissionRejectsStaleGeneration(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.ViewMetadataAdmissionEnabled = true
	rsm.state.ViewMetadataAdmissionEpoch = 3
	rsm.state.ViewMetadataRevalidationRequired = true
	rsm.state.ViewMetadataCatalogFencedEpoch = 3

	batch := updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ServiceAddress:                  "new-address",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 12,
		ViewMetadataObservedEpoch:       3,
		ViewMetadataIngressReady:        true,
	})
	require.Equal(t, uint64(12), batch.ViewMetadataAdmission.Generation)
	require.True(t, batch.ViewMetadataAdmission.Ready)

	batch = updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ServiceAddress:                  "stale-address",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 11,
		ViewMetadataObservedEpoch:       3,
	})
	require.Equal(t, uint64(12), batch.ViewMetadataAdmission.Generation)
	require.Equal(t, "new-address", rsm.state.CNState.Stores["cn-1"].ServiceAddress)
	require.Equal(t, uint64(12),
		rsm.state.CNState.Stores["cn-1"].ViewMetadataAdmissionGeneration)
}

func TestViewMetadataAdmissionAllowsGenerationRollbackBeforeActivation(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.Tick = 3
	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ServiceAddress:                  "new-cn",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 12,
	})
	updateViewMetadataProxy(t, rsm, pb.ProxyHeartbeat{
		UUID:                            "proxy-1",
		ListenAddress:                   "new-proxy",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 13,
	})

	rsm.state.Tick = 7
	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:           "cn-1",
		ServiceAddress: "rolled-back-cn",
	})
	updateViewMetadataProxy(t, rsm, pb.ProxyHeartbeat{
		UUID:          "proxy-1",
		ListenAddress: "rolled-back-proxy",
	})

	cn := rsm.state.CNState.Stores["cn-1"]
	require.Equal(t, uint64(0), cn.ViewMetadataAdmissionGeneration)
	require.Equal(t, uint64(7), cn.Tick)
	require.Equal(t, "rolled-back-cn", cn.ServiceAddress)
	proxy := rsm.state.ProxyState.Stores["proxy-1"]
	require.Equal(t, uint64(0), proxy.ViewMetadataAdmissionGeneration)
	require.Equal(t, uint64(7), proxy.Tick)
	require.Equal(t, "rolled-back-proxy", proxy.ListenAddress)
}

func TestViewMetadataAdmissionReplacementKeepsOldGenerationUntilTimeout(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.Tick = 10
	rsm.state.ViewMetadataAdmissionEnabled = true
	rsm.state.ViewMetadataAdmissionEpoch = 3
	rsm.state.ViewMetadataRevalidationRequired = true
	rsm.state.ViewMetadataCatalogFencedEpoch = 3
	rsm.state.CNState.Stores["cn-1"] = pb.CNStoreInfo{
		Tick:                            10,
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 10,
		ViewMetadataObservedEpoch:       3,
		ViewMetadataAdmissionReady:      true,
		ViewMetadataIngressReady:        true,
	}
	rsm.state.ProxyState.Stores["proxy-1"] = pb.ProxyStore{
		UUID:                            "proxy-1",
		Tick:                            10,
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 20,
		ViewMetadataObservedEpoch:       3,
		ViewMetadataAdmissionReady:      true,
	}

	rsm.state.Tick = 11
	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 11,
		ViewMetadataObservedEpoch:       3,
		ViewMetadataIngressReady:        true,
		ViewMetadataRefreshSupported:    true,
	})
	updateViewMetadataProxy(t, rsm, pb.ProxyHeartbeat{
		UUID:                            "proxy-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 21,
		ViewMetadataObservedEpoch:       3,
	})
	require.Equal(t, uint64(10), rsm.state.ViewMetadataAdmissionCNTargets["cn-1"])
	require.Equal(t, uint64(10), rsm.state.ViewMetadataAdmissionCNTargetTicks["cn-1"])
	require.Equal(t, uint64(20), rsm.state.ViewMetadataAdmissionProxyTargets["proxy-1"])
	require.Equal(t, uint64(10), rsm.state.ViewMetadataAdmissionProxyTargetTicks["proxy-1"])

	// Rejected heartbeats from the replaced processes receive the authoritative
	// generation but cannot refresh their captured target ticks.
	rsm.state.Tick = 12
	cnBatch := updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 10,
	})
	proxyBatch := updateViewMetadataProxy(t, rsm, pb.ProxyHeartbeat{
		UUID:                            "proxy-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 20,
	})
	require.Equal(t, uint64(11), cnBatch.ViewMetadataAdmission.Generation)
	require.Equal(t, uint64(21), proxyBatch.ViewMetadataAdmission.Generation)
	require.Equal(t, uint64(10), rsm.state.ViewMetadataAdmissionCNTargetTicks["cn-1"])
	require.Equal(t, uint64(10), rsm.state.ViewMetadataAdmissionProxyTargetTicks["proxy-1"])

	// A third overlapping process extends the revocation deadline to the latest
	// replaced owner without forgetting the still-unexpired oldest generation.
	rsm.state.Tick = 13
	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 12,
		ViewMetadataObservedEpoch:       3,
		ViewMetadataIngressReady:        true,
		ViewMetadataRefreshSupported:    true,
	})
	require.Equal(t, uint64(10), rsm.state.ViewMetadataAdmissionCNTargets["cn-1"])
	require.Equal(t, uint64(11), rsm.state.ViewMetadataAdmissionCNTargetTicks["cn-1"])
	rsm.state.Tick = 14
	cnBatch = updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 11,
	})
	require.Equal(t, uint64(12), cnBatch.ViewMetadataAdmission.Generation)
	require.Equal(t, uint64(11), rsm.state.ViewMetadataAdmissionCNTargetTicks["cn-1"])

	cfg := Config{
		TickPerSecond:     1,
		CNStoreTimeout:    5 * time.Second,
		ProxyStoreTimeout: 5 * time.Second,
	}
	rsm.state.Tick = 16
	result, err := rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmdForConfig(cfg),
	})
	require.NoError(t, err)
	require.Zero(t, result.Value)
	require.True(t, rsm.state.ViewMetadataAdmissionPending)

	rsm.state.Tick = 17
	result, err = rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmdForConfig(cfg),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Value)
	require.False(t, rsm.state.ViewMetadataAdmissionPending)
	require.True(t, rsm.state.CNState.Stores["cn-1"].ViewMetadataAdmissionReady)
	require.True(t, rsm.state.ProxyState.Stores["proxy-1"].ViewMetadataAdmissionReady)
}

func TestViewMetadataAdmissionPreparingWaitsForReplacedGenerationTimeout(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.Tick = 5
	rsm.state.ViewMetadataAdmissionPreparing = true
	rsm.state.ViewMetadataAdmissionEpoch = 1
	rsm.state.ViewMetadataRevalidationRequired = true
	rsm.state.ViewMetadataCatalogFencedEpoch = 1
	rsm.state.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
		Replicas: map[uint64]string{1: "log-1"},
	}
	rsm.state.ViewMetadataAdmissionLogReady = map[string]bool{"log-1": true}
	rsm.state.ViewMetadataAdmissionCNReady = map[string]bool{"cn-1": true}
	rsm.state.ViewMetadataAdmissionProxyReady = make(map[string]bool)
	rsm.state.CNState.Stores["cn-1"] = pb.CNStoreInfo{
		Tick:                            5,
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 2,
		ViewMetadataObservedEpoch:       1,
		ViewMetadataCatalogFencedEpoch:  1,
		ViewMetadataIngressReady:        true,
	}
	rsm.state.ViewMetadataAdmissionCNTargets = map[string]uint64{"cn-1": 1}
	rsm.state.ViewMetadataAdmissionCNTargetTicks = map[string]uint64{"cn-1": 1}

	cfg := Config{TickPerSecond: 1, CNStoreTimeout: 10 * time.Second}
	result, err := rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmdForConfig(cfg),
	})
	require.NoError(t, err)
	require.Zero(t, result.Value)
	require.True(t, rsm.state.ViewMetadataAdmissionPreparing)

	rsm.state.Tick = 12
	result, err = rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetEnableViewMetadataAdmissionCmdForConfig(cfg),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Value)
	require.True(t, rsm.state.ViewMetadataAdmissionEnabled)
}

func TestLifecycleUnawareJoinAdvancesEpochBeforeAdmission(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.ViewMetadataAdmissionEnabled = true
	rsm.state.ViewMetadataAdmissionEpoch = 7
	rsm.state.ViewMetadataCatalogFencedEpoch = 7
	rsm.state.CNState.Stores["active-cn"] = pb.CNStoreInfo{
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 1,
		ViewMetadataObservedEpoch:       7,
		ViewMetadataAdmissionReady:      true,
		ViewMetadataIngressReady:        true,
	}
	rsm.state.ProxyState.Stores["active-proxy"] = pb.ProxyStore{
		UUID:                            "active-proxy",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 2,
		ViewMetadataObservedEpoch:       7,
		ViewMetadataAdmissionReady:      true,
	}

	batch := updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "joining-cn",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 3,
		ViewMetadataObservedEpoch:       7,
		ViewMetadataRefreshSupported:    false,
	})
	require.Equal(t, uint64(8), rsm.state.ViewMetadataAdmissionEpoch)
	require.True(t, rsm.state.ViewMetadataRevalidationRequired)
	require.False(t, batch.ViewMetadataAdmission.Ready)
	require.Equal(t, uint64(1), rsm.state.ViewMetadataAdmissionCNTargets["active-cn"])
	require.Equal(t, uint64(2), rsm.state.ViewMetadataAdmissionProxyTargets["active-proxy"])

	updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "active-cn",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 1,
		ViewMetadataObservedEpoch:       8,
		ViewMetadataCatalogFencedEpoch:  8,
		ViewMetadataIngressReady:        true,
	})
	updateViewMetadataProxy(t, rsm, pb.ProxyHeartbeat{
		UUID:                            "active-proxy",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 2,
		ViewMetadataObservedEpoch:       8,
	})
	batch = updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "joining-cn",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 3,
		ViewMetadataObservedEpoch:       8,
	})
	require.True(t, batch.ViewMetadataAdmission.Admitted)
	require.False(t, batch.ViewMetadataAdmission.Ready)
	batch = updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "joining-cn",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 3,
		ViewMetadataObservedEpoch:       8,
		ViewMetadataIngressReady:        true,
	})
	require.True(t, batch.ViewMetadataAdmission.Ready)
	require.False(t, rsm.state.ViewMetadataAdmissionPending)
}

func TestViewMetadataAdmissionNewEpochRecoversLegacyTargetTicks(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.ViewMetadataAdmissionEnabled = true
	rsm.state.ViewMetadataAdmissionEpoch = 4
	rsm.state.ViewMetadataAdmissionCNTargets = map[string]uint64{"cn-1": 7}
	rsm.state.ViewMetadataAdmissionProxyTargets = map[string]uint64{"proxy-1": 8}
	rsm.state.CNState.Stores["cn-1"] = pb.CNStoreInfo{
		Tick:                            41,
		ViewMetadataAdmissionGeneration: 7,
	}
	rsm.state.ProxyState.Stores["proxy-1"] = pb.ProxyStore{
		UUID:                            "proxy-1",
		Tick:                            42,
		ViewMetadataAdmissionGeneration: 8,
	}

	rsm.startViewMetadataRequiredEpoch()
	require.Equal(t, uint64(41), rsm.state.ViewMetadataAdmissionCNTargetTicks["cn-1"])
	require.Equal(t, uint64(42), rsm.state.ViewMetadataAdmissionProxyTargetTicks["proxy-1"])
}

func TestLifecycleCapableJoinKeepsCurrentEpoch(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.ViewMetadataAdmissionEnabled = true
	rsm.state.ViewMetadataAdmissionEpoch = 7
	rsm.state.ViewMetadataCatalogFencedEpoch = 7

	batch := updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "joining-cn",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 3,
		ViewMetadataObservedEpoch:       7,
		ViewMetadataRefreshSupported:    true,
	})
	require.Equal(t, uint64(7), rsm.state.ViewMetadataAdmissionEpoch)
	require.False(t, rsm.state.ViewMetadataRevalidationRequired)
	require.True(t, batch.ViewMetadataAdmission.Admitted)
	require.False(t, batch.ViewMetadataAdmission.Ready)
	batch = updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "joining-cn",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 3,
		ViewMetadataObservedEpoch:       7,
		ViewMetadataRefreshSupported:    true,
		ViewMetadataIngressReady:        true,
	})
	require.True(t, batch.ViewMetadataAdmission.Ready)
	require.False(t, rsm.state.ViewMetadataAdmissionPending)
}

func TestCNAdmissionDoesNotPublishBeforeIngressIsPrepared(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.ViewMetadataAdmissionEnabled = true
	rsm.state.ViewMetadataAdmissionEpoch = 3
	rsm.state.ViewMetadataCatalogFencedEpoch = 3

	batch := updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "starting-cn",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 9,
		ViewMetadataObservedEpoch:       3,
		ViewMetadataRefreshSupported:    true,
	})
	require.True(t, batch.ViewMetadataAdmission.Admitted,
		"admission must let the CN finish starting its listeners")
	require.False(t, batch.ViewMetadataAdmission.Ready,
		"a CN paused after admission must not be routable")
	require.False(t, rsm.state.CNState.Stores["starting-cn"].ViewMetadataAdmissionReady)

	batch = updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "starting-cn",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 9,
		ViewMetadataObservedEpoch:       3,
		ViewMetadataRefreshSupported:    true,
		ViewMetadataIngressReady:        true,
	})
	require.True(t, batch.ViewMetadataAdmission.Ready)
	require.True(t, rsm.state.CNState.Stores["starting-cn"].ViewMetadataAdmissionReady)
}

func TestViewMetadataAdmissionRejectsStaleCatalogFence(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.ViewMetadataAdmissionEnabled = true
	rsm.state.ViewMetadataAdmissionEpoch = 5
	rsm.state.ViewMetadataRevalidationRequired = true

	batch := updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 3,
		ViewMetadataObservedEpoch:       4,
		ViewMetadataCatalogFencedEpoch:  4,
	})
	require.Zero(t, rsm.state.ViewMetadataCatalogFencedEpoch)
	require.False(t, batch.ViewMetadataAdmission.Ready)

	batch = updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
		UUID:                            "cn-1",
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 3,
		ViewMetadataObservedEpoch:       5,
		ViewMetadataCatalogFencedEpoch:  5,
		ViewMetadataIngressReady:        true,
	})
	require.Equal(t, uint64(5), rsm.state.ViewMetadataCatalogFencedEpoch)
	require.True(t, batch.ViewMetadataAdmission.Ready)
}

func TestClusterDetailsKeepsSupportedPendingButHidesLegacyIngress(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.ViewMetadataAdmissionEnabled = true
	rsm.state.ViewMetadataAdmissionEpoch = 1
	rsm.state.CNState.Stores["legacy"] = pb.CNStoreInfo{}
	rsm.state.CNState.Stores["pending"] = pb.CNStoreInfo{
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 2,
	}
	rsm.state.CNState.Stores["ready"] = pb.CNStoreInfo{
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 3,
		ViewMetadataObservedEpoch:       1,
		ViewMetadataAdmissionReady:      true,
		ViewMetadataIngressReady:        true,
	}

	details := rsm.handleClusterDetailsQuery(Config{})
	require.Len(t, details.CNStores, 2)
	byID := make(map[string]pb.CNStore, len(details.CNStores))
	for _, store := range details.CNStores {
		byID[store.UUID] = store
	}
	require.Contains(t, byID, "pending")
	require.False(t, byID["pending"].ViewMetadataAdmissionReady)
	require.Contains(t, byID, "ready")
	require.NotContains(t, byID, "legacy")
}

func TestViewMetadataAdmissionSnapshotRoundTripAndOldSnapshotFailClosed(t *testing.T) {
	source := NewStateMachine(0, 1).(*stateMachine)
	source.state.ViewMetadataAdmissionEnabled = true
	source.state.ViewMetadataAdmissionEpoch = 9
	source.state.ViewMetadataRevalidationRequired = true
	source.state.ViewMetadataCatalogFencedEpoch = 9
	source.state.ViewMetadataAdmissionPending = true
	source.state.ViewMetadataAdmissionCNTargets = map[string]uint64{"cn-1": 7}
	source.state.ViewMetadataAdmissionProxyTargets = map[string]uint64{"proxy-1": 8}
	source.state.ViewMetadataAdmissionCNTargetTicks = map[string]uint64{"cn-1": 70}
	source.state.ViewMetadataAdmissionProxyTargetTicks = map[string]uint64{"proxy-1": 80}

	buf := bytes.NewBuffer(nil)
	require.NoError(t, source.SaveSnapshot(buf, nil, nil))
	recovered := NewStateMachine(0, 2).(*stateMachine)
	require.NoError(t, recovered.RecoverFromSnapshot(buf, nil, nil))
	require.True(t, recovered.state.ViewMetadataAdmissionEnabled)
	require.Equal(t, uint64(9), recovered.state.ViewMetadataAdmissionEpoch)
	require.True(t, recovered.state.ViewMetadataRevalidationRequired)
	require.True(t, recovered.state.ViewMetadataAdmissionPending)
	require.Equal(t, map[string]uint64{"cn-1": 7},
		recovered.state.ViewMetadataAdmissionCNTargets)
	require.Equal(t, map[string]uint64{"proxy-1": 8},
		recovered.state.ViewMetadataAdmissionProxyTargets)
	require.Equal(t, map[string]uint64{"cn-1": 70},
		recovered.state.ViewMetadataAdmissionCNTargetTicks)
	require.Equal(t, map[string]uint64{"proxy-1": 80},
		recovered.state.ViewMetadataAdmissionProxyTargetTicks)

	legacy := pb.NewRSMState()
	legacyBytes, err := legacy.Marshal()
	require.NoError(t, err)
	recovered.state.ViewMetadataAdmissionEnabled = true
	recovered.state.ViewMetadataAdmissionEpoch = 99
	recovered.state.ViewMetadataAdmissionPending = true
	require.NoError(t, recovered.RecoverFromSnapshot(bytes.NewReader(legacyBytes), nil, nil))
	require.False(t, recovered.state.ViewMetadataAdmissionEnabled)
	require.Zero(t, recovered.state.ViewMetadataAdmissionEpoch)
	require.False(t, recovered.state.ViewMetadataAdmissionPending)
}

func TestViewMetadataAdmissionFencesLateHAKeeperMember(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.ViewMetadataAdmissionEnabled = true
	command := pb.ScheduleCommand{
		UUID:        "joining-log",
		ServiceType: pb.LogService,
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.AddReplica,
			Replica: pb.Replica{
				UUID:    "joining-log",
				ShardID: DefaultHAKeeperShardID,
			},
		},
	}
	_, err := rsm.Update(sm.Entry{
		Index: 10,
		Cmd:   GetUpdateCommandsCmd(1, []pb.ScheduleCommand{command}),
	})
	require.NoError(t, err)

	heartbeat := func(admissionSupported bool) pb.CommandBatch {
		t.Helper()
		data, marshalErr := (&pb.LogStoreHeartbeat{
			UUID:                           "joining-log",
			CommandDeliverySupported:       true,
			ViewMetadataAdmissionSupported: admissionSupported,
		}).Marshal()
		require.NoError(t, marshalErr)
		result, updateErr := rsm.Update(sm.Entry{
			Index: rsm.state.Index + 1,
			Cmd:   GetLogStoreHeartbeatCmd(data),
		})
		require.NoError(t, updateErr)
		var batch pb.CommandBatch
		require.NoError(t, batch.Unmarshal(result.Data))
		return batch
	}
	require.Empty(t, heartbeat(false).Commands)
	require.Equal(t, []pb.ScheduleCommand{command}, heartbeat(true).Commands)
}

func TestViewMetadataAdmissionReconciliationExpiresAbandonedGeneration(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.Tick = 20
	rsm.state.ViewMetadataAdmissionEnabled = true
	rsm.state.ViewMetadataAdmissionEpoch = 4
	rsm.state.ViewMetadataRevalidationRequired = true
	rsm.state.ViewMetadataCatalogFencedEpoch = 4
	rsm.state.ViewMetadataAdmissionPending = true
	rsm.state.CNState.Stores["abandoned-cn"] = pb.CNStoreInfo{
		Tick:                            1,
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 9,
	}
	rsm.state.ProxyState.Stores["abandoned-proxy"] = pb.ProxyStore{
		UUID:                            "abandoned-proxy",
		Tick:                            1,
		ViewMetadataAdmissionSupported:  true,
		ViewMetadataAdmissionGeneration: 10,
	}
	rsm.state.ViewMetadataAdmissionCNTargets = map[string]uint64{"abandoned-cn": 9}
	rsm.state.ViewMetadataAdmissionProxyTargets = map[string]uint64{"abandoned-proxy": 10}
	rsm.state.ViewMetadataAdmissionCNTargetTicks = map[string]uint64{"abandoned-cn": 1}
	rsm.state.ViewMetadataAdmissionProxyTargetTicks = map[string]uint64{"abandoned-proxy": 1}

	result, err := rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd: GetEnableViewMetadataAdmissionCmdForConfig(Config{
			TickPerSecond:     1,
			CNStoreTimeout:    10 * time.Second,
			ProxyStoreTimeout: 10 * time.Second,
		}),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Value)
	require.False(t, rsm.state.ViewMetadataAdmissionPending)
	require.False(t,
		rsm.state.CNState.Stores["abandoned-cn"].ViewMetadataAdmissionReady)
	require.False(t,
		rsm.state.ProxyState.Stores["abandoned-proxy"].ViewMetadataAdmissionReady)
	require.Empty(t, rsm.state.ViewMetadataAdmissionCNTargets)
	require.Empty(t, rsm.state.ViewMetadataAdmissionProxyTargets)
	require.Empty(t, rsm.state.ViewMetadataAdmissionCNTargetTicks)
	require.Empty(t, rsm.state.ViewMetadataAdmissionProxyTargetTicks)
}

func TestViewMetadataAdmissionDeletionPrunesHistoricalMembership(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.ViewMetadataAdmissionPreparing = true
	rsm.state.ViewMetadataAdmissionEpoch = 3
	rsm.state.ViewMetadataAdmissionCNReady = make(map[string]bool)
	rsm.state.ViewMetadataAdmissionProxyReady = make(map[string]bool)
	rsm.state.ViewMetadataAdmissionCNTargets = make(map[string]uint64)
	rsm.state.ViewMetadataAdmissionProxyTargets = make(map[string]uint64)
	rsm.state.ViewMetadataAdmissionCNTargetTicks = make(map[string]uint64)
	rsm.state.ViewMetadataAdmissionProxyTargetTicks = make(map[string]uint64)

	for i := range 64 {
		cnID := fmt.Sprintf("cn-%d", i)
		proxyID := fmt.Sprintf("proxy-%d", i)
		generation := uint64(i + 1)
		updateViewMetadataCN(t, rsm, pb.CNStoreHeartbeat{
			UUID:                            cnID,
			ViewMetadataAdmissionSupported:  true,
			ViewMetadataAdmissionGeneration: generation,
			ViewMetadataObservedEpoch:       3,
		})
		updateViewMetadataProxy(t, rsm, pb.ProxyHeartbeat{
			UUID:                            proxyID,
			ViewMetadataAdmissionSupported:  true,
			ViewMetadataAdmissionGeneration: generation,
			ViewMetadataObservedEpoch:       3,
		})
		rsm.state.ViewMetadataAdmissionCNTargets[cnID] = generation
		rsm.state.ViewMetadataAdmissionProxyTargets[proxyID] = generation
		rsm.state.ViewMetadataAdmissionCNTargetTicks[cnID] = generation
		rsm.state.ViewMetadataAdmissionProxyTargetTicks[proxyID] = generation

		rsm.handleDeleteCNCmd(cnID)
		rsm.handleDeleteProxyCmd(proxyID)
		require.Empty(t, rsm.state.CNState.Stores)
		require.Empty(t, rsm.state.ProxyState.Stores)
		require.Empty(t, rsm.state.ViewMetadataAdmissionCNReady)
		require.Empty(t, rsm.state.ViewMetadataAdmissionProxyReady)
		require.Empty(t, rsm.state.ViewMetadataAdmissionCNTargets)
		require.Empty(t, rsm.state.ViewMetadataAdmissionProxyTargets)
		require.Empty(t, rsm.state.ViewMetadataAdmissionCNTargetTicks)
		require.Empty(t, rsm.state.ViewMetadataAdmissionProxyTargetTicks)
	}

	buf := bytes.NewBuffer(nil)
	require.NoError(t, rsm.SaveSnapshot(buf, nil, nil))
	recovered := NewStateMachine(0, 2).(*stateMachine)
	require.NoError(t, recovered.RecoverFromSnapshot(buf, nil, nil))
	require.Empty(t, recovered.state.ViewMetadataAdmissionCNReady)
	require.Empty(t, recovered.state.ViewMetadataAdmissionProxyReady)
	require.Empty(t, recovered.state.ViewMetadataAdmissionCNTargets)
	require.Empty(t, recovered.state.ViewMetadataAdmissionProxyTargets)
	require.Empty(t, recovered.state.ViewMetadataAdmissionCNTargetTicks)
	require.Empty(t, recovered.state.ViewMetadataAdmissionProxyTargetTicks)
}
