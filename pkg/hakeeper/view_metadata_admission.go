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
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// ViewMetadataAdmissionStateQuery reads the durable admission protocol state.
type ViewMetadataAdmissionStateQuery struct{}

// ViewMetadataAdmissionState is leader-local readback used only to decide
// whether the next replicated barrier/reconciliation entry can be proposed.
type ViewMetadataAdmissionState struct {
	Preparing              bool
	Enabled                bool
	Pending                bool
	HAKeeperAdmissionReady bool
	LogReady               map[string]bool
	CNReady                map[string]bool
	ProxyReady             map[string]bool
}

// GetEnableViewMetadataAdmissionCmd starts phase one. It must only be proposed
// after every HAKeeper replica has advertised support through its LogStore.
func GetEnableViewMetadataAdmissionCmd() []byte {
	return getEnableViewMetadataAdmissionCmd(nil)
}

// GetEnableViewMetadataAdmissionCmdForConfig performs phase two while
// preparing, and expires dead transition targets while enabled. Timeout ticks
// are carried in the replicated entry so every RSM makes the same decision.
func GetEnableViewMetadataAdmissionCmdForConfig(cfg Config) []byte {
	cfg.Fill()
	targets := pb.ViewMetadataAdmissionTargets{
		Explicit:               true,
		CNStoreTimeoutTicks:    uint64(cfg.CNStoreTimeout/time.Second) * uint64(cfg.TickPerSecond),
		ProxyStoreTimeoutTicks: uint64(cfg.ProxyStoreTimeout/time.Second) * uint64(cfg.TickPerSecond),
		EvaluateCurrentStores:  true,
	}
	return getEnableViewMetadataAdmissionCmd(&targets)
}

func getEnableViewMetadataAdmissionCmd(targets *pb.ViewMetadataAdmissionTargets) []byte {
	payload := []byte(nil)
	if targets != nil {
		payload = make([]byte, targets.ProtoSize())
		if _, err := targets.MarshalTo(payload); err != nil {
			panic(err)
		}
	}
	cmd := make([]byte, headerSize+len(payload))
	binaryEnc.PutUint32(cmd, uint32(pb.EnableViewMetadataAdmissionUpdate))
	copy(cmd[headerSize:], payload)
	return cmd
}

func parseEnableViewMetadataAdmissionCmd(cmd []byte) (pb.ViewMetadataAdmissionTargets, bool) {
	if len(cmd) <= headerSize {
		return pb.ViewMetadataAdmissionTargets{}, false
	}
	var targets pb.ViewMetadataAdmissionTargets
	if err := targets.Unmarshal(cmd[headerSize:]); err != nil {
		panic(err)
	}
	return targets, targets.Explicit
}

func (s *stateMachine) viewMetadataAdmissionActive() bool {
	return s.state.ViewMetadataAdmissionPreparing || s.state.ViewMetadataAdmissionEnabled
}

func (s *stateMachine) cnViewMetadataAdmitted(store pb.CNStoreInfo) bool {
	if !s.viewMetadataAdmissionActive() {
		return true
	}
	if !s.viewMetadataAdmissionTargetsReady() ||
		!store.ViewMetadataAdmissionSupported ||
		store.ViewMetadataAdmissionGeneration == 0 ||
		store.ViewMetadataObservedEpoch < s.state.ViewMetadataAdmissionEpoch {
		return false
	}
	return !s.state.ViewMetadataRevalidationRequired ||
		s.state.ViewMetadataCatalogFencedEpoch >= s.state.ViewMetadataAdmissionEpoch ||
		store.ViewMetadataCatalogFencedEpoch >= s.state.ViewMetadataAdmissionEpoch
}

func (s *stateMachine) proxyViewMetadataAdmitted(store pb.ProxyStore) bool {
	return !s.viewMetadataAdmissionActive() ||
		(s.viewMetadataAdmissionTargetsReady() &&
			store.ViewMetadataAdmissionSupported &&
			store.ViewMetadataAdmissionGeneration != 0 &&
			store.ViewMetadataObservedEpoch >= s.state.ViewMetadataAdmissionEpoch)
}

func (s *stateMachine) resetViewMetadataAdmissionBarrier() {
	for uuid, store := range s.state.LogState.Stores {
		store.ViewMetadataAdmissionSupported = false
		s.state.LogState.Stores[uuid] = store
	}
	for uuid, store := range s.state.CNState.Stores {
		// These stores were already serving before phase one. Keep them active
		// during preparation, but discard every pre-barrier capability/epoch ack.
		// A legacy binary has no ingress-ready field, so its zero value must not
		// withdraw an already-serving CN during a rolling upgrade. A capable CN,
		// however, is routable only after it has published ingress readiness.
		if store.ViewMetadataAdmissionSupported &&
			store.ViewMetadataAdmissionGeneration != 0 {
			store.ViewMetadataAdmissionReady = store.ViewMetadataIngressReady
		} else {
			store.ViewMetadataAdmissionReady = true
		}
		store.ViewMetadataAdmissionSupported = false
		store.ViewMetadataObservedEpoch = 0
		store.ViewMetadataCatalogFencedEpoch = 0
		store.ViewMetadataRefreshSupported = false
		store.ViewMetadataRevalidatedEpoch = 0
		s.state.CNState.Stores[uuid] = store
	}
	for uuid, store := range s.state.ProxyState.Stores {
		store.ViewMetadataAdmissionReady = true
		store.ViewMetadataAdmissionSupported = false
		store.ViewMetadataObservedEpoch = 0
		s.state.ProxyState.Stores[uuid] = store
	}
	s.state.ViewMetadataAdmissionLogReady = make(map[string]bool)
	s.state.ViewMetadataAdmissionCNReady = make(map[string]bool)
	s.state.ViewMetadataAdmissionProxyReady = make(map[string]bool)
	s.state.ViewMetadataAdmissionCNTargets = nil
	s.state.ViewMetadataAdmissionProxyTargets = nil
	s.state.ViewMetadataAdmissionCNTargetTicks = nil
	s.state.ViewMetadataAdmissionProxyTargetTicks = nil
	s.state.ViewMetadataAdmissionPending = false
}

func (s *stateMachine) startViewMetadataRequiredEpoch() {
	previousCNTargets := s.state.ViewMetadataAdmissionCNTargets
	previousProxyTargets := s.state.ViewMetadataAdmissionProxyTargets
	previousCNTargetTicks := s.state.ViewMetadataAdmissionCNTargetTicks
	previousProxyTargetTicks := s.state.ViewMetadataAdmissionProxyTargetTicks
	s.state.ViewMetadataAdmissionEpoch++
	s.state.ViewMetadataRevalidationRequired = true
	s.state.ViewMetadataCatalogFencedEpoch = 0
	s.state.ViewMetadataAdmissionCNTargets = make(map[string]uint64)
	s.state.ViewMetadataAdmissionProxyTargets = make(map[string]uint64)
	s.state.ViewMetadataAdmissionCNTargetTicks = make(map[string]uint64)
	s.state.ViewMetadataAdmissionProxyTargetTicks = make(map[string]uint64)
	for uuid, generation := range previousCNTargets {
		s.state.ViewMetadataAdmissionCNTargets[uuid] = generation
		tick, ok := previousCNTargetTicks[uuid]
		if !ok {
			if store, exists := s.state.CNState.Stores[uuid]; exists &&
				store.ViewMetadataAdmissionGeneration == generation {
				tick = store.Tick
			}
		}
		s.state.ViewMetadataAdmissionCNTargetTicks[uuid] = tick
	}
	for uuid, generation := range previousProxyTargets {
		s.state.ViewMetadataAdmissionProxyTargets[uuid] = generation
		tick, ok := previousProxyTargetTicks[uuid]
		if !ok {
			if store, exists := s.state.ProxyState.Stores[uuid]; exists &&
				store.ViewMetadataAdmissionGeneration == generation {
				tick = store.Tick
			}
		}
		s.state.ViewMetadataAdmissionProxyTargetTicks[uuid] = tick
	}
	for uuid, store := range s.state.CNState.Stores {
		if store.ViewMetadataAdmissionReady {
			s.captureCNViewMetadataAdmissionTarget(uuid, store)
		}
	}
	for uuid, store := range s.state.ProxyState.Stores {
		if store.ViewMetadataAdmissionReady {
			s.captureProxyViewMetadataAdmissionTarget(uuid, store)
		}
	}
	s.state.ViewMetadataAdmissionPending = true
}

func (s *stateMachine) captureCNViewMetadataAdmissionTarget(uuid string, store pb.CNStoreInfo) {
	if s.state.ViewMetadataAdmissionCNTargets == nil {
		s.state.ViewMetadataAdmissionCNTargets = make(map[string]uint64)
	}
	if s.state.ViewMetadataAdmissionCNTargetTicks == nil {
		s.state.ViewMetadataAdmissionCNTargetTicks = make(map[string]uint64)
	}
	if _, ok := s.state.ViewMetadataAdmissionCNTargets[uuid]; !ok {
		s.state.ViewMetadataAdmissionCNTargets[uuid] = store.ViewMetadataAdmissionGeneration
	}
	if tick, ok := s.state.ViewMetadataAdmissionCNTargetTicks[uuid]; !ok || store.Tick > tick {
		s.state.ViewMetadataAdmissionCNTargetTicks[uuid] = store.Tick
	}
	s.state.ViewMetadataAdmissionPending = true
}

func (s *stateMachine) captureProxyViewMetadataAdmissionTarget(uuid string, store pb.ProxyStore) {
	if s.state.ViewMetadataAdmissionProxyTargets == nil {
		s.state.ViewMetadataAdmissionProxyTargets = make(map[string]uint64)
	}
	if s.state.ViewMetadataAdmissionProxyTargetTicks == nil {
		s.state.ViewMetadataAdmissionProxyTargetTicks = make(map[string]uint64)
	}
	if _, ok := s.state.ViewMetadataAdmissionProxyTargets[uuid]; !ok {
		s.state.ViewMetadataAdmissionProxyTargets[uuid] = store.ViewMetadataAdmissionGeneration
	}
	if tick, ok := s.state.ViewMetadataAdmissionProxyTargetTicks[uuid]; !ok || store.Tick > tick {
		s.state.ViewMetadataAdmissionProxyTargetTicks[uuid] = store.Tick
	}
	s.state.ViewMetadataAdmissionPending = true
}

func (s *stateMachine) viewMetadataAdmissionTargetsReady() bool {
	for uuid, generation := range s.state.ViewMetadataAdmissionCNTargets {
		store, ok := s.state.CNState.Stores[uuid]
		if !ok || store.ViewMetadataAdmissionGeneration != generation ||
			store.ViewMetadataObservedEpoch < s.state.ViewMetadataAdmissionEpoch {
			return false
		}
	}
	for uuid, generation := range s.state.ViewMetadataAdmissionProxyTargets {
		store, ok := s.state.ProxyState.Stores[uuid]
		if !ok || store.ViewMetadataAdmissionGeneration != generation ||
			store.ViewMetadataObservedEpoch < s.state.ViewMetadataAdmissionEpoch {
			return false
		}
	}
	return true
}

func (s *stateMachine) tryPromoteViewMetadataAdmissions(
	targets *pb.ViewMetadataAdmissionTargets,
) {
	if !s.state.ViewMetadataAdmissionEnabled || !s.state.ViewMetadataAdmissionPending ||
		s.state.ViewMetadataCatalogFencedEpoch < s.state.ViewMetadataAdmissionEpoch ||
		!s.viewMetadataAdmissionTargetsReady() {
		return
	}

	pending := false
	for uuid, store := range s.state.CNState.Stores {
		if s.cnViewMetadataAdmitted(store) && store.ViewMetadataIngressReady {
			store.ViewMetadataAdmissionReady = true
			s.state.CNState.Stores[uuid] = store
		} else if store.ViewMetadataAdmissionSupported &&
			store.ViewMetadataAdmissionGeneration != 0 &&
			!store.ViewMetadataAdmissionReady &&
			(targets == nil || !targets.EvaluateCurrentStores ||
				!commandDeliveryStoreExpired(
					store.Tick,
					s.state.Tick,
					targets.CNStoreTimeoutTicks,
				)) {
			pending = true
		}
	}
	for uuid, store := range s.state.ProxyState.Stores {
		if s.proxyViewMetadataAdmitted(store) {
			store.ViewMetadataAdmissionReady = true
			s.state.ProxyState.Stores[uuid] = store
		} else if store.ViewMetadataAdmissionSupported &&
			store.ViewMetadataAdmissionGeneration != 0 &&
			!store.ViewMetadataAdmissionReady &&
			(targets == nil || !targets.EvaluateCurrentStores ||
				!commandDeliveryStoreExpired(
					store.Tick,
					s.state.Tick,
					targets.ProxyStoreTimeoutTicks,
				)) {
			pending = true
		}
	}
	if !pending {
		s.state.ViewMetadataAdmissionCNTargets = nil
		s.state.ViewMetadataAdmissionProxyTargets = nil
		s.state.ViewMetadataAdmissionCNTargetTicks = nil
		s.state.ViewMetadataAdmissionProxyTargetTicks = nil
	}
	s.state.ViewMetadataAdmissionPending = pending
}

func (s *stateMachine) expireViewMetadataAdmissionTargets(targets pb.ViewMetadataAdmissionTargets) {
	if !targets.EvaluateCurrentStores {
		return
	}
	for uuid, generation := range s.state.ViewMetadataAdmissionCNTargets {
		lastTick, ok := s.state.ViewMetadataAdmissionCNTargetTicks[uuid]
		if !ok {
			// Snapshots written before target ticks were introduced can recover
			// the tick only while the captured generation still owns the UUID.
			if store, exists := s.state.CNState.Stores[uuid]; exists &&
				store.ViewMetadataAdmissionGeneration == generation {
				lastTick = store.Tick
			}
		}
		if commandDeliveryStoreExpired(lastTick, s.state.Tick, targets.CNStoreTimeoutTicks) {
			delete(s.state.ViewMetadataAdmissionCNTargets, uuid)
			delete(s.state.ViewMetadataAdmissionCNTargetTicks, uuid)
		}
	}
	for uuid, generation := range s.state.ViewMetadataAdmissionProxyTargets {
		lastTick, ok := s.state.ViewMetadataAdmissionProxyTargetTicks[uuid]
		if !ok {
			if store, exists := s.state.ProxyState.Stores[uuid]; exists &&
				store.ViewMetadataAdmissionGeneration == generation {
				lastTick = store.Tick
			}
		}
		if commandDeliveryStoreExpired(lastTick, s.state.Tick, targets.ProxyStoreTimeoutTicks) {
			delete(s.state.ViewMetadataAdmissionProxyTargets, uuid)
			delete(s.state.ViewMetadataAdmissionProxyTargetTicks, uuid)
		}
	}
}

func (s *stateMachine) handleEnableViewMetadataAdmission(cmd []byte) sm.Result {
	targets, hasTargets := parseEnableViewMetadataAdmissionCmd(cmd)
	if s.state.ViewMetadataAdmissionEnabled {
		if hasTargets {
			s.expireViewMetadataAdmissionTargets(targets)
			s.tryPromoteViewMetadataAdmissions(&targets)
		} else {
			s.tryPromoteViewMetadataAdmissions(nil)
		}
		if s.state.ViewMetadataAdmissionPending {
			return sm.Result{}
		}
		return sm.Result{Value: 1}
	}
	if !s.state.ViewMetadataAdmissionPreparing {
		if s.hasPendingHAKeeperAdmission() {
			return sm.Result{}
		}
		s.state.ViewMetadataAdmissionPreparing = true
		if s.state.ViewMetadataAdmissionEpoch == 0 {
			s.state.ViewMetadataAdmissionEpoch = 1
		}
		s.state.ViewMetadataRevalidationRequired = true
		s.state.ViewMetadataCatalogFencedEpoch = 0
		s.resetViewMetadataAdmissionBarrier()
		return sm.Result{Value: 2}
	}
	if s.state.ViewMetadataAdmissionLogReady == nil ||
		s.state.ViewMetadataAdmissionCNReady == nil ||
		s.state.ViewMetadataAdmissionProxyReady == nil {
		s.resetViewMetadataAdmissionBarrier()
		return sm.Result{Value: 2}
	}
	shard, ok := s.state.LogState.Shards[DefaultHAKeeperShardID]
	if !ok || len(shard.Replicas) == 0 {
		return sm.Result{}
	}
	for _, uuid := range shard.Replicas {
		if !s.state.ViewMetadataAdmissionLogReady[uuid] {
			return sm.Result{}
		}
	}
	for _, uuid := range shard.NonVotingReplicas {
		if !s.state.ViewMetadataAdmissionLogReady[uuid] {
			return sm.Result{}
		}
	}
	if !hasTargets || !targets.EvaluateCurrentStores {
		return sm.Result{}
	}
	s.expireViewMetadataAdmissionTargets(targets)
	if len(s.state.ViewMetadataAdmissionCNTargets) != 0 ||
		len(s.state.ViewMetadataAdmissionProxyTargets) != 0 {
		return sm.Result{}
	}
	for uuid, store := range s.state.CNState.Stores {
		if commandDeliveryStoreExpired(store.Tick, s.state.Tick, targets.CNStoreTimeoutTicks) {
			continue
		}
		if !s.state.ViewMetadataAdmissionCNReady[uuid] {
			return sm.Result{}
		}
	}
	for uuid, store := range s.state.ProxyState.Stores {
		if commandDeliveryStoreExpired(store.Tick, s.state.Tick, targets.ProxyStoreTimeoutTicks) {
			continue
		}
		if !s.state.ViewMetadataAdmissionProxyReady[uuid] {
			return sm.Result{}
		}
	}
	if s.state.ViewMetadataCatalogFencedEpoch < s.state.ViewMetadataAdmissionEpoch {
		return sm.Result{}
	}
	for uuid, store := range s.state.CNState.Stores {
		store.ViewMetadataAdmissionReady = false
		if !commandDeliveryStoreExpired(store.Tick, s.state.Tick, targets.CNStoreTimeoutTicks) &&
			s.cnViewMetadataAdmitted(store) && store.ViewMetadataIngressReady {
			store.ViewMetadataAdmissionReady = true
		}
		s.state.CNState.Stores[uuid] = store
	}
	for uuid, store := range s.state.ProxyState.Stores {
		store.ViewMetadataAdmissionReady = false
		if !commandDeliveryStoreExpired(store.Tick, s.state.Tick, targets.ProxyStoreTimeoutTicks) &&
			store.ViewMetadataAdmissionSupported &&
			store.ViewMetadataObservedEpoch >= s.state.ViewMetadataAdmissionEpoch {
			store.ViewMetadataAdmissionReady = true
		}
		s.state.ProxyState.Stores[uuid] = store
	}
	s.state.ViewMetadataAdmissionEnabled = true
	s.state.ViewMetadataAdmissionPreparing = false
	s.state.ViewMetadataAdmissionLogReady = nil
	s.state.ViewMetadataAdmissionCNReady = nil
	s.state.ViewMetadataAdmissionProxyReady = nil
	s.state.ViewMetadataAdmissionCNTargets = nil
	s.state.ViewMetadataAdmissionProxyTargets = nil
	s.state.ViewMetadataAdmissionCNTargetTicks = nil
	s.state.ViewMetadataAdmissionProxyTargetTicks = nil
	s.state.ViewMetadataAdmissionPending = false
	return sm.Result{Value: 1}
}

// updateCNViewMetadataAdmission installs a heartbeat unless a newer process
// generation already owns this UUID. It returns false for a stale heartbeat.
func (s *stateMachine) updateCNViewMetadataAdmission(hb pb.CNStoreHeartbeat) bool {
	previous, existed := s.state.CNState.Stores[hb.UUID]
	active := s.viewMetadataAdmissionActive()
	if active && existed &&
		hb.ViewMetadataAdmissionGeneration < previous.ViewMetadataAdmissionGeneration {
		return false
	}
	newGeneration := !existed ||
		hb.ViewMetadataAdmissionGeneration > previous.ViewMetadataAdmissionGeneration
	if active && s.state.ViewMetadataAdmissionEnabled && newGeneration &&
		!s.state.ViewMetadataRevalidationRequired && !hb.ViewMetadataRefreshSupported {
		// Capture the old owner before CNState.Update replaces it.
		s.startViewMetadataRequiredEpoch()
	}
	if active && existed && newGeneration &&
		(previous.ViewMetadataAdmissionReady || previous.ViewMetadataIngressReady) {
		s.captureCNViewMetadataAdmissionTarget(hb.UUID, previous)
	}
	s.state.CNState.Update(hb, s.state.Tick)
	store := s.state.CNState.Stores[hb.UUID]
	if !active {
		return true
	}
	if generation, ok := s.state.ViewMetadataAdmissionCNTargets[hb.UUID]; ok &&
		generation == hb.ViewMetadataAdmissionGeneration {
		if s.state.ViewMetadataAdmissionCNTargetTicks == nil {
			s.state.ViewMetadataAdmissionCNTargetTicks = make(map[string]uint64)
		}
		s.state.ViewMetadataAdmissionCNTargetTicks[hb.UUID] = store.Tick
	}
	if newGeneration {
		store.ViewMetadataAdmissionReady = false
	}
	if !hb.ViewMetadataIngressReady {
		store.ViewMetadataAdmissionReady = false
	}
	if !hb.ViewMetadataAdmissionSupported || hb.ViewMetadataAdmissionGeneration == 0 {
		if s.state.ViewMetadataAdmissionEnabled {
			store.ViewMetadataAdmissionReady = false
		}
		delete(s.state.ViewMetadataAdmissionCNReady, hb.UUID)
		s.state.CNState.Stores[hb.UUID] = store
		return true
	}
	if hb.ViewMetadataObservedEpoch >= s.state.ViewMetadataAdmissionEpoch {
		if s.state.ViewMetadataAdmissionPreparing {
			if s.state.ViewMetadataAdmissionCNReady == nil {
				s.state.ViewMetadataAdmissionCNReady = make(map[string]bool)
			}
			s.state.ViewMetadataAdmissionCNReady[hb.UUID] = true
		}
	}
	if hb.ViewMetadataCatalogFencedEpoch == s.state.ViewMetadataAdmissionEpoch &&
		hb.ViewMetadataObservedEpoch >= hb.ViewMetadataCatalogFencedEpoch {
		s.state.ViewMetadataCatalogFencedEpoch = hb.ViewMetadataCatalogFencedEpoch
	}
	if s.state.ViewMetadataAdmissionEnabled && !store.ViewMetadataAdmissionReady {
		s.state.ViewMetadataAdmissionPending = true
	}
	s.state.CNState.Stores[hb.UUID] = store
	s.tryPromoteViewMetadataAdmissions(nil)
	return true
}

func (s *stateMachine) updateProxyViewMetadataAdmission(hb pb.ProxyHeartbeat) bool {
	previous, existed := s.state.ProxyState.Stores[hb.UUID]
	active := s.viewMetadataAdmissionActive()
	if active && existed &&
		hb.ViewMetadataAdmissionGeneration < previous.ViewMetadataAdmissionGeneration {
		return false
	}
	newGeneration := !existed ||
		hb.ViewMetadataAdmissionGeneration > previous.ViewMetadataAdmissionGeneration
	if active && existed && newGeneration && previous.ViewMetadataAdmissionReady {
		s.captureProxyViewMetadataAdmissionTarget(hb.UUID, previous)
	}
	s.state.ProxyState.Update(hb, s.state.Tick)
	store := s.state.ProxyState.Stores[hb.UUID]
	if !active {
		return true
	}
	if generation, ok := s.state.ViewMetadataAdmissionProxyTargets[hb.UUID]; ok &&
		generation == hb.ViewMetadataAdmissionGeneration {
		if s.state.ViewMetadataAdmissionProxyTargetTicks == nil {
			s.state.ViewMetadataAdmissionProxyTargetTicks = make(map[string]uint64)
		}
		s.state.ViewMetadataAdmissionProxyTargetTicks[hb.UUID] = store.Tick
	}
	if newGeneration {
		store.ViewMetadataAdmissionReady = false
	}
	if !hb.ViewMetadataAdmissionSupported || hb.ViewMetadataAdmissionGeneration == 0 {
		if s.state.ViewMetadataAdmissionEnabled {
			store.ViewMetadataAdmissionReady = false
		}
		delete(s.state.ViewMetadataAdmissionProxyReady, hb.UUID)
		s.state.ProxyState.Stores[hb.UUID] = store
		return true
	}
	if hb.ViewMetadataObservedEpoch >= s.state.ViewMetadataAdmissionEpoch &&
		s.state.ViewMetadataAdmissionPreparing {
		if s.state.ViewMetadataAdmissionProxyReady == nil {
			s.state.ViewMetadataAdmissionProxyReady = make(map[string]bool)
		}
		s.state.ViewMetadataAdmissionProxyReady[hb.UUID] = true
	}
	if s.state.ViewMetadataAdmissionEnabled && !store.ViewMetadataAdmissionReady {
		s.state.ViewMetadataAdmissionPending = true
	}
	s.state.ProxyState.Stores[hb.UUID] = store
	s.tryPromoteViewMetadataAdmissions(nil)
	return true
}

func (s *stateMachine) viewMetadataAdmissionSnapshot(uuid string, proxy bool) *pb.ViewMetadataAdmission {
	snapshot := &pb.ViewMetadataAdmission{
		Preparing:            s.state.ViewMetadataAdmissionPreparing,
		Enabled:              s.state.ViewMetadataAdmissionEnabled,
		Epoch:                s.state.ViewMetadataAdmissionEpoch,
		RevalidationRequired: s.state.ViewMetadataRevalidationRequired,
		CatalogFencedEpoch:   s.state.ViewMetadataCatalogFencedEpoch,
		Ready:                !s.viewMetadataAdmissionActive(),
		Admitted:             !s.viewMetadataAdmissionActive(),
	}
	if proxy {
		if store, ok := s.state.ProxyState.Stores[uuid]; ok {
			snapshot.Generation = store.ViewMetadataAdmissionGeneration
			snapshot.Ready = store.ViewMetadataAdmissionReady
			snapshot.Admitted = s.proxyViewMetadataAdmitted(store)
		}
	} else if store, ok := s.state.CNState.Stores[uuid]; ok {
		snapshot.Generation = store.ViewMetadataAdmissionGeneration
		snapshot.Ready = store.ViewMetadataAdmissionReady
		snapshot.Admitted = s.cnViewMetadataAdmitted(store)
	}
	return snapshot
}

func (s *stateMachine) attachViewMetadataAdmission(
	result sm.Result,
	uuid string,
	proxy bool,
) sm.Result {
	supported := false
	if proxy {
		supported = s.state.ProxyState.Stores[uuid].ViewMetadataAdmissionSupported
	} else {
		supported = s.state.CNState.Stores[uuid].ViewMetadataAdmissionSupported
	}
	if !supported {
		return result
	}
	var batch pb.CommandBatch
	if len(result.Data) > 0 {
		if err := batch.Unmarshal(result.Data); err != nil {
			panic(err)
		}
	}
	batch.ViewMetadataAdmission = s.viewMetadataAdmissionSnapshot(uuid, proxy)
	data, err := batch.Marshal()
	if err != nil {
		panic(err)
	}
	result.Data = data
	return result
}
