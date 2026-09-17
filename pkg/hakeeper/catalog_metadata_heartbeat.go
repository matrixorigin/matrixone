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
	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func (s *stateMachine) catalogMetadataTargetsCurrent(b *pb.CatalogMetadataBarrierState) bool {
	current, ok := s.captureCatalogTargets(b.RequiredViewDependencyProtocol, b.RequiredRecoveryProtocol)
	if !ok || len(current) != len(b.Targets) {
		return false
	}
	for i := range current {
		a, target := current[i], b.Targets[i]
		if a.ServiceType != target.ServiceType || a.UUID != target.UUID || a.Generation != target.Generation {
			return false
		}
	}
	return true
}

func (s *stateMachine) attachCatalogMetadataBarrier(result sm.Result, uuid string, proxy bool) sm.Result {
	var generation uint64
	var capability *pb.CatalogMetadataCapabilities
	var ack *pb.CatalogMetadataAck
	kind := pb.CNService
	if proxy {
		kind = pb.ProxyService
		store := s.state.ProxyState.Stores[uuid]
		generation, capability, ack = store.ViewMetadataAdmissionGeneration, store.CatalogMetadataCapabilities, store.CatalogMetadataAck
	} else {
		store := s.state.CNState.Stores[uuid]
		generation, capability, ack = store.ViewMetadataAdmissionGeneration, store.CatalogMetadataCapabilities, store.CatalogMetadataAck
	}
	b := s.state.CatalogMetadataBarrier
	if b == nil || !b.EvidenceInitialized || generation == 0 || capability == nil || capability.BarrierParticipantProtocol < 1 {
		return result
	}
	admitted := false
	for i := range b.Targets {
		t := &b.Targets[i]
		if t.UUID != uuid || t.ServiceType != kind || t.Generation != generation {
			continue
		}
		if ack != nil && ack.Generation == generation && ack.MembershipEpoch == b.MembershipEpoch && ack.RequiredGeneration == b.RequiredGeneration &&
			ack.ObservedPhase >= pb.CATALOG_METADATA_BARRIER_PREPARING && ack.ObservedPhase <= b.Phase {
			// This is observation only. SealComplete is never accepted from the
			// generic heartbeat path: it needs the authority issuer's drain proof.
			t.ObservedPreparing = true
		}
		admitted = b.Phase == pb.CATALOG_METADATA_BARRIER_ACTIVATED && t.SealComplete && s.catalogMetadataTargetsCurrent(b) && s.catalogMetadataReplicasReady()
	}
	var batch pb.CommandBatch
	if len(result.Data) != 0 {
		if err := batch.Unmarshal(result.Data); err != nil {
			panic(err)
		}
	}
	batch.CatalogMetadataBarrier = &pb.CatalogMetadataBarrier{
		Phase: b.Phase, MembershipEpoch: b.MembershipEpoch, RequiredGeneration: b.RequiredGeneration,
		CompletedGeneration: b.CompletedGeneration, RecipientGeneration: generation, Admitted: admitted,
		MetadataReadsEnabled: false,
	}
	data, err := batch.Marshal()
	if err != nil {
		panic(err)
	}
	result.Data = data
	return result
}
