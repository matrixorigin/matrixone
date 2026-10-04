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

import "sync/atomic"

// CatalogMetadataParticipant records observation only. In particular, receiving
// SEALED does not prove local authority drain and never manufactures SealComplete.
// It is independent of legacy View admission and does not authorize public reads.
type CatalogMetadataParticipant struct {
	observed atomic.Pointer[CatalogMetadataAck]
}

func (p *CatalogMetadataParticipant) Observe(generation uint64, snapshot *CatalogMetadataBarrier) {
	if snapshot == nil || generation == 0 || snapshot.RecipientGeneration != generation ||
		snapshot.MembershipEpoch == 0 || snapshot.RequiredGeneration == 0 ||
		snapshot.Phase < CATALOG_METADATA_BARRIER_PREPARING || snapshot.Phase > CATALOG_METADATA_BARRIER_ACTIVATED {
		return
	}
	next := &CatalogMetadataAck{Generation: generation, MembershipEpoch: snapshot.MembershipEpoch,
		RequiredGeneration: snapshot.RequiredGeneration, ObservedPhase: snapshot.Phase}
	for {
		old := p.observed.Load()
		if old != nil {
			if next.Generation < old.Generation || next.MembershipEpoch < old.MembershipEpoch || next.RequiredGeneration < old.RequiredGeneration {
				return
			}
			if next.MembershipEpoch == old.MembershipEpoch && next.RequiredGeneration == old.RequiredGeneration && next.ObservedPhase < old.ObservedPhase {
				return
			}
		}
		if p.observed.CompareAndSwap(old, next) {
			return
		}
	}
}

func (p *CatalogMetadataParticipant) Ack(generation uint64) *CatalogMetadataAck {
	value := p.observed.Load()
	if value == nil || value.Generation != generation {
		return nil
	}
	copy := *value
	return &copy
}
