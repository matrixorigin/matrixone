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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func validateCatalogMetadataArbitration(b *pb.CatalogMetadataBarrierState) error {
	a := b.Arbitration
	if a == nil {
		if b.EvidenceInitialized {
			return moerr.NewInvalidInputNoCtx("initialized catalog evidence has no arbitration owner")
		}
		return nil
	}
	invalid := func() error { return moerr.NewInvalidInputNoCtx("invalid catalog metadata arbitration state") }
	if !a.MaintenanceEnabled && (a.LastOperationID != 0 || a.Fence != nil || a.Reservation != nil || len(a.StartPermits) != 0 || a.ClaimID != 0) {
		return invalid()
	}
	if a.MaintenanceEnabled && len(a.Members) == 0 {
		return invalid()
	}
	if !a.MaintenanceEnabled && (len(a.Members) != 0 || a.LastConsumedFence != 0) {
		return invalid()
	}
	for id, member := range a.Members {
		if id == 0 || member.UUID == "" || member.StoreIncarnation == "" {
			return invalid()
		}
	}
	if b.EvidenceInitialized && (!a.MaintenanceEnabled || a.LastConsumedFence == 0) {
		return invalid()
	}
	if b.EvidenceInitialized && ((b.Phase >= pb.CATALOG_METADATA_BARRIER_CATALOG_REQUIRED && a.RequiredReceipt == nil) ||
		(b.Phase >= pb.CATALOG_METADATA_BARRIER_RECOVERING && a.StartedReceipt == nil) ||
		(b.Phase == pb.CATALOG_METADATA_BARRIER_ACTIVATED && a.CompletedReceipt == nil)) {
		return invalid()
	}
	if a.LastConsumedFence > a.LastOperationID || a.ClaimID > a.LastOperationID {
		return invalid()
	}
	if a.Fence != nil {
		f := a.Fence
		if f.Token == 0 || f.Token > a.LastOperationID || f.Token <= a.LastConsumedFence || f.Owner == "" ||
			f.ExpectedPhase != b.Phase || f.MembershipEpoch != b.MembershipEpoch || f.RequiredGeneration != b.RequiredGeneration ||
			a.Reservation != nil || catalogMetadataPendingStart(a) {
			return invalid()
		}
	}
	if a.Reservation != nil {
		m := a.Reservation
		if len(m.ExpectedVoting) == 0 {
			return invalid()
		}
		for _, members := range []map[uint64]string{m.ExpectedVoting, m.ExpectedNonVoting} {
			for id, uuid := range members {
				if id == 0 || uuid == "" {
					return invalid()
				}
			}
		}
		for id := range m.ExpectedVoting {
			if _, both := m.ExpectedNonVoting[id]; both {
				return invalid()
			}
		}
		if m.Token == 0 || m.Token > a.LastOperationID || m.ConfigChangeIndex == 0 || m.ReplicaID == 0 || m.UUID == "" || m.StoreIncarnation == "" || catalogMetadataPendingStart(a) {
			return invalid()
		}
		switch m.ChangeType {
		case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
		default:
			return invalid()
		}
	}
	for i, p := range a.StartPermits {
		member, admitted := a.Members[p.ReplicaID]
		if admitted {
			if member.UUID != p.UUID || member.StoreIncarnation != p.StoreIncarnation || member.NonVoting != p.NonVoting {
				return invalid()
			}
		} else if !p.RevocationPending {
			return invalid()
		}
		if p.Token == 0 || p.Token > a.LastOperationID || p.ReplicaID == 0 || p.UUID == "" || p.StoreIncarnation == "" || (p.Completed && p.Revoked) || (p.RevocationPending && (p.Completed || p.Revoked)) ||
			(i > 0 && a.StartPermits[i-1].UUID >= p.UUID) {
			return invalid()
		}
	}
	for _, stage := range []struct {
		r      *pb.CatalogMetadataReceipt
		action pb.CatalogMetadataAction
		phase  pb.CatalogMetadataBarrierPhase
	}{
		{a.RequiredReceipt, pb.CATALOG_ACTION_CATALOG_REQUIRED, pb.CATALOG_METADATA_BARRIER_CATALOG_REQUIRED},
		{a.StartedReceipt, pb.CATALOG_ACTION_RECOVERY_STARTED, pb.CATALOG_METADATA_BARRIER_RECOVERING},
		{a.CompletedReceipt, pb.CATALOG_ACTION_COMPLETE, pb.CATALOG_METADATA_BARRIER_ACTIVATED},
	} {
		r := stage.r
		if r == nil {
			continue
		}
		if !b.EvidenceInitialized || !a.MaintenanceEnabled || b.Phase < stage.phase || r.Action != stage.action || len(r.Digest) != 32 ||
			r.MembershipEpoch != b.MembershipEpoch || r.RequiredGeneration != b.RequiredGeneration {
			return invalid()
		}
		if stage.action == pb.CATALOG_ACTION_CATALOG_REQUIRED {
			if r.ClaimID != 0 {
				return invalid()
			}
		} else if r.ClaimID == 0 || r.ClaimID != a.ClaimID {
			return invalid()
		}
	}
	if a.StartedReceipt != nil && a.RequiredReceipt == nil || a.CompletedReceipt != nil && a.StartedReceipt == nil {
		return invalid()
	}
	return nil
}
