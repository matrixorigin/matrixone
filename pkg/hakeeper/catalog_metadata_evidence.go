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

const catalogMetadataBarrierProtocolVersion uint64 = 1
const catalogMetadataEvidenceVersion uint32 = 1

// validateCatalogMetadataEvidence validates the decoder contract separately
// from phase. A legacy active phase is recoverable but is not runtime evidence.
// An uninitialized v1 state likewise never implies a successful empty capture.
func validateCatalogMetadataEvidence(state *pb.HAKeeperRSMState) error {
	floor := state.CatalogMetadataBarrierRequiredProtocolVersion
	if floor > catalogMetadataBarrierProtocolVersion {
		return moerr.NewInvalidInputNoCtx("unsupported catalog metadata barrier decoder floor")
	}
	barrier := state.CatalogMetadataBarrier
	if barrier == nil {
		if floor != 0 {
			return moerr.NewInvalidInputNoCtx("catalog metadata decoder floor has no evidence schema")
		}
		return nil
	}
	if floor == 0 {
		if barrier.RuntimeEvidenceVersion != 0 || barrier.EvidenceInitialized || len(barrier.Targets) != 0 {
			return moerr.NewInvalidInputNoCtx("catalog metadata runtime evidence has no decoder floor")
		}
		return nil
	}
	if barrier.RuntimeEvidenceVersion != catalogMetadataEvidenceVersion {
		return moerr.NewInvalidInputNoCtx("unsupported catalog metadata runtime evidence version")
	}
	if !barrier.EvidenceInitialized {
		if len(barrier.Targets) != 0 {
			return moerr.NewInvalidInputNoCtx("uninitialized catalog metadata evidence contains targets")
		}
		return nil
	}
	if barrier.Phase == pb.CATALOG_METADATA_BARRIER_DISABLED {
		return moerr.NewInvalidInputNoCtx("disabled catalog metadata barrier has initialized evidence")
	}
	for i := range barrier.Targets {
		target := &barrier.Targets[i]
		if (target.ServiceType != pb.CNService && target.ServiceType != pb.ProxyService) ||
			target.UUID == "" || target.Generation == 0 || target.CapturedTick > state.Tick {
			return moerr.NewInvalidInputNoCtx("invalid catalog metadata target identity or capture tick")
		}
		if target.SealComplete && (!target.ObservedPreparing || barrier.Phase < pb.CATALOG_METADATA_BARRIER_SEALED) {
			return moerr.NewInvalidInputNoCtx("catalog metadata target has premature seal evidence")
		}
		if i > 0 && !catalogMetadataTargetLess(&barrier.Targets[i-1], target) {
			return moerr.NewInvalidInputNoCtx("catalog metadata targets are duplicate or out of order")
		}
	}
	return nil
}

func catalogMetadataTargetLess(a, b *pb.CatalogMetadataBarrierTarget) bool {
	if a.ServiceType != b.ServiceType {
		return a.ServiceType < b.ServiceType
	}
	if a.UUID != b.UUID {
		return a.UUID < b.UUID
	}
	return a.Generation < b.Generation
}
