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
	"sort"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type catalogTargetKey struct {
	kind pb.ServiceType
	uuid string
}

func recaptureCatalogTargets(b *pb.CatalogMetadataBarrierState, current []pb.CatalogMetadataBarrierTarget) []pb.CatalogMetadataBarrierTarget {
	positions := make(map[catalogTargetKey]int, len(current))
	for i, target := range current {
		positions[catalogTargetKey{target.ServiceType, target.UUID}] = i
	}
	for _, old := range b.Targets {
		key := catalogTargetKey{old.ServiceType, old.UUID}
		position, exists := positions[key]
		if len(old.AuthorityRetirementDigest) != 0 {
			if exists && current[position].Generation == old.Generation {
				// Retired generations cannot reacquire authority just because
				// their last heartbeat remains in the store map.
				current[position] = old
			}
			continue
		}
		if old.SealComplete && b.Phase != pb.CATALOG_METADATA_BARRIER_ACTIVATED {
			continue
		}
		old.ObservedPreparing, old.SealComplete = false, false
		if exists {
			current[position] = old
		} else {
			positions[key] = len(current)
			current = append(current, old)
		}
	}
	sort.Slice(current, func(i, j int) bool { return catalogMetadataTargetLess(&current[i], &current[j]) })
	return current
}
