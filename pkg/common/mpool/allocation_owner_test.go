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

package mpool

import "testing"

func TestAllocationOwnerCatalog(t *testing.T) {
	owners := []struct {
		owner AllocationOwner
		id    AllocationOwner
		name  string
	}{
		{AllocationOwnerHashBuild, 1, "hash_build"},
		{AllocationOwnerIndexBuild, 2, "index_build"},
		{AllocationOwnerFuzzyFilter, 3, "fuzzy_filter"},
		{AllocationOwnerGroup, 4, "group"},
		{AllocationOwnerOrder, 5, "order"},
		{AllocationOwnerTop, 6, "top"},
		{AllocationOwnerFill, 7, "fill"},
		{AllocationOwnerCTE, 8, "cte"},
		{AllocationOwnerSet, 9, "set"},
		{AllocationOwnerFulltext, 10, "fulltext"},
		{AllocationOwnerDML, 11, "dml"},
		{AllocationOwnerSample, 12, "sample"},
	}
	seen := make(map[AllocationOwner]struct{}, len(owners))
	seenNames := make(map[string]struct{}, len(owners))
	for _, entry := range owners {
		if entry.owner != entry.id {
			t.Fatalf("owner %q ID = %d, want stable ID %d", entry.name, entry.owner, entry.id)
		}
		if entry.owner < AllocationOwnerMin || entry.owner > AllocationOwnerCatalogMax {
			t.Fatalf("owner %d is outside [%d,%d]", entry.owner, AllocationOwnerMin, AllocationOwnerCatalogMax)
		}
		if _, ok := seen[entry.owner]; ok {
			t.Fatalf("duplicate owner %d", entry.owner)
		}
		seen[entry.owner] = struct{}{}
		if got := entry.owner.String(); got != entry.name {
			t.Fatalf("owner %d label = %q, want %q", entry.owner, got, entry.name)
		}
		if _, ok := seenNames[entry.name]; ok {
			t.Fatalf("duplicate owner label %q", entry.name)
		}
		seenNames[entry.name] = struct{}{}
	}
	if AllocationOwnerCatalogMax != AllocationOwnerSample {
		t.Fatalf("catalog max = %d, want %d", AllocationOwnerCatalogMax, AllocationOwnerSample)
	}
	if AllocationOwnerCatalogMax > AllocationOwnerMax {
		t.Fatalf("catalog max %d exceeds reserved max %d", AllocationOwnerCatalogMax, AllocationOwnerMax)
	}
	if got := AllocationOwnerMax.String(); got != "owner-63" {
		t.Fatalf("unknown owner label = %q", got)
	}
}
