// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"cmp"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// selectStore selects the best store for a replica.
func selectStore(
	shardInfo logservice.LogShardInfo,
	workingStoreMap map[string]logservice.Locality,
	locality logservice.Locality,
) string {
	workingStores := make([]*util.Store, 0, len(workingStoreMap))
	for id, loc := range workingStoreMap {
		if hakeeper.LogStoreEligibleLocality(loc, locality) {
			workingStores = append(workingStores, &util.Store{ID: id})
		}
	}

	excluded := make([]string, 0, len(shardInfo.Replicas))
	for _, storeID := range shardInfo.Replicas {
		excluded = append(excluded, storeID)
	}
	for _, storeID := range shardInfo.NonVotingReplicas {
		excluded = append(excluded, storeID)
	}

	candidates := util.FilterStore(workingStores, []util.IFilter{util.NewExcludedFilter(excluded...)})
	if len(candidates) == 0 {
		return ""
	}

	slices.SortFunc(candidates, func(a, b *util.Store) int {
		return cmp.Compare(a.ID, b.ID)
	})

	return candidates[0].ID
}
