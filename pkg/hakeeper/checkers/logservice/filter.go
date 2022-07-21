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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func selectStore(shardInfo logservice.LogShardInfo, stores *util.ClusterStores) util.StoreID {
	workingStores := stores.WorkingStores()
	excluded := make([]string, 0, len(shardInfo.Replicas))
	for _, storeID := range shardInfo.Replicas {
		excluded = append(excluded, storeID)
	}

	candidates := util.FilterStore(workingStores, []util.IFilter{util.NewExcludedFilter(excluded...)})
	if len(candidates) == 0 {
		return ""
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ID < candidates[j].ID
	})

	return candidates[0].ID
}
