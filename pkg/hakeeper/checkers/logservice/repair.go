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

import pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"

func isBlockedLogShardStore(
	repairs map[uint64]pb.LogShardRepairState,
	uuid string,
	shardID uint64,
) bool {
	if len(repairs) == 0 {
		return false
	}
	repair, ok := repairs[shardID]
	if !ok {
		return false
	}
	return repair.BlockedStores[uuid]
}

func filterBlockedWorkingStores(
	working map[string]pb.Locality,
	repairs map[uint64]pb.LogShardRepairState,
	shardID uint64,
) map[string]pb.Locality {
	if len(repairs) == 0 {
		return working
	}
	filtered := make(map[string]pb.Locality, len(working))
	for uuid, locality := range working {
		if isBlockedLogShardStore(repairs, uuid, shardID) {
			continue
		}
		filtered[uuid] = locality
	}
	return filtered
}
