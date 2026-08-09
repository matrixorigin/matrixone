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

package clusterservice

import "github.com/matrixorigin/matrixone/pkg/pb/metadata"

// AllKnownCNsSupportViewMetadataRefresh is the rolling-upgrade activation
// barrier. An empty cluster snapshot is not ready, and one old CN, including a
// draining CN that may still finish in-flight DDL, keeps the feature in legacy
// mode until that CN leaves the snapshot or advertises support.
func AllKnownCNsSupportViewMetadataRefresh(serviceID string) bool {
	return allKnownCNsSupportViewMetadataRefresh(GetMOCluster(serviceID))
}

func allKnownCNsSupportViewMetadataRefresh(cluster MOCluster) bool {
	found, supported := false, true
	cluster.GetCNService(NewSelectAll(), func(service metadata.CNService) bool {
		found = true
		if !service.ViewMetadataRefreshSupported {
			supported = false
			return false
		}
		return true
	})
	return found && supported
}
