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

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestAllKnownCNsSupportViewMetadataRefresh(t *testing.T) {
	newCluster := func(cns ...metadata.CNService) *cluster {
		ready := make(chan struct{})
		close(ready)
		c := &cluster{readyC: ready}
		snapshot := &services{}
		snapshot.addCN(cns)
		c.services.Store(snapshot)
		return c
	}

	require.False(t, allKnownCNsSupportViewMetadataRefresh(newCluster()))
	require.False(t, allKnownCNsSupportViewMetadataRefresh(newCluster(
		metadata.CNService{ServiceID: "new", WorkState: metadata.WorkState_Working,
			ViewMetadataRefreshSupported: true},
		metadata.CNService{ServiceID: "old", WorkState: metadata.WorkState_Working},
	)))
	require.False(t, allKnownCNsSupportViewMetadataRefresh(newCluster(
		metadata.CNService{ServiceID: "new", WorkState: metadata.WorkState_Working,
			ViewMetadataRefreshSupported: true},
		metadata.CNService{ServiceID: "old-draining", WorkState: metadata.WorkState_Draining},
	)))
	require.True(t, allKnownCNsSupportViewMetadataRefresh(newCluster(
		metadata.CNService{ServiceID: "new-1", WorkState: metadata.WorkState_Working,
			ViewMetadataRefreshSupported: true},
		metadata.CNService{ServiceID: "new-2", WorkState: metadata.WorkState_Working,
			ViewMetadataRefreshSupported: true},
	)))
}
