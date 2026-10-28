// Copyright 2026 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

// TestDataBranchFencingWorkspaceFloorsPinnedMatchDisttae locks the
// certificate's pinned floor set (DataBranchFencingWorkspaceFloorsPinned) to
// the engine's compiled constants, per D184(B.1)(ii) carried by D185. Drift
// on either side — the constants or the pin — breaks this equality before it
// can silently weaken the certification.
func TestDataBranchFencingWorkspaceFloorsPinnedMatchDisttae(t *testing.T) {
	f := DataBranchFencingWorkspaceFloorsPinned()
	require.Equal(t, disttae.CommitWorkspaceThreshold, f.Commit)
	require.Equal(t, disttae.WriteWorkspaceThreshold, f.Write)
	require.Equal(t, disttae.ExtraWorkspaceThreshold, f.Extra)
	require.Equal(t, uint64(disttae.InsertEntryThreshold), f.InsertEntry)
}
