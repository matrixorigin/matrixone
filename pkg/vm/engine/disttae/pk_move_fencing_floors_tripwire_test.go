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

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

// TestDataBranchFencingWorkspaceFloorsTripwire is the same-package tripwire
// of D184(B.1)(ii), carried by D185: the data-branch fencing certificate
// asserts staged lane shapes against these compiled workspace floors. Any
// edit of the constants must trip here, and the live default wiring must
// deliver exactly these values to a zero-config engine.
func TestDataBranchFencingWorkspaceFloorsTripwire(t *testing.T) {
	require.Equal(t, uint64(1*mpool.MB), CommitWorkspaceThreshold,
		"CommitWorkspaceThreshold drifted from the certified 1MB floor")
	require.Equal(t, uint64(5*mpool.MB), WriteWorkspaceThreshold,
		"WriteWorkspaceThreshold drifted from the certified 5MB floor")
	require.Equal(t, uint64(500*mpool.MB), ExtraWorkspaceThreshold,
		"ExtraWorkspaceThreshold drifted from the certified 500MB floor")
	require.Equal(t, 5000, InsertEntryThreshold,
		"InsertEntryThreshold drifted from the certified 5000 floor")
}

// TestDataBranchFencingFillDefaultsEquality is the live fillDefaults
// equality arm: a zero-config engine must materialize exactly the certified
// floors, so the certificate's premises hold for the production default
// deployment. An explicit override (WithInsertEntryMaxCount and friends)
// stays an operator-visible, certification-relevant deviation.
func TestDataBranchFencingFillDefaultsEquality(t *testing.T) {
	var e Engine
	e.fillDefaults()
	require.Equal(t, InsertEntryThreshold, e.config.insertEntryMaxCount)
	require.Equal(t, CommitWorkspaceThreshold, e.config.commitWorkspaceThreshold)
	require.Equal(t, WriteWorkspaceThreshold, e.config.writeWorkspaceThreshold)
	require.Equal(t, ExtraWorkspaceThreshold, e.config.extraWorkspaceThreshold)
}
