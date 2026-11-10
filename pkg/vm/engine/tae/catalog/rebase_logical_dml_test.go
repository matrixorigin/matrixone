// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestRebaseLogicalDMLLifecycle(t *testing.T) {
	entry := MockTableEntryWithDB(nil, 42)
	fixed, committed := types.BuildTS(10, 0), types.BuildTS(11, 0)
	require.Error(t, entry.RegisterRebaseLogicalDML(""))
	entry.RecordKnownDMLPrepare(committed)
	require.False(t, entry.HasRebaseLogicalDMLAfter(fixed), "allocator/physical watermark is independent")
	require.NoError(t, entry.RegisterRebaseLogicalDML("rolled-back"))
	require.NoError(t, entry.RegisterRebaseLogicalDML("rolled-back"))
	require.True(t, entry.HasRebaseLogicalDMLAfter(fixed))
	entry.ResolveRebaseLogicalDML("foreign", &committed)
	require.Equal(t, types.TS{}, entry.RebaseLogicalDMLBound())
	entry.ResolveRebaseLogicalDML("rolled-back", nil)
	require.False(t, entry.HasRebaseLogicalDMLAfter(fixed))
	require.NoError(t, entry.RegisterRebaseLogicalDML("committed"))
	entry.ResolveRebaseLogicalDML("committed", &committed)
	require.Equal(t, committed, entry.RebaseLogicalDMLBound())
	require.True(t, entry.HasRebaseLogicalDMLAfter(fixed))
	require.False(t, entry.HasRebaseLogicalDMLAfter(committed))
	entry.RestoreRebaseLogicalDMLBound(fixed)
	entry.ResolveRebaseLogicalDML("rolled-back", &committed)
	require.Equal(t, committed, entry.RebaseLogicalDMLBound())
}
