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

package runtime

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestStarCountTestConfigScopeAndRestore(t *testing.T) {
	sid := t.Name()
	rt := NewRuntime(metadata.ServiceType_CN, sid, nil)
	SetupServiceBasedRuntime(sid, rt)
	t.Cleanup(func() { CompareAndDeleteServiceBasedRuntime(sid, rt) })
	const tableID = 42
	_, ok := GetStarCountTestConfig(sid, tableID)
	require.False(t, ok, "ordinary runtime keeps the deployment settings")

	SetupServiceRuntimeTestingContext(sid)
	tc := MustGetTestingContext(sid)
	_, ok = GetStarCountTestConfig(sid, tableID)
	require.False(t, ok, "default testing configuration is inert")

	previous := tc.SetStarCountTestConfig(StarCountTestConfig{
		TableID:      tableID,
		ProbeTimeout: time.Second,
	})
	for _, candidate := range []struct {
		service string
		table   uint64
	}{
		{"", tableID},
		{sid + "-other", tableID},
		{sid, 0},
		{sid, tableID + 1},
	} {
		_, ok := GetStarCountTestConfig(candidate.service, candidate.table)
		require.False(t, ok, candidate)
	}
	cfg, ok := GetStarCountTestConfig(sid, tableID)
	require.True(t, ok)
	require.Zero(t, cfg.TombstoneThreshold)
	require.Equal(t, time.Second, cfg.ProbeTimeout)
	tc.SetStarCountTestConfig(previous)
	_, ok = GetStarCountTestConfig(sid, tableID)
	require.False(t, ok, "restoring the previous configuration removes the override")
}
