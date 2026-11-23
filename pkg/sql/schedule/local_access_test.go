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

package schedule

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalAccessIntentPlacement(t *testing.T) {
	intent := LocalAccessIntent{CurrentCN: Worker{ID: "cn-local", Addr: "local:6001", Route: WorkerRouteLocal}}
	got := intent
	require.True(t, got.AllowsPinnedLocalAccess())
	require.True(t, got.AllowsLocalIndexPlacement())
	got.Scheduling.CurrentCNPolicy = CurrentCNExcluded
	require.True(t, got.AllowsPinnedLocalAccess())
	require.False(t, got.AllowsLocalIndexPlacement())
	got.Scheduling.CurrentCNPolicy = CurrentCNRequired
	got.CurrentCN = Worker{}
	require.False(t, got.AllowsLocalIndexPlacement())
	require.False(t, got.AllowsPinnedLocalAccess())
	got = intent
	got.CurrentCN.Route = WorkerRouteRemote
	require.False(t, got.AllowsLocalIndexPlacement())
	got = intent
	got.Scheduling.Explicit = true
	require.False(t, got.AllowsLocalIndexPlacement())
	got = intent
	got.Scheduling.PoolFallback = PoolFallbackStrict
	require.False(t, got.AllowsLocalIndexPlacement())
	got = intent
	got.Sirius = true
	require.False(t, got.AllowsPinnedLocalAccess())
	require.False(t, got.AllowsLocalIndexPlacement())
	got = intent
	got.Scheduling.PoolFallback = PoolFallbackPolicy(255)
	require.False(t, got.AllowsPinnedLocalAccess())
}
