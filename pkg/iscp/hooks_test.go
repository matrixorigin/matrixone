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

package iscp

import (
	"context"
	"testing"

	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestSinkerTypeConsistency locks in the invariant that the
// catalog-plugin SinkerType_IndexSync constant (used by every
// algorithm's SyncDescriptor) equals iscp.ConsumerType_IndexSync (the
// actual value consumers compare against). They are declared in
// separate packages to avoid an import cycle; if either side ever
// drifts, plugin-driven CDC dispatch silently routes to the wrong
// consumer type. This test fires on the first drift.
func TestSinkerTypeConsistency(t *testing.T) {
	require.Equal(t,
		int8(ConsumerType_IndexSync),
		catalogplugin.SinkerType_IndexSync,
		"iscp.ConsumerType_IndexSync and catalogplugin.SinkerType_IndexSync must match",
	)
}

func TestRegisterAndGet(t *testing.T) {
	defer snapshotHooks()()

	h := stubHooks{}
	Register("test-algo", h)

	got, ok := GetHooks("test-algo")
	require.True(t, ok)
	require.Equal(t, h, got)

	require.True(t, HasHooks("test-algo"))
	require.True(t, HasHooks("  TEST-ALGO  "), "lookup should be case-insensitive and trim whitespace")
}

func TestRegisterDuplicatesPanic(t *testing.T) {
	defer snapshotHooks()()

	Register("dup", stubHooks{})
	require.Panics(t, func() {
		Register("dup", stubHooks{})
	})
	require.Panics(t, func() {
		Register("  DUP ", stubHooks{}) // normalized collision
	})
}

func TestGetMissing(t *testing.T) {
	defer snapshotHooks()()
	_, ok := GetHooks("nope-no-such-algo")
	require.False(t, ok)
	require.False(t, HasHooks("nope-no-such-algo"))
}

// snapshotHooks captures the current registry and returns a function
// that restores it. Use as `defer snapshotHooks()()` so tests that
// mutate the registry don't leak state into siblings (especially the
// production-equivalent hooks installed by hooks_testinit_test.go).
func snapshotHooks() func() {
	hooksMu.Lock()
	saved := make(map[string]Hooks, len(hooks))
	for k, v := range hooks {
		saved[k] = v
	}
	hooksMu.Unlock()
	return func() {
		hooksMu.Lock()
		defer hooksMu.Unlock()
		hooks = saved
	}
}

// stubHooks is a no-op Hooks implementation used by registry tests.
type stubHooks struct{}

func (stubHooks) NewSqlWriter(JobID, *ConsumerInfo, *plan.TableDef, []*plan.IndexDef) (IndexSqlWriter, error) {
	return nil, nil
}
func (stubHooks) Run(*IndexConsumer, context.Context, chan error, DataRetriever) {}
