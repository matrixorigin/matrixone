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

package plugin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/coverage"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
)

// bareAlgo implements AlgoPlugin only — the common case, and the one that must
// be treated as "never covered" without carrying a no-op hook.
type bareAlgo struct{ algo string }

func (p bareAlgo) Algo() string               { return p.algo }
func (bareAlgo) Catalog() catalogplugin.Hooks { return nil }
func (bareAlgo) Compile() compileplugin.Hooks { return nil }
func (bareAlgo) Plan() planplugin.Hooks       { return nil }
func (bareAlgo) Idxcron() idxcronplugin.Hooks { return nil }

// coveringAlgo also answers the optional coverage capability.
type coveringAlgo struct {
	bareAlgo
	covered bool
	err     error
}

type stubHooks struct {
	covered bool
	err     error
	calls   *int
}

func (h stubHooks) CoversSnapshot(context.Context, coverage.Request) (bool, error) {
	if h.calls != nil {
		*h.calls++
	}
	return h.covered, h.err
}

func (p coveringAlgo) Coverage() coverage.Hooks { return stubHooks{covered: p.covered, err: p.err} }

// registerForTest installs a plugin and removes it when the test ends, so the
// process-wide registry is not left mutated.
func registerForTest(t *testing.T, p AlgoPlugin) {
	t.Helper()
	Register(p)
	t.Cleanup(func() {
		registryMu.Lock()
		delete(registry, p.Algo())
		registryMu.Unlock()
	})
}

// The whole point of the capability is that only algorithms that can answer
// honestly do, and everything else declines rather than lying.
func TestCoversSnapshotFailsClosed(t *testing.T) {
	registerForTest(t, bareAlgo{algo: "cov_bare"})
	registerForTest(t, coveringAlgo{bareAlgo: bareAlgo{algo: "cov_err"}, err: moerr.NewInternalErrorNoCtx("boom")})
	registerForTest(t, coveringAlgo{bareAlgo: bareAlgo{algo: "cov_no"}, covered: false})

	ctx := context.Background()

	// unregistered algo: nothing to ask
	covered, err := CoversSnapshot(ctx, "cov_missing", coverage.Request{})
	require.NoError(t, err)
	require.False(t, covered)

	// registered but without the capability
	covered, err = CoversSnapshot(ctx, "cov_bare", coverage.Request{})
	require.NoError(t, err)
	require.False(t, covered)

	// the hook errored: the error is surfaced for logging, the answer is no
	covered, err = CoversSnapshot(ctx, "cov_err", coverage.Request{})
	require.Error(t, err)
	require.False(t, covered)

	// the hook simply said no
	covered, err = CoversSnapshot(ctx, "cov_no", coverage.Request{})
	require.NoError(t, err)
	require.False(t, covered)
}

// A capable algorithm's "yes" is passed through, and the request reaches it.
func TestCoversSnapshotDelegates(t *testing.T) {
	calls := 0
	p := coveringAlgo{bareAlgo: bareAlgo{algo: "cov_yes"}, covered: true}
	registerForTest(t, p)

	covered, err := CoversSnapshot(context.Background(), "cov_yes", coverage.Request{TableID: 42})
	require.NoError(t, err)
	require.True(t, covered)

	// and the dispatch really goes through the hook, not a shortcut
	registerForTest(t, countingAlgo{bareAlgo: bareAlgo{algo: "cov_count"}, calls: &calls})
	_, err = CoversSnapshot(context.Background(), "cov_count", coverage.Request{})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

type countingAlgo struct {
	bareAlgo
	calls *int
}

func (p countingAlgo) Coverage() coverage.Hooks { return stubHooks{calls: p.calls} }

var (
	_ AlgoPlugin     = bareAlgo{}
	_ CoveragePlugin = coveringAlgo{}
)
