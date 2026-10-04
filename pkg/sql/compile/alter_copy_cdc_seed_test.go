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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// fakeAlterCopyHooks embeds the compile Hooks interface (nil) so it satisfies it while
// overriding only AlterCopyInitSQL; the helper never calls the other methods. It records
// the idxDefs it was handed so the name-filter branch can be asserted.
type fakeAlterCopyHooks struct {
	compileplugin.Hooks
	startFromNow bool
	initSQL      string
	err          error
	gotDefs      map[string]*plan.IndexDef
}

func (f *fakeAlterCopyHooks) AlterCopyInitSQL(_ compileplugin.CompileContext, defs map[string]*plan.IndexDef) (bool, string, error) {
	f.gotDefs = defs
	return f.startFromNow, f.initSQL, f.err
}

var _ compileplugin.Hooks = (*fakeAlterCopyHooks)(nil)

func TestAlterCopyCdcSeed(t *testing.T) {
	indexes := []*plan.IndexDef{
		{IndexName: "ftidx", IndexAlgoTableType: "metadata"},
		{IndexName: "ftidx", IndexAlgoTableType: "ftv2_index"},
		{IndexName: "other", IndexAlgoTableType: "metadata"},
	}

	// A rebuild seed passes through unchanged.
	t.Run("rebuild passthrough", func(t *testing.T) {
		h := &fakeAlterCopyHooks{startFromNow: true, initSQL: "ALTER ... REINDEX ... FORCE_SYNC"}
		sfn, sql, err := alterCopyCdcSeed(h, nil, "ftidx", indexes)
		require.NoError(t, err)
		require.True(t, sfn)
		require.Equal(t, "ALTER ... REINDEX ... FORCE_SYNC", sql)
		// only the named index's hidden-table defs are collected, not "other".
		require.Len(t, h.gotDefs, 2)
		for _, d := range h.gotDefs {
			require.Equal(t, "ftidx", d.IndexName)
		}
	})

	// No InitSQL must force startFromNow=false so the CDC replays from ts=0 rather than
	// arming the tail from now and dropping pre-existing rows.
	t.Run("empty initSQL forces ts=0 replay", func(t *testing.T) {
		h := &fakeAlterCopyHooks{startFromNow: true, initSQL: ""}
		sfn, sql, err := alterCopyCdcSeed(h, nil, "ftidx", indexes)
		require.NoError(t, err)
		require.False(t, sfn, "empty InitSQL must not start from now")
		require.Empty(t, sql)
	})

	// The common (false,"") plugin: no rebuild, consume the full log.
	t.Run("false empty algo", func(t *testing.T) {
		sfn, sql, err := alterCopyCdcSeed(&fakeAlterCopyHooks{}, nil, "ftidx", indexes)
		require.NoError(t, err)
		require.False(t, sfn)
		require.Empty(t, sql)
	})

	// A plugin error propagates and clears the returns.
	t.Run("error propagates", func(t *testing.T) {
		h := &fakeAlterCopyHooks{startFromNow: true, initSQL: "x", err: moerr.NewInternalErrorNoCtx("boom")}
		sfn, sql, err := alterCopyCdcSeed(h, nil, "ftidx", indexes)
		require.Error(t, err)
		require.False(t, sfn)
		require.Empty(t, sql)
	})
}
