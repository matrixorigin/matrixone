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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/stretchr/testify/require"
)

// TestPluginHooks exercises New(), Algo() and every hook accessor on the
// fulltext2 Plugin, and confirms it satisfies the indexplugin.AlgoPlugin
// interface (the `var _ ...AlgoPlugin` assertion in plugin.go).
func TestPluginHooks(t *testing.T) {
	p := New()
	require.NotNil(t, p)

	// Algo string is the fulltext2 algo name.
	require.Equal(t, catalog.MoIndexFullText2Algo.ToString(), p.Algo())
	require.Equal(t, "fulltext2", p.Algo())

	// Every hook accessor returns a usable (non-nil interface) hook struct.
	require.NotNil(t, p.Catalog())
	require.NotNil(t, p.Compile())
	require.NotNil(t, p.Plan())
	require.NotNil(t, p.Idxcron())

	// The Plugin satisfies the AlgoPlugin interface (mirrors the compile-time
	// `var _ indexplugin.AlgoPlugin = (*Plugin)(nil)` assertion).
	var ap indexplugin.AlgoPlugin = p
	require.Equal(t, p.Algo(), ap.Algo())
}

// TestPluginRegistered confirms init() registered the fulltext2 plugin so the
// framework can look it up by algo name.
func TestPluginRegistered(t *testing.T) {
	got, ok := indexplugin.Get(catalog.MoIndexFullText2Algo.ToString())
	require.True(t, ok)
	require.NotNil(t, got)
	require.Equal(t, "fulltext2", got.Algo())
}
