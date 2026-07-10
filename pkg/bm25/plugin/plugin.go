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

// Package plugin is the bm25 index plugin registration point.
//
// bm25 is a position-free BM25 ranked-retrieval index over a TEXT/VARCHAR
// column, created via `CREATE INDEX ... USING bm25 [WITH PARSER <tok>]` and
// queried via `MATCH(col) AGAINST('query')`. Structurally it follows the
// vector plugins (parsed to *tree.Index, dispatched through
// BuildSecondaryIndexDefs), with the WAND binary engine in pkg/bm25/wand.
//
// # Phase 1b (current)
//
// Skeleton: catalog hooks (runtime/) are fully implemented; compile/ and
// plan/ schema hooks are STUBS (return NYI). Registration is intentionally
// DEFERRED — an unregistered bm25 algo makes `CREATE INDEX ... USING bm25`
// fail cleanly with "unsupported index type" rather than dispatching to the
// NYI stubs. init() is uncommented in Phase 2 once BuildSecondaryIndexDefs
// (hidden tables) and HandleCreateIndex (build) are real.
package plugin

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"

	bm25compile "github.com/matrixorigin/matrixone/pkg/bm25/plugin/compile"
	bm25idxcron "github.com/matrixorigin/matrixone/pkg/bm25/plugin/idxcron"
	bm25plan "github.com/matrixorigin/matrixone/pkg/bm25/plugin/plan"
	bm25runtime "github.com/matrixorigin/matrixone/pkg/bm25/plugin/runtime"
)

// Plugin is the bm25 AlgoPlugin.
type Plugin struct {
	catalogHooks catalogplugin.Hooks
	compileHooks compileplugin.Hooks
	planHooks    planplugin.Hooks
	idxcronHooks idxcronplugin.Hooks
}

func New() *Plugin {
	return &Plugin{
		catalogHooks: bm25runtime.CatalogHooks{},
		compileHooks: bm25compile.Hooks{},
		planHooks:    bm25plan.Hooks{},
		idxcronHooks: bm25idxcron.Hooks{},
	}
}

func (*Plugin) Algo() string                   { return catalog.MoIndexBm25Algo.ToString() }
func (p *Plugin) Catalog() catalogplugin.Hooks { return p.catalogHooks }
func (p *Plugin) Compile() compileplugin.Hooks { return p.compileHooks }
func (p *Plugin) Plan() planplugin.Hooks       { return p.planHooks }
func (p *Plugin) Idxcron() idxcronplugin.Hooks { return p.idxcronHooks }

// Compile-time check that *Plugin satisfies the AlgoPlugin interface.
var _ plugin.AlgoPlugin = (*Plugin)(nil)

// Registration is deferred until Phase 2 (see package doc). Uncomment to
// enable dispatch once compile.HandleCreateIndex and
// plan.BuildSecondaryIndexDefs are implemented.
//
// func init() { plugin.Register(New()) }
