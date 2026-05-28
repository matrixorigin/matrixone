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

// Package plugin is the fulltext index plugin registration point.
//
// All four hook surfaces (catalog / compile / plan / idxcron) are
// live: catalog hooks in runtime/, compile hooks in compile/, plan
// hooks in plan/, idxcron hook in idxcron/. The plan-layer
// BuildSecondaryIndexDefs returns an error by contract — fulltext is
// reached via BuildFullTextIndexDefs instead, since CREATE FULLTEXT
// INDEX parses to *tree.FullTextIndex, a distinct AST node from
// *tree.Index.
//
// DML hooks (preinsert / postinsert / delete) remain inline in
// pkg/sql/plan/build_dml_util.go; the plugin framework does not yet
// expose a DML hook surface.
package plugin

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"

	fulltextcompile "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/compile"
	fulltextidxcron "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/idxcron"
	fulltextplan "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/plan"
	fulltextruntime "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/runtime"
)

// Plugin is the fulltext AlgoPlugin.
type Plugin struct {
	catalogHooks catalogplugin.Hooks
	compileHooks compileplugin.Hooks
	planHooks    planplugin.Hooks
	idxcronHooks idxcronplugin.Hooks
}

func New() *Plugin {
	return &Plugin{
		catalogHooks: fulltextruntime.CatalogHooks{},
		compileHooks: fulltextcompile.Hooks{},
		planHooks:    fulltextplan.Hooks{},
		idxcronHooks: fulltextidxcron.Hooks{},
	}
}

func (*Plugin) Algo() string                   { return catalog.MOIndexFullTextAlgo.ToString() }
func (p *Plugin) Catalog() catalogplugin.Hooks { return p.catalogHooks }
func (p *Plugin) Compile() compileplugin.Hooks { return p.compileHooks }
func (p *Plugin) Plan() planplugin.Hooks       { return p.planHooks }
func (p *Plugin) Idxcron() idxcronplugin.Hooks { return p.idxcronHooks }

// Compile-time check that *Plugin satisfies the AlgoPlugin interface.
var _ plugin.AlgoPlugin = (*Plugin)(nil)

// init registers fulltext with the global plugin registry.
func init() { plugin.Register(New()) }
