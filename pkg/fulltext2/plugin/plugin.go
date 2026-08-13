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

// Package plugin is the fulltext2 index plugin registration point.
//
// fulltext2 is the WAND positional fulltext engine (NL phrase + boolean, TF-IDF
// and BM25), a DISTINCT algo from classic fulltext so its plugin hooks stay
// static (bm25-shaped: storage+metadata hidden tables, always-async CDC). It is
// created via `CREATE FULLTEXT2 INDEX name ON t(col) [WITH PARSER <tok>]` (parsed
// to *tree.FullTextIndex with IsV2, dispatched through BuildFullTextIndexDefs)
// and queried via `MATCH(col) AGAINST('query')`. The engine lives in pkg/fulltext2.
package plugin

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"

	ft2compile "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/compile"
	ft2idxcron "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/idxcron"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	ft2runtime "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/runtime"
)

// Plugin is the fulltext2 AlgoPlugin.
type Plugin struct {
	catalogHooks catalogplugin.Hooks
	compileHooks compileplugin.Hooks
	planHooks    planplugin.Hooks
	idxcronHooks idxcronplugin.Hooks
}

func New() *Plugin {
	return &Plugin{
		catalogHooks: ft2runtime.CatalogHooks{},
		compileHooks: ft2compile.Hooks{},
		planHooks:    ft2plan.Hooks{},
		idxcronHooks: ft2idxcron.Hooks{},
	}
}

func (*Plugin) Algo() string                   { return catalog.MoIndexFullText2Algo.ToString() }
func (p *Plugin) Catalog() catalogplugin.Hooks { return p.catalogHooks }
func (p *Plugin) Compile() compileplugin.Hooks { return p.compileHooks }
func (p *Plugin) Plan() planplugin.Hooks       { return p.planHooks }
func (p *Plugin) Idxcron() idxcronplugin.Hooks { return p.idxcronHooks }

var _ indexplugin.AlgoPlugin = (*Plugin)(nil)

// init registers fulltext2. CREATE FULLTEXT2 INDEX now creates the hidden tables;
// build-from-source / MATCH search / CDC / reindex land in the following steps.
func init() { indexplugin.Register(New()) }
