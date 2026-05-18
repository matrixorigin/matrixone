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
// # Phase 1 (current)
//
// Skeleton landed. Catalog hooks (HiddenTableTypes, SyncDescriptor,
// ShouldTruncateHiddenTable, …) are fully implemented in runtime/.
// Compile and plan hooks are STUBS that return errors.
//
// The plugin IS registered with the global registry — but the inline
// fulltext arms in pkg/sql/compile/ddl.go::CreateTable (line ~788)
// and pkg/sql/plan/build_ddl.go::buildFullTextIndexTable still handle
// the actual DDL. The fulltext-specific if-arms take precedence over
// vectorplugin.IsVectorIndexAlgo, so the stubs here never run.
//
// # Phase 2 — Compile lift
//
// Lift pkg/sql/compile/ddl_index_algo.go::handleFullTextIndexTable and
// pkg/sql/compile/util.go::genInsertIndexTableSqlForFullTextIndex into
// compile/compile.go. Route fulltext through the multiTableIndexes
// loop alongside vector indexes.
//
// # Phase 3 — Plan lift
//
// Lift pkg/sql/plan/build_ddl.go::buildFullTextIndexTable into
// plan/plan.go's BuildFullTextIndexDefs body. Switch the three
// inline call sites in build_ddl.go to type-switch and dispatch via
// the plugin's plan.Hooks. Collapse the remaining
// catalog.IsFullTextIndexAlgo arms in non-DML dispatch chains.
//
// # Phase 4 — DML lift (deferred)
//
// Out of scope. buildPreInsertFullTextIndex etc. in
// pkg/sql/plan/build_dml_util.go stay inline until a DML plugin
// surface (pre/post insert/delete) is added.
package plugin

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/plugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/compile"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"

	fulltextcompile "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/compile"
	fulltextplan "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/plan"
	fulltextruntime "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/runtime"
)

// Plugin is the fulltext AlgoPlugin.
type Plugin struct {
	catalogHooks catalogplugin.Hooks
	compileHooks compileplugin.Hooks
	planHooks    planplugin.Hooks
}

func New() *Plugin {
	return &Plugin{
		catalogHooks: fulltextruntime.CatalogHooks{},
		compileHooks: fulltextcompile.Hooks{},
		planHooks:    fulltextplan.Hooks{},
	}
}

func (*Plugin) Algo() string                   { return catalog.MOIndexFullTextAlgo.ToString() }
func (p *Plugin) Catalog() catalogplugin.Hooks { return p.catalogHooks }
func (p *Plugin) Compile() compileplugin.Hooks { return p.compileHooks }
func (p *Plugin) Plan() planplugin.Hooks       { return p.planHooks }

// Compile-time check that *Plugin satisfies the AlgoPlugin interface.
var _ plugin.AlgoPlugin = (*Plugin)(nil)

// init registers fulltext with the global plugin registry. Compile +
// plan hooks are Phase 1 stubs — the inline arms in pkg/sql/compile
// and pkg/sql/plan continue to drive fulltext DDL until Phases 2 and
// 3 lift them.
func init() { plugin.Register(New()) }
