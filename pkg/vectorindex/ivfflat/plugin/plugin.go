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

// Package plugin is the IVF-FLAT vector index plugin registration point.
//
// # Phase 4a (current)
//
// Skeleton landed. Catalog hooks (HiddenTableTypes, ParamsFromTree,
// DefaultOptions, SupportedOpTypes, ExperimentalFlag, SyncDescriptor)
// are fully implemented in runtime/. Compile and plan hooks are STUBS
// (see compile/compile.go and plan/plan.go). The plugin is intentionally
// NOT registered in pkg/vectorindex/plugin/all/all.go yet — the
// stub hooks would break IVF-FLAT DDL/query if dispatch routed through
// them. The remaining inline IVFFLAT case arms in pkg/sql/compile and
// pkg/sql/plan continue to handle IVF-FLAT until the lifts complete.
//
// # Phases 4b–4g (remaining)
//
//   - 4b: collapse the inline `else if catalog.IsIvfIndexAlgo(...)`
//     fallbacks in pkg/sql/compile/iscp_util.go and other dispatch
//     sites — they're dead once the plugin is registered. Defer
//     until 4c lands so registration is safe.
//   - 4c: lift compile DDL (handleVectorIvfFlatIndex + 5 helpers).
//   - 4d: lift buildIvfFlatSecondaryIndexDef.
//   - 4e: lift apply_indices_ivfflat.go (auto/pre/post mode, two-scan).
//   - 4f: lift DML sync (appendPreInsertSkVectorPlan + DELETE arms).
//   - 4g: lift IVF-FLAT case of indexParamsToMap + ivfflat.go
//     table-function builders. Add init() registration once 4c–4f
//     are complete (uncomment the `func init()` block below).
package plugin

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/plugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/compile"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"

	ivfflatcompile "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/compile"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	ivfflatruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/runtime"
)

// Plugin is the IVF-FLAT AlgoPlugin. Mirrors the IVF-PQ structure.
type Plugin struct {
	catalogHooks catalogplugin.Hooks
	compileHooks compileplugin.Hooks
	planHooks    planplugin.Hooks
}

func New() *Plugin {
	return &Plugin{
		catalogHooks: ivfflatruntime.CatalogHooks{},
		compileHooks: ivfflatcompile.Hooks{},
		planHooks:    ivfflatplan.Hooks{},
	}
}

func (*Plugin) Algo() string                   { return catalog.MoIndexIvfFlatAlgo.ToString() }
func (p *Plugin) Catalog() catalogplugin.Hooks { return p.catalogHooks }
func (p *Plugin) Compile() compileplugin.Hooks { return p.compileHooks }
func (p *Plugin) Plan() planplugin.Hooks       { return p.planHooks }

// Compile-time check that *Plugin satisfies the AlgoPlugin interface.
// If a new method is added to AlgoPlugin and this plugin hasn't been
// updated, this line stops the build.
var _ plugin.AlgoPlugin = (*Plugin)(nil)

// init registers IVF-FLAT with the global plugin registry. Compile
// hooks are fully lifted (Phase 4c); plan hooks are still stubs (Phases
// 4d–4f to come). Until those land, pkg/sql/plan retains the inline
// IVFFLAT arms for plan-rewrite + DML sync, so the plan stubs never get
// invoked.
func init() { plugin.Register(New()) }
