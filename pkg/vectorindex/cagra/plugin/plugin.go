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

// Package plugin is the CAGRA vector index integration. See
// pkg/vectorindex/ivfpq/plugin (the canonical template) for the full
// "how to add a vector index" walkthrough.
package plugin

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"

	cagracompile "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/compile"
	cagraplan "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/plan"
	cagraruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/runtime"
)

type Plugin struct {
	catalogHooks catalogplugin.Hooks
	compileHooks compileplugin.Hooks
	planHooks    planplugin.Hooks
}

func New() *Plugin {
	return &Plugin{
		catalogHooks: cagraruntime.CatalogHooks{},
		compileHooks: cagracompile.Hooks{},
		planHooks:    cagraplan.Hooks{},
	}
}

func (*Plugin) Algo() string                   { return catalog.MoIndexCagraAlgo.ToString() }
func (p *Plugin) Catalog() catalogplugin.Hooks { return p.catalogHooks }
func (p *Plugin) Compile() compileplugin.Hooks { return p.compileHooks }
func (p *Plugin) Plan() planplugin.Hooks       { return p.planHooks }

var _ plugin.AlgoPlugin = (*Plugin)(nil)

func init() { plugin.Register(New()) }
