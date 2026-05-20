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

// Package plugin is the HNSW vector index integration. See
// pkg/vectorindex/ivfpq/plugin (the canonical template) for the full
// "how to add a vector index" walkthrough.
package plugin

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"

	hnswcompile "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/compile"
	hnswidxcron "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/idxcron"
	hnswplan "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/plan"
	hnswruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/runtime"
)

type Plugin struct {
	catalogHooks catalogplugin.Hooks
	compileHooks compileplugin.Hooks
	planHooks    planplugin.Hooks
	idxcronHooks idxcronplugin.Hooks
}

func New() *Plugin {
	return &Plugin{
		catalogHooks: hnswruntime.CatalogHooks{},
		compileHooks: hnswcompile.Hooks{},
		planHooks:    hnswplan.Hooks{},
		idxcronHooks: hnswidxcron.Hooks{},
	}
}

func (*Plugin) Algo() string                   { return catalog.MoIndexHnswAlgo.ToString() }
func (p *Plugin) Catalog() catalogplugin.Hooks { return p.catalogHooks }
func (p *Plugin) Compile() compileplugin.Hooks { return p.compileHooks }
func (p *Plugin) Plan() planplugin.Hooks       { return p.planHooks }
func (p *Plugin) Idxcron() idxcronplugin.Hooks { return p.idxcronHooks }

var _ plugin.AlgoPlugin = (*Plugin)(nil)

func init() { plugin.Register(New()) }
