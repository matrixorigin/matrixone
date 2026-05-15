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

// Package plugin defines the integration contract for vector index algorithms.
//
// Every vector index algorithm (HNSW, IVFFLAT, IVF-PQ, CAGRA, …) provides one
// AlgoPlugin that bundles the three per-algorithm callback surfaces (catalog,
// compile, plan). The SQL layer resolves algorithm-specific behaviour
// exclusively through Get(algo); there is no per-algorithm switch statement.
//
// Adding a new algorithm means: implement the three Hooks interfaces, return
// them from a single AlgoPlugin, call Register() in an init(), and blank-
// import the package from plugin/all. If the new plugin compiles, every
// dispatch point is already wired.
package plugin

import (
	"strings"
	"sync"

	catalogplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/compile"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
)

// AlgoPlugin is the integration contract for a vector index algorithm.
// One implementation per algorithm; registered at package init() time.
type AlgoPlugin interface {
	// Algo returns the algorithm token used in `INDEX … USING <algo>`. It
	// must match catalog.MoIndex<X>Algo.ToString() (already lower-cased).
	Algo() string

	Catalog() catalogplugin.Hooks
	Compile() compileplugin.Hooks
	Plan() planplugin.Hooks
}

var (
	registryMu sync.RWMutex
	registry   = map[string]AlgoPlugin{}
)

// Register installs a plugin. Panics on duplicate registration; intended for
// init() bodies.
func Register(p AlgoPlugin) {
	registryMu.Lock()
	defer registryMu.Unlock()
	key := normalize(p.Algo())
	if _, ok := registry[key]; ok {
		panic("vectorindex/plugin: duplicate registration for algo " + key)
	}
	registry[key] = p
}

// Get returns the plugin for an algo string, or (nil, false) if no plugin is
// registered. The match is case-insensitive and trims whitespace.
func Get(algo string) (AlgoPlugin, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	p, ok := registry[normalize(algo)]
	return p, ok
}

// All returns every registered plugin. Useful for catalog enumeration.
func All() []AlgoPlugin {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]AlgoPlugin, 0, len(registry))
	for _, p := range registry {
		out = append(out, p)
	}
	return out
}

// IsVectorIndexAlgo reports whether algo is a registered vector index
// algorithm. Replaces the chain
//
//	catalog.IsIvfIndexAlgo(a) || catalog.IsHnswIndexAlgo(a) ||
//	catalog.IsCagraIndexAlgo(a) || catalog.IsIvfpqIndexAlgo(a)
//
// at every site that needs to gate "is this a multi-table vector index?".
func IsVectorIndexAlgo(algo string) bool {
	_, ok := Get(algo)
	return ok
}

func normalize(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
