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
	"context"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/coverage"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
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

	// Idxcron returns the cron-side hooks used by
	// pkg/vectorindex/idxcron/executor.go to decide whether a
	// scheduled rebuild should fire for a given (table, index).
	// Algorithms with no minimum-size constraint (HNSW, fulltext)
	// return a trivial Hooks impl whose Updatable always says yes;
	// IVF-FLAT / CAGRA / IVF-PQ implementations consult the storage
	// table to enforce their respective minimums.
	Idxcron() idxcronplugin.Hooks
}

// SearchPlugin is an optional capability implemented by algorithms that have
// migrated query execution from a table function to INDEX_SEARCH_SCAN.  The
// separate interface lets algorithms migrate independently without a SQL-layer
// name switch or no-op hooks on unrelated fulltext/GPU plugins.
type SearchPlugin interface {
	AlgoPlugin
	Search() searchplugin.Hooks
}

// CoveragePlugin is an optional capability implemented by algorithms that can
// report whether their durable state covers a read snapshot. Only an algorithm
// that can answer honestly implements it; the rest are treated as never covered,
// so no plugin carries a no-op hook.
type CoveragePlugin interface {
	AlgoPlugin
	Coverage() coverage.Hooks
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
		panic("indexplugin: duplicate registration for algo " + key)
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

// CoversSnapshot asks algo whether its index is current enough to be used as a
// MANDATORY filter at req.Snapshot, and returns the build_ts of the generation a probe would
// search (0 = unknown) for the planner's partial-plan gap decision.
//
// It FAILS CLOSED at every step: an unregistered algo, one that does not
// implement CoveragePlugin, or a hook that errors all report false. A caller may
// therefore treat the result as "safe to filter with" without inspecting the
// error, which is reported only for logging.
func CoversSnapshot(ctx context.Context, algo string, req coverage.Request) (bool, types.TS, error) {
	p, ok := Get(algo)
	if !ok {
		return false, types.TS{}, nil
	}
	cp, ok := p.(CoveragePlugin)
	if !ok {
		return false, types.TS{}, nil
	}
	covered, buildTS, err := cp.Coverage().CoversSnapshot(ctx, req)
	if err != nil {
		return false, types.TS{}, err
	}
	return covered, buildTS, nil
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

// IsVectorIndexAlgo reports whether algo is a registered vector
// index algorithm (HNSW, CAGRA, IVF-PQ, IVF-FLAT) — i.e. plugin-
// registered AND of the vector KIND. Replaces the chain
//
//	catalog.IsIvfIndexAlgo(a) || catalog.IsHnswIndexAlgo(a) ||
//	catalog.IsCagraIndexAlgo(a) || catalog.IsIvfpqIndexAlgo(a)
//
// at every site that needs to gate "is this a multi-table vector
// index?". Use IsFullTextIndexAlgo for fulltext and IsPluginAlgo for
// "registered with the plugin system, vector OR fulltext".
//
// Kind is classified by the plugin's static CAPABILITY (Catalog().IsVectorIndex()),
// not by an algo-name check: vector plugins (HNSW / IVF-FLAT / IVF-PQ / CAGRA)
// declare true, the fulltext-family plugins (classic fulltext AND fulltext2)
// declare false. This keeps fulltext2 correctly NON-vector without a second
// fulltext algo-name exception here; a future fulltext-style engine is classified
// right for free. (Previously this special-cased only classic fulltext by name, so
// a registered fulltext2 wrongly reported as a vector index and had to be excluded
// again ad hoc at call sites, e.g. compile/ddl.go.)
func IsVectorIndexAlgo(algo string) bool {
	p, ok := Get(algo)
	if !ok {
		return false
	}
	return p.Catalog().IsVectorIndex()
}

// SupportsMembership reports whether algo's search applies a membership key set
// inside the index search (searchplugin.MembershipHooks). An unregistered algo,
// one without a SearchPlugin, or one without the hook reports false.
func SupportsMembership(algo string) bool {
	p, ok := Get(algo)
	if !ok {
		return false
	}
	sp, ok := p.(SearchPlugin)
	if !ok {
		return false
	}
	h, ok := sp.Search().(searchplugin.MembershipHooks)
	return ok && h.SupportsMembership()
}

// SupportsIndexFilter reports whether algo's search evaluates filters on INCLUDE
// columns and the primary key inside the index search (searchplugin.IndexFilterHooks).
// An unregistered algo, one without a SearchPlugin, or one without the hook reports false.
func SupportsIndexFilter(algo string) bool {
	p, ok := Get(algo)
	if !ok {
		return false
	}
	sp, ok := p.(SearchPlugin)
	if !ok {
		return false
	}
	h, ok := sp.Search().(searchplugin.IndexFilterHooks)
	return ok && h.SupportsIndexFilter()
}

// SupportsProbeKnobs reports whether algo's search probes inverted lists
// (searchplugin.ProbeKnobsHooks), so nprobe and max_nprobe apply to it. An
// unregistered algo, one without a SearchPlugin, or one without the hook reports false.
func SupportsProbeKnobs(algo string) bool {
	p, ok := Get(algo)
	if !ok {
		return false
	}
	sp, ok := p.(SearchPlugin)
	if !ok {
		return false
	}
	h, ok := sp.Search().(searchplugin.ProbeKnobsHooks)
	return ok && h.SupportsProbeKnobs()
}

// MaxInternalTopKLimit returns the largest internal top-k algo's search accepts
// (searchplugin.InternalTopKLimitHooks), 0 for no limit. An unregistered algo, one
// without a SearchPlugin, or one without the hook reports 0.
func MaxInternalTopKLimit(algo string) uint64 {
	p, ok := Get(algo)
	if !ok {
		return 0
	}
	sp, ok := p.(SearchPlugin)
	if !ok {
		return 0
	}
	h, ok := sp.Search().(searchplugin.InternalTopKLimitHooks)
	if !ok {
		return 0
	}
	return h.MaxInternalTopKLimit()
}

// CheckInternalTopK rejects an itopk or max_itopk above MaxInternalTopKLimit(algo).
func CheckInternalTopK(ctx context.Context, algo string, itopk, maxITopK uint64) error {
	limit := MaxInternalTopKLimit(algo)
	if limit == 0 {
		return nil
	}
	if itopk > limit {
		return moerr.NewInvalidInputf(ctx, "itopk %d exceeds the %s limit %d", itopk, algo, limit)
	}
	if maxITopK > limit {
		return moerr.NewInvalidInputf(ctx, "max_itopk %d exceeds the %s limit %d", maxITopK, algo, limit)
	}
	return nil
}

// IsFullTextIndexAlgo reports whether algo is the fulltext index
// algorithm AND the fulltext plugin is registered.
func IsFullTextIndexAlgo(algo string) bool {
	if normalize(algo) != catalog.MOIndexFullTextAlgo.ToString() {
		return false
	}
	_, ok := Get(algo)
	return ok
}

// IsPluginAlgo reports whether algo is registered with the plugin
// system, regardless of kind (vector or fulltext). Use this at
// dispatch sites that route through the plugin's HandleCreateIndex /
// Plan() hooks; use IsVectorIndexAlgo / IsFullTextIndexAlgo when the
// kind matters.
func IsPluginAlgo(algo string) bool {
	_, ok := Get(algo)
	return ok
}

func normalize(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
