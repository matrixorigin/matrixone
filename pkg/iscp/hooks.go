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

package iscp

import (
	"context"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Hooks is the per-algorithm ISCP hook layer. One implementation per
// index type (HNSW, IVF-FLAT, fulltext, CAGRA, IVF-PQ). Hooks are
// registered from pkg/indexplugin/all via Register; pkg/iscp looks them
// up by algo string from NewIndexConsumer / NewIndexSqlWriter.
//
// Implementations live alongside the algorithm's other plugin hooks at
// pkg/<algopkg>/plugin/iscp/ (e.g. pkg/vectorindex/hnsw/plugin/iscp/),
// not in pkg/iscp — keeps algorithm-specific code (and its build-tag
// constraints, where applicable) out of the ISCP framework.
type Hooks interface {
	// NewSqlWriter constructs the per-(table,index) writer that turns
	// CDC row events into either a SQL statement (fulltext / ivfflat)
	// or a JSON CDC blob (hnsw / cagra / ivfpq). The returned writer
	// satisfies IndexSqlWriter; the caller takes ownership.
	NewSqlWriter(jobID JobID, info *ConsumerInfo,
		tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (IndexSqlWriter, error)

	// Run drives one consumer iteration. SQL-based algorithms delegate
	// to the shared RunIndex helper; HNSW / CAGRA / IVF-PQ run their
	// algorithm-specific Update+Save loop driven by the writer's CDC
	// JSON output. The consumer goroutine calls this once per iteration
	// and the implementation is responsible for draining sqlBufSendCh
	// until close, then updating watermarks (for tail data) and
	// reporting errors via errch.
	Run(c *IndexConsumer, ctx context.Context, errch chan error, r DataRetriever)
}

var (
	hooksMu sync.RWMutex
	hooks   = map[string]Hooks{}
)

// Register installs an ISCP hook for an algorithm. Algo names are
// case-insensitive (lower-cased on store). Intended to be called from
// pkg/indexplugin/all init() bodies; panics on duplicate registration.
func Register(algo string, h Hooks) {
	hooksMu.Lock()
	defer hooksMu.Unlock()
	key := normalizeAlgo(algo)
	if _, ok := hooks[key]; ok {
		panic("iscp: duplicate Hooks registration for algo " + key)
	}
	hooks[key] = h
}

// GetHooks returns the Hooks for an algorithm, or (nil, false) if no
// hook is registered.
func GetHooks(algo string) (Hooks, bool) {
	hooksMu.RLock()
	defer hooksMu.RUnlock()
	h, ok := hooks[normalizeAlgo(algo)]
	return h, ok
}

// HasHooks reports whether an algorithm has a registered ISCP hook.
func HasHooks(algo string) bool {
	_, ok := GetHooks(algo)
	return ok
}

func normalizeAlgo(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
