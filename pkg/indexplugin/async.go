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

package plugin

import "github.com/matrixorigin/matrixone/pkg/catalog"

// Async resolution — one canonical entry point: IsAsync.
//
// An index's hidden tables are maintained either SYNCHRONOUSLY (updated in the
// same transaction as the source DML) or ASYNCHRONOUSLY (a background ISCP CDC
// pipeline catches up). Whether an index is async has three inputs, which used
// to be combined ad-hoc at every call site (and were easy to get wrong by
// checking only one):
//
//   - algorithm identity — hnsw / bm25 / cagra / ivfpq are ALWAYS async,
//     declared in their static catalog SyncDescriptor().AlwaysAsync;
//   - engine version — fulltext VERSION=2 (the WAND engine) is always async,
//     a per-index param (catalog.IndexAlgoParamVersionOf);
//   - user opt-in — a normally-synchronous engine (classic fulltext v1,
//     ivfflat) put into async via the `async` param (catalog.IndexParamAsync).
//
// These resolvers live here, not in catalog, because the first input needs the
// plugin registry (catalog sits below it). They read the STATIC SyncDescriptor
// (no algoParams) — the descriptor is a per-algorithm constant, so nothing here
// needs a per-index descriptor and no caller has to synthesize one.
//
// IsAsync is the union of all three and is what callers should reach for;
// AlwaysAsync is exposed for the few generic sites (clone / CDC validity) that
// must distinguish "intrinsically async" from "user opted in".

// AlwaysAsync reports whether the algorithm is intrinsically async (CDC-only)
// regardless of the user's async param: true for an identity-async algorithm
// (hnsw/bm25/cagra/ivfpq, via its static SyncDescriptor) OR a versioned
// always-async engine (fulltext VERSION=2). An unregistered algo with no
// version is never always-async.
func AlwaysAsync(algo, algoParams string) bool {
	if p, ok := Get(algo); ok && p.Catalog().SyncDescriptor().AlwaysAsync {
		return true
	}
	return catalog.IndexAlgoParamVersionOf(algoParams) >= 2
}

// IsAsync is the canonical async check: an index is asynchronously (CDC)
// maintained iff the algorithm is always-async (AlwaysAsync) OR the user opted
// in via the async param. Use this unless you specifically need one source.
func IsAsync(algo, algoParams string) (bool, error) {
	if AlwaysAsync(algo, algoParams) {
		return true, nil
	}
	return catalog.IndexParamAsync(algoParams)
}
