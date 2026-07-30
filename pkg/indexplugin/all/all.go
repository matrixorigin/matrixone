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

// Package all is the central registration list for every vector-index
// plugin. Each blank import below transitively runs the plugin's init(),
// which calls plugin.Register(...) to install it into the global registry
// (pkg/indexplugin/plugin.go). The SQL layer's dispatch sites then
// look the plugin up by algo string at runtime.
//
// # Who imports this package
//
//	pkg/sql/plan/plugin_context.go     — every plan-mode build
//	pkg/sql/compile/plugin_context.go  — every compile-mode build
//
// Both blank-import this package, so production binaries (cmd/mo-service)
// and every test that touches plan / compile pick up the full plugin set
// automatically. A package that needs the registry without dragging in
// pkg/sql/plan or pkg/sql/compile can blank-import pkg/indexplugin/all
// directly.
//
// # Adding a new vector-index algorithm
//
// See pkg/vectorindex/ivfpq/plugin/plugin.go for the canonical "how to add
// a new algorithm" walkthrough. The summary:
//
//  1. Add the algo token to pkg/catalog (MoIndex<Foo>Algo) and the parser
//     keyword (tree.INDEX_TYPE_<FOO>) if the algorithm introduces a new
//     CREATE INDEX syntax.
//
//  2. Copy pkg/vectorindex/ivfpq/plugin/ to pkg/vectorindex/<foo>/plugin/
//     and implement the three Hooks interfaces (catalog / compile / plan).
//
//  3. If the algorithm supports ANN ORDER BY rewrites, add the body methods
//     (*QueryBuilder).applyIndicesForSortUsing<Foo> and prepare<Foo>IndexContext
//     in pkg/sql/plan/apply_indices_<foo>.go, plus a case in the dispatch
//     switch at pkg/sql/plan/apply_indices.go.
//
//  4. Add one line below — a blank import of the new plugin package. That
//     is the only edit needed to make production binaries and tests
//     register the algorithm:
//
//     _ "github.com/matrixorigin/matrixone/pkg/vectorindex/<foo>/plugin"
//
//     For GPU-only algorithms (CAGRA, IVF-PQ), add the blank import to
//     all_gpu.go instead — it carries //go:build gpu so CPU binaries
//     skip the registration. Plan-build then surfaces "unsupported
//     index type: <foo>" via pkg/sql/plan/build_ddl.go's existing
//     indexplugin.Get dispatch.
//
//  5. Add a SQL case under test/distributed/cases/vector/ that exercises
//     CREATE INDEX, ORDER BY <distfn>(col, v) LIMIT k, ALTER REINDEX, and
//     DROP INDEX.
package all

import (
	_ "github.com/matrixorigin/matrixone/pkg/fulltext/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin"
)
