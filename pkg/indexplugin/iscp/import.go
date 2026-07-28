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

// Package iscp is the central wiring point for per-algorithm ISCP
// hooks. Each blank import below transitively runs the plugin's iscp
// sub-package init(), which calls iscp.Register(...) to install its
// Hooks into the pkg/iscp registry. Look this up by algo string at
// runtime from NewIndexConsumer / NewIndexSqlWriter.
//
// Mirrors pkg/indexplugin/all/all.go's role for the AlgoPlugin
// (catalog / compile / plan) side. Kept in a separate sub-package so
// the wiring sites don't share an import graph — pkg/iscp transitively
// pulls in pkg/sql/plan (via pkg/cdc), and pkg/sql/plan blank-imports
// pkg/indexplugin/all; adding iscp.Register calls under
// pkg/indexplugin/all would close that loop into a cycle. This package
// is reachable only from pkg/sql/compile/plugin_context.go (downstream
// of both pkg/iscp and pkg/sql/plan), so the one-way edge stays clean.
//
// # Adding a new ISCP-participating algorithm
//
// Add a blank import below for the algorithm's plugin/iscp/ package.
// That package's own init() does the iscp.Register call. GPU-only
// algorithms (CAGRA, IVF-PQ) go under a `//go:build gpu` import file
// alongside their AlgoPlugin registration in
// pkg/indexplugin/all/all_gpu.go.
package iscp

import (
	_ "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/iscp"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin/iscp"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/iscp"
)
