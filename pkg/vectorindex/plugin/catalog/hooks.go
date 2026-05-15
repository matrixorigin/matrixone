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

// Package catalog defines the catalog-layer hooks every vector index plugin
// must implement: parameter parsing, hidden-table layout, and op-type set.
//
// These replace the per-algorithm cases of
// catalog.indexParamsToMap (pkg/catalog/secondary_index_utils.go) and the
// IsXxxIndexAlgo predicate fan-out.
package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Hooks bundles every catalog-layer callback for one algorithm.
type Hooks interface {
	// HiddenTableTypes lists the IndexAlgoTableType strings this algorithm
	// uses for its hidden tables, e.g. {"metadata","storage"} for IVF-PQ or
	// {"metadata","centroids","entries"} for IVF-FLAT. Order is irrelevant;
	// callers index by name.
	HiddenTableTypes() []string

	// ParamsFromTree extracts and validates the WITH(...) options from a
	// CREATE INDEX statement, returning the canonical params map that gets
	// JSON-encoded into mo_indexes. Replaces one switch arm of
	// catalog.indexParamsToMap.
	ParamsFromTree(idx *tree.Index) (map[string]string, error)

	// DefaultOptions is the map produced when no WITH(...) clause is given.
	// May be nil if the algorithm requires explicit options.
	DefaultOptions() map[string]string

	// SupportedOpTypes maps the SQL-visible op_type strings (e.g.
	// "vector_l2_ops") to the internal metric identifier. Used by
	// plan-side op_type validation.
	SupportedOpTypes() map[string]string
}
