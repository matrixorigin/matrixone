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

	// SyncDescriptor returns this algorithm's index-sync descriptor,
	// covering both the ISCP CDC pipeline (event-driven) and the
	// idxcron scheduler (time-driven). The zero value (SyncDescriptor{})
	// means "no CDC, no idxcron" — algorithms without either return
	// SyncDescriptor{}.
	//
	// Consumed by pkg/sql/compile/iscp_util.go:
	//   getSinkerTypeFromAlgo, checkValidIndexCdcByIndexdef,
	//   checkValidIndexUpdateByIndexdef, CreateAllIndexUpdateTasks,
	//   DropAllIndexUpdateTasks.
	SyncDescriptor() SyncDescriptor
}

// SinkerType_IndexSync mirrors iscp.ConsumerType_IndexSync (value 0).
// Declared here so plugin packages don't have to import pkg/iscp, which
// transitively pulls in pkg/vectorindex and would create a cycle.
//
// Stays in lock-step with pkg/iscp/types.go's ConsumerType_IndexSync; if
// the iscp value ever changes, update this and add a build-time
// assertion (e.g. via a test that compares the two).
const SinkerType_IndexSync int8 = 0

// SyncDescriptor declares how an algorithm keeps its hidden index
// tables in sync with the source table — through the ISCP CDC pipeline
// (event-driven) and/or the idxcron scheduler (time-driven). Returned
// by Hooks.SyncDescriptor().
//
// CDC and idxcron are distinct sync mechanisms but conceptually one
// "how does this algo stay in sync?" bundle, so they share a descriptor.
//
// Field-by-field defaults (the zero value):
//
//	UsesCDC=false        — algorithm has no CDC pipeline. Other CDC
//	                       fields are ignored.
//	SinkerType=0         — meaningful only when UsesCDC=true. Use
//	                       SinkerType_IndexSync for the common case.
//	AlwaysAsync=false    — async-ness derives from the index's `async`
//	                       param in IndexAlgoParams. Set to true for
//	                       algorithms that are always async (e.g. HNSW).
//	IdxcronAction=""     — algorithm has no scheduled-rebuild task.
//	                       Non-empty values are passed to
//	                       idxcron.RegisterUpdate / UnregisterUpdate as
//	                       the action key.
//
// The runtime metadata blob for idxcron is built separately by
// compile.Hooks.IdxcronMetadata (it needs a CompileContext for
// session-variable lookups, which can't live in a value descriptor).
type SyncDescriptor struct {
	UsesCDC       bool
	SinkerType    int8
	AlwaysAsync   bool
	IdxcronAction string
}
