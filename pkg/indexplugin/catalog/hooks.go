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

	// ExperimentalFlag returns the experimental-feature flag name that
	// must be enabled (set to true via SET / system var) for this
	// algorithm to be usable. Returns "" for non-experimental
	// algorithms.
	//
	// Consumed by pkg/sql/compile/util.go:checkTableWithValidIndexes
	// during DDL paths that re-validate an existing table's indexes,
	// and by each plugin's compile.HandleCreateIndex at CREATE INDEX
	// time. HNSW returns "experimental_hnsw_index", CAGRA returns
	// "experimental_cagra_index", IVF-PQ returns
	// "experimental_ivfpq_index".
	ExperimentalFlag() string

	// AlterTableCloneBehavior returns the per-hidden-table clone semantics
	// this algorithm wants applied during ALTER TABLE COPY's
	// cloneUnaffectedIndex pass. Scope is intentionally narrow — this
	// hook governs the unaffected-index clone in alter only, not any
	// other clone or copy path. Most algorithms return the zero value
	// (no DELETE before clone, no skip on async) — only IVF-FLAT is
	// non-trivial today, see pkg/vectorindex/ivfflat/plugin/runtime
	// for the rationale.
	//
	// Consumed by pkg/sql/compile/alter.go::cloneUnaffectedIndex.
	AlterTableCloneBehavior() AlterTableCloneBehavior

	// ShouldTruncateHiddenTable reports whether the hidden table of the
	// given IndexAlgoTableType (one of HiddenTableTypes()) should be
	// included in a TRUNCATE TABLE on the source table.
	//
	// Most algorithms return true unconditionally — the index is
	// derived from source rows and must be reset alongside it.
	// IVF-FLAT returns true only for the entries table; metadata +
	// centroids preserve the k-means model so a subsequent ALTER
	// REINDEX is cheap.
	//
	// Consumed by pkg/sql/plan/build_ddl.go on TRUNCATE TABLE plan
	// build. Hot path — keep the implementation allocation-free.
	ShouldTruncateHiddenTable(algoTableType string) bool

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

	// IdxcronFrontendProbeVar is a system-variable name used by the
	// SQL layer to distinguish a frontend (user-session) invocation
	// from a background idxcron re-entry when re-registering the
	// scheduled task. The variable must exist in the frontend's
	// system-variable table AND be absent from this plugin's
	// IdxcronMetadata blob, so that:
	//
	//   - frontend session:   ResolveVariable(probe) succeeds
	//   - idxcron background: ResolveVariable(probe) fails (key not
	//                         in Metadata JSON)
	//
	// Empty string disables the gate (the caller always proceeds).
	// Only meaningful when IdxcronAction != "".
	IdxcronFrontendProbeVar string
}

// AlterTableCloneBehavior declares the per-hidden-table semantics
// ALTER TABLE COPY's cloneUnaffectedIndex pass must honor for an
// algorithm. Both lists name IndexAlgoTableType strings (members of
// HiddenTableTypes()).
//
// Scope: this type is consulted only by
// pkg/sql/compile/alter.go::cloneUnaffectedIndex — the loop that
// copies an index's hidden tables from the source table onto a
// schema-modified temp copy. It is not a general "clone an index"
// API.
//
// The zero value means "no special behaviour" — the unaffected-index
// loop clones each hidden table verbatim from source to the new copy.
// Today only IVF-FLAT populates both fields; HNSW / CAGRA / IVF-PQ /
// fulltext leave their hidden tables empty at CREATE-INDEX time, so
// nothing needs deletion before clone, and their async-skip story is
// "skip the whole index" (handled by SyncDescriptor.UsesCDC +
// .AlwaysAsync at the top of cloneUnaffectedIndex), not per table.
//
// Field-by-field:
//
//	DeleteBeforeClone — hidden tables that were already seeded by the
//	  CREATE-INDEX side effects of the temp table's DDL (e.g. for
//	  IVF-FLAT: a "version=0" metadata row, an initial centroid, the
//	  bootstrapped entries). The clone target must be DELETE'd first
//	  or the source rows duplicate the seed.
//	SkipWhenAsync — hidden tables the algorithm rebuilds from ts=0
//	  via its CDC pipeline on the new table once the index is
//	  re-registered. Cloning these AND letting CDC rebuild produces
//	  duplicates. Only consulted when the index's async param is set.
type AlterTableCloneBehavior struct {
	DeleteBeforeClone []string
	SkipWhenAsync     []string
}

// ContainsDelete reports whether algoTableType is in the
// DeleteBeforeClone list. Linear scan — list is always <= len(HiddenTableTypes()).
func (b AlterTableCloneBehavior) ContainsDelete(algoTableType string) bool {
	for _, t := range b.DeleteBeforeClone {
		if t == algoTableType {
			return true
		}
	}
	return false
}

// ContainsSkipWhenAsync reports whether algoTableType is in the
// SkipWhenAsync list.
func (b AlterTableCloneBehavior) ContainsSkipWhenAsync(algoTableType string) bool {
	for _, t := range b.SkipWhenAsync {
		if t == algoTableType {
			return true
		}
	}
	return false
}
