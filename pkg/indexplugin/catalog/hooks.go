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
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

	// SupportedVectorTypes lists the indexed (vector) column types this
	// algorithm accepts, e.g. {types.T_array_float32} for CAGRA / IVF-PQ
	// (cuvs is f32-only) or {types.T_array_float32, types.T_array_float64}
	// for HNSW / IVF-FLAT. Consumed by plan-side CREATE INDEX column-type
	// validation (each plugin's BuildSecondaryIndexDefs) via
	// catalog.SupportsVectorType.
	//
	// SPECIAL CASE: nil/empty means "this index has no vector column" (NOT
	// "all types") — only fulltext returns nil. So SupportsVectorType reports
	// false for an empty list. Every real vector index enumerates its
	// concrete element types.
	SupportedVectorTypes() []types.T

	// SupportedPrimaryKeyTypes lists the source-table primary-key column
	// types this algorithm supports, e.g. {types.T_int64} for CAGRA / IVF-PQ
	// / HNSW. Consumed by plan-side PK validation via
	// catalog.SupportsPrimaryKeyType.
	//
	// SPECIAL CASE: nil/empty means "no constraint — any PK type is accepted"
	// (the opposite convention from SupportedVectorTypes). IVF-FLAT and
	// fulltext return nil. So SupportsPrimaryKeyType reports true for an empty
	// list.
	SupportedPrimaryKeyTypes() []types.T

	// SupportedIncludeColumnTypes lists the scalar column types accepted as
	// INCLUDE (pre-filter) columns, e.g. {types.T_int32, types.T_int64,
	// types.T_float32, types.T_float64} for CAGRA / IVF-PQ. Consumed by
	// plan-side INCLUDE validation (validateIncludeColumns) via
	// catalog.SupportsIncludeColumnType.
	//
	// SPECIAL CASE: nil/empty means "this index does not support INCLUDE
	// columns" (like SupportedVectorTypes — NOT "all types"). HNSW, IVF-FLAT
	// and fulltext return nil, so SupportsIncludeColumnType reports false.
	SupportedIncludeColumnTypes() []types.T

	// ValidQuantization reports whether QUANTIZATION='quant' is usable by this
	// algorithm under op_type 'op', returning a descriptive error when not
	// (nil = valid). It is the single per-algorithm rule for the
	// (quantization, op_type) pair, so CREATE (plan-side schema validation) and
	// REINDEX (compile-side ValidateReindexParams) gate it identically instead
	// of duplicating the check. An empty quant means "no quantization / default
	// storage" (valid); an empty op means "no metric in play" (only the
	// storage-type rule applies). Example: the cuvs (CAGRA / IVF-PQ) backend
	// rejects int8/uint8 with inner-product / cosine because its affine scalar
	// quantizer only preserves L2 geometry.
	ValidQuantization(quant, op string) error

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

	// RestoreBehavior declares how snapshot/restore should reconstruct this
	// algorithm's hidden tables, the restore-path analogue of
	// AlterTableCloneBehavior. The zero value means "rebuild every hidden
	// table via the algorithm's normal mechanism from the restored
	// main-table rows" — the historical behavior (the snapshot's prebuilt
	// model is discarded and rebuilt: async CDC for async indexes, a
	// synchronous k-means re-run for sync IVF-FLAT). The hook exists so a
	// plugin can opt specific hidden tables into a direct restore of the
	// prebuilt model. Every plugin returns the zero value today.
	RestoreBehavior() RestoreBehavior

	// BuildSessionVars returns the names of the session variables this
	// algorithm's index build depends on (e.g. "kmeans_train_percent",
	// "kmeans_max_iteration"). At CREATE INDEX these are read from the session
	// resolver and captured — typed — into algo_params' reserved session_vars
	// object, so every later background build (restore reindex, idxcron, async
	// create) resolves them from the index def instead of the background
	// defaults (DefaultResolveVariable). Empty/nil = none.
	BuildSessionVars() []string

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

// SupportsVectorType reports whether the given column type is an accepted
// indexed (vector) column type for the algorithm described by h. It is the
// single source of truth other code should consult instead of hardcoding
// per-algorithm VECF32/VECF64 checks.
//
// SPECIAL CASE: an empty SupportedVectorTypes() means "no vector column" (e.g.
// fulltext), so this returns false — NOT "all types". This is deliberately the
// OPPOSITE of SupportsPrimaryKeyType's empty-list convention: no real vector
// index wants "any vector type", whereas several indexes accept any PK type.
func SupportsVectorType(h Hooks, t types.T) bool {
	for _, s := range h.SupportedVectorTypes() {
		if s == t {
			return true
		}
	}
	return false
}

// SupportsPrimaryKeyType reports whether t is an accepted primary-key column
// type for h.
//
// SPECIAL CASE: an empty SupportedPrimaryKeyTypes() means "no constraint — any
// PK type is accepted" (IVF-FLAT, fulltext), so this returns true. This is the
// OPPOSITE of SupportsVectorType's empty-list convention (see there).
func SupportsPrimaryKeyType(h Hooks, t types.T) bool {
	pks := h.SupportedPrimaryKeyTypes()
	if len(pks) == 0 {
		return true
	}
	for _, s := range pks {
		if s == t {
			return true
		}
	}
	return false
}

// SupportsIncludeColumnType reports whether t is an accepted INCLUDE
// (pre-filter) column type for h.
//
// SPECIAL CASE: an empty SupportedIncludeColumnTypes() means "INCLUDE columns
// not supported" (like SupportsVectorType), so this returns false.
func SupportsIncludeColumnType(h Hooks, t types.T) bool {
	for _, s := range h.SupportedIncludeColumnTypes() {
		if s == t {
			return true
		}
	}
	return false
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

	// IdxcronAlgoToken is the algorithm keyword the idxcron executor
	// uses when constructing the cron-triggered ALTER REINDEX SQL —
	// e.g. "IVFFLAT", "CAGRA", "IVFPQ". Empty when IdxcronAction == ""
	// (this algorithm has no cron rebuild).
	//
	// Consumed by pkg/vectorindex/idxcron/executor.go's plugin-driven
	// dispatch in place of the previously hardcoded "IVFFLAT" literal.
	IdxcronAlgoToken string

	// IdxcronListsAware enables the IVF-FLAT-specific lists/nsample
	// heuristic inside the idxcron executor's checkIndexUpdatable:
	//
	//   - skip the rebuild when the source table has fewer rows than nlist
	//   - shrink kmeans_train_percent when dataset > 256 * nlist
	//
	// false for cuvs algorithms (CAGRA, IVF-PQ) which have no "lists"
	// or training-sample concept — they always rebuild on cadence.
	IdxcronListsAware bool
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
// IVF-FLAT populates the per-hidden-table fields; HNSW / CAGRA / IVF-PQ /
// fulltext leave their hidden tables empty at CREATE-INDEX time, so nothing
// needs deletion before clone — they set SkipWholeIndex instead, and the
// whole index is skipped when async rather than handled per table.
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

	// SkipWholeIndex is the explicit "skip the entire index" clone policy.
	// When true and the index is async, cloneUnaffectedIndex skips the whole
	// index — none of its hidden tables are cloned — because the algorithm
	// leaves every hidden table empty at CREATE-INDEX time and rebuilds all of
	// them via CDC from ts=0 on the new table (HNSW / CAGRA / IVF-PQ / fulltext).
	// IVF-FLAT leaves this false: its metadata + centroids must be cloned (the
	// CDC pipeline only rebuilds entries), so it relies on the per-hidden-table
	// DeleteBeforeClone / SkipWhenAsync fields above.
	//
	// This is intentionally NOT inferred from SyncDescriptor.UsesCDC: a CDC
	// algorithm can still need its model tables cloned (IVF-FLAT), so the
	// whole-index skip must be declared explicitly per algorithm.
	SkipWholeIndex bool
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

// RestoreBehavior declares how snapshot/restore should reconstruct an index's
// hidden tables — the restore-path analogue of AlterTableCloneBehavior. The
// restore replays `create table … clone`, whose table_clone operator copies
// index hidden tables block-level — an APPEND, not an overwrite. So any hidden
// table that CreateTable seeds non-empty (e.g. IVF-FLAT's metadata/centroids/
// entries) must be emptied first, or the clone lays the source data on top of
// the seed and duplicates it. The zero value (no tables to delete) is correct
// for algorithms whose storage is keyed and overwrites on append (cuVS keys by
// index_id).
//
// Consulted on the restore path — the `create table … clone` the restore
// replays; see compileplugin.Context.IsTableClone().
type RestoreBehavior struct {
	// DeleteBeforeClone names the hidden tables (IndexAlgoTableType, members of
	// HiddenTableTypes()) that CreateTable seeds non-empty and so must be
	// emptied with `DELETE … WHERE TRUE` (a content delete that keeps the table
	// and its id — not truncate) before the block-level clone appends the
	// source's data. Mirrors AlterTableCloneBehavior.DeleteBeforeClone. Empty =
	// nothing to delete (current behavior).
	DeleteBeforeClone []string
}

// ContainsDeleteBeforeClone reports whether algoTableType is in the
// DeleteBeforeClone list.
func (b RestoreBehavior) ContainsDeleteBeforeClone(algoTableType string) bool {
	for _, t := range b.DeleteBeforeClone {
		if t == algoTableType {
			return true
		}
	}
	return false
}
