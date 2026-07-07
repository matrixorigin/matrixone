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

// Package runtime holds the fulltext index's catalog-side metadata.
// Fulltext fits the catalog hook contract cleanly even though it parses
// to *tree.FullTextIndex (handled separately by BuildFullTextIndexDefs):
// it has a single hidden table, no op-types, and async CDC support.
package runtime

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Compile-time interface check.
var _ catalogplugin.Hooks = CatalogHooks{}

// CatalogHooks implements plugin/catalog.Hooks for fulltext indexes.
type CatalogHooks struct{}

// HiddenTableTypes — fulltext uses a single hidden table holding
// (doc_id, pos, word) rows clustered by word.
func (CatalogHooks) HiddenTableTypes() []string {
	return []string{catalog.FullTextIndex_TblType}
}

// ShouldTruncateHiddenTable — fulltext has no preserved-across-truncate
// state; the single hidden table is rebuilt from source rows.
func (CatalogHooks) ShouldTruncateHiddenTable(_ string) bool { return true }

// AlterTableCloneBehavior — fulltext's single hidden table is empty
// at CREATE-INDEX time (rows land via the populate step or CDC), so
// no DELETE before clone is needed. Async fulltext is skipped at the
// whole-index level via SyncDescriptor, not per table.
func (CatalogHooks) AlterTableCloneBehavior() catalogplugin.AlterTableCloneBehavior {
	return catalogplugin.AlterTableCloneBehavior{SkipWholeIndex: true}
}

// RestoreBehavior — empty, like the other empty-at-create algos (HNSW/CAGRA/IVF-PQ).
// A fulltext index's hidden tables are empty at CreateTable during a clone (the source
// rows arrive with the block-level clone, not the inline populate), so there is no seed
// to DELETE before the clone — DeleteBeforeClone is an IVF-FLAT need (it seeds
// centroids/entries non-empty), not fulltext's. Reconstruction happens in the compile
// hook's RestoreInitSQL: a retrieval index rebuilds its tag=0 base (ALTER … REINDEX …
// FULLTEXT FORCE_SYNC — its sync build would otherwise seed a base the block-clone
// doubles), while a postings/ngram index keeps the no-op "SELECT 1" (the clone copies
// its one hidden table verbatim; CDC catch-up handles increments). (A DeleteBeforeClone
// list would also be dead here: the postings def's IndexAlgoTableType is "", which never
// matches the table-type-keyed list.)
func (CatalogHooks) RestoreBehavior() catalogplugin.RestoreBehavior {
	return catalogplugin.RestoreBehavior{}
}

// BuildSessionVars captures fulltext_max_index_capacity into algo_params at
// CREATE INDEX. The retrieval (WAND) index is always-async: its build runs in an
// internal ISCP proc whose live resolver is nil, so the session var can only
// reach the sinker if it is snapshotted here (the ISCP overlay reads it back from
// algo_params.session_vars). Harmless for a postings/ngram index (which ignores
// capacity). Older indexes without the blob fall back to the sinker default.
func (CatalogHooks) BuildSessionVars() []string {
	return []string{"fulltext_max_index_capacity"}
}

// DefaultOptions — fulltext defaults are inferred at build time; no
// statement-level option JSON is required when the WITH(...) clause is
// omitted. Matches the legacy catalog.IndexParamsToJsonString path
// returning "" for an empty option map.
func (CatalogHooks) DefaultOptions() map[string]string { return nil }

// ExperimentalFlag — `experimental_fulltext_index` exists at
// pkg/frontend/variables.go but is not enforced anywhere today.
// Returning "" preserves that behavior.
func (CatalogHooks) ExperimentalFlag() string { return "" }

// SupportedVectorTypes: fulltext has no vector column.
func (CatalogHooks) SupportedVectorTypes() []types.T { return nil }

// SupportedPrimaryKeyTypes: fulltext imposes no PK-type constraint.
func (CatalogHooks) SupportedPrimaryKeyTypes() []types.T { return nil }

// ValidQuantization — full-text indexes have no quantization, so nothing to gate.
func (CatalogHooks) ValidQuantization(_, _ string) error { return nil }

// SupportedOpTypes — fulltext has no metric/op-type concept.
// SupportedIncludeColumnTypes: this index has no INCLUDE-column support.
func (CatalogHooks) SupportedIncludeColumnTypes() []types.T { return nil }

func (CatalogHooks) SupportedOpTypes() map[string]string { return nil }

// ParamsFromTree — fulltext parses to *tree.FullTextIndex, not
// *tree.Index, so this hook is never reached for fulltext in
// practice. The fulltext-specific parser lives at
// pkg/fulltext/plugin/plan/schema.go::buildFullTextParams and is
// invoked from BuildFullTextIndexDefs.
func (CatalogHooks) ParamsFromTree(_ *tree.Index) (map[string]string, error) {
	return nil, moerr.NewNotSupportedNoCtx("fulltext index parses to *tree.FullTextIndex, not *tree.Index")
}

// SyncDescriptor — fulltext participates in ISCP CDC when the index
// is async (per the Async param in IndexAlgoParams). No idxcron action
// today, matching the legacy inline behaviour.
func (CatalogHooks) SyncDescriptor() catalogplugin.SyncDescriptor {
	return catalogplugin.SyncDescriptor{
		UsesCDC:    true,
		SinkerType: catalogplugin.SinkerType_IndexSync,
	}
}

// AlwaysAsync — fulltext is always async ONLY for the `retrieval` parser
// (a serialized binary WAND index that can't be row-patched inline, so it
// is maintained via CDC + idxcron). ngram/default/empty parsers keep their
// per-index `async`-param-gated behaviour (this returns false for them).
func (CatalogHooks) AlwaysAsync(indexAlgoParams string) bool {
	return catalog.GetIndexParser(indexAlgoParams) == "retrieval"
}
