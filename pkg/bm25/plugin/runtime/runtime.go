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

// Package runtime holds the bm25 index plugin's catalog-side metadata:
// hidden-table types, parameter schema, sync descriptor.
//
// bm25 is a position-free BM25 ranked-retrieval index over a TEXT/VARCHAR
// column. Structurally it follows the vector plugins (created via
// `CREATE INDEX ... USING bm25`, parsed to *tree.Index, dispatched through
// BuildSecondaryIndexDefs) — NOT the fulltext plugin. Its two hidden tables
// (storage + metadata) hold the chunked WAND binary index; there is no
// postings table (the index builds directly from source rows).
package runtime

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// actionBm25Reindex is the idxcron action key for bm25's scheduled
// merge-compaction. Inlined here (rather than importing
// pkg/vectorindex/idxcron) to avoid an import cycle, mirroring the
// ivfflat plugin's actionIvfflatReindex. Stays in lock-step with the
// bm25 arm of pkg/vectorindex/idxcron/executor.go.
const actionBm25Reindex = "bm25_reindex"

// DefaultParser is the tokenizer used when WITH PARSER is omitted. The WAND
// engine's word-id layer is jieba-backed, so gojieba is the default (and the
// only parser wired end-to-end until the word-id layer is generalized).
const DefaultParser = "gojieba"

// supportedParsers is the set of tokenizers a bm25 index accepts. gojieba
// works today; ngram/default are accepted at DDL time but only become
// functional once the engine word-id layer is generalized (Phase 5).
var supportedParsers = map[string]struct{}{
	"gojieba": {},
	"ngram":   {},
	"default": {},
}

// Compile-time interface check.
var _ catalogplugin.Hooks = CatalogHooks{}

// CatalogHooks implements plugin/catalog.Hooks for bm25.
type CatalogHooks struct{}

// HiddenTableTypes — bm25 uses two hidden tables: the chunked binary index
// store and its metadata. No postings table (the index is built from source).
func (CatalogHooks) HiddenTableTypes() []string {
	return []string{
		catalog.Bm25Index_TblType_Storage,
		catalog.Bm25Index_TblType_Metadata,
	}
}

// ShouldTruncateHiddenTable — both hidden tables are derived from the source
// rows and must be reset alongside a TRUNCATE of the source table.
func (CatalogHooks) ShouldTruncateHiddenTable(string) bool { return true }

// AlterTableCloneBehavior — bm25 is async CDC-maintained and rebuilds its
// whole binary index from the source rows on the new table (via the re-armed
// CDC's InitSQL), so the unaffected-index clone skips the whole index rather
// than block-copying a base that would then be doubled by the rebuild.
func (CatalogHooks) AlterTableCloneBehavior() catalogplugin.AlterTableCloneBehavior {
	return catalogplugin.AlterTableCloneBehavior{SkipWholeIndex: true}
}

// RestoreBehavior — the restore rebuilds the binary index from the restored
// rows via RestoreInitSQL (ALTER … REINDEX … FORCE_SYNC), so the seeded
// storage+metadata must be emptied before the block-clone appends, else the
// rebuild doubles the tag=0 base.
func (h CatalogHooks) RestoreBehavior() catalogplugin.RestoreBehavior {
	return catalogplugin.RestoreBehavior{DeleteBeforeClone: h.HiddenTableTypes()}
}

// BuildSessionVars — bm25 captures no session vars; max_index_capacity is an
// explicit WITH option persisted in algo_params by ParamsFromTree.
func (CatalogHooks) BuildSessionVars() []string { return nil }

// DefaultOptions — no WITH(...) clause defaults the tokenizer to gojieba.
func (CatalogHooks) DefaultOptions() map[string]string {
	return map[string]string{"parser": DefaultParser}
}

// ExperimentalFlag — bm25 is not feature-gated.
func (CatalogHooks) ExperimentalFlag() string { return "" }

// SupportedOpTypes — bm25 is text ranking, not a vector metric; no op_types.
func (CatalogHooks) SupportedOpTypes() map[string]string { return nil }

// SupportedVectorTypes — bm25 has NO vector column (like fulltext). nil is the
// "no vector column" sentinel, so plan-side vector-type validation is skipped.
func (CatalogHooks) SupportedVectorTypes() []types.T { return nil }

// SupportedPrimaryKeyTypes — no PK-type constraint (any PK). nil = "no constraint".
func (CatalogHooks) SupportedPrimaryKeyTypes() []types.T { return nil }

// SupportedIncludeColumnTypes — bm25 does not support INCLUDE columns.
func (CatalogHooks) SupportedIncludeColumnTypes() []types.T { return nil }

// ValidQuantization — bm25 has no quantization; reject any non-empty value.
func (CatalogHooks) ValidQuantization(quant, _ string) error {
	if quant != "" {
		return moerr.NewNotSupportedNoCtxf("bm25 index does not support quantization")
	}
	return nil
}

// SyncDescriptor — bm25 is always CDC-maintained (AlwaysAsync) and runs a
// scheduled idxcron merge-compaction (action bm25_reindex, token BM25). It is
// not lists-aware (no k-means / nlist concept).
func (CatalogHooks) SyncDescriptor() catalogplugin.SyncDescriptor {
	return catalogplugin.SyncDescriptor{
		UsesCDC:          true,
		SinkerType:       catalogplugin.SinkerType_IndexSync,
		AlwaysAsync:      true,
		IdxcronAction:    actionBm25Reindex,
		IdxcronAlgoToken: "BM25",
		IdxcronListsAware: false,
	}
}

// ParamsFromTree extracts the WITH(...) options from CREATE INDEX ... USING bm25
// into the canonical algo_params map. bm25's knobs are the tokenizer (parser),
// the async/idxcron cadence flags, and max_index_capacity.
func (CatalogHooks) ParamsFromTree(idx *tree.Index) (map[string]string, error) {
	res := make(map[string]string)

	parser := strings.ToLower(idx.IndexOption.ParserName)
	if parser == "" {
		parser = DefaultParser
	}
	if _, ok := supportedParsers[parser]; !ok {
		return nil, moerr.NewNotSupportedNoCtxf(
			"bm25 parser %q (supported: gojieba, ngram, default)", parser)
	}
	res["parser"] = parser

	if idx.IndexOption.Async {
		res[catalog.Async] = "true"
	}
	if idx.IndexOption.AutoUpdate {
		res[catalog.AutoUpdate] = "true"
	}
	if idx.IndexOption.Day > 0 {
		res[catalog.Day] = strconv.FormatInt(idx.IndexOption.Day, 10)
	}
	if idx.IndexOption.Hour > 0 {
		res[catalog.Hour] = strconv.FormatInt(idx.IndexOption.Hour, 10)
	}
	if idx.IndexOption.MaxIndexCapacity > 0 {
		res[catalog.IndexAlgoParamMaxIndexCapacity] = strconv.FormatInt(idx.IndexOption.MaxIndexCapacity, 10)
	}
	return res, nil
}
