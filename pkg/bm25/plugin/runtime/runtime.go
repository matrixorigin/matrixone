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

// AlterTableCloneBehavior — Phase 2 (sync-only): the clone copies the hidden
// tables as-is (zero value). Phase 4 flips this to SkipWholeIndex once bm25 is
// CDC-maintained and rebuilds from source on the new table.
func (CatalogHooks) AlterTableCloneBehavior() catalogplugin.AlterTableCloneBehavior {
	return catalogplugin.AlterTableCloneBehavior{}
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

// SyncDescriptor — Phase 2 (sync-only): NO CDC, NO idxcron. The index builds
// synchronously from source on CREATE / ALTER REINDEX; post-create DML does not
// yet flow in. Phase 4 flips this to the CDC-maintained descriptor (UsesCDC +
// AlwaysAsync + idxcron bm25_reindex / token BM25) once the CDC consumer and
// idxcron merge-compaction are wired.
func (CatalogHooks) SyncDescriptor() catalogplugin.SyncDescriptor {
	return catalogplugin.SyncDescriptor{}
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
