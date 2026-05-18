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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/catalog"
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

// DefaultOptions — fulltext defaults are inferred at build time; no
// statement-level option JSON is required when the WITH(...) clause is
// omitted. Matches the legacy catalog.IndexParamsToJsonString path
// returning "" for an empty option map.
func (CatalogHooks) DefaultOptions() map[string]string { return nil }

// ExperimentalFlag — `experimental_fulltext_index` exists at
// pkg/frontend/variables.go but is not enforced anywhere today.
// Returning "" preserves that behavior.
func (CatalogHooks) ExperimentalFlag() string { return "" }

// SupportedOpTypes — fulltext has no metric/op-type concept.
func (CatalogHooks) SupportedOpTypes() map[string]string { return nil }

// ParamsFromTree — fulltext parses to *tree.FullTextIndex, not
// *tree.Index, so this hook is never reached for fulltext in
// practice. The fulltext-specific parser lives at
// pkg/catalog/secondary_index_utils.go::fullTextIndexParamsToMap
// and is invoked through indexParamsToMap's *tree.FullTextIndex
// type-assertion arm.
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
