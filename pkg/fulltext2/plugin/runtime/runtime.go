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

// Package runtime holds the fulltext2 index's catalog-side metadata. fulltext2
// is a DISTINCT algo (CREATE FULLTEXT2 INDEX, the WAND positional engine), so —
// unlike a version-router — every hook here is a clean STATIC answer: the
// storage+metadata hidden-table layout (bm25-shaped), always-async CDC
// maintenance, and index_id-keyed clone/restore.
package runtime

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var _ catalogplugin.Hooks = CatalogHooks{}

// CatalogHooks implements plugin/catalog.Hooks for fulltext2.
type CatalogHooks struct{}

// HiddenTableTypes — a chunked segment store + a metadata table (no postings
// table; segments are built from source and CDC-maintained).
func (CatalogHooks) HiddenTableTypes() []string {
	return []string{
		catalog.FullText2Index_TblType_Storage,
		catalog.FullText2Index_TblType_Metadata,
	}
}

// ShouldTruncateHiddenTable — both hidden tables are derived from source rows and
// reset on TRUNCATE.
func (CatalogHooks) ShouldTruncateHiddenTable(_ string) bool { return true }

// AlterTableCloneBehavior — both hidden tables are empty at CREATE-INDEX time
// (segments land via the build/CDC), so the whole index is skipped when cloning
// (rebuilt from source + CDC on the copy).
func (CatalogHooks) AlterTableCloneBehavior() catalogplugin.AlterTableCloneBehavior {
	return catalogplugin.AlterTableCloneBehavior{SkipWholeIndex: true}
}

// RestoreBehavior — the storage+metadata rows are index_id-keyed, so a clone
// overwrites rather than appends: no delete-before-clone. The compile hook's
// RestoreInitSQL registers CDC from the post-clone TS.
func (CatalogHooks) RestoreBehavior() catalogplugin.RestoreBehavior {
	return catalogplugin.RestoreBehavior{}
}

// BuildSessionVars — fulltext2 captures no session vars into algo_params.
func (CatalogHooks) BuildSessionVars() []string { return nil }

// DefaultOptions — no statement-level option JSON required.
func (CatalogHooks) DefaultOptions() map[string]string { return nil }

// ExperimentalFlag — not gated (yet).
func (CatalogHooks) ExperimentalFlag() string { return "" }

// SupportedVectorTypes / SupportedOpTypes / SupportedIncludeColumnTypes —
// fulltext2 has no vector/op-type/include concept.
func (CatalogHooks) SupportedVectorTypes() []types.T        { return nil }
func (CatalogHooks) SupportedOpTypes() map[string]string    { return nil }
func (CatalogHooks) SupportedIncludeColumnTypes() []types.T { return nil }

// SupportedPrimaryKeyTypes — nil = no PK-type constraint (the segment pk codec
// accepts int/varchar/uuid/temporal/decimal; validated at build).
func (CatalogHooks) SupportedPrimaryKeyTypes() []types.T { return nil }

// ValidQuantization — no quantization.
func (CatalogHooks) ValidQuantization(_, _ string) error { return nil }

// ParamsFromTree — fulltext2 parses to *tree.FullTextIndex (handled by the plan
// hook's BuildFullTextIndexDefs), not *tree.Index, so this is never reached.
func (CatalogHooks) ParamsFromTree(_ *tree.Index) (map[string]string, error) {
	return nil, moerr.NewNotSupportedNoCtx("fulltext2 index parses to *tree.FullTextIndex, not *tree.Index")
}

// SyncDescriptor — fulltext2 is ALWAYS async (CDC-maintained), a clean static
// answer (no version branch). UsesCDC drives the ISCP consumer (RunFulltext2)
// registered in pkg/fulltext2/plugin/iscp. The idxcron MERGE fields (like bm25's)
// land with the compaction step.
func (CatalogHooks) SyncDescriptor() catalogplugin.SyncDescriptor {
	return catalogplugin.SyncDescriptor{
		UsesCDC:     true,
		SinkerType:  catalogplugin.SinkerType_IndexSync,
		AlwaysAsync: true,
	}
}
