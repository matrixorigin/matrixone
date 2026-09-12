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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ALTER-column plan hooks for fulltext2 INCLUDE columns. Without these the
// rename path (pkg/sql/plan/vector_index_alter.go) skips fulltext2, leaving the
// include-column metadata (both IndexDef.IncludedColumns and the IndexAlgoParams
// JSON) pointing at the old column name — SHOW CREATE TABLE stays stale and the
// ISCP CDC pipeline can no longer resolve the include column, so incremental
// indexing silently stops (issue #27964). Mirrors ivfflat's alter hooks.
var _ planplugin.AlterColumnHooks = Hooks{}
var _ planplugin.RenameColumnRebuildHook = Hooks{}
var _ planplugin.UpdateColumnRewriteHook = Hooks{}

// HandleAlterDropColumn reports whether dropping colName touches a fulltext2
// INCLUDE column (so the caller rejects/rebuilds rather than orphaning it).
func (Hooks) HandleAlterDropColumn(_ *planpb.TableDef, indexDef *planpb.IndexDef, colName string) (bool, error) {
	return planplugin.IncludedColumnAffected(indexDef, colName)
}

// HandleAlterRenameColumn rewrites the new name into the fulltext2 include
// metadata (IndexDef.IncludedColumns + the IndexAlgoParams JSON) so post-rename
// metadata is consistent and the rebuilt index/CDC pipeline resolves the column.
func (Hooks) HandleAlterRenameColumn(tableDef *planpb.TableDef, oldColName, newColName string) ([]string, error) {
	return planplugin.RenameIncludedColumnsForAlgo(tableDef, catalog.MoIndexFullText2Algo.ToString(), oldColName, newColName)
}

// RenameColumnRequiresIndexRebuild triggers a reindex when the renamed column is
// a fulltext2 INCLUDE column: the rebuild re-registers the index and restarts
// incremental (ISCP CDC) indexing from a consistent metadata state.
func (Hooks) RenameColumnRequiresIndexRebuild(_ *planpb.TableDef, indexDef *planpb.IndexDef, oldColName string) (bool, error) {
	return planplugin.IncludedColumnAffected(indexDef, oldColName)
}

// UpdateColumnRequiresIndexRewrite triggers a rewrite when an ALTER ... MODIFY
// touches a fulltext2 INCLUDE column (its stored type/value may change).
func (Hooks) UpdateColumnRequiresIndexRewrite(_ *planpb.TableDef, indexDef *planpb.IndexDef, colName string) (bool, error) {
	return planplugin.IncludedColumnAffected(indexDef, colName)
}
