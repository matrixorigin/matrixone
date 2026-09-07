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

// Package plan implements the fulltext plugin's plan-layer hooks.
//
// Fulltext uses a parallel hook BuildFullTextIndexDefs (taking
// *tree.FullTextIndex) instead of BuildSecondaryIndexDefs (which
// takes *tree.Index, the vector-index parse-tree shape). The vector
// plugins stub BuildFullTextIndexDefs; the fulltext plugin stubs
// BuildSecondaryIndexDefs; the SQL-layer dispatch picks the right
// hook by parse-tree type.
package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Compile-time interface check.
var _ planplugin.Hooks = Hooks{}
var _ planplugin.DMLMaintenanceNoOpHook = Hooks{}

// Hooks implements plugin/plan.Hooks for fulltext indexes.
type Hooks struct{}

// DMLMaintenanceNoOpColumns declares when an old fulltext posting set is
// provably identical to the final one. FULLTEXT input includes the row
// identity/doc_id as well as the indexed values. VARCHAR and TEXT preserve the
// bytes that determine tokenization. CHAR, JSON, and DATALINK stay conservative
// because SQL NULL-safe equality is not a proof of identical tokenizer input for
// them.
func (Hooks) DMLMaintenanceNoOpColumns(
	tableDef *plan.TableDef,
	indexDef *plan.IndexDef,
) ([]string, bool, error) {
	if tableDef == nil || tableDef.Pkey == nil || indexDef == nil || len(indexDef.Parts) == 0 {
		return nil, false, nil
	}
	pkName := catalog.ResolveAlias(tableDef.Pkey.PkeyColName)
	if pkName == "" {
		return nil, false, nil
	}
	pkPos, ok := tableDef.Name2ColIndex[pkName]
	if !ok || pkPos < 0 || int(pkPos) >= len(tableDef.Cols) || tableDef.Cols[pkPos] == nil {
		return nil, false, nil
	}

	columns := make([]string, 0, len(indexDef.Parts)+1)
	seen := make(map[string]struct{}, len(indexDef.Parts)+1)
	seen[pkName] = struct{}{}
	columns = append(columns, pkName)
	for _, part := range indexDef.Parts {
		name := catalog.ResolveAlias(part)
		pos, ok := tableDef.Name2ColIndex[name]
		if !ok || pos < 0 || int(pos) >= len(tableDef.Cols) || tableDef.Cols[pos] == nil {
			return nil, false, nil
		}
		switch types.T(tableDef.Cols[pos].Typ.Id) {
		case types.T_varchar, types.T_text:
		default:
			return nil, false, nil
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		columns = append(columns, name)
	}
	return columns, len(columns) > 0, nil
}

// BuildSecondaryIndexDefs is unreachable for fulltext — the
// plan-build dispatch routes *tree.Index parse trees to the vector
// plugins. Fulltext receives its parse tree via BuildFullTextIndexDefs.
func (Hooks) BuildSecondaryIndexDefs(
	_ planplugin.CompilerContext,
	_ *tree.Index,
	_ map[string]*plan.ColDef,
	_ []*plan.IndexDef,
	_ string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {
	return nil, nil, moerr.NewNotSupportedNoCtx("fulltext plugin uses BuildFullTextIndexDefs (not BuildSecondaryIndexDefs)")
}

// CanApply — fulltext has no ANN-style ORDER BY rewrite; report
// inapplicable.
func (Hooks) CanApply(_ planplugin.PlanBuilder, _ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef) (bool, error) {
	return false, nil
}

// ApplyForSort — same, never applies; passthrough.
func (Hooks) ApplyForSort(_ planplugin.PlanBuilder, _ *planplugin.VectorSortContext, _ *planplugin.MultiTableIndexRef, nodeID int32, _ planplugin.ApplyForSortOpts) (int32, bool, error) {
	return nodeID, false, nil
}

// ValidateViewDefinition refuses to persist a view whose MATCH() AGAINST() no FULLTEXT
// index can serve.
//
// Unlike a vector index, fulltext has no brute-force fallback, so such a view is not slow,
// it is unrunnable: every query against it fails with the error above. Before this check
// the DDL committed anyway, and ALTER / CREATE OR REPLACE reported success while replacing
// a working definition with a broken one (#27027).
//
// The check is on the optimized plan, so the rewrite has already had its chance and a
// surviving placeholder is proof no index matched. That reuses the planner's own decision
// rather than restating findMatchFullTextIndex's rules (exact column-set match,
// storage-vs-metadata index defs, literal-only mode argument), which would drift from it.
// Covers classic fulltext and fulltext2 alike: both bind MATCH to the same placeholders
// and share the same matcher.
func (Hooks) ValidateViewDefinition(ctx planplugin.CompilerContext, query *plan.Query) error {
	return planplugin.RefuseUnservableMatch(ctx, query)
}
