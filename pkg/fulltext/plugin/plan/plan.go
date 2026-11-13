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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Compile-time interface check.
var _ planplugin.Hooks = Hooks{}
var _ planplugin.DMLMaintenanceNoOpHook = Hooks{}
var _ planplugin.UpdateColumnRewriteHook = Hooks{}
var _ planplugin.UpdatePrimaryKeyHook = Hooks{}

// Hooks implements plugin/plan.Hooks for fulltext indexes.
type Hooks struct{}

// UpdateColumnRequiresIndexRewrite declares the source identity and parent
// partition dependencies of the posting rows. Indexed text columns are
// handled by the shared index-part dependency check.
func (Hooks) UpdateColumnRequiresIndexRewrite(tableDef *plan.TableDef, _ *plan.IndexDef, colName string) (bool, error) {
	if tableDef == nil {
		return false, nil
	}
	columnName := func(name string) string {
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
			name = name[dot+1:]
		}
		return strings.ToLower(catalog.ResolveAlias(name))
	}
	changed := columnName(colName)
	if changed == "" {
		return false, nil
	}
	if pk := tableDef.Pkey; pk != nil {
		for _, name := range pk.Names {
			if columnName(name) == changed {
				return true, nil
			}
		}
		if pk.PkeyColName != "" && pk.PkeyColName != catalog.CPrimaryKeyColName && columnName(pk.PkeyColName) == changed {
			return true, nil
		}
	}
	if tableDef.Partition == nil {
		return false, nil
	}

	matched := false
	visited := make(map[int32]bool)
	var visit func(*plan.Expr) error
	visit = func(expr *plan.Expr) error {
		return plan.VisitExprTree(expr, func(current *plan.Expr) error {
			if matched {
				return nil
			}
			ref := current.GetCol()
			if ref == nil {
				return nil
			}
			name := columnName(ref.Name)
			if name != "" && name == changed {
				matched = true
				return nil
			}
			pos, ok := tableDef.Name2ColIndex[name]
			if !ok {
				pos = ref.ColPos
			}
			if pos < 0 || int(pos) >= len(tableDef.Cols) || tableDef.Cols[pos] == nil {
				return moerr.NewInternalErrorNoCtxf("invalid fulltext partition dependency %q at position %d", ref.Name, ref.ColPos)
			}
			col := tableDef.Cols[pos]
			if columnName(col.Name) == changed {
				matched = true
				return nil
			}
			if col.GeneratedCol != nil && !visited[pos] {
				visited[pos] = true
				return visit(col.GeneratedCol.Expr)
			}
			return nil
		})
	}
	for _, partition := range tableDef.Partition.PartitionDefs {
		if partition == nil {
			continue
		}
		if err := visit(partition.Def); err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

// The existing inline route can move a fulltext document key only for a
// partitioned table; non-partitioned and RETURNING updates remain rejected.
func (Hooks) SupportsPrimaryKeyUpdate(tableDef *plan.TableDef, returning bool) bool {
	return tableDef != nil && features.IsPartitioned(tableDef.FeatureFlag) && !returning
}

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
	// Physical posting ownership follows the final parent route even when
	// doc_id and tokenization inputs compare equal. The plugin owns this
	// dependency declaration for every SQL-layer no-op consumer.
	if tableDef.Partition != nil {
		for _, partition := range tableDef.Partition.PartitionDefs {
			if partition == nil {
				continue
			}
			valid := true
			err := plan.VisitExprTree(partition.Def, func(expr *plan.Expr) error {
				ref := expr.GetCol()
				if ref == nil {
					return nil
				}
				name := catalog.ResolveAlias(ref.Name)
				if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
					name = name[dot+1:]
				}
				pos, ok := tableDef.Name2ColIndex[name]
				if !ok {
					pos = ref.ColPos
				}
				if pos < 0 || int(pos) >= len(tableDef.Cols) || tableDef.Cols[pos] == nil {
					valid = false
					return nil
				}
				name = tableDef.Cols[pos].Name
				if _, exists := seen[name]; !exists {
					seen[name] = struct{}{}
					columns = append(columns, name)
				}
				return nil
			})
			if err != nil {
				return nil, false, err
			}
			if !valid {
				return nil, false, nil
			}
		}
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
