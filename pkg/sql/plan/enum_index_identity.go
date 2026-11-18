// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// enumExpressionHasIdentity also checks an ENUM-typed intermediate in a
// generated expression. A generated key need not itself have ENUM type.
func enumExpressionHasIdentity(root *planpb.Expr) bool {
	seen := make(map[*planpb.Expr]bool)
	var visit func(*planpb.Expr) bool
	visit = func(expr *planpb.Expr) bool {
		if expr == nil || seen[expr] {
			return false
		}
		seen[expr] = true
		if expr.Typ.Id == int32(types.T_enum) && expr.Typ.EnumCollation != 0 {
			return true
		}
		switch e := expr.Expr.(type) {
		case *planpb.Expr_F:
			if e.F != nil {
				for _, arg := range e.F.Args {
					if visit(arg) {
						return true
					}
				}
			}
		case *planpb.Expr_List:
			if e.List != nil {
				for _, arg := range e.List.List {
					if visit(arg) {
						return true
					}
				}
			}
		case *planpb.Expr_Lit:
			return e.Lit != nil && visit(e.Lit.Src)
		case *planpb.Expr_Sub:
			return e.Sub != nil && visit(e.Sub.Child)
		case *planpb.Expr_W:
			if e.W == nil {
				return false
			}
			if visit(e.W.WindowFunc) {
				return true
			}
			for _, arg := range e.W.PartitionBy {
				if visit(arg) {
					return true
				}
			}
			for _, order := range e.W.OrderBy {
				if order != nil && visit(order.Expr) {
					return true
				}
			}
			if frame := e.W.Frame; frame != nil {
				return frame.Start != nil && visit(frame.Start.Val) || frame.End != nil && visit(frame.End.Val)
			}
		}
		return false
	}
	return visit(root)
}

// EnumIndexUsesNewIdentity reports whether this index consumes a column whose
// ENUM collation identity was introduced by #32814. Legacy-only indexes must
// retain their branch's existing representation. INCLUDE and generated-column
// dependencies matter as well as ordinary key and primary-key components.
func EnumIndexUsesNewIdentity(table *planpb.TableDef, index *planpb.IndexDef) (bool, error) {
	invalid := func() error { return moerr.NewInvalidInputNoCtx("cannot determine ENUM index identity dependencies") }
	if table == nil || index == nil {
		return false, invalid()
	}
	hasNew := false
	for _, col := range table.Cols {
		if col == nil {
			return false, invalid()
		}
		if col.Typ.Id == int32(types.T_enum) && col.Typ.EnumCollation != 0 {
			if err := types.ValidateEnumCollation(col.Typ.EnumCollation); err != nil {
				return false, err
			}
			hasNew = true
		}
		if col.GeneratedCol != nil && enumExpressionHasIdentity(col.GeneratedCol.Expr) {
			hasNew = true
		}
	}
	if !hasNew {
		return false, nil
	}

	names := append([]string(nil), index.Parts...)
	if table.Pkey != nil {
		names = append(names, table.Pkey.PkeyColName)
		names = append(names, table.Pkey.Names...)
	}
	include := index.IncludedColumns
	if len(include) == 0 && index.IndexAlgoParams != "" {
		var params map[string]json.RawMessage
		if err := json.Unmarshal([]byte(index.IndexAlgoParams), &params); err != nil {
			return false, err
		}
		value, ok := params[catalog.IncludedColumns]
		if !ok {
			value, ok = params["include_columns"]
		}
		if ok {
			var raw string
			if err := json.Unmarshal(value, &raw); err != nil {
				return false, err
			}
			var err error
			include, err = catalog.ParseIncludeColumnsValue(raw)
			if err != nil {
				return false, err
			}
		}
	}
	names = append(names, include...)
	lookup := func(name string) (int, error) {
		name = catalog.ResolveAlias(name)
		found := -1
		for i, col := range table.Cols {
			if strings.EqualFold(col.Name, name) {
				if found >= 0 {
					return -1, invalid()
				}
				found = i
			}
		}
		if found < 0 {
			return -1, invalid()
		}
		return found, nil
	}
	state := make(map[int]uint8)
	result := make(map[int]bool)
	var visitCol func(int) (bool, error)
	var visitExpr func(*planpb.Expr) (bool, error)
	visitCol = func(pos int) (bool, error) {
		if pos < 0 || pos >= len(table.Cols) || state[pos] == 1 {
			return false, invalid()
		}
		col := table.Cols[pos]
		if state[pos] == 2 {
			return result[pos], nil
		}
		state[pos] = 1
		dependent := col.Typ.Id == int32(types.T_enum) && col.Typ.EnumCollation != 0
		if col.GeneratedCol != nil {
			fromExpr, err := visitExpr(col.GeneratedCol.Expr)
			if err != nil {
				return false, err
			}
			dependent = dependent || fromExpr
		}
		state[pos] = 2
		result[pos] = dependent
		return dependent, nil
	}
	visitExpr = func(expr *planpb.Expr) (bool, error) {
		if expr == nil {
			return false, invalid()
		}
		dependent := expr.Typ.Id == int32(types.T_enum) && expr.Typ.EnumCollation != 0
		if dependent {
			if err := types.ValidateEnumCollation(expr.Typ.EnumCollation); err != nil {
				return false, err
			}
		}
		switch e := expr.Expr.(type) {
		case *planpb.Expr_Col:
			if e.Col == nil || e.Col.RelPos != 0 {
				return false, invalid()
			}
			child, err := visitCol(int(e.Col.ColPos))
			return dependent || child, err
		case *planpb.Expr_F:
			if e.F == nil {
				return false, invalid()
			}
			for _, arg := range e.F.Args {
				child, err := visitExpr(arg)
				if err != nil {
					return false, err
				}
				dependent = dependent || child
			}
		case *planpb.Expr_List:
			if e.List == nil {
				return false, invalid()
			}
			for _, arg := range e.List.List {
				child, err := visitExpr(arg)
				if err != nil {
					return false, err
				}
				dependent = dependent || child
			}
		case *planpb.Expr_Lit:
			if e.Lit == nil {
				return false, invalid()
			}
			if e.Lit.Src != nil {
				child, err := visitExpr(e.Lit.Src)
				if err != nil {
					return false, err
				}
				dependent = dependent || child
			}
		case *planpb.Expr_T:
		default:
			return false, invalid()
		}
		return dependent, nil
	}
	dependent := false
	for _, name := range names {
		if name == "" {
			continue
		}
		pos, err := lookup(name)
		if err != nil {
			return false, err
		}
		partDependent, err := visitCol(pos)
		if err != nil {
			return false, err
		}
		dependent = dependent || partDependent
	}
	return dependent, nil
}

// indexReferencesColumn includes transitive generated-column sources, INCLUDE
// payload, and the primary-key payload carried by every secondary index.
func indexReferencesColumn(table *planpb.TableDef, index *planpb.IndexDef, column string) (bool, error) {
	if table == nil || index == nil {
		return false, moerr.NewInvalidInputNoCtx("cannot determine index column dependencies")
	}
	want := strings.ToLower(catalog.ResolveAlias(column))
	positions := make(map[int]bool)
	for i, col := range table.Cols {
		if col != nil && strings.EqualFold(catalog.ResolveAlias(col.Name), want) {
			positions[i] = true
		}
	}
	if len(positions) == 0 {
		// A newly added column cannot be referenced by an index on the old
		// table, so it has no existing index dependency to rebuild.
		return false, nil
	}
	names := append([]string(nil), index.Parts...)
	if table.Pkey != nil {
		names = append(names, table.Pkey.PkeyColName)
		names = append(names, table.Pkey.Names...)
	}
	include := index.IncludedColumns
	if len(index.IndexAlgoParams) > 0 {
		var params map[string]json.RawMessage
		if err := json.Unmarshal([]byte(index.IndexAlgoParams), &params); err != nil {
			return false, err
		}
		for _, key := range []string{catalog.IncludedColumns, "include_columns"} {
			if value, ok := params[key]; ok {
				var raw string
				if err := json.Unmarshal(value, &raw); err != nil {
					return false, err
				}
				parsed, err := catalog.ParseIncludeColumnsValue(raw)
				if err != nil {
					return false, err
				}
				include = append(append([]string(nil), include...), parsed...)
			}
		}
	}
	names = append(names, include...)
	lookup := func(name string) (int, error) {
		name = catalog.ResolveAlias(name)
		found := -1
		for i, col := range table.Cols {
			if col != nil && strings.EqualFold(col.Name, name) {
				if found >= 0 {
					return -1, moerr.NewInvalidInputNoCtx("ambiguous ALTER index dependency")
				}
				found = i
			}
		}
		if found < 0 {
			return -1, moerr.NewInvalidInputNoCtx("missing ALTER index dependency")
		}
		return found, nil
	}
	state := make(map[int]uint8)
	var visitCol func(int) (bool, error)
	var visitExpr func(*planpb.Expr) (bool, error)
	visitCol = func(pos int) (bool, error) {
		if pos < 0 || pos >= len(table.Cols) || state[pos] == 1 {
			return false, moerr.NewInvalidInputNoCtx("invalid generated index dependency")
		}
		if positions[pos] {
			return true, nil
		}
		col := table.Cols[pos]
		if col == nil {
			return false, moerr.NewInvalidInputNoCtx("invalid index dependency column")
		}
		if state[pos] == 2 {
			return false, nil
		}
		state[pos] = 1
		if col.GeneratedCol != nil {
			found, err := visitExpr(col.GeneratedCol.Expr)
			if err != nil {
				return false, err
			}
			state[pos] = 2
			return found, nil
		}
		state[pos] = 2
		return false, nil
	}
	visitExpr = func(expr *planpb.Expr) (bool, error) {
		if expr == nil {
			return false, moerr.NewInvalidInputNoCtx("missing generated index dependency expression")
		}
		switch e := expr.Expr.(type) {
		case *planpb.Expr_Col:
			if e.Col == nil || e.Col.RelPos != 0 {
				return false, moerr.NewInvalidInputNoCtx("invalid generated index dependency reference")
			}
			return visitCol(int(e.Col.ColPos))
		case *planpb.Expr_F:
			if e.F == nil {
				return false, moerr.NewInvalidInputNoCtx("invalid generated index dependency function")
			}
			for _, arg := range e.F.Args {
				found, err := visitExpr(arg)
				if err != nil || found {
					return found, err
				}
			}
		case *planpb.Expr_List:
			if e.List == nil {
				return false, moerr.NewInvalidInputNoCtx("invalid generated index dependency list")
			}
			for _, arg := range e.List.List {
				found, err := visitExpr(arg)
				if err != nil || found {
					return found, err
				}
			}
		case *planpb.Expr_Lit:
			if e.Lit != nil && e.Lit.Src != nil {
				return visitExpr(e.Lit.Src)
			}
		case *planpb.Expr_T:
		default:
			return false, moerr.NewInvalidInputNoCtx("unknown generated index dependency expression")
		}
		return false, nil
	}
	for _, name := range names {
		pos, err := lookup(name)
		if err != nil {
			return false, err
		}
		found, err := visitCol(pos)
		if err != nil || found {
			return found, err
		}
	}
	return false, nil
}

// rejectUnsafeEnumIdentityDowngrade blocks ENUM identity changes whose target
// still represents a previously physical ENUM domain as a legacy ENUM key.
// columnSources is the same source mapping used to build ALTER COPY's INSERT.
func rejectUnsafeEnumIdentityDowngrade(
	oldTable, newTable *planpb.TableDef,
	columnSources map[string]selectExpr,
) error {
	if oldTable == nil || newTable == nil || columnSources == nil {
		return moerr.NewInvalidInputNoCtx("cannot validate ALTER ENUM index transition")
	}
	for _, oldCol := range oldTable.Cols {
		if oldCol == nil || oldCol.Typ.Id != int32(types.T_enum) || oldCol.Typ.EnumCollation == 0 {
			continue
		}
		// Resolve identity through ALTER COPY's source expression, not ColId:
		// ALTER can leave different columns with the same ColId. Seqnum must
		// remain stable for a direct source-column copy.
		var newCol *planpb.ColDef
		for targetName, source := range columnSources {
			if source.sexprType != exprColumnName || !strings.EqualFold(source.sexprStr, oldCol.Name) {
				continue
			}
			var candidate *planpb.ColDef
			for _, col := range newTable.Cols {
				if col != nil && strings.EqualFold(col.Name, targetName) {
					if candidate != nil {
						return moerr.NewInvalidInputNoCtx("ambiguous ALTER ENUM target column")
					}
					candidate = col
				}
			}
			if candidate == nil || candidate.Seqnum != oldCol.Seqnum || newCol != nil {
				return moerr.NewInvalidInputNoCtx("ambiguous ALTER ENUM source column")
			}
			newCol = candidate
		}
		if newCol == nil || newCol.Typ.Id != int32(types.T_enum) || newCol.Typ.EnumCollation != 0 {
			continue
		}
		oldPrimaryDepends, err := indexReferencesColumn(oldTable, &planpb.IndexDef{}, oldCol.Name)
		if err != nil {
			return err
		}
		if oldPrimaryDepends {
			newPrimaryDepends, err := indexReferencesColumn(newTable, &planpb.IndexDef{}, newCol.Name)
			if err != nil {
				return err
			}
			if newPrimaryDepends {
				return moerr.NewNotSupportedNoCtx("ALTER from ENUM collation identity to legacy ENUM with a dependent index")
			}
		}
		for _, oldIndex := range oldTable.Indexes {
			if oldIndex == nil {
				return moerr.NewInvalidInputNoCtx("invalid ALTER ENUM index definition")
			}
			newIndex := (*planpb.IndexDef)(nil)
			for _, candidate := range newTable.Indexes {
				if candidate != nil && strings.EqualFold(candidate.IndexName, oldIndex.IndexName) {
					newIndex = candidate
					break
				}
			}
			if newIndex == nil {
				continue
			}
			depends, err := indexReferencesColumn(oldTable, oldIndex, oldCol.Name)
			if err != nil {
				return err
			}
			if !depends {
				continue
			}
			survives, err := indexReferencesColumn(newTable, newIndex, newCol.Name)
			if err != nil {
				return err
			}
			if survives {
				return moerr.NewNotSupportedNoCtx("ALTER from ENUM collation identity to legacy ENUM with a dependent index")
			}
		}
	}
	return nil
}

// validateNewEnumIndexSafety rejects index formats whose handling of a new
// ENUM identity is not proven. Run this at index authoring or final ALTER
// schema validation, not when reading or dropping an existing legacy index.
func validateNewEnumIndexSafety(table *planpb.TableDef, indexes []*planpb.IndexDef) error {
	if table == nil {
		return moerr.NewInvalidInputNoCtx("missing ENUM index source table")
	}
	for _, index := range indexes {
		if index == nil {
			return moerr.NewInvalidInputNoCtx("missing ENUM index definition")
		}
		if !catalog.IsRegularIndexAlgo(index.IndexAlgo) || catalog.IsRTreeIndexAlgo(index.IndexAlgo) {
			usesNew, err := EnumIndexUsesNewIdentity(table, index)
			if err != nil {
				return err
			}
			// Plugins have different authoritative INCLUDE representations.
			// Until each one has a stable dependency hook, check both catalog
			// forms and reject a new identity found in either.
			if len(index.IncludedColumns) > 0 {
				paramsOnly := *index
				paramsOnly.IncludedColumns = nil
				paramsUseNew, err := EnumIndexUsesNewIdentity(table, &paramsOnly)
				if err != nil {
					return err
				}
				usesNew = usesNew || paramsUseNew
			}
			if usesNew {
				return moerr.NewNotSupportedNoCtx("index algorithm with a new ENUM collation identity")
			}
		}
		// The prefix metadata keys are canonical catalog names. Avoid parsing
		// unrelated algorithm parameters when there is no prefix metadata.
		if !strings.Contains(index.IndexAlgoParams, "prefix_lengths") {
			continue
		}
		prefixes, err := catalog.IndexPrefixLengthsFromParamsWithError(index.IndexAlgoParams)
		if err != nil {
			return err
		}
		for _, part := range index.Parts {
			name := catalog.ResolveAlias(part)
			isPrefix := false
			for prefixName, length := range prefixes {
				if length > 0 && strings.EqualFold(prefixName, name) {
					isPrefix = true
					break
				}
			}
			if !isPrefix {
				continue
			}
			var source *planpb.ColDef
			for _, col := range table.Cols {
				if col != nil && strings.EqualFold(col.Name, name) {
					if source != nil {
						return moerr.NewInvalidInputNoCtx("ambiguous ENUM index prefix column")
					}
					source = col
				}
			}
			if source == nil {
				return moerr.NewInvalidInputNoCtx("missing ENUM index prefix column")
			}
			if source.Typ.Id != int32(types.T_enum) {
				continue
			}
			usesNew, err := EnumIndexUsesNewIdentity(table, index)
			if err != nil {
				return err
			}
			if usesNew {
				return moerr.NewNotSupportedNoCtx("ENUM prefix index combined with a new ENUM collation identity")
			}
		}
	}
	return nil
}
