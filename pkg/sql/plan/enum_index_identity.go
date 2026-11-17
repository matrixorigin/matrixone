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

// validateNewEnumIndexPrefixes prevents a new-identity index from inheriting
// legacy ENUM prefix keys, whose ordinal-based encoding does not implement the
// declared label prefix. Run this at index authoring or final ALTER schema
// validation, not when reading or dropping an existing legacy-only index.
func validateNewEnumIndexPrefixes(table *planpb.TableDef, indexes []*planpb.IndexDef) error {
	if table == nil {
		return moerr.NewInvalidInputNoCtx("missing ENUM index source table")
	}
	for _, index := range indexes {
		if index == nil {
			return moerr.NewInvalidInputNoCtx("missing ENUM index definition")
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
