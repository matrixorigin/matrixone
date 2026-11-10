// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"slices"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// validateRebaseTableSchema runs for ALL tables before destination creation.
// A no-PK layout is not permission to replay it: the coordinator must separately
// prove unchanged visible bags, otherwise reject the whole request pre-write.
func validateRebaseTableSchema(ctx context.Context, defs [4]*plan.TableDef) ([]rebaseSQLColumn, error) {
	var baseline []*plan.ColDef
	var baselineKeys []string
	var result []rebaseSQLColumn
	for endpoint, def := range defs {
		if def == nil || def.Hidden || def.IsTemporary || def.IsDynamic || def.Partition != nil ||
			def.ClusterBy != nil || def.ViewSql != nil || def.TblFunc != nil ||
			(def.TableType != "" && def.TableType != catalog.SystemOrdinaryRel) {
			return nil, moerr.NewNotSupported(ctx, "Rebase requires ordinary stored tables")
		}
		if len(def.Indexes) != 0 || len(def.Fkeys) != 0 || len(def.RefChildTbls) != 0 || len(def.Checks) != 0 {
			return nil, moerr.NewNotSupported(ctx, "Rebase does not support non-PK indexes, foreign keys or checks")
		}
		kind, keys := dataBranchPrimaryKeyColumns(def)
		if kind == fakeKind || kind < 0 {
			keys = nil
		} else if len(keys) == 0 {
			return nil, moerr.NewInvalidInput(ctx, "Rebase primary key has no columns")
		}
		var cols []*plan.ColDef
		seen := make(map[string]bool, len(def.Cols))
		for _, col := range def.Cols {
			if col == nil {
				return nil, moerr.NewInvalidInput(ctx, "nil Rebase schema column")
			}
			if !isDataBranchUserVisibleColumn(col) {
				continue
			}
			if col.Name == "" || strings.ContainsRune(col.Name, 0) || seen[strings.ToLower(col.Name)] {
				return nil, moerr.NewInvalidInput(ctx, "invalid Rebase schema column")
			}
			seen[strings.ToLower(col.Name)] = true
			if col.Typ.AutoIncr || col.GeneratedCol != nil || col.OnUpdate != nil ||
				(col.Default != nil && !rebaseLiteralDefault(col.Default.Expr)) || !rebaseScalarType(types.T(col.Typ.Id)) {
				return nil, moerr.NewNotSupported(ctx, "Rebase requires stored scalar columns with literal defaults")
			}
			cols = append(cols, col)
		}
		if len(cols) == 0 {
			return nil, moerr.NewNotSupported(ctx, "Rebase requires visible stored columns")
		}
		keyNames := make(map[string]bool, len(keys))
		for _, key := range keys {
			name := strings.ToLower(key)
			if !seen[name] || keyNames[name] {
				return nil, moerr.NewInvalidInput(ctx, "Rebase primary key is not a visible column")
			}
			keyNames[name] = true
		}
		for _, col := range cols {
			if keyNames[strings.ToLower(col.Name)] && types.T(col.Typ.Id).IsFloat() {
				return nil, moerr.NewNotSupported(ctx, "Rebase does not yet support floating-point primary keys")
			}
		}
		if endpoint == 0 {
			baseline, baselineKeys = cols, keys
			for _, col := range cols {
				result = append(result, rebaseSQLColumn{col.Name, types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale), keyNames[strings.ToLower(col.Name)]})
			}
			continue
		}
		if !slices.Equal(baselineKeys, keys) || len(cols) != len(baseline) {
			return nil, moerr.NewNotSupported(ctx, "Rebase endpoint schemas differ")
		}
		for i, col := range cols {
			base := baseline[i]
			left, right := base.Typ, col.Typ
			// Table is expression provenance, not a stored type/collation. The
			// actual historical table identities belong to endpoint admission.
			left.Table, right.Table = "", ""
			if base.Name != col.Name || base.NotNull != col.NotNull || !proto.Equal(&left, &right) || !proto.Equal(base.Default, col.Default) {
				return nil, moerr.NewNotSupported(ctx, "Rebase endpoint schemas differ")
			}
		}
	}
	return result, nil
}

// Omitted tables are cloned from U without replay. Their ordinary secondary
// indexes still need identical logical definitions; hidden table names and IDs
// are clone-specific and are checked by the normal database-clone path.
func validateRebaseOmittedTableSchema(ctx context.Context, defs [4]*plan.TableDef) ([]rebaseSQLColumn, error) {
	var withoutIndexes [4]*plan.TableDef
	var baseline []*plan.IndexDef
	for endpoint, def := range defs {
		if def == nil {
			return nil, moerr.NewNotSupported(ctx, "Rebase endpoint table inventories differ")
		}
		indexes := make([]*plan.IndexDef, 0, len(def.Indexes))
		seen := make(map[string]bool, len(def.Indexes))
		for _, idx := range def.Indexes {
			if idx == nil || idx.IndexName == "" || idx.IndexTableName == "" || !idx.TableExist || idx.Unique || idx.IndexAlgoTableType != "" ||
				len(idx.Parts) == 0 || (catalog.ToLower(idx.IndexAlgo) != "" && catalog.ToLower(idx.IndexAlgo) != catalog.MoIndexBTreeAlgo.ToString()) ||
				idx.IndexAlgoParams != "" || len(idx.IncludedColumns) != 0 {
				return nil, moerr.NewNotSupported(ctx, "Rebase omitted tables require ordinary non-unique secondary indexes")
			}
			name := strings.ToLower(idx.IndexName)
			if seen[name] {
				return nil, moerr.NewInvalidInput(ctx, "duplicate Rebase index name")
			}
			seen[name] = true
			logical := *idx
			logical.IdxId = ""
			logical.IndexTableName = ""
			indexes = append(indexes, &logical)
		}
		slices.SortFunc(indexes, func(a, b *plan.IndexDef) int { return strings.Compare(a.IndexName, b.IndexName) })
		if endpoint == 0 {
			baseline = indexes
		} else {
			if len(baseline) != len(indexes) {
				return nil, moerr.NewNotSupported(ctx, "Rebase endpoint index schemas differ")
			}
			for i := range indexes {
				if !proto.Equal(baseline[i], indexes[i]) {
					return nil, moerr.NewNotSupported(ctx, "Rebase endpoint index schemas differ")
				}
			}
		}
		copyDef := *def
		copyDef.Indexes = nil
		withoutIndexes[endpoint] = &copyDef
	}
	return validateRebaseTableSchema(ctx, withoutIndexes)
}

func rebaseLiteralDefault(expr *plan.Expr) bool {
	if expr == nil {
		return true
	}
	if literal := expr.GetLit(); literal != nil {
		return rebaseLiteralDefault(literal.Src)
	}
	// The binder can cast a literal to the declared column type. Do not turn
	// arbitrary constant-looking function calls into an accepted default.
	f := expr.GetF()
	return f != nil && f.Func != nil && f.Func.ObjName == "cast" && len(f.Args) == 2 &&
		f.Args[0] != nil && rebaseLiteralDefault(f.Args[0]) && f.Args[1].GetT() != nil
}

func rebaseScalarType(typ types.T) bool {
	// typeswitch:partial low-precision float storage and replay identity are not supported by this restricted executor.
	switch typ {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_text, types.T_blob,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_uuid, types.T_enum:
		return true
	default:
		return false
	}
}
