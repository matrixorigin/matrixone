// Copyright 2021 - 2022 Matrix Origin
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
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// func appendQueryNode(query *Query, node *Node) int32 {
// 	nodeID := int32(len(query.Nodes))
// 	node.NodeId = nodeID
// 	query.Nodes = append(query.Nodes, node)

// 	return nodeID
// }

func getTypeFromAst(ctx context.Context, typ tree.ResolvableTypeReference) (*plan.Type, error) {
	if n, ok := typ.(*tree.T); ok {
		switch defines.MysqlType(n.InternalType.Oid) {
		case defines.MYSQL_TYPE_TINY:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint8), Width: n.InternalType.Width, Size: 1}, nil
			}
			return &plan.Type{Id: int32(types.T_int8), Width: n.InternalType.Width, Size: 1}, nil
		case defines.MYSQL_TYPE_SHORT:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint16), Width: n.InternalType.Width, Size: 2}, nil
			}
			return &plan.Type{Id: int32(types.T_int16), Width: n.InternalType.Width, Size: 2}, nil
		case defines.MYSQL_TYPE_LONG:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint32), Width: n.InternalType.Width, Size: 4}, nil
			}
			return &plan.Type{Id: int32(types.T_int32), Width: n.InternalType.Width, Size: 4}, nil
		case defines.MYSQL_TYPE_LONGLONG:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint64), Width: n.InternalType.Width, Size: 8}, nil
			}
			return &plan.Type{Id: int32(types.T_int64), Width: n.InternalType.Width, Size: 8}, nil
		case defines.MYSQL_TYPE_FLOAT:
			return &plan.Type{Id: int32(types.T_float32), Width: n.InternalType.DisplayWith, Size: 4, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_DOUBLE:
			return &plan.Type{Id: int32(types.T_float64), Width: n.InternalType.DisplayWith, Size: 8, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_STRING:
			width := n.InternalType.DisplayWith
			// for char type,if we didn't specify the length,
			// the default width should be 1, and for varchar,it's
			// the defaultMaxLength
			fstr := strings.ToLower(n.InternalType.FamilyString)
			if width == -1 {
				// create table t1(a char) -> DisplayWith = -1；but get width=1 in MySQL and PgSQL
				if fstr == "char" {
					width = 1
				} else {
					width = types.MaxVarcharLen
				}
			}
			if fstr == "char" && width > types.MaxCharLen {
				return nil, moerr.NewOutOfRange(ctx, "char", " typeLen is over the MaxCharLen: %v", types.MaxCharLen)
			} else if fstr == "varchar" && width > types.MaxVarcharLen {
				return nil, moerr.NewOutOfRange(ctx, "varchar", " typeLen is over the MaxVarcharLen: %v", types.MaxVarcharLen)
			}
			if fstr == "char" { // type char
				return &plan.Type{Id: int32(types.T_char), Size: 24, Width: width}, nil
			}
			return &plan.Type{Id: int32(types.T_varchar), Size: 24, Width: width}, nil
		case defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VARCHAR:
			width := n.InternalType.DisplayWith
			// for char type,if we didn't specify the length,
			// the default width should be 1, and for varchar,it's
			// the defaultMaxLength
			fstr := strings.ToLower(n.InternalType.FamilyString)
			if width == -1 {
				// create table t1(a char) -> DisplayWith = -1；but get width=1 in MySQL and PgSQL
				if fstr == "char" {
					width = 1
				} else {
					width = types.MaxVarcharLen
				}
			}
			if fstr == "char" && width > types.MaxCharLen {
				return nil, moerr.NewOutOfRange(ctx, "char", " typeLen is over the MaxCharLen: %v", types.MaxCharLen)
			} else if fstr == "varchar" && width > types.MaxVarcharLen {
				return nil, moerr.NewOutOfRange(ctx, "varchar", " typeLen is over the MaxVarcharLen: %v", types.MaxVarcharLen)
			}
			if fstr == "char" { // type char
				return &plan.Type{Id: int32(types.T_char), Size: 24, Width: width}, nil
			}
			return &plan.Type{Id: int32(types.T_varchar), Size: 24, Width: width}, nil
		case defines.MYSQL_TYPE_DATE:
			return &plan.Type{Id: int32(types.T_date), Size: 4}, nil
		case defines.MYSQL_TYPE_TIME:
			return &plan.Type{Id: int32(types.T_time), Size: 8, Width: n.InternalType.Width, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_DATETIME:
			// currently the ast's width for datetime's is 26, this is not accurate and may need revise, not important though, as we don't need it anywhere else except to differentiate empty vector.Typ.
			return &plan.Type{Id: int32(types.T_datetime), Size: 8, Width: n.InternalType.Width, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_TIMESTAMP:
			return &plan.Type{Id: int32(types.T_timestamp), Size: 8, Width: n.InternalType.Width, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_DECIMAL:
			if n.InternalType.DisplayWith > 15 {
				return &plan.Type{Id: int32(types.T_decimal128), Size: 16, Width: n.InternalType.DisplayWith, Scale: n.InternalType.Precision}, nil
			}
			return &plan.Type{Id: int32(types.T_decimal64), Size: 8, Width: n.InternalType.DisplayWith, Scale: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_BOOL:
			return &plan.Type{Id: int32(types.T_bool), Size: 1}, nil
		case defines.MYSQL_TYPE_BLOB:
			return &plan.Type{Id: int32(types.T_blob), Size: 24}, nil
		case defines.MYSQL_TYPE_TEXT:
			return &plan.Type{Id: int32(types.T_text), Size: 24}, nil
		case defines.MYSQL_TYPE_JSON:
			return &plan.Type{Id: int32(types.T_json), Size: types.VarlenaSize}, nil
		case defines.MYSQL_TYPE_UUID:
			return &plan.Type{Id: int32(types.T_uuid), Size: 16}, nil
		case defines.MYSQL_TYPE_TINY_BLOB:
			return &plan.Type{Id: int32(types.T_blob), Size: types.VarlenaSize}, nil
		case defines.MYSQL_TYPE_MEDIUM_BLOB:
			return &plan.Type{Id: int32(types.T_blob), Size: types.VarlenaSize}, nil
		case defines.MYSQL_TYPE_LONG_BLOB:
			return &plan.Type{Id: int32(types.T_blob), Size: types.VarlenaSize}, nil
		default:
			return nil, moerr.NewNYI(ctx, "data type: '%s'", tree.String(&n.InternalType, dialect.MYSQL))
		}
	}
	return nil, moerr.NewInternalError(ctx, "unknown data type")
}

func buildDefaultExpr(col *tree.ColumnTableDef, typ *plan.Type, proc *process.Process) (*plan.Default, error) {
	nullAbility := true
	var expr tree.Expr = nil
	for _, attr := range col.Attributes {
		if s, ok := attr.(*tree.AttributeNull); ok {
			nullAbility = s.Is
			break
		}
	}

	for _, attr := range col.Attributes {
		if s, ok := attr.(*tree.AttributeDefault); ok {
			expr = s.Expr
			break
		}
	}

	if typ.Id == int32(types.T_json) {
		if expr != nil && !isNullAstExpr(expr) {
			return nil, moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("JSON column '%s' cannot have default value", col.Name.Parts[0]))
		}
	}
	if !nullAbility && isNullAstExpr(expr) {
		return nil, moerr.NewInvalidInput(proc.Ctx, "invalid default value for column '%s'", col.Name.Parts[0])
	}

	if expr == nil {
		return &plan.Default{
			NullAbility:  nullAbility,
			Expr:         nil,
			OriginString: "",
		}, nil
	}

	binder := NewDefaultBinder(proc.Ctx, nil, nil, typ, nil)
	planExpr, err := binder.BindExpr(expr, 0, false)
	if err != nil {
		return nil, err
	}

	if defaultFunc := planExpr.GetF(); defaultFunc != nil {
		if int(typ.Id) != int(types.T_uuid) && defaultFunc.Func.ObjName == "uuid" {
			return nil, moerr.NewInvalidInput(proc.Ctx, "invalid default value for column '%s'", col.Name.Parts[0])
		}
	}

	defaultExpr, err := makePlan2CastExpr(proc.Ctx, planExpr, typ)
	if err != nil {
		return nil, err
	}

	// try to calculate default value, return err if fails
	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	newExpr, err := ConstantFold(bat, DeepCopyExpr(defaultExpr), proc)
	if err != nil {
		return nil, err
	}

	fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithSingleQuoteString())
	fmtCtx.PrintExpr(expr, expr, false)
	return &plan.Default{
		NullAbility:  nullAbility,
		Expr:         newExpr,
		OriginString: fmtCtx.String(),
	}, nil
}

func buildOnUpdate(col *tree.ColumnTableDef, typ *plan.Type, proc *process.Process) (*plan.OnUpdate, error) {
	var expr tree.Expr = nil

	for _, attr := range col.Attributes {
		if s, ok := attr.(*tree.AttributeOnUpdate); ok {
			expr = s.Expr
			break
		}
	}

	if expr == nil {
		return nil, nil
	}

	binder := NewDefaultBinder(proc.Ctx, nil, nil, typ, nil)
	planExpr, err := binder.BindExpr(expr, 0, false)
	if err != nil {
		return nil, err
	}

	onUpdateExpr, err := makePlan2CastExpr(proc.Ctx, planExpr, typ)
	if err != nil {
		return nil, err
	}

	// try to calculate on update value, return err if fails
	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	v, err := colexec.EvalExpr(bat, proc, onUpdateExpr)
	if err != nil {
		return nil, err
	}
	v.Free(proc.Mp())
	ret := &plan.OnUpdate{
		Expr:         onUpdateExpr,
		OriginString: tree.String(expr, dialect.MYSQL),
	}
	return ret, nil
}

func isNullExpr(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch ef := expr.Expr.(type) {
	case *plan.Expr_C:
		return expr.Typ.Id == int32(types.T_any) && ef.C.Isnull
	default:
		return false
	}
}

func isNullAstExpr(expr tree.Expr) bool {
	if expr == nil {
		return false
	}
	v, ok := expr.(*tree.NumVal)
	return ok && v.ValType == tree.P_null
}

func convertValueIntoBool(name string, args []*Expr, isLogic bool) error {
	if !isLogic && (len(args) != 2 || (args[0].Typ.Id != int32(types.T_bool) && args[1].Typ.Id != int32(types.T_bool))) {
		return nil
	}
	for _, arg := range args {
		if arg.Typ.Id == int32(types.T_bool) {
			continue
		}
		switch ex := arg.Expr.(type) {
		case *plan.Expr_C:
			switch value := ex.C.Value.(type) {
			case *plan.Const_I64Val:
				if value.I64Val == 0 {
					ex.C.Value = &plan.Const_Bval{Bval: false}
				} else {
					ex.C.Value = &plan.Const_Bval{Bval: true}
				}
				arg.Typ.Id = int32(types.T_bool)
			}
		}
	}
	return nil
}

func getFunctionObjRef(funcID int64, name string) *ObjectRef {
	return &ObjectRef{
		Obj:     funcID,
		ObjName: name,
	}
}

// getAccountIds transforms the account names into account ids.
// if accounts is nil, return the id of the sys account.
func getAccountIds(ctx CompilerContext, accounts tree.IdentifierList) ([]uint32, error) {
	var accountIds []uint32
	var err error
	if len(accounts) != 0 {
		accountNames := make([]string, len(accounts))
		for i, account := range accounts {
			accountNames[i] = string(account)
		}
		accountIds, err = ctx.ResolveAccountIds(accountNames)
		if err != nil {
			return nil, err
		}
	} else {
		accountIds = []uint32{catalog.System_Account}
	}
	if len(accountIds) == 0 {
		return nil, moerr.NewInternalError(ctx.GetContext(), "need specify account for the cluster tables")
	}
	return accountIds, err
}

func getAccountInfoOfClusterTable(ctx CompilerContext, accounts tree.IdentifierList, tableDef *TableDef, isClusterTable bool) (*plan.ClusterTable, error) {
	var accountIds []uint32
	var columnIndexOfAccountId int32 = -1
	var err error
	if isClusterTable {
		accountIds, err = getAccountIds(ctx, accounts)
		if err != nil {
			return nil, err
		}
		for i, col := range tableDef.GetCols() {
			if util.IsClusterTableAttribute(col.Name) {
				if columnIndexOfAccountId >= 0 {
					return nil, moerr.NewInternalError(ctx.GetContext(), "there are two account_ids in the cluster table")
				} else {
					columnIndexOfAccountId = int32(i)
				}
			}
		}

		if columnIndexOfAccountId == -1 {
			return nil, moerr.NewInternalError(ctx.GetContext(), "there is no account_id in the cluster table")
		} else if columnIndexOfAccountId >= int32(len(tableDef.GetCols())) {
			return nil, moerr.NewInternalError(ctx.GetContext(), "the index of the account_id in the cluster table is invalid")
		}
	} else {
		if len(accounts) != 0 {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "can not specify the accounts for the non cluster table")
		}
	}
	return &plan.ClusterTable{
		IsClusterTable:         isClusterTable,
		AccountIDs:             accountIds,
		ColumnIndexOfAccountId: columnIndexOfAccountId,
	}, nil
}

func getDefaultExpr(ctx context.Context, d *plan.ColDef) (*Expr, error) {
	if !d.Default.NullAbility && d.Default.Expr == nil && !d.Typ.AutoIncr {
		return nil, moerr.NewInvalidInput(ctx, "invalid default value")
	}
	if d.Default.Expr == nil {
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: true,
				},
			},
			Typ: &plan.Type{
				Id:          d.Typ.Id,
				NotNullable: false,
			},
		}, nil
	}
	return d.Default.Expr, nil
}

func judgeUnixTimestampReturnType(timestr string) types.T {
	retDecimal := 0
	if dotIdx := strings.LastIndex(timestr, "."); dotIdx >= 0 {
		retDecimal = len(timestr) - dotIdx - 1
	}

	if retDecimal > 6 || retDecimal == -1 {
		retDecimal = 6
	}

	if retDecimal == 0 {
		return types.T_int64
	} else {
		return types.T_decimal128
	}
}

// Get the primary key name of the table
func GetTablePriKeyName(cols []*plan.ColDef, cPkeyCol *plan.ColDef) string {
	for _, col := range cols {
		if col.Name != catalog.Row_ID && col.Primary {
			return col.Name
		}

	}

	if cPkeyCol != nil {
		return cPkeyCol.Name
	}
	return ""
}
