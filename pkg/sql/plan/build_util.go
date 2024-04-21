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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// func appendQueryNode(query *Query, node *Node) int32 {
// 	nodeID := int32(len(query.Nodes))
// 	node.NodeId = nodeID
// 	query.Nodes = append(query.Nodes, node)

// 	return nodeID
// }

// ifNeedLockWholeTable checks if the whole table needs to be locked based on the last node's statistics.
// It returns true if the out count of the last node is greater than the maximum lock count, otherwise it returns false.
func ifNeedLockWholeTable(builder *QueryBuilder, lastNodeId int32) bool {
	lastNode := builder.qry.Nodes[lastNodeId]
	if lastNode == nil {
		return false
	}

	lockService := builder.compCtx.GetProcess().LockService
	if lockService == nil {
		// MockCompilerContext
		return false
	}
	lockconfig := lockService.GetConfig()
	return lastNode.Stats.Outcnt > float64(lockconfig.MaxLockRowCount)
}

// GetFunctionArgTypeStrFromAst function arg type do not have scale and width, it depends on the data that it process
func GetFunctionArgTypeStrFromAst(arg tree.FunctionArg) (string, error) {
	argDecl := arg.(*tree.FunctionArgDecl)
	return GetFunctionTypeStrFromAst(argDecl.Type)
}

func GetFunctionTypeStrFromAst(typRef tree.ResolvableTypeReference) (string, error) {
	typ, err := getTypeFromAst(moerr.Context(), typRef)
	if err != nil {
		return "", err
	}
	ret := strings.ToLower(types.T(typ.Id).String())
	// do not display precision, because the choice of decimal64 or decimal128 is not exposed to user
	if strings.HasPrefix(ret, "decimal") {
		return "decimal", nil
	}
	return ret, nil
}

func getTypeFromAst(ctx context.Context, typ tree.ResolvableTypeReference) (*plan.Type, error) {
	if n, ok := typ.(*tree.T); ok {
		switch defines.MysqlType(n.InternalType.Oid) {
		case defines.MYSQL_TYPE_BIT:
			return &plan.Type{Id: int32(types.T_bit), Width: n.InternalType.DisplayWith, Scale: -1}, nil
		case defines.MYSQL_TYPE_TINY:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint8), Width: n.InternalType.Width, Scale: -1}, nil
			}
			return &plan.Type{Id: int32(types.T_int8), Width: n.InternalType.Width, Scale: -1}, nil
		case defines.MYSQL_TYPE_SHORT:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint16), Width: n.InternalType.Width, Scale: -1}, nil
			}
			return &plan.Type{Id: int32(types.T_int16), Width: n.InternalType.Width, Scale: -1}, nil
		case defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_INT24:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint32), Width: n.InternalType.Width, Scale: -1}, nil
			}
			return &plan.Type{Id: int32(types.T_int32), Width: n.InternalType.Width, Scale: -1}, nil
		case defines.MYSQL_TYPE_LONGLONG:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint64), Width: n.InternalType.Width, Scale: -1}, nil
			}
			return &plan.Type{Id: int32(types.T_int64), Width: n.InternalType.Width, Scale: -1}, nil
		case defines.MYSQL_TYPE_FLOAT:
			return &plan.Type{Id: int32(types.T_float32), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_DOUBLE:
			return &plan.Type{Id: int32(types.T_float64), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
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
				return &plan.Type{Id: int32(types.T_char), Width: width}, nil
			}
			return &plan.Type{Id: int32(types.T_varchar), Width: width}, nil
		case defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VARCHAR:
			width := n.InternalType.DisplayWith
			// for char type,if we didn't specify the length,
			// the default width should be 1, and for varchar,it's
			// the defaultMaxLength
			// Should always specify length to varbinary.
			fstr := strings.ToLower(n.InternalType.FamilyString)
			// Check explicit casting.
			if fstr == "binary" && n.InternalType.Scale == -1 {
				r := &plan.Type{Id: int32(types.T_binary), Width: width}
				r.Scale = -1
				return r, nil
			}
			if width == -1 {
				// create table t1(a char) -> DisplayWith = -1；but get width=1 in MySQL and PgSQL
				if fstr == "char" || fstr == "binary" {
					width = 1
				} else if fstr == "vecf32" || fstr == "vecf64" {
					width = types.MaxArrayDimension
				} else {
					width = types.MaxVarcharLen
				}
			}

			if (fstr == "char" || fstr == "binary") && width > types.MaxCharLen {
				return nil, moerr.NewOutOfRange(ctx, fstr, " typeLen is over the MaxCharLen: %v", types.MaxCharLen)
			} else if (fstr == "varchar" || fstr == "varbinary") && width > types.MaxVarcharLen {
				return nil, moerr.NewOutOfRange(ctx, fstr, " typeLen is over the MaxVarcharLen: %v", types.MaxVarcharLen)
			} else if fstr == "vecf32" || fstr == "vecf64" {
				if width > types.MaxArrayDimension {
					return nil, moerr.NewOutOfRange(ctx, fstr, " typeLen is over the MaxVectorLen : %v", types.MaxArrayDimension)
				}
				if width < 1 {
					return nil, moerr.NewOutOfRange(ctx, fstr, " typeLen cannot be less than 1")
				}
			}
			switch fstr {
			case "char":
				return &plan.Type{Id: int32(types.T_char), Width: width}, nil
			case "binary":
				return &plan.Type{Id: int32(types.T_binary), Width: width}, nil
			case "varchar":
				return &plan.Type{Id: int32(types.T_varchar), Width: width}, nil
			case "vecf32":
				return &plan.Type{Id: int32(types.T_array_float32), Width: width}, nil
			case "vecf64":
				return &plan.Type{Id: int32(types.T_array_float64), Width: width}, nil
			}
			// varbinary
			return &plan.Type{Id: int32(types.T_varbinary), Width: width}, nil
		case defines.MYSQL_TYPE_DATE:
			return &plan.Type{Id: int32(types.T_date)}, nil
		case defines.MYSQL_TYPE_TIME:
			return &plan.Type{Id: int32(types.T_time), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_DATETIME:
			// currently the ast's width for datetime's is 26, this is not accurate and may need revise, not important though, as we don't need it anywhere else except to differentiate empty vector.Typ.
			return &plan.Type{Id: int32(types.T_datetime), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_TIMESTAMP:
			return &plan.Type{Id: int32(types.T_timestamp), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_DECIMAL:
			if n.InternalType.DisplayWith > 16 {
				return &plan.Type{Id: int32(types.T_decimal128), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
			}
			return &plan.Type{Id: int32(types.T_decimal64), Width: n.InternalType.DisplayWith, Scale: n.InternalType.Scale}, nil
		case defines.MYSQL_TYPE_BOOL:
			return &plan.Type{Id: int32(types.T_bool)}, nil
		case defines.MYSQL_TYPE_BLOB:
			return &plan.Type{Id: int32(types.T_blob)}, nil
		case defines.MYSQL_TYPE_TEXT:
			return &plan.Type{Id: int32(types.T_text)}, nil
		case defines.MYSQL_TYPE_JSON:
			return &plan.Type{Id: int32(types.T_json)}, nil
		case defines.MYSQL_TYPE_UUID:
			return &plan.Type{Id: int32(types.T_uuid)}, nil
		case defines.MYSQL_TYPE_TINY_BLOB:
			return &plan.Type{Id: int32(types.T_blob)}, nil
		case defines.MYSQL_TYPE_MEDIUM_BLOB:
			return &plan.Type{Id: int32(types.T_blob)}, nil
		case defines.MYSQL_TYPE_LONG_BLOB:
			return &plan.Type{Id: int32(types.T_blob)}, nil
		case defines.MYSQL_TYPE_ENUM:
			if len(n.InternalType.EnumValues) > types.MaxEnumLen {
				return nil, moerr.NewNYI(ctx, "enum type out of max length")
			}
			if len(n.InternalType.EnumValues) == 0 {
				return nil, moerr.NewNYI(ctx, "enum type length err")
			}

			return &plan.Type{Id: int32(types.T_enum), Enumvalues: strings.Join(n.InternalType.EnumValues, ",")}, nil
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
	newExpr, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(defaultExpr), proc, false)
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
	executor, err := colexec.NewExpressionExecutor(proc, onUpdateExpr)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	_, err = executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch})
	if err != nil {
		return nil, err
	}

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
	case *plan.Expr_Lit:
		return expr.Typ.Id == int32(types.T_any) && ef.Lit.Isnull
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
		case *plan.Expr_Lit:
			switch value := ex.Lit.Value.(type) {
			case *plan.Literal_I64Val:
				if value.I64Val == 0 {
					ex.Lit.Value = &plan.Literal_Bval{Bval: false}
				} else {
					ex.Lit.Value = &plan.Literal_Bval{Bval: true}
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
// func getAccountIds(ctx CompilerContext, accounts tree.IdentifierList) ([]uint32, error) {
// 	var accountIds []uint32
// 	var err error
// 	if len(accounts) != 0 {
// 		accountNames := make([]string, len(accounts))
// 		for i, account := range accounts {
// 			accountNames[i] = string(account)
// 		}
// 		accountIds, err = ctx.ResolveAccountIds(accountNames)
// 		if err != nil {
// 			return nil, err
// 		}
// 	} else {
// 		accountIds = []uint32{catalog.System_Account}
// 	}
// 	if len(accountIds) == 0 {
// 		return nil, moerr.NewInternalError(ctx.GetContext(), "need specify account for the cluster tables")
// 	}
// 	return accountIds, err
// }

// func getAccountInfoOfClusterTable(ctx CompilerContext, accounts tree.IdentifierList, tableDef *TableDef, isClusterTable bool) (*plan.ClusterTable, error) {
// 	var accountIds []uint32
// 	var columnIndexOfAccountId int32 = -1
// 	var err error
// 	if isClusterTable {
// 		accountIds, err = getAccountIds(ctx, accounts)
// 		if err != nil {
// 			return nil, err
// 		}
// 		for i, col := range tableDef.GetCols() {
// 			if util.IsClusterTableAttribute(col.Name) {
// 				if columnIndexOfAccountId >= 0 {
// 					return nil, moerr.NewInternalError(ctx.GetContext(), "there are two account_ids in the cluster table")
// 				} else {
// 					columnIndexOfAccountId = int32(i)
// 				}
// 			}
// 		}

// 		if columnIndexOfAccountId == -1 {
// 			return nil, moerr.NewInternalError(ctx.GetContext(), "there is no account_id in the cluster table")
// 		} else if columnIndexOfAccountId >= int32(len(tableDef.GetCols())) {
// 			return nil, moerr.NewInternalError(ctx.GetContext(), "the index of the account_id in the cluster table is invalid")
// 		}
// 	} else {
// 		if len(accounts) != 0 {
// 			return nil, moerr.NewInvalidInput(ctx.GetContext(), "can not specify the accounts for the non cluster table")
// 		}
// 	}
// 	return &plan.ClusterTable{
// 		IsClusterTable:         isClusterTable,
// 		AccountIDs:             accountIds,
// 		ColumnIndexOfAccountId: columnIndexOfAccountId,
// 	}, nil
// }

func getDefaultExpr(ctx context.Context, d *plan.ColDef) (*Expr, error) {
	if !d.Default.NullAbility && d.Default.Expr == nil && !d.Typ.AutoIncr {
		return nil, moerr.NewInvalidInput(ctx, "invalid default value for column '%s'", d.Name)
	}
	if d.Default.Expr == nil {
		return &Expr{
			Expr: &plan.Expr_Lit{
				Lit: &Const{
					Isnull: true,
				},
			},
			Typ: plan.Type{
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
func getTablePriKeyName(priKeyDef *plan.PrimaryKeyDef) string {
	if priKeyDef == nil {
		return ""
	} else {
		return priKeyDef.PkeyColName
	}
}

// Check whether the table column name is an internal key
func checkTableColumnNameValid(name string) bool {
	if name == catalog.Row_ID || name == catalog.CPrimaryKeyColName ||
		name == catalog.TableTailAttrCommitTs || name == catalog.TableTailAttrAborted || name == catalog.TableTailAttrPKVal {
		return false
	}
	return true
}

// Check the expr has paramExpr
func checkExprHasParamExpr(exprs []tree.Expr) bool {
	for _, expr := range exprs {
		if _, ok := expr.(*tree.ParamExpr); ok {
			return true
		} else if e, ok := expr.(*tree.FuncExpr); ok {
			return checkExprHasParamExpr(e.Exprs)
		}
	}
	return false
}

// makeSelectList forms SELECT Clause "Select t.a,t.b,... "
func makeSelectList(table string, strs []string) string {
	bb := strings.Builder{}
	for i, str := range strs {
		if i > 0 {
			bb.WriteByte(',')
		}
		//table
		bb.WriteByte('`')
		bb.WriteString(table)
		bb.WriteByte('`')
		bb.WriteByte('.')
		//column
		bb.WriteByte('`')
		bb.WriteString(str)
		bb.WriteByte('`')
	}
	return bb.String()
}

// makeWhere forms WHERE Clause "Where t.a is not null and ..."
func makeWhere(table string, strs []string) string {
	bb := strings.Builder{}
	for i, str := range strs {
		if i > 0 {
			bb.WriteString(" and ")
		}
		//table
		bb.WriteByte('`')
		bb.WriteString(table)
		bb.WriteByte('`')
		bb.WriteByte('.')
		//column
		bb.WriteByte('`')
		bb.WriteString(str)
		bb.WriteByte('`')
		//is not null
		bb.WriteString(" is not null")
	}
	bb.WriteByte(' ')
	return bb.String()
}

// colIdsToNames convert the colId to the col name
func colIdsToNames(ctx context.Context, colIds []uint64, colDefs []*plan.ColDef) ([]string, error) {
	colId2Name := make(map[uint64]string)
	for _, def := range colDefs {
		colId2Name[def.ColId] = def.Name
	}
	names := make([]string, 0)
	for _, colId := range colIds {
		if name, has := colId2Name[colId]; !has {
			return nil, moerr.NewInternalError(ctx, fmt.Sprintf("colId %d does exist", colId))
		} else {
			names = append(names, name)
		}
	}
	return names, nil
}

/*
genSqlForCheckFKConstraints generates the fk constraint checking sql.

basic logic of fk constraint check.

	parent table:
		T(a)
	child table:
		S(b)
		foreign key (b) references T(a)


	generated sql :
		select count(*) == 0 from (
			select distinct S.b from S where S.b is not null
			except
			select distinct T.a from T
		)
	if the result is true, then the fk constraint confirmed.
*/
func genSqlForCheckFKConstraints(ctx context.Context,
	fkey *plan.ForeignKeyDef,
	childDbName, childTblName string, colsOfChild []*plan.ColDef,
	parentDbName, parentTblName string, colsOfParent []*plan.ColDef) (string, error) {

	//fk column names
	fkCols, err := colIdsToNames(ctx, fkey.Cols, colsOfChild)
	if err != nil {
		return "", err
	}
	//referred column names
	referCols, err := colIdsToNames(ctx, fkey.ForeignCols, colsOfParent)
	if err != nil {
		return "", err
	}

	childTableClause := fmt.Sprintf("`%s`.`%s`", childDbName, childTblName)
	parentTableClause := fmt.Sprintf("`%s`.`%s`", parentDbName, parentTblName)
	where := fmt.Sprintf("where %s", makeWhere(childTblName, fkCols))
	except := fmt.Sprintf("select distinct %s from %s %s except select distinct %s from %s",
		makeSelectList(childTblName, fkCols),
		childTableClause,
		where,
		makeSelectList(parentTblName, referCols),
		parentTableClause,
	)

	//make detect sql
	sql := strings.Join([]string{
		"select count(*) = 0 from (",
		except,
		")",
	}, " ")
	return sql, nil
}

// genSqlsForCheckFKSelfRefer generates the fk constraint checking sql.
// the only difference between genSqlsForCheckFKSelfRefer and genSqlForCheckFKConstraints
// is the parent table and child table are same in the fk self refer.
func genSqlsForCheckFKSelfRefer(ctx context.Context,
	dbName, tblName string,
	cols []*plan.ColDef, fkeys []*plan.ForeignKeyDef) ([]string, error) {
	ret := make([]string, 0)
	for _, fkey := range fkeys {
		if fkey.ForeignTbl != 0 {
			continue
		}
		sql, err := genSqlForCheckFKConstraints(ctx, fkey, dbName, tblName, cols, dbName, tblName, cols)
		if err != nil {
			return nil, err
		}
		ret = append(ret, sql)
	}
	return ret, nil
}

func rewriteForCreateTableLike(stmt *tree.CreateTable, ctx CompilerContext) (newStmt tree.Statement, err error) {
	oldTable := stmt.LikeTableName
	newTable := stmt.Table

	tblName := formatStr(string(oldTable.ObjectName))
	dbName := formatStr(string(oldTable.SchemaName))
	dbName, err = databaseIsValid(getSuitableDBName(dbName, ""), ctx)
	if err != nil {
		return nil, err
	}

	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}
	if tableDef.TableType == catalog.SystemViewRel || tableDef.TableType == catalog.SystemExternalRel || tableDef.TableType == catalog.SystemClusterRel {
		return nil, moerr.NewInternalError(ctx.GetContext(), "%s.%s is not BASE TABLE", dbName, tblName)
	}

	var createStr string
	if tableDef.TableType == catalog.SystemOrdinaryRel {
		createStr = fmt.Sprintf("CREATE TABLE `%s` (", formatStr(string(newTable.ObjectName)))
	} else if tblName == catalog.MO_DATABASE || tblName == catalog.MO_TABLES || tblName == catalog.MO_COLUMNS {
		createStr = fmt.Sprintf("CREATE TABLE `%s` (", formatStr(string(newTable.ObjectName)))
	}

	rowCount := 0
	var pkDefs []string
	colIdToName := make(map[uint64]string)
	for _, col := range tableDef.Cols {
		if col.Hidden {
			continue
		}
		colName := col.Name
		colIdToName[col.ColId] = col.Name
		if colName == catalog.Row_ID {
			continue
		}

		nullOrNot := "NOT NULL"
		// col.Default must be not nil
		if len(col.Default.OriginString) > 0 {
			if !col.Primary {
				nullOrNot = "DEFAULT " + formatStr(col.Default.OriginString)
			}
		} else if col.Default.NullAbility {
			nullOrNot = "DEFAULT NULL"
		}

		if col.Typ.AutoIncr {
			nullOrNot = "NOT NULL AUTO_INCREMENT"
		}

		var hasAttrComment string
		if col.Comment != "" {
			hasAttrComment = " COMMENT '" + col.Comment + "'"
		}

		if rowCount == 0 {
			createStr += "\n"
		} else {
			createStr += ",\n"
		}
		typ := types.T(col.Typ.Id).ToType()
		typeStr := typ.String()
		if typ.Oid.IsDecimal() { //after decimal fix,remove this
			typeStr = fmt.Sprintf("DECIMAL(%d,%d)", col.Typ.Width, col.Typ.Scale)
		}
		if typ.Oid == types.T_varchar || typ.Oid == types.T_char ||
			typ.Oid == types.T_binary || typ.Oid == types.T_varbinary ||
			typ.Oid.IsArrayRelate() || typ.Oid == types.T_bit {
			typeStr += fmt.Sprintf("(%d)", col.Typ.Width)
		}
		if typ.Oid.IsFloat() && col.Typ.Scale != -1 {
			typeStr += fmt.Sprintf("(%d,%d)", col.Typ.Width, col.Typ.Scale)
		}

		if typ.Oid.IsEnum() {
			enums := strings.Split(col.Typ.GetEnumvalues(), ",")
			typeStr += "("
			for i, enum := range enums {
				typeStr += fmt.Sprintf("'%s'", enum)
				if i < len(enums)-1 {
					typeStr += ","
				}
			}
			typeStr += ")"
		}

		updateOpt := ""
		if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
			updateOpt = " ON UPDATE " + col.OnUpdate.OriginString
		}
		createStr += fmt.Sprintf("`%s` %s %s%s%s", formatStr(colName), typeStr, nullOrNot, updateOpt, hasAttrComment)
		rowCount++
		if col.Primary {
			pkDefs = append(pkDefs, colName)
		}
	}

	// If it is a composite primary key, get the component columns of the composite primary key
	if tableDef.Pkey != nil && len(tableDef.Pkey.Names) > 1 {
		pkDefs = append(pkDefs, tableDef.Pkey.Names...)
	}

	if len(pkDefs) != 0 {
		pkStr := "PRIMARY KEY ("
		for i, def := range pkDefs {
			if i == len(pkDefs)-1 {
				pkStr += fmt.Sprintf("`%s`", formatStr(def))
			} else {
				pkStr += fmt.Sprintf("`%s`,", formatStr(def))
			}
		}
		pkStr += ")"
		if rowCount != 0 {
			createStr += ",\n"
		}
		createStr += pkStr
	}

	if tableDef.Indexes != nil {

		// We only print distinct index names. This is used to avoid printing the same index multiple times for IVFFLAT or
		// other multi-table indexes.
		indexNames := make(map[string]bool)

		for _, indexdef := range tableDef.Indexes {
			if _, ok := indexNames[indexdef.IndexName]; ok {
				continue
			} else {
				indexNames[indexdef.IndexName] = true
			}

			var indexStr string
			if indexdef.Unique {
				indexStr = "UNIQUE KEY "
			} else {
				indexStr = "KEY "
			}
			indexStr += fmt.Sprintf("`%s` ", formatStr(indexdef.IndexName))
			if !catalog.IsNullIndexAlgo(indexdef.IndexAlgo) {
				indexStr += fmt.Sprintf("USING %s ", indexdef.IndexAlgo)
			}
			indexStr += "("
			i := 0
			for _, part := range indexdef.Parts {
				if catalog.IsAlias(part) {
					continue
				}
				if i > 0 {
					indexStr += ","
				}

				indexStr += fmt.Sprintf("`%s`", formatStr(part))
				i++
			}

			indexStr += ")"
			if indexdef.IndexAlgoParams != "" {
				var paramList string
				paramList, err = catalog.IndexParamsToStringList(indexdef.IndexAlgoParams)
				if err != nil {
					return nil, err
				}
				indexStr += paramList
			}
			if indexdef.Comment != "" {
				indexdef.Comment = strings.Replace(indexdef.Comment, "'", "\\'", -1)
				indexStr += fmt.Sprintf(" COMMENT '%s'", formatStr(indexdef.Comment))
			}
			if rowCount != 0 {
				createStr += ",\n"
			}
			createStr += indexStr
		}
	}

	for _, fk := range tableDef.Fkeys {
		colNames := make([]string, len(fk.Cols))
		for i, colId := range fk.Cols {
			colNames[i] = colIdToName[colId]
		}

		var fkTableDef *TableDef

		//fk self reference
		if fk.ForeignTbl == 0 {
			fkTableDef = tableDef
		} else {
			_, fkTableDef = ctx.ResolveById(fk.ForeignTbl)
		}

		fkColIdToName := make(map[uint64]string)
		for _, col := range fkTableDef.Cols {
			fkColIdToName[col.ColId] = col.Name
		}
		fkColNames := make([]string, len(fk.ForeignCols))
		for i, colId := range fk.ForeignCols {
			fkColNames[i] = fkColIdToName[colId]
		}

		if rowCount != 0 {
			createStr += ",\n"
		}
		createStr += fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`) ON DELETE %s ON UPDATE %s",
			formatStr(fk.Name), strings.Join(colNames, "`,`"), formatStr(fkTableDef.Name), strings.Join(fkColNames, "`,`"), fk.OnDelete.String(), fk.OnUpdate.String())
	}

	if rowCount != 0 {
		createStr += "\n"
	}
	createStr += ")"

	var comment string
	var partition string
	for _, def := range tableDef.Defs {
		if proDef, ok := def.Def.(*plan.TableDef_DefType_Properties); ok {
			for _, kv := range proDef.Properties.Properties {
				if kv.Key == catalog.SystemRelAttr_Comment {
					comment = " COMMENT='" + kv.Value + "'"
				}
			}
		}
	}

	if tableDef.Partition != nil {
		partition = ` ` + tableDef.Partition.PartitionMsg
	}

	createStr += comment
	createStr += partition

	if tableDef.ClusterBy != nil {
		clusterby := " CLUSTER BY ("
		if util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
			//multi column clusterby
			cbNames := util.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
			for i, cbName := range cbNames {
				if i != 0 {
					clusterby += fmt.Sprintf(", `%s`", formatStr(cbName))
				} else {
					clusterby += fmt.Sprintf("`%s`", formatStr(cbName))
				}
			}
		} else {
			//single column cluster by
			clusterby += fmt.Sprintf("`%s`", formatStr(tableDef.ClusterBy.Name))
		}
		clusterby += ")"
		createStr += clusterby
	}

	var buf bytes.Buffer
	for _, ch := range createStr {
		if ch == '"' {
			buf.WriteRune('"')
		}
		buf.WriteRune(ch)
	}
	sql := buf.String()
	return getRewriteSQLStmt(ctx, sql)
}
