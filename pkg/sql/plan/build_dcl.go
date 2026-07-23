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
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func getPreparePlan(ctx CompilerContext, stmt tree.Statement) (*Plan, error) {
	if s, ok := stmt.(*tree.Insert); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			return BuildPlan(ctx, stmt, true)
		}
	} else if s, ok := stmt.(*tree.Replace); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			return BuildPlan(ctx, stmt, true)
		}
	}

	switch stmt := stmt.(type) {
	case *tree.Select, *tree.ParenSelect,
		*tree.Update, *tree.Delete, *tree.Insert,
		*tree.ShowDatabases, *tree.ShowTables, *tree.ShowSequences, *tree.ShowColumns,
		*tree.ShowCreateDatabase, *tree.ShowCreateTable:
		opt := NewPrepareOptimizer(ctx)
		optimized, err := opt.Optimize(stmt, true)
		if err != nil {
			return nil, err
		}
		return &Plan{
			Plan: &Plan_Query{
				Query: optimized,
			},
		}, nil
	default:
		return BuildPlan(ctx, stmt, true)
	}
}

func buildPrepare(stmt tree.Prepare, ctx CompilerContext) (*Plan, error) {
	var preparePlan *Plan
	var preparedStmt tree.Statement
	var err error
	var stmtName string

	switch pstmt := stmt.(type) {
	case *tree.PrepareStmt:
		stmtName = string(pstmt.Name)
		preparedStmt = pstmt.Stmt
		preparePlan, err = getPreparePlan(ctx, pstmt.Stmt)
		if err != nil {
			return nil, err
		}
		preparePlan.IsPrepare = true

	case *tree.PrepareString:
		var v interface{}
		v, err = ctx.ResolveVariable("lower_case_table_names", true, false)
		if err != nil {
			v = int64(1)
		}
		sqlMode := ""
		if mode, modeErr := ctx.ResolveVariable("sql_mode", true, false); modeErr == nil {
			if modeStr, ok := mode.(string); ok {
				sqlMode = mysql.SessionSQLModeForParser(modeStr)
			}
		}
		stmts, err := mysql.ParseWithSQLMode(ctx.GetContext(), pstmt.Sql, v.(int64), sqlMode)
		if err != nil {
			return nil, err
		}
		defer func() {
			for _, s := range stmts {
				s.Free()
			}
		}()
		if len(stmts) > 1 {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot prepare multi statements")
		}
		stmtName = string(pstmt.Name)
		preparedStmt = stmts[0]
		preparePlan, err = getPreparePlan(ctx, stmts[0])
		if err != nil {
			return nil, err
		}
		preparePlan.IsPrepare = true
	}

	schemas, paramTypes, err := ResetPreparePlan(ctx, preparePlan)
	if err != nil {
		return nil, err
	}
	viewSchemas, err := collectPrepareViewSchemas(ctx)
	if err != nil {
		return nil, err
	}
	schemas = appendPrepareSchemas(schemas, viewSchemas...)
	ddlSchemas, err := collectPrepareDdlSchemas(ctx, preparedStmt, preparePlan)
	if err != nil {
		return nil, err
	}
	schemas = appendPrepareSchemas(schemas, ddlSchemas...)
	if len(paramTypes) > math.MaxUint16 {
		return nil, moerr.NewErrTooManyParameter(ctx.GetContext())
	}

	prepare := &plan.Prepare{
		Name:       stmtName,
		Schemas:    schemas,
		Plan:       preparePlan,
		ParamTypes: paramTypes,
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_PREPARE,
				Control: &plan.DataControl_Prepare{
					Prepare: prepare,
				},
			},
		},
	}, nil
}

func collectPrepareDdlSchemas(ctx CompilerContext, stmt tree.Statement, preparePlan *Plan) ([]*plan.ObjectRef, error) {
	var tableNames []*tree.TableName
	var schemas []*plan.ObjectRef

	addTableNames := func(names tree.TableNames) {
		for _, name := range names {
			tableNames = append(tableNames, name)
		}
	}
	addForeignKey := func(fk *tree.ForeignKey) {
		if fk != nil && fk.Refer != nil {
			tableNames = append(tableNames, fk.Refer.TableName)
		}
	}
	addQuerySchemas := func(selectStmt *tree.Select) error {
		if selectStmt == nil {
			return nil
		}
		queryPlan, err := bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, selectStmt, false, true)
		if err != nil {
			return err
		}
		querySchemas, _, err := ResetPreparePlan(ctx, queryPlan)
		if err != nil {
			return err
		}
		schemas = appendPrepareSchemas(schemas, querySchemas...)
		return nil
	}
	addDatabaseSchema := func(databaseName string) error {
		if databaseName == "" {
			databaseName = ctx.DefaultDatabase()
		}
		var databaseID uint64
		var err error
		if ctx.DatabaseExists(databaseName, nil) {
			databaseID, err = ctx.GetDatabaseId(databaseName, nil)
			if err != nil {
				return err
			}
		}
		schemas = appendPrepareSchemas(schemas, &plan.ObjectRef{
			Db:         int64(databaseID),
			Schema:     int64(databaseID),
			SchemaName: databaseName,
		})
		return nil
	}

	switch ddl := stmt.(type) {
	case *tree.AlterTable:
		tableNames = append(tableNames, ddl.Table)
		for _, option := range ddl.Options {
			if add, ok := option.(*tree.AlterOptionAdd); ok {
				if fk, ok := add.Def.(*tree.ForeignKey); ok {
					addForeignKey(fk)
				}
			}
		}
	case *tree.RenameTable:
		for _, alterTable := range ddl.AlterTables {
			tableNames = append(tableNames, alterTable.Table)
		}
	case *tree.CreateIndex:
		tableNames = append(tableNames, ddl.Table)
	case *tree.DropIndex:
		tableNames = append(tableNames, ddl.TableName)
	case *tree.TruncateTable:
		tableNames = append(tableNames, ddl.Name)
	case *tree.DropTable:
		addTableNames(ddl.Names)
	case *tree.DropView:
		addTableNames(ddl.Names)
	case *tree.DropSequence:
		addTableNames(ddl.Names)
	case *tree.AlterSequence:
		tableNames = append(tableNames, ddl.Name)
	case *tree.AlterView:
		tableNames = append(tableNames, ddl.Name)
		if err := addQuerySchemas(ddl.AsSource); err != nil {
			return nil, err
		}
	case *tree.CreateView:
		tableNames = append(tableNames, ddl.Name)
		if err := addQuerySchemas(ddl.AsSource); err != nil {
			return nil, err
		}
	case *tree.CreateTable:
		tableNames = append(tableNames, &ddl.Table)
		if ddl.IsAsLike {
			tableNames = append(tableNames, &ddl.LikeTableName)
		}
		for _, def := range ddl.Defs {
			if fk, ok := def.(*tree.ForeignKey); ok {
				addForeignKey(fk)
			}
		}
		if ddl.IsDynamicTable {
			if err := addQuerySchemas(ddl.AsSource); err != nil {
				return nil, err
			}
		}
	case *tree.CloneTable:
		if clone := preparePlan.GetDdl().GetCloneTable(); clone != nil {
			schemas = appendPrepareSchemas(schemas, prepareSchemaRef(clone.GetSrcObjDef(), clone.GetSrcTableDef()))
		}
	}

	switch ddl := stmt.(type) {
	case *tree.CreateSequence:
		if err := addDatabaseSchema(string(ddl.Name.SchemaName)); err != nil {
			return nil, err
		}
	case *tree.CreateSource:
		if err := addDatabaseSchema(string(ddl.SourceName.SchemaName)); err != nil {
			return nil, err
		}
	case *tree.CloneTable:
		if err := addDatabaseSchema(string(ddl.CreateTable.Table.SchemaName)); err != nil {
			return nil, err
		}
	}

	for _, tableName := range tableNames {
		if tableName == nil {
			continue
		}
		databaseName := string(tableName.SchemaName)
		if databaseName == "" {
			databaseName = ctx.DefaultDatabase()
		}
		name := string(tableName.ObjectName)
		objRef, tableDef, err := ctx.Resolve(databaseName, name, nil)
		if err != nil {
			return nil, err
		}
		if objRef == nil || tableDef == nil {
			var databaseID uint64
			if ctx.DatabaseExists(databaseName, nil) {
				databaseID, err = ctx.GetDatabaseId(databaseName, nil)
				if err != nil {
					return nil, err
				}
			}
			schemas = appendPrepareSchemas(schemas, &plan.ObjectRef{
				Db:         int64(databaseID),
				Schema:     int64(databaseID),
				SchemaName: databaseName,
				ObjName:    name,
			})
			continue
		}
		schemas = appendPrepareSchemas(schemas, prepareSchemaRef(objRef, tableDef))
	}

	createTable := preparePlan.GetDdl().GetCreateTable()
	if clone := preparePlan.GetDdl().GetCloneTable(); createTable == nil && clone != nil {
		createTable = clone.GetCreateTable().GetDdl().GetCreateTable()
	}
	if createTable != nil {
		// A child table that forward-references this table can be created in any
		// database of the account after PREPARE. Track account-wide table changes
		// so FksReferToMe is rebuilt before CREATE TABLE executes.
		schemas = appendPrepareSchemas(schemas, &plan.ObjectRef{})
		for i, tableName := range createTable.GetFkTables() {
			if i >= len(createTable.GetFkDbs()) {
				return nil, moerr.NewInternalError(ctx.GetContext(), "foreign key table is missing its database")
			}
			databaseName := createTable.GetFkDbs()[i]
			objRef, tableDef, err := ctx.Resolve(databaseName, tableName, nil)
			if err != nil {
				return nil, err
			}
			if objRef == nil || tableDef == nil {
				return nil, moerr.NewNoSuchTable(ctx.GetContext(), databaseName, tableName)
			}
			schemas = appendPrepareSchemas(schemas, prepareSchemaRef(objRef, tableDef))
		}
		for _, fk := range createTable.GetFksReferToMe() {
			objRef, tableDef, err := ctx.Resolve(fk.GetDb(), fk.GetTable(), nil)
			if err != nil {
				return nil, err
			}
			if objRef == nil || tableDef == nil {
				return nil, moerr.NewNoSuchTable(ctx.GetContext(), fk.GetDb(), fk.GetTable())
			}
			schemas = appendPrepareSchemas(schemas, prepareSchemaRef(objRef, tableDef))
		}
	}

	return schemas, nil
}

func collectPrepareViewSchemas(ctx CompilerContext) ([]*plan.ObjectRef, error) {
	var schemas []*plan.ObjectRef
	for _, viewKey := range ctx.GetViews() {
		viewKey, _, _ = strings.Cut(viewKey, ViewSnapshotKeySuffix)
		databaseName, tableName, ok := strings.Cut(viewKey, "#")
		if !ok || databaseName == "" || tableName == "" {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "invalid view dependency %q", viewKey)
		}
		objRef, tableDef, err := ctx.Resolve(databaseName, tableName, ctx.GetSnapshot())
		if err != nil {
			return nil, err
		}
		if objRef == nil || tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), databaseName, tableName)
		}
		schemas = appendPrepareSchemas(schemas, prepareSchemaRef(objRef, tableDef))
	}
	return schemas, nil
}

func prepareSchemaRef(objRef *plan.ObjectRef, tableDef *plan.TableDef) *plan.ObjectRef {
	if objRef == nil || tableDef == nil {
		return nil
	}
	ref := DeepCopyObjectRef(objRef)
	ref.Server = int64(tableDef.Version)
	ref.Db = int64(tableDef.DbId)
	ref.Schema = int64(tableDef.DbId)
	ref.Obj = int64(tableDef.TblId)
	if ref.SchemaName == "" {
		ref.SchemaName = tableDef.DbName
	}
	if ref.ObjName == "" {
		ref.ObjName = tableDef.Name
	}
	return ref
}

func appendPrepareSchemas(schemas []*plan.ObjectRef, refs ...*plan.ObjectRef) []*plan.ObjectRef {
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		duplicate := false
		for _, schema := range schemas {
			sameTenant := (schema.PubInfo == nil && ref.PubInfo == nil) ||
				(schema.PubInfo != nil && ref.PubInfo != nil &&
					schema.PubInfo.GetTenantId() == ref.PubInfo.GetTenantId())
			sameID := sameTenant && schema.Obj != 0 && ref.Obj != 0 && schema.Db == ref.Db && schema.Obj == ref.Obj
			sameName := sameTenant && schema.SchemaName == ref.SchemaName && schema.ObjName == ref.ObjName
			if sameID || sameName {
				duplicate = true
				break
			}
		}
		if !duplicate {
			schemas = append(schemas, ref)
		}
	}
	return schemas
}

func buildExecute(stmt *tree.Execute, ctx CompilerContext) (*Plan, error) {
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	binder := NewWhereBinder(builder, &BindContext{})

	args := make([]*Expr, len(stmt.Variables))
	for idx, variable := range stmt.Variables {
		arg, err := binder.baseBindExpr(variable, 0, true)
		if err != nil {
			return nil, err
		}
		args[idx] = arg
	}

	execute := &plan.Execute{
		Name: string(stmt.Name),
		Args: args,
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_EXECUTE,
				Control: &plan.DataControl_Execute{
					Execute: execute,
				},
			},
		},
	}, nil
}

func buildDeallocate(stmt *tree.Deallocate, _ CompilerContext) (*Plan, error) {
	deallocate := &plan.Deallocate{
		Name: string(stmt.Name),
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_DEALLOCATE,
				Control: &plan.DataControl_Deallocate{
					Deallocate: deallocate,
				},
			},
		},
	}, nil
}

func buildSetVariables(stmt *tree.SetVar, ctx CompilerContext) (*Plan, error) {
	var err error
	items := make([]*plan.SetVariablesItem, len(stmt.Assignments))

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	binder := NewWhereBinder(builder, &BindContext{})

	for idx, assignment := range stmt.Assignments {
		item := &plan.SetVariablesItem{
			System: assignment.System,
			Global: assignment.Global,
			Name:   assignment.Name,
		}
		if assignment.Value == nil {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "Set statement has no value")
		}
		item.Value, err = binder.baseBindExpr(assignment.Value, 0, true)
		if err != nil {
			return nil, err
		}
		if assignment.Reserved != nil {
			item.Reserved, err = binder.baseBindExpr(assignment.Reserved, 0, true)
			if err != nil {
				return nil, err
			}
		}
		items[idx] = item
	}

	setVariables := &plan.SetVariables{
		Items: items,
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_SET_VARIABLES,
				Control: &plan.DataControl_SetVariables{
					SetVariables: setVariables,
				},
			},
		},
	}, nil
}

func buildCreateAccount(stmt *tree.CreateAccount, ctx CompilerContext, isPrepareStmt bool) (*Plan, error) {
	params := []tree.Expr{
		stmt.Name,
		stmt.AuthOption.AdminName,
		stmt.AuthOption.IdentifiedType.Str,
	}
	paramTypes, err := getParamTypes(params, ctx, isPrepareStmt)
	if err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_CREATE_ACCOUNT,
				Control: &plan.DataControl_Other{
					Other: &plan.OtherDCL{
						ParamTypes: paramTypes,
					},
				},
			},
		},
	}, nil
}

func buildAlterAccount(stmt *tree.AlterAccount, ctx CompilerContext, isPrepareStmt bool) (*Plan, error) {
	params := []tree.Expr{
		stmt.Name,
		stmt.AuthOption.AdminName,
		stmt.AuthOption.IdentifiedType.Str,
	}
	paramTypes, err := getParamTypes(params, ctx, isPrepareStmt)
	if err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_ALTER_ACCOUNT,
				Control: &plan.DataControl_Other{
					Other: &plan.OtherDCL{
						ParamTypes: paramTypes,
					},
				},
			},
		},
	}, nil
}

func buildDropAccount(stmt *tree.DropAccount, ctx CompilerContext, isPrepareStmt bool) (*Plan, error) {
	params := []tree.Expr{
		stmt.Name,
	}
	paramTypes, err := getParamTypes(params, ctx, isPrepareStmt)
	if err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Dcl{
			Dcl: &plan.DataControl{
				DclType: plan.DataControl_DROP_ACCOUNT,
				Control: &plan.DataControl_Other{
					Other: &plan.OtherDCL{
						ParamTypes: paramTypes,
					},
				},
			},
		},
	}, nil
}
