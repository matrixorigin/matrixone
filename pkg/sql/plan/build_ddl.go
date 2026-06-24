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
	"encoding/json"
	"fmt"
	"iter"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/externalwrite"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	mokafka "github.com/matrixorigin/matrixone/pkg/stream/adapter/kafka"
)

func genDynamicTableDef(ctx CompilerContext, stmt *tree.Select) (*plan.TableDef, error) {
	var tableDef plan.TableDef

	// check view statement
	var stmtPlan *Plan
	var err error
	switch s := stmt.Select.(type) {
	case *tree.ParenSelect:
		stmtPlan, err = bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, s.Select, false, true)
		if err != nil {
			return nil, err
		}
	default:
		stmtPlan, err = bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, stmt, false, true)
		if err != nil {
			return nil, err
		}
	}

	query := stmtPlan.GetQuery()
	cols := make([]*plan.ColDef, len(query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList))
	for idx, expr := range query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList {
		cols[idx] = &plan.ColDef{
			Name: strings.ToLower(query.Headings[idx]),
			Alg:  plan.CompressType_Lz4,
			Typ:  expr.Typ,
			Default: &plan.Default{
				NullAbility:  !expr.Typ.NotNullable,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	tableDef.Cols = cols

	viewData, err := json.Marshal(ViewData{
		Stmt:            ctx.GetRootSql(),
		DefaultDatabase: ctx.DefaultDatabase(),
		SecurityType:    getViewSecurityTypeFromContext(ctx),
	})
	if err != nil {
		return nil, err
	}
	tableDef.ViewSql = &plan.ViewDef{
		View: string(viewData),
	}
	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: ctx.GetRootSql(),
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})

	return &tableDef, nil
}

func getViewSecurityTypeFromContext(ctx CompilerContext) string {
	securityType := ""
	val, err := ctx.ResolveVariable("view_security_type", true, false)
	if err == nil {
		if s, ok := val.(string); ok {
			securityType = s
		}
	}
	securityType = strings.TrimSpace(strings.ToUpper(securityType))
	if securityType == "INVOKER" {
		return "INVOKER"
	}
	// Default to DEFINER to match SQL SECURITY behavior.
	return "DEFINER"
}

func genViewTableDef(ctx CompilerContext, stmt *tree.Select) (*plan.TableDef, error) {
	var tableDef plan.TableDef

	// check view statement
	var stmtPlan *Plan
	var err error
	switch s := stmt.Select.(type) {
	case *tree.ParenSelect:
		stmtPlan, err = bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, s.Select, false, true)
		if err != nil {
			return nil, err
		}
	default:
		stmtPlan, err = bindAndOptimizeSelectQuery(plan.Query_SELECT, ctx, stmt, false, true)
		if err != nil {
			return nil, err
		}
	}

	query := stmtPlan.GetQuery()
	cols := make([]*plan.ColDef, len(query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList))
	for idx, expr := range query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList {
		cols[idx] = &plan.ColDef{
			Name: strings.ToLower(query.Headings[idx]),
			Alg:  plan.CompressType_Lz4,
			Typ:  expr.Typ,
			Default: &plan.Default{
				NullAbility:  !expr.Typ.NotNullable,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	tableDef.Cols = cols

	// Check alter and change the viewsql.
	viewSql := ctx.GetRootSql()
	// remove sql hint
	viewSql = cleanHint(viewSql)
	if len(viewSql) != 0 {
		if viewSql[0] == 'A' {
			viewSql = strings.Replace(viewSql, "ALTER", "CREATE", 1)
		}
		if viewSql[0] == 'a' {
			viewSql = strings.Replace(viewSql, "alter", "create", 1)
		}
	}

	viewData, err := json.Marshal(ViewData{
		Stmt:            viewSql,
		DefaultDatabase: ctx.DefaultDatabase(),
		SecurityType:    getViewSecurityTypeFromContext(ctx),
	})
	if err != nil {
		return nil, err
	}
	tableDef.ViewSql = &plan.ViewDef{
		View: string(viewData),
	}
	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemViewRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: ctx.GetRootSql(),
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})

	return &tableDef, nil
}

func genAsSelectCols(ctx CompilerContext, stmt *tree.Select) ([]*ColDef, error) {
	var err error
	var rootId int32
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	bindCtx := NewBindContext(builder, nil)

	getTblAndColName := func(relPos, colPos int32) (string, string) {
		name := builder.nameByColRef[[2]int32{relPos, colPos}]
		// name pattern: tableName.colName
		splits := strings.Split(name, ".")
		if len(splits) < 2 {
			return "", ""
		}
		return splits[0], splits[1]
	}

	if s, ok := stmt.Select.(*tree.ParenSelect); ok {
		stmt = s.Select
	}
	if rootId, err = builder.bindSelect(stmt, bindCtx, true); err != nil {
		return nil, err
	}
	rootNode := builder.qry.Nodes[rootId]

	cols := make([]*plan.ColDef, len(rootNode.ProjectList))
	for i, expr := range rootNode.ProjectList {
		defaultVal := ""
		typ := &expr.Typ
		switch e := expr.Expr.(type) {
		case *plan.Expr_Col:
			tblName, colName := getTblAndColName(e.Col.RelPos, e.Col.ColPos)
			if binding, ok := bindCtx.bindingByTable[tblName]; ok {
				defaultVal = binding.defaults[binding.colIdByName[colName]]
			}
		case *plan.Expr_F:
			// enum
			if e.F.Func.ObjName == moEnumCastIndexToValueFun || e.F.Func.ObjName == moSetCastIndexToValueFun {
				// cast_index_to_value('apple,banana,orange', cast(col_name as T_uint16))
				colRef := e.F.Args[1].Expr.(*plan.Expr_Col).Col
				tblName, colName := getTblAndColName(colRef.RelPos, colRef.ColPos)
				if binding, ok := bindCtx.bindingByTable[tblName]; ok {
					typ = binding.types[binding.colIdByName[colName]]
				}
			}
		}

		cols[i] = &plan.ColDef{
			Name: strings.ToLower(bindCtx.headings[i]),
			Alg:  plan.CompressType_Lz4,
			Typ:  *typ,
			Default: &plan.Default{
				NullAbility:  !expr.Typ.NotNullable,
				Expr:         nil,
				OriginString: defaultVal,
			},
		}
	}
	return cols, nil
}

func buildCreateSource(stmt *tree.CreateSource, ctx CompilerContext) (*Plan, error) {
	streamName := string(stmt.SourceName.ObjectName)
	createStream := &plan.CreateTable{
		IfNotExists: stmt.IfNotExists,
		TableDef: &TableDef{
			TableType: catalog.SystemSourceRel,
			Name:      streamName,
		},
	}
	if len(stmt.SourceName.SchemaName) == 0 {
		createStream.Database = ctx.DefaultDatabase()
	} else {
		createStream.Database = string(stmt.SourceName.SchemaName)
	}

	if sub, err := ctx.GetSubscriptionMeta(createStream.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create stream in subscription database")
	}

	if err := buildSourceDefs(stmt, ctx, createStream); err != nil {
		return nil, err
	}

	var properties []*plan.Property
	properties = append(properties, &plan.Property{
		Key:   catalog.SystemRelAttr_Kind,
		Value: catalog.SystemSourceRel,
	})
	configs := make(map[string]interface{})
	for _, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.CreateSourceWithOption:
			key := strings.ToLower(string(opt.Key))
			val := opt.Val.(*tree.NumVal).String()
			properties = append(properties, &plan.Property{
				Key:   key,
				Value: val,
			})
			configs[key] = val
		}
	}
	if err := mokafka.ValidateConfig(context.Background(), configs, mokafka.NewKafkaAdapter); err != nil {
		return nil, err
	}
	createStream.TableDef.Defs = append(createStream.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_TABLE,
				Definition: &plan.DataDefinition_CreateTable{
					CreateTable: createStream,
				},
			},
		},
	}, nil
}

func buildSourceDefs(stmt *tree.CreateSource, ctx CompilerContext, createStream *plan.CreateTable) error {
	colMap := make(map[string]*ColDef)
	for _, item := range stmt.Defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			colName := def.Name.ColName()
			colNameOrigin := def.Name.ColNameOrigin()
			if _, ok := colMap[colName]; ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "duplicate column name: %s", colNameOrigin)
			}
			colType, err := getTypeFromAst(ctx.GetContext(), def.Type)
			if err != nil {
				return err
			}
			if err = applyColumnAttributesToType(ctx.GetContext(), &colType, def.Attributes); err != nil {
				return err
			}
			if colType.Id == int32(types.T_char) || colType.Id == int32(types.T_varchar) ||
				colType.Id == int32(types.T_binary) || colType.Id == int32(types.T_varbinary) {
				if colType.GetWidth() > types.MaxStringSize {
					return moerr.NewInvalidInputf(ctx.GetContext(), "string width (%d) is too long", colType.GetWidth())
				}
			}
			col := &ColDef{
				Name:       colName,
				OriginName: colNameOrigin,
				Alg:        plan.CompressType_Lz4,
				Typ:        colType,
			}
			colMap[colName] = col
			for _, attr := range def.Attributes {
				switch a := attr.(type) {
				case *tree.AttributeKey:
					col.Primary = true
				case *tree.AttributeHeader:
					col.Header = a.Key
				case *tree.AttributeHeaders:
					col.Headers = true
				}
			}
			createStream.TableDef.Cols = append(createStream.TableDef.Cols, col)
		case *tree.CreateSourceWithOption:
		default:
			return moerr.NewNYIf(ctx.GetContext(), "stream def: '%v'", def)
		}
	}
	return nil
}

func buildCreateView(stmt *tree.CreateView, ctx CompilerContext) (*Plan, error) {
	viewName := stmt.Name.ObjectName

	createView := &plan.CreateView{
		Replace:     stmt.Replace,
		IfNotExists: stmt.IfNotExists,
		TableDef: &TableDef{
			Name: string(viewName),
		},
	}

	// get database name
	if len(stmt.Name.SchemaName) == 0 {
		createView.Database = ""
	} else {
		createView.Database = string(stmt.Name.SchemaName)
	}
	if len(createView.Database) == 0 {
		createView.Database = ctx.DefaultDatabase()
	}

	snapshot := &Snapshot{TS: &timestamp.Timestamp{}}
	if IsSnapshotValid(ctx.GetSnapshot()) {
		snapshot = ctx.GetSnapshot()
	}

	if sub, err := ctx.GetSubscriptionMeta(createView.Database, snapshot); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create view in subscription database")
	}

	tableDef, err := genViewTableDef(ctx, stmt.AsSource)
	if err != nil {
		return nil, err
	}

	createView.TableDef.Cols = tableDef.Cols
	createView.TableDef.ViewSql = tableDef.ViewSql
	createView.TableDef.Defs = tableDef.Defs

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_VIEW,
				Definition: &plan.DataDefinition_CreateView{
					CreateView: createView,
				},
			},
		},
	}, nil
}

func buildSequenceTableDef(stmt *tree.CreateSequence, ctx CompilerContext, cs *plan.CreateSequence) error {
	// Sequence table got 1 row and 7 col
	// sequence_value, maxvalue,minvalue,startvalue,increment,cycleornot,iscalled.
	cols := make([]*plan.ColDef, len(Sequence_cols_name))

	typ, err := getTypeFromAst(ctx.GetContext(), stmt.Type)
	if err != nil {
		return err
	}
	for i := range cols {
		if i == 4 {
			break
		}
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ:  typ,
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	cols[4] = &plan.ColDef{
		Name: Sequence_cols_name[4],
		Alg:  plan.CompressType_Lz4,
		Typ: plan.Type{
			Id:    int32(types.T_int64),
			Width: 0,
			Scale: 0,
		},
		Primary: true,
		Default: &plan.Default{
			NullAbility:  true,
			Expr:         nil,
			OriginString: "",
		},
	}
	cs.TableDef.Pkey = &PrimaryKeyDef{
		Names:       []string{Sequence_cols_name[4]},
		PkeyColName: Sequence_cols_name[4],
	}
	for i := 5; i <= 6; i++ {
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_bool),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}

	cs.TableDef.Cols = cols

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemSequenceRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: ctx.GetRootSql(),
		},
	}

	cs.TableDef.Defs = append(cs.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})
	return nil
}

func buildAlterSequenceTableDef(stmt *tree.AlterSequence, ctx CompilerContext, as *plan.AlterSequence) error {
	// Sequence table got 1 row and 7 col
	// sequence_value, maxvalue,minvalue,startvalue,increment,cycleornot,iscalled.
	cols := make([]*plan.ColDef, len(Sequence_cols_name))

	var typ plan.Type
	var err error
	if stmt.Type == nil {
		_, tableDef, err := ctx.Resolve(as.GetDatabase(), as.TableDef.Name, nil)
		if err != nil {
			return err
		}
		if tableDef == nil {
			return moerr.NewInvalidInputf(ctx.GetContext(), "no such sequence %s", as.TableDef.Name)
		} else {
			typ = tableDef.Cols[0].Typ
		}
	} else {
		typ, err = getTypeFromAst(ctx.GetContext(), stmt.Type.Type)
		if err != nil {
			return err
		}
	}

	for i := range cols {
		if i == 4 {
			break
		}
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ:  typ,
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	cols[4] = &plan.ColDef{
		Name: Sequence_cols_name[4],
		Alg:  plan.CompressType_Lz4,
		Typ: plan.Type{
			Id:    int32(types.T_int64),
			Width: 0,
			Scale: 0,
		},
		Primary: true,
		Default: &plan.Default{
			NullAbility:  true,
			Expr:         nil,
			OriginString: "",
		},
	}
	as.TableDef.Pkey = &PrimaryKeyDef{
		Names:       []string{Sequence_cols_name[4]},
		PkeyColName: Sequence_cols_name[4],
	}
	for i := 5; i <= 6; i++ {
		cols[i] = &plan.ColDef{
			Name: Sequence_cols_name[i],
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_bool),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}

	as.TableDef.Cols = cols

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemSequenceRel,
		},
		{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: ctx.GetRootSql(),
		},
	}

	as.TableDef.Defs = append(as.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})
	return nil

}

func buildDropSequence(stmt *tree.DropSequence, ctx CompilerContext) (*Plan, error) {
	dropSequence := &plan.DropSequence{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, moerr.NewNotSupportedf(ctx.GetContext(), "drop multiple (%d) Sequence in one statement", len(stmt.Names))
	}
	dropSequence.Database = string(stmt.Names[0].SchemaName)
	if dropSequence.Database == "" {
		dropSequence.Database = ctx.DefaultDatabase()
	}
	dropSequence.Table = string(stmt.Names[0].ObjectName)

	obj, tableDef, err := ctx.Resolve(dropSequence.Database, dropSequence.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil || tableDef.TableType != catalog.SystemSequenceRel {
		if !dropSequence.IfExists {
			return nil, moerr.NewNoSuchSequence(ctx.GetContext(), dropSequence.Database, dropSequence.Table)
		}
		dropSequence.Table = ""
	}
	if obj != nil && obj.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot drop sequence in subscription database")
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_SEQUENCE,
				Definition: &plan.DataDefinition_DropSequence{
					DropSequence: dropSequence,
				},
			},
		},
	}, nil
}

func buildAlterSequence(stmt *tree.AlterSequence, ctx CompilerContext) (*Plan, error) {
	if stmt.Type == nil && stmt.IncrementBy == nil && stmt.MaxValue == nil && stmt.MinValue == nil && stmt.StartWith == nil && stmt.Cycle == nil {
		return nil, moerr.NewSyntaxErrorf(ctx.GetContext(), "synatx error, %s has nothing to alter", string(stmt.Name.ObjectName))
	}

	alterSequence := &plan.AlterSequence{
		IfExists: stmt.IfExists,
		TableDef: &TableDef{
			Name: string(stmt.Name.ObjectName),
		},
	}
	// Get database name.
	if len(stmt.Name.SchemaName) == 0 {
		alterSequence.Database = ctx.DefaultDatabase()
	} else {
		alterSequence.Database = string(stmt.Name.SchemaName)
	}

	if sub, err := ctx.GetSubscriptionMeta(alterSequence.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter sequence in subscription database")
	}

	err := buildAlterSequenceTableDef(stmt, ctx, alterSequence)
	if err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_SEQUENCE,
				Definition: &plan.DataDefinition_AlterSequence{
					AlterSequence: alterSequence,
				},
			},
		},
	}, nil
}

func buildCreateSequence(stmt *tree.CreateSequence, ctx CompilerContext) (*Plan, error) {
	createSequence := &plan.CreateSequence{
		IfNotExists: stmt.IfNotExists,
		TableDef: &TableDef{
			Name: string(stmt.Name.ObjectName),
		},
	}
	// Get database name.
	if len(stmt.Name.SchemaName) == 0 {
		createSequence.Database = ctx.DefaultDatabase()
	} else {
		createSequence.Database = string(stmt.Name.SchemaName)
	}

	if sub, err := ctx.GetSubscriptionMeta(createSequence.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create sequence in subscription database")
	}

	err := buildSequenceTableDef(stmt, ctx, createSequence)
	if err != nil {
		return nil, err
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_SEQUENCE,
				Definition: &plan.DataDefinition_CreateSequence{
					CreateSequence: createSequence,
				},
			},
		},
	}, nil
}

// preserveIndexSessionVars re-attaches algo_params.session_vars from the source
// table def onto a freshly-built CLONE/LIKE plan's matching index defs.
// ConstructCreateTableSQL rebuilds each index from its flat options only and
// drops session_vars (it isn't an index option), so without this the clone (the
// restore mechanism) loses the captured build-time vars — e.g.
// kmeans_train_percent — that the background restore reindex needs to reproduce
// the original build instead of falling back to defaults.
func preserveIndexSessionVars(p *Plan, src *plan.TableDef) error {
	if p == nil || src == nil {
		return nil
	}
	ct := p.GetDdl().GetCreateTable()
	if ct == nil || ct.GetTableDef() == nil {
		return nil
	}
	for _, ni := range ct.GetTableDef().Indexes {
		for _, si := range src.Indexes {
			if si.IndexName != ni.IndexName || si.IndexAlgoTableType != ni.IndexAlgoTableType {
				continue
			}
			sv, err := catalog.IndexParamsSessionVars(si.IndexAlgoParams)
			if err != nil {
				return err
			}
			if len(sv) == 0 {
				break // source carries no session_vars — nothing to preserve
			}
			flat, err := catalog.IndexParamsStringToMap(ni.IndexAlgoParams)
			if err != nil {
				return err
			}
			merged, err := catalog.IndexParamsMapToJsonStringWithSessionVars(flat, sv)
			if err != nil {
				return err
			}
			ni.IndexAlgoParams = merged
			break
		}
	}
	return nil
}

func buildCreateTable(
	ctx CompilerContext,
	stmt *tree.CreateTable,
	cloneStmt *tree.CloneTable,
) (*Plan, error) {

	if stmt.IsAsLike {
		var err error
		oldTable := stmt.LikeTableName
		newTable := stmt.Table
		tblName := formatStr(string(oldTable.ObjectName))
		dbName := formatStr(string(oldTable.SchemaName))

		snapshot := ctx.GetSnapshot()

		if dbName, err = databaseIsValid(getSuitableDBName(dbName, ""), ctx, snapshot); err != nil {
			return nil, err
		}

		// check if the database is a subscription
		sub, err := ctx.GetSubscriptionMeta(dbName, snapshot)
		if err != nil {
			return nil, err
		}
		if sub != nil {
			ctx.SetQueryingSubscription(sub)
			defer func() {
				ctx.SetQueryingSubscription(nil)
			}()
		}

		_, tableDef, err := ctx.Resolve(dbName, tblName, snapshot)
		if err != nil {
			return nil, err
		}
		if tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
		}
		// TODO WHY?
		if tableDef.TableType == catalog.SystemViewRel || tableDef.TableType == catalog.SystemExternalRel {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "%s.%s is not BASE TABLE", dbName, tblName)
		}

		tableDef.Name = string(newTable.ObjectName)
		tableDef.DbName = string(newTable.SchemaName)
		if len(tableDef.DbName) == 0 {
			tableDef.DbName = ctx.DefaultDatabase()
		}
		tableDef.IsTemporary = stmt.Temporary

		_, newStmt, err := ConstructCreateTableSQL(ctx, tableDef, snapshot, true, cloneStmt)
		if err != nil {
			return nil, err
		}
		if stmtLike, ok := newStmt.(*tree.CreateTable); ok {
			p, err := buildCreateTable(ctx, stmtLike, nil)
			if err != nil {
				return nil, err
			}
			// ConstructCreateTableSQL above rebuilds each index from its flat
			// options only (session_vars is not an index option), so re-attach the
			// source's algo_params.session_vars onto the clone — otherwise the
			// restore reindex loses the captured build-time vars (e.g.
			// kmeans_train_percent) and falls back to defaults.
			if err := preserveIndexSessionVars(p, tableDef); err != nil {
				return nil, err
			}
			return p, nil
		}

		return nil, moerr.NewInternalError(ctx.GetContext(), "rewrite for create table like failed")
	}

	createTable := &plan.CreateTable{
		IfNotExists: stmt.IfNotExists,
		Temporary:   stmt.Temporary,
		TableDef: &TableDef{
			Name: string(stmt.Table.ObjectName),
		},
	}

	if stmt.PartitionOption != nil {
		createTable.RawSQL = tree.StringWithOpts(
			stmt,
			dialect.MYSQL,
			tree.WithQuoteIdentifier(),
			tree.WithSingleQuoteString(),
		)
		createTable.TableDef.FeatureFlag |= features.Partitioned
	}

	// get database name
	if len(stmt.Table.SchemaName) == 0 {
		createTable.Database = ctx.DefaultDatabase()
	} else {
		createTable.Database = string(stmt.Table.SchemaName)
	}

	if stmt.Temporary && stmt.PartitionOption != nil {
		return nil, moerr.NewPartitionNoTemporary(ctx.GetContext())
	}

	if sub, err := ctx.GetSubscriptionMeta(createTable.Database, nil); err != nil {
		return nil, err
	} else if sub != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create table in subscription database")
	}

	// set tableDef
	var err error
	if stmt.IsDynamicTable {
		tableDef, err := genDynamicTableDef(ctx, stmt.AsSource)
		if err != nil {
			return nil, err
		}

		createTable.TableDef.Cols = tableDef.Cols
		// createTable.TableDef.ViewSql = tableDef.ViewSql
		// createTable.TableDef.Defs = tableDef.Defs
	}

	var asSelectCols []*ColDef
	if stmt.IsAsSelect {
		if asSelectCols, err = genAsSelectCols(ctx, stmt.AsSource); err != nil {
			return nil, err
		}
	}

	if err = buildTableDefs(stmt, ctx, createTable, asSelectCols); err != nil {
		return nil, err
	}

	v, ok := getAutoIncrementOffsetFromVariables(ctx)
	if ok {
		createTable.TableDef.AutoIncrOffset = v
	}

	// set option
	for _, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.TableOptionProperties:
			properties := make([]*plan.Property, len(opt.Preperties))
			for idx, property := range opt.Preperties {
				properties[idx] = &plan.Property{
					Key:   property.Key,
					Value: property.Value,
				}
			}
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: properties,
					},
				},
			})
		// todo confirm: option data store like this?
		case *tree.TableOptionComment:
			if getNumOfCharacters(opt.Comment) > maxLengthOfTableComment {
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "comment for field '%s' is too long", createTable.TableDef.Name)
			}

			properties := []*plan.Property{
				{
					Key:   catalog.SystemRelAttr_Comment,
					Value: opt.Comment,
				},
			}
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: properties,
					},
				},
			})
		case *tree.TableOptionAutoIncrement:
			if opt.Value != 0 {
				createTable.TableDef.AutoIncrOffset = opt.Value - 1
			}

		// these table options is not support in plan
		// case *tree.TableOptionEngine, *tree.TableOptionSecondaryEngine, *tree.TableOptionCharset,
		// 	*tree.TableOptionCollate, *tree.TableOptionAutoIncrement, *tree.TableOptionComment,
		// 	*tree.TableOptionAvgRowLength, *tree.TableOptionChecksum, *tree.TableOptionCompression,
		// 	*tree.TableOptionConnection, *tree.TableOptionPassword, *tree.TableOptionKeyBlockSize,
		// 	*tree.TableOptionMaxRows, *tree.TableOptionMinRows, *tree.TableOptionDelayKeyWrite,
		// 	*tree.TableOptionRowFormat, *tree.TableOptionStatsPersistent, *tree.TableOptionStatsAutoRecalc,
		// 	*tree.TableOptionPackKeys, *tree.TableOptionTablespace, *tree.TableOptionDataDirectory,
		// 	*tree.TableOptionIndexDirectory, *tree.TableOptionStorageMedia, *tree.TableOptionStatsSamplePages,
		// 	*tree.TableOptionUnion, *tree.TableOptionEncryption:
		// 	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
		case *tree.TableOptionAUTOEXTEND_SIZE, *tree.TableOptionAvgRowLength,
			*tree.TableOptionCharset, *tree.TableOptionChecksum, *tree.TableOptionCollate, *tree.TableOptionCompression,
			*tree.TableOptionConnection, *tree.TableOptionDataDirectory, *tree.TableOptionIndexDirectory,
			*tree.TableOptionDelayKeyWrite, *tree.TableOptionEncryption, *tree.TableOptionEngine, *tree.TableOptionEngineAttr,
			*tree.TableOptionKeyBlockSize, *tree.TableOptionMaxRows, *tree.TableOptionMinRows, *tree.TableOptionPackKeys,
			*tree.TableOptionPassword, *tree.TableOptionRowFormat, *tree.TableOptionStartTrans, *tree.TableOptionSecondaryEngineAttr,
			*tree.TableOptionStatsAutoRecalc, *tree.TableOptionStatsPersistent, *tree.TableOptionStatsSamplePages,
			*tree.TableOptionTablespace, *tree.TableOptionUnion:

		default:
			return nil, moerr.NewNotSupportedf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
		}
	}

	// After handleTableOptions, so begin the partitions processing depend on TableDef
	if stmt.Param != nil {
		for i := 0; i < len(stmt.Param.Option); i += 2 {
			switch strings.ToLower(stmt.Param.Option[i]) {
			case "endpoint", "region", "access_key_id", "secret_access_key", "bucket", "filepath", "compression", "format", "jsondata", "provider", "role_arn", "external_id", "hive_partitioning", "hive_partition_columns", ExternalWriteFilePatternKey, CSVCommentKey:
			default:
				return nil, moerr.NewBadConfigf(ctx.GetContext(), "the keyword '%s' is not support", strings.ToLower(stmt.Param.Option[i]))
			}
		}

		if err := validateWriteFilePattern(ctx.GetContext(), stmt.Param, createTable.TableDef); err != nil {
			return nil, err
		}

		if err := validateAndSetHivePartitionOptions(ctx.GetContext(), stmt, createTable); err != nil {
			return nil, err
		}

		if err := InitNullMap(stmt.Param, ctx); err != nil {
			return nil, err
		}
		json_byte, err := json.Marshal(stmt.Param)
		if err != nil {
			return nil, err
		}
		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemExternalRel,
			},
			{
				Key:   catalog.SystemRelAttr_CreateSQL,
				Value: string(json_byte),
			},
		}
		createTable.TableDef.TableType = catalog.SystemExternalRel
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	} else {
		kind := catalog.SystemOrdinaryRel
		if stmt.IsClusterTable {
			kind = catalog.SystemClusterRel
		}
		// when create hidden talbe(like: auto_incr_table, index_table)， we set relKind to empty
		if catalog.IsHiddenTable(createTable.TableDef.Name) {
			kind = ""
		}
		fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
		stmt.Format(fmtCtx)
		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: kind,
			},
			{
				Key:   catalog.SystemRelAttr_CreateSQL,
				Value: ctx.GetRootSql(),
			},
		}
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	bindContext := NewBindContext(builder, nil)

	// set partition(unsupport now)
	if stmt.PartitionOption != nil {
		// Foreign keys are not yet supported in conjunction with partitioning
		// see: https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-14.html
		if len(createTable.TableDef.Fkeys) > 0 {
			return nil, moerr.NewErrForeignKeyOnPartitioned(ctx.GetContext())
		}

		nodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			Stats:       nil,
			ObjRef:      nil,
			TableDef:    createTable.TableDef,
			BindingTags: []int32{builder.genNewBindTag()},
		}, bindContext)

		err = builder.addBinding(nodeID, tree.AliasClause{}, bindContext)
		if err != nil {
			return nil, err
		}

		partitionBinder := NewPartitionBinder(builder, bindContext)
		createTable.TableDef.Partition, err = partitionBinder.buildPartitionDefs(ctx.GetContext(), stmt.PartitionOption)
		if err != nil {
			return nil, err
		}
	}

	if stmt.Temporary {
		createTable.TableDef.TableType = catalog.SystemTemporaryTable
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_TABLE,
				Definition: &plan.DataDefinition_CreateTable{
					CreateTable: createTable,
				},
			},
		},
	}, nil
}

func buildTableDefs(stmt *tree.CreateTable, ctx CompilerContext, createTable *plan.CreateTable, asSelectCols []*ColDef) error {
	// all below fields' key is lower case
	var primaryKeys []string
	colMap := make(map[string]*ColDef)
	defaultMap := make(map[string]string)
	uniqueIndexInfos := make([]*tree.UniqueIndex, 0)
	fullTextIndexInfos := make([]*tree.FullTextIndex, 0)
	secondaryIndexInfos := make([]*tree.Index, 0)
	fkDatasOfFKSelfRefer := make([]*FkData, 0)
	dedupFkName := make(UnorderedSet[string])

	if stmt.Param != nil {
		if err := rejectExternalTableInlineIndexes(ctx.GetContext(), stmt); err != nil {
			return err
		}
	}

	// Pre-scan all column definitions so that generated columns can reference
	// base columns defined later in the CREATE TABLE statement (forward reference).
	var allColDefs []*ColDef
	var isGeneratedCol []bool
	for _, item := range stmt.Defs {
		if def, ok := item.(*tree.ColumnTableDef); ok {
			cType, err := getTypeFromAst(ctx.GetContext(), def.Type)
			if err != nil {
				return err
			}
			isGen := false
			for _, attr := range def.Attributes {
				switch attr.(type) {
				case *tree.AttributeGeneratedAlways:
					isGen = true
				case *tree.AttributeAutoIncrement:
					cType.AutoIncr = true
				}
			}
			allColDefs = append(allColDefs, &ColDef{Name: def.Name.ColName(), Typ: cType})
			isGeneratedCol = append(isGeneratedCol, isGen)
		}
	}

	genColIdx := 0 // tracks the current column's position in allColDefs
	for _, item := range stmt.Defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			colType, err := getTypeFromAst(ctx.GetContext(), def.Type)
			if err != nil {
				return err
			}
			if err = applyColumnAttributesToType(ctx.GetContext(), &colType, def.Attributes); err != nil {
				return err
			}
			if colType.Id == int32(types.T_char) || colType.Id == int32(types.T_varchar) ||
				colType.Id == int32(types.T_binary) || colType.Id == int32(types.T_varbinary) {
				if colType.GetWidth() > types.MaxStringSize {
					return moerr.NewInvalidInputf(ctx.GetContext(), "string width (%d) is too long", colType.GetWidth())
				}
			}
			if colType.Id == int32(types.T_array_float32) || colType.Id == int32(types.T_array_float64) {
				if colType.GetWidth() > types.MaxArrayDimension {
					return moerr.NewInvalidInputf(ctx.GetContext(), "vector width (%d) is too long", colType.GetWidth())
				}
			}
			if colType.Id == int32(types.T_bit) {
				if colType.Width == 0 {
					colType.Width = 1
				}
				if colType.Width > types.MaxBitLen {
					return moerr.NewInvalidInputf(ctx.GetContext(), "bit width (%d) is too long (max = %d) ", colType.GetWidth(), types.MaxBitLen)
				}
			}
			var pks []string
			var comment string
			var auto_incr bool
			var isGenerated bool
			colName := def.Name.ColName()
			// only used in error message and ColDef.OriginName
			colNameOrigin := def.Name.ColNameOrigin()
			for _, attr := range def.Attributes {
				switch attribute := attr.(type) {
				case *tree.AttributeGeneratedAlways:
					isGenerated = true
				case *tree.AttributePrimaryKey, *tree.AttributeKey:
					if colType.GetId() == int32(types.T_blob) {
						return moerr.NewNotSupported(ctx.GetContext(), "blob type in primary key")
					}
					if colType.GetId() == int32(types.T_text) {
						return moerr.NewNotSupported(ctx.GetContext(), "text type in primary key")
					}
					if colType.GetId() == int32(types.T_datalink) {
						return moerr.NewNotSupported(ctx.GetContext(), "datalink type in primary key")
					}
					if colType.GetId() == int32(types.T_json) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("JSON column '%s' cannot be in primary key", colNameOrigin))
					}
					if colType.GetId() == int32(types.T_array_float32) || colType.GetId() == int32(types.T_array_float64) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("VECTOR column '%s' cannot be in primary key", colNameOrigin))
					}
					if isEnumPlanType(&colType) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("ENUM column '%s' cannot be in primary key", colNameOrigin))
					}
					if isSetPlanType(&colType) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("SET column '%s' cannot be in primary key", colNameOrigin))

					}
					if isGeometryPlanType(&colType) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("GEOMETRY column '%s' cannot be in primary key", colNameOrigin))
					}
					pks = append(pks, colName)
				case *tree.AttributeComment:
					comment = attribute.CMT.String()
					if getNumOfCharacters(comment) > maxLengthOfColumnComment {
						return moerr.NewInvalidInputf(ctx.GetContext(), "comment for column '%s' is too long", colNameOrigin)
					}
				case *tree.AttributeAutoIncrement:
					auto_incr = true
					if !types.T(colType.GetId()).IsInteger() {
						return moerr.NewNotSupported(ctx.GetContext(), "the auto_incr column is only support integer type now")
					}
				case *tree.AttributeUnique, *tree.AttributeUniqueKey:
					if isSetPlanType(&colType) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("SET column '%s' cannot be in unique index", colNameOrigin))

					}
					if isGeometryPlanType(&colType) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("GEOMETRY column '%s' cannot be in unique index", colNameOrigin))
					}
					uniqueIndexInfos = append(uniqueIndexInfos, &tree.UniqueIndex{
						KeyParts: []*tree.KeyPart{{ColName: def.Name}},
						Name:     colName,
					})
				}
			}
			if len(pks) > 0 {
				if len(primaryKeys) > 0 {
					return moerr.NewInvalidInput(ctx.GetContext(), "more than one primary key defined")
				}
				primaryKeys = pks
			}

			var defaultValue *plan.Default
			var onUpdateExpr *plan.OnUpdate
			var generatedCol *plan.GeneratedCol

			if isGenerated {
				// Build generated column expression using the full column list
				// so that base columns defined later can be referenced (forward reference).
				generatedCol, err = buildGeneratedExpr(def, colType, allColDefs, ctx.GetProcess())
				if err != nil {
					return err
				}
				// Self-reference is still invalid even though base-column forward references are allowed.
				if exprReferencesColumn(generatedCol.Expr, colName, allColDefs) {
					return moerr.NewInvalidInputf(ctx.GetContext(), "generated column '%s' cannot refer to itself", colNameOrigin)
				}
				// Validate: no forward reference to generated columns defined later
				if err := validateNoForwardGenRef(ctx.GetContext(), generatedCol.Expr, genColIdx, allColDefs, isGeneratedCol); err != nil {
					return err
				}
				// Generated columns preserve declared nullability but use no default expr for storage layer compatibility
				defaultValue = &plan.Default{
					NullAbility:  getColumnNullAbility(def),
					Expr:         nil,
					OriginString: "",
				}
			} else {
				defaultValue, err = buildDefaultExpr(def, colType, ctx.GetProcess())
				if err != nil {
					return err
				}
				if auto_incr && defaultValue.Expr != nil {
					return moerr.NewInvalidInputf(ctx.GetContext(), "invalid default value for '%s'", colNameOrigin)
				}

				onUpdateExpr, err = buildOnUpdate(def, colType, ctx.GetProcess())
				if err != nil {
					return err
				}
			}

			if !checkTableColumnNameValid(colName) {
				return moerr.NewInvalidInputf(ctx.GetContext(), "table column name '%s' is illegal and conflicts with internal keyword", colNameOrigin)
			}

			colType.AutoIncr = auto_incr
			col := &ColDef{
				Name:         colName,
				OriginName:   colNameOrigin,
				Alg:          plan.CompressType_Lz4,
				Typ:          colType,
				Default:      defaultValue,
				OnUpdate:     onUpdateExpr,
				Comment:      comment,
				GeneratedCol: generatedCol,
			}
			// if same name col in asSelectCols, overwrite it; add into colMap && createTable.TableDef.Cols later
			if idx := slices.IndexFunc(asSelectCols, func(c *ColDef) bool { return c.Name == col.Name }); idx != -1 {
				asSelectCols[idx] = col
			} else {
				colMap[colName] = col
				createTable.TableDef.Cols = append(createTable.TableDef.Cols, col)

				// get default val from ast node
				attrIdx := slices.IndexFunc(def.Attributes, func(a tree.ColumnAttribute) bool {
					_, ok := a.(*tree.AttributeDefault)
					return ok
				})
				if attrIdx != -1 {
					defaultAttr := def.Attributes[attrIdx].(*tree.AttributeDefault)
					fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
					defaultAttr.Format(fmtCtx)
					// defaultAttr.Format start with "default ", trim first 8 chars
					defaultMap[colName] = fmtCtx.String()[8:]
				} else {
					defaultMap[colName] = "NULL"
				}
			}
			genColIdx++
		case *tree.PrimaryKeyIndex:
			if len(primaryKeys) > 0 {
				return moerr.NewInvalidInput(ctx.GetContext(), "more than one primary key defined")
			}
			pksMap := map[string]bool{}
			for _, key := range def.KeyParts {
				name := key.ColName.ColName() // name of primary key column
				if _, ok := pksMap[name]; ok {
					return moerr.NewInvalidInputf(ctx.GetContext(), "duplicate column name '%s' in primary key", key.ColName.ColNameOrigin())
				}

				if col, ok := colMap[name]; ok {
					if err := checkIndexColumnSupportability(ctx.GetContext(), col, key, "primary"); err != nil {
						return err
					}
				}

				primaryKeys = append(primaryKeys, name)
				pksMap[name] = true
			}
		case *tree.Index:
			err := checkIndexKeypartSupportability(ctx.GetContext(), def.KeyParts)
			if err != nil {
				return err
			}
			if err = checkSpatialIndexColumnSupport(ctx, def, colMap); err != nil {
				return err
			}

			secondaryIndexInfos = append(secondaryIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.ColName()

				if col, ok := colMap[name]; ok {
					if err := checkIndexColumnSupportability(ctx.GetContext(), col, key, indexColumnCheckKind(def.KeyType)); err != nil {
						return err
					}
				}
			}
		case *tree.UniqueIndex:
			err := checkIndexKeypartSupportability(ctx.GetContext(), def.KeyParts)
			if err != nil {
				return err
			}

			uniqueIndexInfos = append(uniqueIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.ColName()

				if col, ok := colMap[name]; ok {
					if err := checkIndexColumnSupportability(ctx.GetContext(), col, key, "unique"); err != nil {
						return err
					}
				}
			}
		case *tree.FullTextIndex:
			err := checkIndexKeypartSupportability(ctx.GetContext(), def.KeyParts)
			if err != nil {
				return err
			}

			fullTextIndexInfos = append(fullTextIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.ColName()
				if col, ok := colMap[name]; ok {
					if col.Typ.Id == int32(types.T_blob) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("BLOB column '%s' cannot be in index", key.ColName.ColNameOrigin()))
					}
				}
			}
		case *tree.ForeignKey:
			if createTable.Temporary {
				return moerr.NewNotSupported(ctx.GetContext(), "add foreign key for temporary table")
			}
			if len(asSelectCols) != 0 {
				return moerr.NewNYI(ctx.GetContext(), "add foreign key in create table ... as select statement")
			}
			if IsFkBannedDatabase(createTable.Database) {
				return moerr.NewInternalErrorf(ctx.GetContext(), "can not create foreign keys in %s", createTable.Database)
			}
			err := adjustConstraintName(ctx.GetContext(), def)
			if err != nil {
				return err
			}
			fkData, err := getForeignKeyData(ctx, createTable.Database, createTable.TableDef, def)
			if err != nil {
				return err
			}

			if def.ConstraintSymbol != fkData.Def.Name {
				return moerr.NewInternalErrorf(ctx.GetContext(), "different fk name %s %s", def.ConstraintSymbol, fkData.Def.Name)
			}

			// dedup
			if dedupFkName.Find(fkData.Def.Name) {
				return moerr.NewInternalErrorf(ctx.GetContext(), "duplicate fk name %s", fkData.Def.Name)
			}
			dedupFkName.Insert(fkData.Def.Name)

			// only setups foreign key without forward reference
			if !fkData.ForwardRefer {
				createTable.FkDbs = append(createTable.FkDbs, fkData.ParentDbName)
				createTable.FkTables = append(createTable.FkTables, fkData.ParentTableName)
				createTable.FkCols = append(createTable.FkCols, fkData.Cols)
				createTable.TableDef.Fkeys = append(createTable.TableDef.Fkeys, fkData.Def)
			}

			createTable.UpdateFkSqls = append(createTable.UpdateFkSqls, fkData.UpdateSql)

			// save self reference foreign keys
			if fkData.IsSelfRefer {
				fkDatasOfFKSelfRefer = append(fkDatasOfFKSelfRefer, fkData)
			}
		case *tree.CheckIndex:
			// unsupport in plan. will support in next version.
			// return moerr.NewNYI(ctx.GetContext(), "table def: '%v'", def)
		default:
			return moerr.NewNYIf(ctx.GetContext(), "table def: '%v'", def)
		}
	}

	if stmt.IsAsSelect {
		// add as select cols
		for _, col := range asSelectCols {
			colMap[col.Name] = col
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, col)
		}

		// insert into new_table select default_val1, default_val2, ..., * from (select clause);
		var insertSqlBuilder strings.Builder
		insertSqlBuilder.WriteString(fmt.Sprintf("insert into `%s`.`%s` select ", createTable.Database, createTable.TableDef.Name))

		cols := createTable.TableDef.Cols
		firstCol := true
		for i := range cols {
			// insert default values if col[i] only in create clause
			if !slices.ContainsFunc(asSelectCols, func(c *ColDef) bool { return c.Name == cols[i].Name }) {
				if !firstCol {
					insertSqlBuilder.WriteString(", ")
				}
				insertSqlBuilder.WriteString(defaultMap[cols[i].Name])
				firstCol = false
			}
		}
		if !firstCol {
			insertSqlBuilder.WriteString(", ")
		}
		// add all cols from select clause
		insertSqlBuilder.WriteString("*")

		// from
		fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
		stmt.AsSource.Format(fmtCtx)
		insertSqlBuilder.WriteString(fmt.Sprintf(" from (%s)", restoreIntervalSyntaxForCTAS(fmtCtx.String())))

		createTable.CreateAsSelectSql = insertSqlBuilder.String()
	}

	// table must have one visible column
	if len(createTable.TableDef.Cols) == 0 {
		return moerr.NewTableMustHaveVisibleColumn(ctx.GetContext())
	}

	// add cluster table attribute
	if stmt.IsClusterTable {
		internal := defines.IsInternalExecutor(ctx.GetContext())
		_, has := colMap[util.GetClusterTableAttributeName()]
		if has && !internal {
			return moerr.NewInvalidInput(ctx.GetContext(), "the attribute account_id in the cluster table can not be defined directly by the user")
		}
		if !has {
			colType, err := getTypeFromAst(ctx.GetContext(), util.GetClusterTableAttributeType())
			if err != nil {
				return err
			}
			colDef := &ColDef{
				Name:    util.GetClusterTableAttributeName(),
				Alg:     plan.CompressType_Lz4,
				Typ:     colType,
				NotNull: true,
				Default: &plan.Default{
					Expr: &Expr{
						Expr: &plan.Expr_Lit{
							Lit: &Const{
								Isnull: false,
								Value:  &plan.Literal_U32Val{U32Val: catalog.System_Account},
							},
						},
						Typ: plan.Type{
							Id:          colType.Id,
							NotNullable: true,
						},
					},
					NullAbility: false,
				},
				Comment: "the account_id added by the mo",
			}
			colMap[util.GetClusterTableAttributeName()] = colDef
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
		}
	}

	pkeyName := ""
	// If the primary key is explicitly defined in the ddl statement
	if len(primaryKeys) > 0 {
		for _, primaryKey := range primaryKeys {
			if _, ok := colMap[primaryKey]; !ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' doesn't exist in table", primaryKey)
			}
			// Reject VIRTUAL generated columns in PRIMARY KEY
			col := colMap[primaryKey]
			if col.GeneratedCol != nil && !col.GeneratedCol.IsStored {
				return moerr.NewNotSupported(ctx.GetContext(),
					fmt.Sprintf("defining a virtual generated column '%s' as primary key", col.OriginName))
			}
		}
		if len(primaryKeys) == 1 {
			pkeyName = primaryKeys[0]
			for _, col := range createTable.TableDef.Cols {
				if col.Name == pkeyName {
					col.Primary = true
					createTable.TableDef.Pkey = &PrimaryKeyDef{
						Names:       primaryKeys,
						PkeyColName: pkeyName,
					}
					break
				}
			}
		} else {
			// pkeyName = util.BuildCompositePrimaryKeyColumnName(primaryKeys)
			pkeyName = catalog.CPrimaryKeyColName
			colDef := MakeHiddenColDefByName(pkeyName)
			colDef.Primary = true
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[pkeyName] = colDef

			pkeyDef := &PrimaryKeyDef{
				Names:       primaryKeys,
				PkeyColName: pkeyName,
				CompPkeyCol: colDef,
			}
			createTable.TableDef.Pkey = pkeyDef
		}
		for _, primaryKey := range primaryKeys {
			colMap[primaryKey].Default.NullAbility = false
			colMap[primaryKey].NotNull = true
		}
	} else {
		// If table does not have a explicit primary key in the ddl statement, a new hidden primary key column will be add,
		// which will not be sorted or used for any other purpose, but will only be used to add
		// locks to the Lock operator in pessimistic transaction mode.
		if !createTable.IsSystemExternalRel() {
			pkeyName = catalog.FakePrimaryKeyColName
			colDef := &ColDef{
				ColId:  uint64(len(createTable.TableDef.Cols)),
				Name:   pkeyName,
				Hidden: true,
				Typ: Type{
					Id:       int32(types.T_uint64),
					AutoIncr: true,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
				NotNull: true,
				Primary: true,
			}

			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[pkeyName] = colDef

			createTable.TableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{pkeyName},
				PkeyColName: pkeyName,
			}

			idx := len(createTable.TableDef.Cols) - 1
			// FIXME: due to the special treatment of insert and update for composite primary key, cluster-by, the
			// hidden primary key cannot be placed in the last column, otherwise it will cause the columns sent to
			// tae will not match the definition of schema, resulting in panic.
			if createTable.TableDef.ClusterBy != nil &&
				len(stmt.ClusterByOption.ColumnList) > 1 {
				// we must swap hide pk and cluster_by
				createTable.TableDef.Cols[idx-1], createTable.TableDef.Cols[idx] = createTable.TableDef.Cols[idx], createTable.TableDef.Cols[idx-1]
			}
		}
	}

	// handle cluster by keys
	if stmt.ClusterByOption != nil {
		if stmt.Temporary {
			return moerr.NewNotSupported(ctx.GetContext(), "cluster by with temporary table is not support")
		}
		if len(primaryKeys) > 0 {
			return moerr.NewNotSupported(ctx.GetContext(), "cluster by with primary key is not support")
		}
		lenClusterBy := len(stmt.ClusterByOption.ColumnList)
		var clusterByKeys []string
		for i := 0; i < lenClusterBy; i++ {
			colName := stmt.ClusterByOption.ColumnList[i].ColName()
			if _, ok := colMap[colName]; !ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' doesn't exist in table", stmt.ClusterByOption.ColumnList[i].ColNameOrigin())
			}
			clusterByKeys = append(clusterByKeys, colName)
		}

		if lenClusterBy == 1 {
			clusterByColName := clusterByKeys[0]
			for _, col := range createTable.TableDef.Cols {
				if col.Name == clusterByColName {
					col.ClusterBy = true
				}
			}

			createTable.TableDef.ClusterBy = &plan.ClusterByDef{
				Name: clusterByColName,
			}
		} else {
			clusterByColName := util.BuildCompositeClusterByColumnName(clusterByKeys)
			colDef := MakeHiddenColDefByName(clusterByColName)
			colDef.Default.NullAbility = true
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[clusterByColName] = colDef

			createTable.TableDef.ClusterBy = &plan.ClusterByDef{
				Name:         clusterByColName,
				CompCbkeyCol: colDef,
			}
		}
	}

	// check Constraint Name (include index/ unique)
	err := checkConstraintNames(uniqueIndexInfos, secondaryIndexInfos, ctx.GetContext())
	if err != nil {
		return err
	}

	// build index table
	if len(uniqueIndexInfos) != 0 {
		err = buildUniqueIndexTable(createTable, uniqueIndexInfos, colMap, pkeyName, ctx)
		if err != nil {
			return err
		}
	}
	if len(fullTextIndexInfos) != 0 {
		err = buildFullTextIndexTable(createTable, fullTextIndexInfos, colMap, nil, pkeyName, ctx)
		if err != nil {
			return err
		}
	}
	if len(secondaryIndexInfos) != 0 {
		err = buildSecondaryIndexDef(createTable, secondaryIndexInfos, colMap, nil, pkeyName, ctx)
		if err != nil {
			return err
		}
	}

	// process self reference foreign keys after colDefs and indexes are processed.
	if len(fkDatasOfFKSelfRefer) > 0 {
		// for fk self refer. the column id of the tableDef is not ready.
		// setup fake column id to distinguish the columns
		for i, def := range createTable.TableDef.Cols {
			def.ColId = uint64(i)
		}
		for _, selfRefer := range fkDatasOfFKSelfRefer {
			if err := checkFkColsAreValid(ctx, selfRefer, createTable.TableDef); err != nil {
				return err
			}
		}
	}

	skip := IsFkBannedDatabase(createTable.Database)
	if !skip {
		fks, err := GetFkReferredTo(ctx, createTable.Database, createTable.TableDef.Name)
		if err != nil {
			return err
		}
		// for fk forward reference. the column id of the tableDef is not ready.
		// setup fake column id to distinguish the columns
		for i, def := range createTable.TableDef.Cols {
			def.ColId = uint64(i)
		}
		for rkey, fkDefs := range fks {
			for constraintName, defs := range fkDefs {
				data, err := buildFkDataOfForwardRefer(ctx, constraintName, defs, createTable)
				if err != nil {
					return err
				}
				info := &plan.ForeignKeyInfo{
					Db:           rkey.Db,
					Table:        rkey.Tbl,
					ColsReferred: data.ColsReferred,
					Def:          data.Def,
				}
				createTable.FksReferToMe = append(createTable.FksReferToMe, info)
			}
		}
	}

	return nil
}

func restoreIntervalSyntaxForCTAS(sql string) string {
	var out strings.Builder
	for i := 0; i < len(sql); {
		if !strings.HasPrefix(strings.ToLower(sql[i:]), "interval(") {
			out.WriteByte(sql[i])
			i++
			continue
		}

		expr, unit, next, ok := parseIntervalCall(sql, i)
		if !ok || !isIntervalUnitToken(unit) {
			out.WriteByte(sql[i])
			i++
			continue
		}

		out.WriteString("interval ")
		out.WriteString(strings.TrimSpace(expr))
		out.WriteByte(' ')
		out.WriteString(strings.TrimSpace(unit))
		i = next
	}
	return out.String()
}

func parseIntervalCall(sql string, start int) (expr string, unit string, next int, ok bool) {
	const prefix = "interval("
	pos := start + len(prefix)
	depth := 1
	comma := -1
	inSingleQuote := false
	inDoubleQuote := false

	for pos < len(sql) {
		ch := sql[pos]
		switch ch {
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
		case '(':
			if !inSingleQuote && !inDoubleQuote {
				depth++
			}
		case ')':
			if !inSingleQuote && !inDoubleQuote {
				depth--
				if depth == 0 {
					if comma == -1 {
						return "", "", 0, false
					}
					return sql[start+len(prefix) : comma], sql[comma+1 : pos], pos + 1, true
				}
			}
		case ',':
			if !inSingleQuote && !inDoubleQuote && depth == 1 && comma == -1 {
				comma = pos
			}
		}
		pos++
	}
	return "", "", 0, false
}

func isIntervalUnitToken(unit string) bool {
	switch strings.ToLower(strings.Trim(strings.TrimSpace(unit), "`'\"")) {
	case "microsecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year",
		"second_microsecond", "minute_microsecond", "minute_second", "hour_microsecond",
		"hour_second", "hour_minute", "day_microsecond", "day_second", "day_minute",
		"day_hour", "year_month":
		return true
	default:
		return false
	}
}

func getRefAction(typ tree.ReferenceOptionType) plan.ForeignKeyDef_RefAction {
	switch typ {
	case tree.REFERENCE_OPTION_CASCADE:
		return plan.ForeignKeyDef_CASCADE
	case tree.REFERENCE_OPTION_NO_ACTION:
		return plan.ForeignKeyDef_NO_ACTION
	case tree.REFERENCE_OPTION_RESTRICT:
		return plan.ForeignKeyDef_RESTRICT
	case tree.REFERENCE_OPTION_SET_NULL:
		return plan.ForeignKeyDef_SET_NULL
	case tree.REFERENCE_OPTION_SET_DEFAULT:
		return plan.ForeignKeyDef_SET_DEFAULT
	default:
		return plan.ForeignKeyDef_RESTRICT
	}
}

// buildFullTextIndexTable routes each fulltext index through the
// fulltext plugin's plan.BuildFullTextIndexDefs hook (lifted body
// lives at pkg/fulltext/plugin/plan/schema.go). It keeps the
// batched, in-place-append signature the legacy callers used.
func buildFullTextIndexTable(createTable *plan.CreateTable, indexInfos []*tree.FullTextIndex, colMap map[string]*ColDef, existedIndexes []*plan.IndexDef, pkeyName string, ctx CompilerContext) error {
	p, ok := indexplugin.Get(catalog.MOIndexFullTextAlgo.ToString())
	if !ok {
		return moerr.NewInternalErrorNoCtx("fulltext plugin not registered")
	}
	for _, indexInfo := range indexInfos {
		idxDefs, tblDefs, err := p.Plan().BuildFullTextIndexDefs(
			ctx, indexInfo, colMap, existedIndexes, pkeyName,
		)
		if err != nil {
			return err
		}
		createTable.IndexTables = append(createTable.IndexTables, tblDefs...)
		createTable.TableDef.Indexes = append(createTable.TableDef.Indexes, idxDefs...)
	}
	return nil
}

func buildUniqueIndexTable(createTable *plan.CreateTable, indexInfos []*tree.UniqueIndex, colMap map[string]*ColDef, pkeyName string, ctx CompilerContext) error {
	for _, indexInfo := range indexInfos {
		indexDef := &plan.IndexDef{}
		indexDef.Unique = true

		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), true)

		if err != nil {
			return err
		}
		tableDef := &TableDef{
			Name: indexTableName,
		}
		indexParts := make([]string, 0)

		for _, keyPart := range indexInfo.KeyParts {
			nameOrigin := keyPart.ColName.ColNameOrigin()
			name := keyPart.ColName.ColName()
			if _, ok := colMap[name]; !ok {
				return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", nameOrigin)
			}
			if err := checkIndexColumnSupportability(ctx.GetContext(), colMap[name], keyPart, "unique"); err != nil {
				return err
			}

			indexParts = append(indexParts, name)
		}

		var keyName string
		if len(indexInfo.KeyParts) == 1 {
			keyName = catalog.IndexTableIndexColName
			keyPart := indexInfo.KeyParts[0]
			colName := keyPart.ColName.ColName()
			colDef := &ColDef{
				Name: keyName,
				Alg:  plan.CompressType_Lz4,
				Typ:  indexTableKeyTypeForSinglePart(colMap[colName], keyPart),
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
			tableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{keyName},
				PkeyColName: keyName,
			}
		} else {
			keyName = catalog.IndexTableIndexColName
			colDef := &ColDef{
				Name: keyName,
				Alg:  plan.CompressType_Lz4,
				Typ: Type{
					Id:    int32(types.T_varchar),
					Width: types.MaxVarcharLen,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
			tableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{keyName},
				PkeyColName: keyName,
			}
		}
		if pkeyName != "" {
			colDef := &ColDef{
				Name: catalog.IndexTablePrimaryColName,
				Alg:  plan.CompressType_Lz4,
				Typ: plan.Type{
					// don't copy auto increment
					Id:    colMap[pkeyName].Typ.Id,
					Width: colMap[pkeyName].Typ.Width,
					Scale: colMap[pkeyName].Typ.Scale,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
		}

		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemIndexRel,
			},
		}
		tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})

		// indexDef.IndexName = indexInfo.Name
		indexDef.IndexName = indexInfo.GetIndexName()
		indexDef.IndexTableName = indexTableName
		indexDef.Parts = indexParts
		indexDef.TableExist = true
		if indexInfo.IndexOption != nil {
			indexDef.Comment = indexInfo.IndexOption.Comment
		} else {
			indexDef.Comment = ""
		}
		indexDef.IndexAlgoParams, err = catalog.AddIndexPrefixLengthsToParams(indexDef.IndexAlgoParams, indexInfo.KeyParts)
		if err != nil {
			return err
		}
		createTable.IndexTables = append(createTable.IndexTables, tableDef)
		createTable.TableDef.Indexes = append(createTable.TableDef.Indexes, indexDef)
	}
	return nil
}

// buildIndexAlgoParams converts the parsed CREATE INDEX options into the
// algo_params JSON. Per-algo parameter rules live in each index plugin's
// Catalog().ParamsFromTree hook; non-plugin algorithms (btree/rtree/master)
// fall through to catalog (which produces no algo_params for them).
func buildIndexAlgoParams(indexInfo *tree.Index) (string, error) {
	if p, ok := indexplugin.Get(indexInfo.KeyType.ToString()); ok {
		res, err := p.Catalog().ParamsFromTree(indexInfo)
		if err != nil {
			return "", err
		}
		if len(res) == 0 {
			return "", nil
		}
		return catalog.IndexParamsMapToJsonString(res)
	}
	return catalog.IndexParamsToJsonString(indexInfo)
}

func buildSecondaryIndexDef(createTable *plan.CreateTable, indexInfos []*tree.Index, colMap map[string]*ColDef, existedIndexes []*plan.IndexDef, pkeyName string, ctx CompilerContext) (err error) {
	if len(pkeyName) == 0 {
		return moerr.NewInternalErrorNoCtx("primary key cannot be empty for secondary index")
	}

	for _, indexInfo := range indexInfos {
		err = checkIndexKeypartSupportability(ctx.GetContext(), indexInfo.KeyParts)
		if err != nil {
			return err
		}
		if err = checkSpatialIndexColumnSupport(ctx, indexInfo, colMap); err != nil {
			return err
		}

		var indexDef []*plan.IndexDef
		var tableDef []*TableDef
		switch indexInfo.KeyType {
		case tree.INDEX_TYPE_BTREE, tree.INDEX_TYPE_INVALID, tree.INDEX_TYPE_RTREE:
			indexDef, tableDef, err = buildRegularSecondaryIndexDef(ctx, indexInfo, colMap, pkeyName)
		case tree.INDEX_TYPE_MASTER:
			indexDef, tableDef, err = buildMasterSecondaryIndexDef(ctx, indexInfo, colMap, pkeyName)
		default:
			// Vector-index algorithms live in pkg/vectorindex/<algo>/plugin/plan
			// (BuildSecondaryIndexDefs). Any KeyType registered with the
			// plugin registry is supported; anything else is rejected.
			if p, ok := indexplugin.Get(indexInfo.KeyType.ToString()); ok {
				indexDef, tableDef, err = p.Plan().BuildSecondaryIndexDefs(ctx, indexInfo, colMap, existedIndexes, pkeyName)
			} else {
				return moerr.NewInvalidInputNoCtxf("unsupported index type: %s", indexInfo.KeyType.ToString())
			}
		}

		if err != nil {
			return err
		}
		createTable.IndexTables = append(createTable.IndexTables, tableDef...)
		createTable.TableDef.Indexes = append(createTable.TableDef.Indexes, indexDef...)

	}
	return nil
}

func checkSpatialIndexColumnSupport(ctx CompilerContext, indexInfo *tree.Index, colMap map[string]*ColDef) error {
	if indexInfo.KeyType != tree.INDEX_TYPE_RTREE {
		return nil
	}
	if len(indexInfo.KeyParts) != 1 {
		return moerr.NewNotSupported(ctx.GetContext(), "SPATIAL INDEX only supports a single GEOMETRY column")
	}

	name := indexInfo.KeyParts[0].ColName.ColName()
	nameOrigin := indexInfo.KeyParts[0].ColName.ColNameOrigin()
	col, ok := colMap[name]
	if !ok {
		return moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", nameOrigin)
	}
	if !isGeometryPlanType(&col.Typ) {
		return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("SPATIAL INDEX can only be created on GEOMETRY column '%s'", nameOrigin))
	}
	return nil
}

// buildMasterSecondaryIndexDef will create hidden internal table with schema.
//
// create table __mo_index_secondary_xxx (
//
//	__mo_index_idx_col varchar,
//	__mo_index_pri_col src_pk_type,
//	primary key __mo_index_idx_col,
//
// )
func buildMasterSecondaryIndexDef(ctx CompilerContext, indexInfo *tree.Index, colMap map[string]*ColDef, pkeyName string) ([]*plan.IndexDef, []*TableDef, error) {
	// 1. indexDef init
	indexDef := &plan.IndexDef{}
	indexDef.Unique = false

	// 2. tableDef init
	indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	tableDef := &TableDef{
		Name: indexTableName,
	}

	nameCount := make(map[string]int)
	// Note: Index Parts will store the ColName, as Parts is used to populate mo_index_table.
	// However, when inserting Index, we convert Parts (ie ColName) to ColIdx.
	indexParts := make([]string, 0)

	for _, keyPart := range indexInfo.KeyParts {
		nameOrigin := keyPart.ColName.ColNameOrigin()
		name := keyPart.ColName.ColName()
		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", nameOrigin)
		}
		if colMap[name].Typ.Id != int32(types.T_varchar) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("column '%s' is not varchar type.", nameOrigin))
		}
		indexParts = append(indexParts, name)
	}

	var keyName = catalog.MasterIndexTableIndexColName
	colDef := &ColDef{
		Name: keyName,
		Alg:  plan.CompressType_Lz4,
		Typ: Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		},
		Default: &plan.Default{
			NullAbility:  false,
			Expr:         nil,
			OriginString: "",
		},
	}
	tableDef.Cols = append(tableDef.Cols, colDef)
	tableDef.Pkey = &PrimaryKeyDef{
		Names:       []string{keyName},
		PkeyColName: keyName,
	}
	if pkeyName != "" {
		pkColDef := &ColDef{
			Name: catalog.MasterIndexTablePrimaryColName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				// don't copy auto increment
				Id:    colMap[pkeyName].Typ.Id,
				Width: colMap[pkeyName].Typ.Width,
				Scale: colMap[pkeyName].Typ.Scale,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, pkColDef)
	}

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemIndexRel,
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		}})

	if indexInfo.Name == "" {
		firstPart := indexInfo.KeyParts[0].ColName.ColName()
		nameCount[firstPart]++
		count := nameCount[firstPart]
		indexName := firstPart
		if count > 1 {
			indexName = firstPart + "_" + strconv.Itoa(count)
		}
		indexDef.IndexName = indexName
	} else {
		indexDef.IndexName = indexInfo.Name
	}

	indexDef.IndexTableName = indexTableName
	indexDef.Parts = indexParts
	indexDef.TableExist = true
	indexDef.IndexAlgo = indexInfo.KeyType.ToString()
	indexDef.IndexAlgoTableType = ""

	if indexInfo.IndexOption != nil {
		indexDef.Comment = indexInfo.IndexOption.Comment

		params, err := buildIndexAlgoParams(indexInfo)
		if err != nil {
			return nil, nil, err
		}
		indexDef.IndexAlgoParams = params
	} else {
		indexDef.Comment = ""
		indexDef.IndexAlgoParams = ""
	}
	indexDef.IndexAlgoParams, err = catalog.AddIndexPrefixLengthsToParams(indexDef.IndexAlgoParams, indexInfo.KeyParts)
	if err != nil {
		return nil, nil, err
	}
	return []*plan.IndexDef{indexDef}, []*TableDef{tableDef}, nil
}

// buildRegularSecondingIndexDef will create a hidden index table with schema
//
// when number of primary key == 1
//
// create table __mo_index_secondary_xxx (
//
//	__mo_index_idx_col src_pk_type,
//	__mo_index_pri_col src_pk_type,
//	primary key __mo_index_idx_col,
//
// )
//
// when number of primary key > 1
//
// create table __mo_index_secondary_xxx (
//
//	__mo_index_idx_col varchar,
//	__mo_index_pri_col src_pk_type,
//	primary key __mo_index_idx_col,
//
// )
func buildRegularSecondaryIndexDef(ctx CompilerContext, indexInfo *tree.Index, colMap map[string]*ColDef, pkeyName string) ([]*plan.IndexDef, []*TableDef, error) {

	// 1. indexDef init
	indexDef := &plan.IndexDef{}
	indexDef.Unique = false
	spatialIndex := indexInfo.KeyType == tree.INDEX_TYPE_RTREE

	// 2. tableDef init
	indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	tableDef := &TableDef{
		Name: indexTableName,
	}

	nameCount := make(map[string]int)
	indexParts := make([]string, 0)

	isPkAlreadyPresentInIndexParts := false
	for _, keyPart := range indexInfo.KeyParts {
		name := keyPart.ColName.ColName()
		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", keyPart.ColName.ColNameOrigin())
		}
		indexKind := "secondary"
		if indexInfo.KeyType == tree.INDEX_TYPE_RTREE {
			indexKind = "rtree"
		}
		if err := checkIndexColumnSupportability(ctx.GetContext(), colMap[name], keyPart, indexKind); err != nil {
			return nil, nil, err
		}

		if strings.Compare(name, pkeyName) == 0 || catalog.IsAlias(name) {
			isPkAlreadyPresentInIndexParts = true
		}
		indexParts = append(indexParts, name)
	}

	if !isPkAlreadyPresentInIndexParts {
		indexParts = append(indexParts, catalog.CreateAlias(pkeyName))
	}

	var keyName string
	if len(indexParts) == 1 {
		// This means indexParts only contains the primary key column
		keyName = catalog.IndexTableIndexColName
		colDef := &ColDef{
			Name: keyName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				// don't copy auto increment
				Id:    colMap[pkeyName].Typ.Id,
				Width: colMap[pkeyName].Typ.Width,
				Scale: colMap[pkeyName].Typ.Scale,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)
		tableDef.Pkey = &PrimaryKeyDef{
			Names:       []string{keyName},
			PkeyColName: keyName,
		}
	} else {
		keyName = catalog.IndexTableIndexColName
		idxColType := Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		}
		if spatialIndex {
			idxColType = colMap[indexParts[0]].Typ
		}
		colDef := &ColDef{
			Name: keyName,
			Alg:  plan.CompressType_Lz4,
			Typ:  idxColType,
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)
		tableDef.Pkey = &PrimaryKeyDef{
			Names:       []string{keyName},
			PkeyColName: keyName,
		}
	}
	if pkeyName != "" {
		colDef := &ColDef{
			Name: catalog.IndexTablePrimaryColName,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				// don't copy auto increment
				Id:    colMap[pkeyName].Typ.Id,
				Width: colMap[pkeyName].Typ.Width,
				Scale: colMap[pkeyName].Typ.Scale,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
		tableDef.Cols = append(tableDef.Cols, colDef)
		if spatialIndex {
			tableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{catalog.IndexTablePrimaryColName},
				PkeyColName: catalog.IndexTablePrimaryColName,
			}
		}
	}

	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemIndexRel,
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		}})

	if indexInfo.Name == "" {
		firstPart := indexInfo.KeyParts[0].ColName.ColName()
		nameCount[firstPart]++
		count := nameCount[firstPart]
		indexName := firstPart
		if count > 1 {
			indexName = firstPart + "_" + strconv.Itoa(count)
		}
		indexDef.IndexName = indexName
	} else {
		indexDef.IndexName = indexInfo.Name
	}

	indexDef.IndexTableName = indexTableName
	indexDef.Parts = indexParts
	indexDef.TableExist = true
	indexDef.IndexAlgo = indexInfo.KeyType.ToString()
	indexDef.IndexAlgoTableType = ""

	if indexInfo.IndexOption != nil {
		indexDef.Comment = indexInfo.IndexOption.Comment

		params, err := buildIndexAlgoParams(indexInfo)
		if err != nil {
			return nil, nil, err
		}
		indexDef.IndexAlgoParams = params
	} else {
		indexDef.Comment = ""
		indexDef.IndexAlgoParams = ""
	}
	indexDef.IndexAlgoParams, err = catalog.AddIndexPrefixLengthsToParams(indexDef.IndexAlgoParams, indexInfo.KeyParts)
	if err != nil {
		return nil, nil, err
	}
	return []*plan.IndexDef{indexDef}, []*TableDef{tableDef}, nil
}

// buildIvfFlatSecondIndexDef create three internal tables
//
// with the following schemas,
//
// create __mo_secondary_metadata (
//	__mo_index_key varchar,
//	__mo_index_val varhcar,
// 	primary key __mo_index_key,
// )
//
// create __mo_secondary_centroids (
//	__mo_index_centroid_version bigint,
//	__mo_index_centroid_id bigint,
//	__mo_index_centroid vecf32 or vecf64,
//	primary key (__mo_index_centroid_version, __mo_index_centroid_id),
// )
// create __mo_seconary_entries (
//	__mo_index_centroid_fk_version bigint,
//	__mo_index_centroid_fk_id bigint,
//	__mo_index_pri_col src_pk_type,
//	__mo_index_centroid_fk_entry vecf32 or vecf64,
//	primary key (__mo_index_centriod_fk_version, __mo_index_centroid_fk_id, __mo_index_pri_col)
// )

// validateIncludeColumns enforces DDL-time rules for INCLUDE columns on GPU
// vector (CAGRA / IVF-PQ) indexes. The execute-time path in
// filter_helper_gpu.go validates types lazily, so without this check a bogus
// CREATE INDEX ... INCLUDE (...) only breaks at the first INSERT. Failing up
// front gives users a clear, immediate error.
//
// Rules:
//   - each INCLUDE column must exist on the base table
//   - column type must be one the GPU FilterStore accepts: int32, int64,
//     float32, float64. VARCHAR is NOT accepted — the executor path expects a
//     pre-hashed uint64 and the DDL-side hashing pipeline is not wired in
//     yet, so reject it here until that support lands.
//   - INCLUDE columns must not duplicate each other or the indexed vector
//     column.
//   - INCLUDE must not contain the primary key column. PK predicates are
//     pushed down automatically via the reserved __mo_pk_host_id virtual
//     column (pkg/sql/plan/filter_predicate.go), which evaluates against the
//     index's host_ids array. Listing the PK as an INCLUDE column would
//     duplicate the PK values in filter_host_ for no benefit — the planner
//     would route the predicate to host_ids anyway.
func validateIncludeColumns(ctx CompilerContext,
	includeCols []*tree.UnresolvedName,
	colMap map[string]*ColDef,
	vecColName string,
	pkeyName string,
	supportedTypes []types.T) error {
	if len(includeCols) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(includeCols))
	for _, uc := range includeCols {
		name := uc.ColName()
		origin := uc.ColNameOrigin()
		if name == "" {
			return moerr.NewInvalidInputf(ctx.GetContext(), "INCLUDE column name cannot be empty")
		}
		if name == vecColName {
			return moerr.NewInvalidInputf(ctx.GetContext(),
				"INCLUDE column '%s' cannot be the indexed vector column", origin)
		}
		if pkeyName != "" && name == pkeyName {
			return moerr.NewInvalidInputf(ctx.GetContext(),
				"INCLUDE column '%s' must not be the primary key; "+
					"predicates on the pk are pushed down automatically via the "+
					"__mo_pk_host_id virtual column, so listing it here only "+
					"duplicates storage", origin)
		}
		if _, dup := seen[name]; dup {
			return moerr.NewInvalidInputf(ctx.GetContext(),
				"duplicate INCLUDE column '%s'", origin)
		}
		seen[name] = struct{}{}

		col, ok := colMap[name]
		if !ok {
			return moerr.NewInvalidInputf(ctx.GetContext(),
				"INCLUDE column '%s' is not exist", origin)
		}
		// Supported INCLUDE column types are declared by the index plugin
		// (catalog.Hooks.SupportedIncludeColumnTypes()) and threaded in via
		// supportedTypes — the single source of truth, not hardcoded here.
		colType := types.T(col.Typ.Id)
		supported := false
		for _, st := range supportedTypes {
			if colType == st {
				supported = true
				break
			}
		}
		if !supported {
			return moerr.NewNotSupportedf(ctx.GetContext(),
				"INCLUDE column '%s' has unsupported type %s (supported: int32, int64, float32, float64)",
				origin, colType.String())
		}
	}
	return nil
}

func CreateIndexDef(ctx planplugin.CompilerContext, indexInfo *tree.Index,
	indexTableName, indexAlgoTableType string,
	indexParts []string, isUnique bool) (*plan.IndexDef, error) {

	// TODO: later use this function for RegularSecondaryIndex and UniqueIndex.

	indexDef := &plan.IndexDef{}

	indexDef.IndexTableName = indexTableName
	indexDef.Parts = indexParts

	indexDef.Unique = isUnique
	indexDef.TableExist = true

	// Algorithm related fields
	indexDef.IndexAlgo = indexInfo.KeyType.ToString()
	indexDef.IndexAlgoTableType = indexAlgoTableType
	if indexInfo.IndexOption != nil {
		// Copy Comment as it is
		indexDef.Comment = indexInfo.IndexOption.Comment

		// Create params JSON string and set it
		params, err := buildIndexAlgoParams(indexInfo)
		if err != nil {
			return nil, err
		}
		indexDef.IndexAlgoParams = params
	} else {
		// default indexInfo.IndexOption values
		indexDef.Comment = ""
		indexDef.IndexAlgoParams = ""
		if p, ok := indexplugin.Get(indexInfo.KeyType.ToString()); ok {
			// Vector-index algorithms supply their default params via the
			// plugin (DefaultOptions). Non-vector algos miss the registry
			// and leave the empty defaults set above.
			if defaults := p.Catalog().DefaultOptions(); len(defaults) > 0 {
				params, err := catalog.IndexParamsMapToJsonString(defaults)
				if err != nil {
					return nil, err
				}
				indexDef.IndexAlgoParams = params
			}
		}

	}

	// Capture the plugin's build-time session vars (BuildSessionVars) into the
	// typed algo_params.session_vars blob so background builds (restore reindex,
	// idxcron, async create) reproduce the create-time config. Index-defining
	// knobs (kmeans_*, max_index_capacity) are NOT auto-captured here: they ride
	// flat algo_params keys written by ParamsFromTree only when explicitly set
	// in CREATE INDEX (so an unset option never pollutes algo_params).
	if ctx != nil {
		if p, ok := indexplugin.Get(catalog.ToLower(indexDef.IndexAlgo)); ok {
			if names := p.Catalog().BuildSessionVars(); len(names) > 0 {
				sv, err := compileplugin.CaptureVars(ctx.ResolveVariable, names)
				if err != nil {
					return nil, err
				}
				if len(sv) > 0 {
					flat := map[string]string{}
					if indexDef.IndexAlgoParams != "" {
						if flat, err = catalog.IndexParamsStringToMap(indexDef.IndexAlgoParams); err != nil {
							return nil, err
						}
					}
					if indexDef.IndexAlgoParams, err = catalog.IndexParamsMapToJsonStringWithSessionVars(flat, sv); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	nameCount := make(map[string]int)
	if indexInfo.Name == "" {
		firstPart := indexInfo.KeyParts[0].ColName.ColName()
		nameCount[firstPart]++
		count := nameCount[firstPart]
		indexName := firstPart
		if count > 1 {
			indexName = firstPart + "_" + strconv.Itoa(count)
		}
		indexDef.IndexName = indexName
	} else {
		indexDef.IndexName = indexInfo.Name
	}

	return indexDef, nil
}

func buildTruncateTable(stmt *tree.TruncateTable, ctx CompilerContext) (*Plan, error) {
	truncateTable := &plan.TruncateTable{}

	truncateTable.Database = string(stmt.Name.SchemaName)
	if truncateTable.Database == "" {
		truncateTable.Database = ctx.DefaultDatabase()
	}
	truncateTable.Table = string(stmt.Name.ObjectName)
	obj, tableDef, err := ctx.Resolve(truncateTable.Database, truncateTable.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), truncateTable.Database, truncateTable.Table)
	} else {
		if tableDef.TableType == catalog.SystemSourceRel {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not truncate source '%v' ", truncateTable.Table)
		}

		// TRUNCATE has always been a silent no-op for external tables; keep that
		// for read-only ones, but a writable external table holds INSERTed data
		// the user would expect TRUNCATE to remove — reject rather than report
		// success while the stage files survive.
		if tableDef.TableType == catalog.SystemExternalRel {
			if _, ok := GetWriteFilePattern(getExternParamFromTableDef(tableDef)); ok {
				return nil, moerr.NewNotSupportedf(ctx.GetContext(),
					"truncate writable external table '%v'; its files in the stage are not managed by the table", truncateTable.Table)
			}
		}

		if len(tableDef.RefChildTbls) > 0 {
			// if all children tables are self reference, we can drop the table
			if !HasFkSelfReferOnly(tableDef) {
				return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not truncate table '%v' referenced by some foreign key constraint", truncateTable.Table)
			}
		}

		if tableDef.ViewSql != nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), truncateTable.Database, truncateTable.Table)
		}

		truncateTable.TableId = tableDef.TblId
		if tableDef.Fkeys != nil {
			for _, fk := range tableDef.Fkeys {
				truncateTable.ForeignTbl = append(truncateTable.ForeignTbl, fk.ForeignTbl)
			}
		}

		truncateTable.ClusterTable = &plan.ClusterTable{
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}

		// non-sys account can not truncate the cluster table
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return nil, err
		}
		if truncateTable.GetClusterTable().GetIsClusterTable() && accountId != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can truncate the cluster table")
		}

		if obj.PubInfo != nil {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not truncate table '%v' which is published by other account", truncateTable.Table)
		}

		truncateTable.IndexTableNames = make([]string, 0)
		if tableDef.Indexes != nil {
			for _, indexdef := range tableDef.Indexes {
				if !indexdef.TableExist {
					continue
				}
				if catalog.IsRegularIndexAlgo(indexdef.IndexAlgo) ||
					catalog.IsMasterIndexAlgo(indexdef.IndexAlgo) ||
					catalog.IsFullTextIndexAlgo(indexdef.IndexAlgo) {
					truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, indexdef.IndexTableName)
				} else if p, ok := indexplugin.Get(indexdef.IndexAlgo); ok {
					// Vector indexes delegate to the plugin's catalog
					// hook. HNSW/CAGRA/IVF-PQ truncate all hidden
					// tables; IVF-FLAT preserves metadata + centroids
					// (k-means model) and only drops entries.
					if p.Catalog().ShouldTruncateHiddenTable(indexdef.IndexAlgoTableType) {
						truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, indexdef.IndexTableName)
					}
				}
			}
		}
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_TRUNCATE_TABLE,
				Definition: &plan.DataDefinition_TruncateTable{
					TruncateTable: truncateTable,
				},
			},
		},
	}, nil
}

func buildDropTable(stmt *tree.DropTable, ctx CompilerContext) (*Plan, error) {
	if len(stmt.Names) == 1 {
		dropTable, err := buildDropTableSingle(stmt.IfExists, stmt.Names[0], ctx)
		if err != nil {
			return nil, err
		}
		return &Plan{
			Plan: &plan.Plan_Ddl{
				Ddl: &plan.DataDefinition{
					DdlType: plan.DataDefinition_DROP_TABLE,
					Definition: &plan.DataDefinition_DropTable{
						DropTable: dropTable,
					},
				},
			},
		}, nil
	}

	dropTable := &plan.DropTable{
		IfExists: stmt.IfExists,
		Tables:   make([]*plan.DropTable, 0, len(stmt.Names)),
	}
	for _, name := range stmt.Names {
		entry, err := buildDropTableSingle(stmt.IfExists, name, ctx)
		if err != nil {
			return nil, err
		}
		dropTable.Tables = append(dropTable.Tables, entry)
	}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_TABLE,
				Definition: &plan.DataDefinition_DropTable{
					DropTable: dropTable,
				},
			},
		},
	}, nil
}

func buildDropTableSingle(ifExists bool, name *tree.TableName, ctx CompilerContext) (*plan.DropTable, error) {
	dropTable := &plan.DropTable{
		IfExists: ifExists,
	}

	dropTable.Database = string(name.SchemaName)
	// If the database name is empty, attempt to get default database name
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}
	// If the final database name is still empty, return an error
	if dropTable.Database == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	dropTable.Table = string(name.ObjectName)

	obj, tableDef, err := ctx.Resolve(dropTable.Database, dropTable.Table, nil)
	if err != nil {
		return nil, err
	}

	if tableDef == nil {
		if !dropTable.IfExists {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
		return dropTable, nil
	}

	if obj.PubInfo != nil {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not drop subscription table %s", dropTable.Table)
	}

	enabled, err := IsForeignKeyChecksEnabled(ctx)
	if err != nil {
		return nil, err
	}
	if enabled && len(tableDef.RefChildTbls) > 0 {
		// if all children tables are self reference, we can drop the table
		if !HasFkSelfReferOnly(tableDef) {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not drop table '%v' referenced by some foreign key constraint", dropTable.Table)
		}
	}

	isView := (tableDef.ViewSql != nil)
	dropTable.IsView = isView
	if isView {
		if !dropTable.IfExists {
			// drop table v0, v0 is view
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
		// drop table if exists v0, v0 is view
		dropTable.Table = ""
		dropTable.TableDef = nil
		return dropTable, nil
	}

	// Can not use drop table to drop sequence.
	if tableDef.TableType == catalog.SystemSequenceRel {
		if !dropTable.IfExists {
			return nil, moerr.NewInternalError(ctx.GetContext(), "Should use 'drop sequence' to drop a sequence")
		}
		// If exists, don't drop anything.
		dropTable.Table = ""
		dropTable.TableDef = nil
		return dropTable, nil
	}

	dropTable.ClusterTable = &plan.ClusterTable{
		IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
	}

	// non-sys account can not drop the cluster table
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if dropTable.GetClusterTable().GetIsClusterTable() && accountId != catalog.System_Account {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can drop the cluster table")
	}

	ignore := false
	val := ctx.GetContext().Value(defines.IgnoreForeignKey{})
	if val != nil {
		ignore = val.(bool)
	}

	dropTable.TableId = tableDef.TblId
	if tableDef.Fkeys != nil && !ignore {
		for _, fk := range tableDef.Fkeys {
			if fk.ForeignTbl == 0 {
				continue
			}
			dropTable.ForeignTbl = append(dropTable.ForeignTbl, fk.ForeignTbl)
		}
	}

	// collect child tables that needs remove fk relationships
	// with the table
	if tableDef.RefChildTbls != nil && !ignore {
		for _, childTbl := range tableDef.RefChildTbls {
			if childTbl == 0 {
				continue
			}
			dropTable.FkChildTblsReferToMe = append(dropTable.FkChildTblsReferToMe, childTbl)
		}
	}

	dropTable.IndexTableNames = make([]string, 0)
	if tableDef.Indexes != nil {
		for _, indexdef := range tableDef.Indexes {
			if indexdef.TableExist {
				dropTable.IndexTableNames = append(dropTable.IndexTableNames, indexdef.IndexTableName)
			}
		}
	}

	dropTable.TableDef = tableDef
	dropTable.UpdateFkSqls = []string{getSqlForDeleteTable(dropTable.Database, dropTable.Table)}
	return dropTable, nil
}

func buildDropView(stmt *tree.DropView, ctx CompilerContext) (*Plan, error) {
	dropTable := &plan.DropTable{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, moerr.NewNotSupportedf(ctx.GetContext(), "drop multiple (%d) view", len(stmt.Names))
	}

	dropTable.Database = string(stmt.Names[0].SchemaName)

	// If the database name is empty, attempt to get default database name
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}
	// If the final database name is still empty, return an error
	if dropTable.Database == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	dropTable.Table = string(stmt.Names[0].ObjectName)

	obj, tableDef, err := ctx.Resolve(dropTable.Database, dropTable.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		if !dropTable.IfExists {
			return nil, moerr.NewBadView(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
	} else {
		if tableDef.ViewSql == nil {
			return nil, moerr.NewBadView(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
		if obj.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot drop view in subscription database")
		}
	}
	dropTable.IsView = true

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_TABLE,
				Definition: &plan.DataDefinition_DropTable{
					DropTable: dropTable,
				},
			},
		},
	}, nil
}

func buildCreateDatabase(stmt *tree.CreateDatabase, ctx CompilerContext) (*Plan, error) {

	createDB := &plan.CreateDatabase{
		IfNotExists: stmt.IfNotExists,
		Database:    string(stmt.Name),
	}

	if stmt.SubscriptionOption != nil {
		accName := string(stmt.SubscriptionOption.From)
		pubName := string(stmt.SubscriptionOption.Publication)
		subName := string(stmt.Name)
		if err := ctx.CheckSubscriptionValid(subName, accName, pubName); err != nil {
			return nil, err
		}
		createDB.SubscriptionOption = &plan.SubscriptionOption{
			From:        string(stmt.SubscriptionOption.From),
			Publication: string(stmt.SubscriptionOption.Publication),
		}
	}
	createDB.Sql = stmt.Sql

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_DATABASE,
				Definition: &plan.DataDefinition_CreateDatabase{
					CreateDatabase: createDB,
				},
			},
		},
	}, nil
}

func buildDropDatabase(stmt *tree.DropDatabase, ctx CompilerContext) (*Plan, error) {
	dropDB := &plan.DropDatabase{
		IfExists: stmt.IfExists,
		Database: string(stmt.Name),
	}
	if publishing, err := ctx.IsPublishing(dropDB.Database); err != nil {
		return nil, err
	} else if publishing {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not drop database '%v' which is publishing", dropDB.Database)
	}

	if ctx.DatabaseExists(string(stmt.Name), nil) {
		databaseId, err := ctx.GetDatabaseId(string(stmt.Name), nil)
		if err != nil {
			return nil, err
		}
		dropDB.DatabaseId = databaseId

		// check foreign keys exists or not
		enabled, err := IsForeignKeyChecksEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if enabled {
			dropDB.CheckFKSql = getSqlForCheckHasDBRefersTo(dropDB.Database)
		}
	}

	dropDB.UpdateFkSql = getSqlForDeleteDB(dropDB.Database)

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_DATABASE,
				Definition: &plan.DataDefinition_DropDatabase{
					DropDatabase: dropDB,
				},
			},
		},
	}, nil
}

// In MySQL, the CREATE INDEX syntax can only create one index instance at a time
func buildCreateIndex(stmt *tree.CreateIndex, ctx CompilerContext) (*Plan, error) {
	createIndex := &plan.CreateIndex{}
	if len(stmt.Table.SchemaName) == 0 {
		createIndex.Database = ctx.DefaultDatabase()
	} else {
		createIndex.Database = string(stmt.Table.SchemaName)
	}
	// check table
	tableName := string(stmt.Table.ObjectName)
	obj, tableDef, err := ctx.Resolve(createIndex.Database, tableName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), createIndex.Database, tableName)
	}
	if err := checkCreateIndexTableType(ctx.GetContext(), tableDef); err != nil {
		return nil, err
	}
	if obj.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot create index in subscription database")
	}
	// check index
	indexName := string(stmt.Name)
	for _, def := range tableDef.Indexes {
		if def.IndexName == indexName {
			return nil, moerr.NewDuplicateKey(ctx.GetContext(), indexName)
		}
	}
	// build index
	var ftIdx *tree.FullTextIndex
	var uIdx *tree.UniqueIndex
	var sIdx *tree.Index
	switch stmt.IndexCat {
	case tree.INDEX_CATEGORY_UNIQUE:
		uIdx = &tree.UniqueIndex{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
		}
	case tree.INDEX_CATEGORY_NONE:
		sIdx = &tree.Index{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
			KeyType:     stmt.IndexOption.IType,
		}
	case tree.INDEX_CATEGORY_FULLTEXT:
		ftIdx = &tree.FullTextIndex{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
		}
	case tree.INDEX_CATEGORY_SPATIAL:
		keyType := tree.INDEX_TYPE_RTREE
		if stmt.IndexOption != nil && stmt.IndexOption.IType != tree.INDEX_TYPE_INVALID {
			if stmt.IndexOption.IType != tree.INDEX_TYPE_RTREE {
				return nil, moerr.NewNotSupported(ctx.GetContext(), "SPATIAL INDEX only supports USING RTREE")
			}
			keyType = stmt.IndexOption.IType
		}
		sIdx = &tree.Index{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
			KeyType:     keyType,
		}
	default:
		return nil, moerr.NewNotSupportedf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
	}
	colMap := make(map[string]*ColDef)
	for _, col := range tableDef.Cols {
		colMap[col.Name] = col
	}

	// Check whether the composite primary key column is included
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		colMap[tableDef.Pkey.CompPkeyCol.Name] = tableDef.Pkey.CompPkeyCol
	}

	// index.TableDef.Defs store info of index need to be modified
	// index.IndexTables store index table need to be created
	oriPriKeyName := getTablePriKeyName(tableDef.Pkey)
	createIndex.OriginTablePrimaryKey = oriPriKeyName

	indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
	if uIdx != nil {
		if err := buildUniqueIndexTable(indexInfo, []*tree.UniqueIndex{uIdx}, colMap, oriPriKeyName, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = true
	}
	if ftIdx != nil {
		if err := buildFullTextIndexTable(indexInfo, []*tree.FullTextIndex{ftIdx}, colMap, tableDef.Indexes, oriPriKeyName, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = true
	}
	if sIdx != nil {
		if err := buildSecondaryIndexDef(indexInfo, []*tree.Index{sIdx}, colMap, tableDef.Indexes, oriPriKeyName, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = true
	}
	createIndex.Index = indexInfo
	createIndex.Table = tableName
	createIndex.TableDef = tableDef

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_INDEX,
				Definition: &plan.DataDefinition_CreateIndex{
					CreateIndex: createIndex,
				},
			},
		},
	}, nil
}

func checkCreateIndexTableType(ctx context.Context, tableDef *TableDef) error {
	if tableDef.TableType == catalog.SystemExternalRel {
		return moerr.NewInvalidInput(ctx, "cannot create index on external table")
	}
	return nil
}

func rejectExternalTableInlineIndexes(ctx context.Context, stmt *tree.CreateTable) error {
	for _, item := range stmt.Defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			for _, attr := range def.Attributes {
				switch attr.(type) {
				case *tree.AttributePrimaryKey, *tree.AttributeKey, *tree.AttributeUnique, *tree.AttributeUniqueKey:
					return moerr.NewInvalidInput(ctx, "cannot create index on external table")
				}
			}
		case *tree.PrimaryKeyIndex, *tree.Index, *tree.UniqueIndex, *tree.FullTextIndex:
			return moerr.NewInvalidInput(ctx, "cannot create index on external table")
		}
	}
	return nil
}

func buildDropIndex(stmt *tree.DropIndex, ctx CompilerContext) (*Plan, error) {
	dropIndex := &plan.DropIndex{}
	if len(stmt.TableName.SchemaName) == 0 {
		dropIndex.Database = ctx.DefaultDatabase()
	} else {
		dropIndex.Database = string(stmt.TableName.SchemaName)
	}

	// If the final database name is still empty, return an error
	if dropIndex.Database == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	// check table
	dropIndex.Table = string(stmt.TableName.ObjectName)
	obj, tableDef, err := ctx.Resolve(dropIndex.Database, dropIndex.Table, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropIndex.Database, dropIndex.Table)
	}

	if obj.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot drop index in subscription database")
	}

	// check index
	dropIndex.IndexName = string(stmt.Name)
	found := false

	for _, indexdef := range tableDef.Indexes {
		if dropIndex.IndexName == indexdef.IndexName {
			found = true
			break
		}
	}

	if !found {
		if stmt.IfExists {
			// An empty index name represents the no-op path for DROP INDEX IF EXISTS.
			dropIndex.IndexName = ""
		} else {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "not found index: %s", dropIndex.IndexName)
		}
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_INDEX,
				Definition: &plan.DataDefinition_DropIndex{
					DropIndex: dropIndex,
				},
			},
		},
	}, nil
}

// Get tabledef(col, viewsql, properties) for alterview.
func buildAlterView(stmt *tree.AlterView, ctx CompilerContext) (*Plan, error) {
	viewName := string(stmt.Name.ObjectName)
	alterView := &plan.AlterView{
		IfExists: stmt.IfExists,
		TableDef: &plan.TableDef{
			Name: viewName,
		},
	}
	// get database name
	if len(stmt.Name.SchemaName) == 0 {
		alterView.Database = ""
	} else {
		alterView.Database = string(stmt.Name.SchemaName)
	}
	if alterView.Database == "" {
		alterView.Database = ctx.DefaultDatabase()
	}

	// step 1: check the view exists or not
	obj, oldViewDef, err := ctx.Resolve(alterView.Database, viewName, nil)
	if err != nil {
		return nil, err
	}
	if oldViewDef == nil {
		if !alterView.IfExists {
			return nil, moerr.NewBadView(ctx.GetContext(),
				alterView.Database,
				viewName)
		}
	} else {
		if obj.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter view in subscription database")
		}
		if oldViewDef.ViewSql == nil {
			return nil, moerr.NewBadView(ctx.GetContext(),
				alterView.Database,
				viewName)
		}
	}

	// step 2: generate new view def
	ctx.SetBuildingAlterView(true, alterView.Database, viewName)
	// restore
	defer func() {
		ctx.SetBuildingAlterView(false, "", "")
	}()
	tableDef, err := genViewTableDef(ctx, stmt.AsSource)
	if err != nil {
		return nil, err
	}

	alterView.TableDef.Cols = tableDef.Cols
	alterView.TableDef.ViewSql = tableDef.ViewSql
	alterView.TableDef.Defs = tableDef.Defs

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_VIEW,
				Definition: &plan.DataDefinition_AlterView{
					AlterView: alterView,
				},
			},
		},
	}, nil
}

func buildRenameTable(stmt *tree.RenameTable, ctx CompilerContext) (*Plan, error) {

	type renamedInfo struct {
		objRef   *ObjectRef
		tableDef *TableDef
	}
	alterTables := stmt.AlterTables
	renameTables := make([]*plan.AlterTable, 0)
	removed := make(map[string]bool)
	nameMapping := make(map[string]*renamedInfo)
	for _, alterTable := range alterTables {
		schemaName, tableName := string(alterTable.Table.Schema()), string(alterTable.Table.Name())
		if schemaName == "" {
			schemaName = ctx.DefaultDatabase()
		}
		srcKey := schemaName + "." + tableName
		var objRef *ObjectRef
		var tableDef *TableDef
		var err error
		if info, ok := nameMapping[srcKey]; ok {
			objRef = info.objRef
			tableDef = DeepCopyTableDef(info.tableDef, true)
			tableDef.Name = tableName
		} else if removed[srcKey] {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tableName)
		} else {
			objRef, tableDef, err = ctx.Resolve(schemaName, tableName, nil)
			if err != nil {
				return nil, err
			}
		}
		if tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tableName)
		}

		if tableDef.IsTemporary {
			return nil, moerr.NewNYI(ctx.GetContext(), "alter table for temporary table")
		}

		if tableDef.ViewSql != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "you should use alter view statemnt for View")
		}
		if objRef.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter table in subscription database")
		}
		isClusterTable := util.TableIsClusterTable(tableDef.GetTableType())
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return nil, err
		}
		if isClusterTable && accountId != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can alter the cluster table")
		}

		alterTablePlan := &plan.AlterTable{
			Actions:        make([]*plan.AlterTable_Action, len(alterTable.Options)),
			AlgorithmType:  plan.AlterTable_INPLACE,
			Database:       schemaName,
			TableDef:       tableDef,
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}

		var updateSqls []string
		for i, option := range alterTable.Options {
			switch opt := option.(type) {
			case *tree.AlterOptionTableName:
				oldName := tableName
				newName := string(opt.Name.ToTableName().ObjectName)
				dstKey := schemaName + "." + newName
				if oldName != newName {
					if _, ok := nameMapping[dstKey]; ok {
						return nil, moerr.NewTableAlreadyExists(ctx.GetContext(), newName)
					}
					if !removed[dstKey] {
						_, existDef, err := ctx.Resolve(schemaName, newName, nil)
						if err != nil {
							return nil, err
						}
						if existDef != nil {
							return nil, moerr.NewTableAlreadyExists(ctx.GetContext(), newName)
						}
					}
				}
				alterTablePlan.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AlterName{
						AlterName: &plan.AlterTableName{
							OldName: oldName,
							NewName: newName,
						},
					},
				}
				updateSqls = append(updateSqls, getSqlForRenameTable(schemaName, oldName, newName)...)
				delete(nameMapping, srcKey)
				removed[srcKey] = true
				nameMapping[dstKey] = &renamedInfo{objRef: objRef, tableDef: tableDef}
				delete(removed, dstKey)

			default:
				return nil, moerr.NewNotSupportedf(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
			}
			alterTablePlan.UpdateFkSqls = updateSqls
		}
		renameTables = append(renameTables, alterTablePlan)
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_RENAME_TABLE,
				Definition: &plan.DataDefinition_RenameTable{
					RenameTable: &plan.RenameTable{
						AlterTables: renameTables,
					},
				},
			},
		},
	}, nil
}

func formatTreeNode(opt tree.NodeFormatter) string {
	// get callsite
	ft := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
	opt.Format(ft)
	return ft.String()
}

func buildAlterTableInplace(stmt *tree.AlterTable, ctx CompilerContext) (*Plan, error) {
	tableName := string(stmt.Table.ObjectName)
	databaseName := string(stmt.Table.SchemaName)
	if databaseName == "" {
		databaseName = ctx.DefaultDatabase()
	}

	_, tableDef, err := ctx.Resolve(databaseName, tableName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), databaseName, tableName)
	}

	alterTable := &plan.AlterTable{
		Actions:        make([]*plan.AlterTable_Action, len(stmt.Options)),
		AlgorithmType:  plan.AlterTable_INPLACE,
		Database:       databaseName,
		TableDef:       tableDef,
		IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		RawSQL: tree.StringWithOpts(
			stmt,
			dialect.MYSQL,
			tree.WithQuoteIdentifier(),
			tree.WithSingleQuoteString(),
		),
	}
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if alterTable.IsClusterTable && accountId != catalog.System_Account {
		return nil, moerr.NewInternalError(
			ctx.GetContext(),
			"only the sys account can alter the cluster table",
		)
	}

	colMap := make(map[string]*ColDef)
	for _, col := range tableDef.Cols {
		colMap[col.Name] = col
	}
	// Check whether the composite primary key column is included
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		colMap[tableDef.Pkey.CompPkeyCol.Name] = tableDef.Pkey.CompPkeyCol
	}
	unsupportedErrFmt := "unsupported alter option in inplace mode: %s"

	var detectSqls []string
	var updateSqls []string
	uniqueIndexInfos := make([]*tree.UniqueIndex, 0)
	secondaryIndexInfos := make([]*tree.Index, 0)
	for i, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.AlterOptionDrop:
			alterTableDrop := new(plan.AlterTableDrop)
			// lower case
			constraintName := string(opt.Name)
			if constraintNameAreWhiteSpaces(constraintName) {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't DROP '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTableDrop.Name = constraintName
			name_not_found := true
			switch opt.Typ {
			case tree.AlterTableDropIndex, tree.AlterTableDropKey:
				alterTableDrop.Typ = plan.AlterTableDrop_INDEX
				// check index
				for _, indexdef := range tableDef.Indexes {
					if constraintName == indexdef.IndexName {
						name_not_found = false
						break
					}
				}
			case tree.AlterTableDropForeignKey:
				alterTableDrop.Typ = plan.AlterTableDrop_FOREIGN_KEY
				for _, fk := range tableDef.Fkeys {
					if fk.Name == constraintName {
						name_not_found = false
						updateSqls = append(
							updateSqls,
							getSqlForDeleteConstraint(
								databaseName,
								tableName,
								constraintName,
							),
						)
						break
					}
				}
			default:
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					unsupportedErrFmt,
					formatTreeNode(opt),
				)
			}
			if name_not_found {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't DROP '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_Drop{
					Drop: alterTableDrop,
				},
			}

		case *tree.AlterOptionAdd:
			switch def := opt.Def.(type) {
			case *tree.ForeignKey:
				err = adjustConstraintName(ctx.GetContext(), def)
				if err != nil {
					return nil, err
				}

				fkData, err := getForeignKeyData(ctx, databaseName, tableDef, def)
				if err != nil {
					return nil, err
				}
				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddFk{
						AddFk: &plan.AlterTableAddFk{
							DbName:    fkData.ParentDbName,
							TableName: fkData.ParentTableName,
							Cols:      fkData.Cols.Cols,
							Fkey:      fkData.Def,
						},
					},
				}
				// for new fk in this alter table, the data in the table must
				// be checked to confirm that it is compliant with foreign key constraints.
				if fkData.IsSelfRefer {
					// fk self refer.
					// check columns of fk self refer are valid
					err = checkFkColsAreValid(ctx, fkData, tableDef)
					if err != nil {
						return nil, err
					}
					sqls, err := genSqlsForCheckFKSelfRefer(
						ctx.GetContext(),
						databaseName,
						tableDef.Name,
						tableDef.Cols,
						[]*plan.ForeignKeyDef{fkData.Def},
					)
					if err != nil {
						return nil, err
					}
					detectSqls = append(detectSqls, sqls...)
				} else {
					// get table def of parent table
					_, parentTableDef, err := ctx.Resolve(
						fkData.ParentDbName,
						fkData.ParentTableName,
						nil,
					)
					if err != nil {
						return nil, err
					}
					if parentTableDef == nil {
						return nil, moerr.NewNoSuchTable(
							ctx.GetContext(),
							fkData.ParentDbName,
							fkData.ParentTableName,
						)
					}
					sql, err := genSqlForCheckFKConstraints(
						ctx.GetContext(),
						fkData.Def,
						databaseName, tableDef.Name, tableDef.Cols,
						fkData.ParentDbName,
						fkData.ParentTableName,
						parentTableDef.Cols,
					)
					if err != nil {
						return nil, err
					}
					detectSqls = append(detectSqls, sql)
				}
				updateSqls = append(updateSqls, fkData.UpdateSql)
			case *tree.UniqueIndex:
				if err := checkCreateIndexTableType(ctx.GetContext(), tableDef); err != nil {
					return nil, err
				}
				if err := checkIndexKeypartSupportability(
					ctx.GetContext(),
					def.KeyParts,
				); err != nil {
					return nil, err
				}

				indexName := def.GetIndexName()
				constrNames := map[string]bool{}
				// Check not empty constraint name whether is duplicated.
				for _, idx := range tableDef.Indexes {
					nameLower := strings.ToLower(idx.IndexName)
					constrNames[nameLower] = true
				}

				if err := checkDuplicateConstraint(
					constrNames,
					indexName,
					false,
					ctx.GetContext(),
				); err != nil {
					return nil, err
				}
				if len(indexName) == 0 {
					// set empty constraint names(index and unique index)
					setEmptyUniqueIndexName(constrNames, def)
				}

				oriPriKeyName := getTablePriKeyName(tableDef.Pkey)
				indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
				if err := buildUniqueIndexTable(
					indexInfo,
					[]*tree.UniqueIndex{def},
					colMap,
					oriPriKeyName,
					ctx,
				); err != nil {
					return nil, err
				}

				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddIndex{
						AddIndex: &plan.AlterTableAddIndex{
							DbName:                databaseName,
							TableName:             tableName,
							OriginTablePrimaryKey: oriPriKeyName,
							IndexInfo:             indexInfo,
							IndexTableExist:       true,
						},
					},
				}
			case *tree.FullTextIndex:
				if err := checkCreateIndexTableType(ctx.GetContext(), tableDef); err != nil {
					return nil, err
				}
				if err := checkIndexKeypartSupportability(
					ctx.GetContext(),
					def.KeyParts,
				); err != nil {
					return nil, err
				}

				indexName := def.Name
				constrNames := map[string]bool{}
				// Check not empty constraint name whether is duplicated.
				for _, idx := range tableDef.Indexes {
					nameLower := strings.ToLower(idx.IndexName)
					constrNames[nameLower] = true
				}

				if err := checkDuplicateConstraint(
					constrNames,
					indexName,
					false,
					ctx.GetContext(),
				); err != nil {
					return nil, err
				}

				if len(indexName) == 0 {
					// set empty constraint names(index and unique index)
					setEmptyFullTextIndexName(constrNames, def)
				}

				oriPriKeyName := getTablePriKeyName(tableDef.Pkey)
				indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
				if err := buildFullTextIndexTable(
					indexInfo,
					[]*tree.FullTextIndex{def},
					colMap,
					tableDef.Indexes,
					oriPriKeyName,
					ctx,
				); err != nil {
					return nil, err
				}

				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddIndex{
						AddIndex: &plan.AlterTableAddIndex{
							DbName:                databaseName,
							TableName:             tableName,
							OriginTablePrimaryKey: oriPriKeyName,
							IndexInfo:             indexInfo,
							IndexTableExist:       true,
						},
					},
				}
			case *tree.Index:
				if err := checkCreateIndexTableType(ctx.GetContext(), tableDef); err != nil {
					return nil, err
				}
				if err := checkIndexKeypartSupportability(
					ctx.GetContext(),
					def.KeyParts,
				); err != nil {
					return nil, err
				}

				indexName := def.Name

				constrNames := map[string]bool{}
				// Check not empty constraint name whether is duplicated.
				for _, idx := range tableDef.Indexes {
					nameLower := strings.ToLower(idx.IndexName)
					constrNames[nameLower] = true
				}

				if err := checkDuplicateConstraint(
					constrNames,
					indexName,
					false,
					ctx.GetContext(),
				); err != nil {
					return nil, err
				}

				if len(indexName) == 0 {
					// set empty constraint names(index and unique index)
					setEmptyIndexName(constrNames, def)
				}

				oriPriKeyName := getTablePriKeyName(tableDef.Pkey)

				indexInfo := &plan.CreateTable{TableDef: &TableDef{}}
				if err := buildSecondaryIndexDef(
					indexInfo,
					[]*tree.Index{def},
					colMap,
					tableDef.Indexes,
					oriPriKeyName,
					ctx,
				); err != nil {
					return nil, err
				}

				alterTable.Actions[i] = &plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AddIndex{
						AddIndex: &plan.AlterTableAddIndex{
							DbName:                databaseName,
							TableName:             tableName,
							OriginTablePrimaryKey: oriPriKeyName,
							IndexInfo:             indexInfo,
							IndexTableExist:       true,
						},
					},
				}
			default:
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					unsupportedErrFmt,
					formatTreeNode(def),
				)
			}

		case *tree.AlterOptionAlterIndex:
			alterTableIndex := new(plan.AlterTableAlterIndex)
			constraintName := string(opt.Name)
			alterTableIndex.IndexName = constraintName
			alterTableIndex.Visible = opt.Visibility == tree.VISIBLE_TYPE_VISIBLE

			name_not_found := true
			// check index
			for _, indexdef := range tableDef.Indexes {
				if constraintName == indexdef.IndexName {
					name_not_found = false
					break
				}
			}
			if name_not_found {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't ALTER '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterIndex{
					AlterIndex: alterTableIndex,
				},
			}

		case *tree.AlterOptionAlterReIndex:
			alterTableReIndex := new(plan.AlterTableAlterReIndex)
			constraintName := string(opt.Name)
			alterTableReIndex.IndexName = constraintName
			// ForceSync (sync vs async rebuild) is the only build-time flag the
			// plan node carries. The shared index_option_list grammar already
			// restricts the algo (REINDEX rules cover only ivfflat/hnsw/ivfpq/
			// cagra) and validates option values (> 0); the per-index option
			// merge + reject happens at compile in Compile.ValidateReindexParams,
			// reading the options straight off the parse tree.
			alterTableReIndex.ForceSync = opt.ForceSync

			name_not_found := true
			// check index
			for _, indexdef := range tableDef.Indexes {
				if constraintName == indexdef.IndexName {
					name_not_found = false
					break
				}
			}
			if name_not_found {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't REINDEX '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterReindex{
					AlterReindex: alterTableReIndex,
				},
			}

		case *tree.AlterOptionAlterAutoUpdate:
			alterTableAutoUpdate := new(plan.AlterTableAlterAutoUpdate)
			constraintName := string(opt.Name)
			alterTableAutoUpdate.IndexName = constraintName

			switch opt.KeyType {
			case tree.INDEX_TYPE_IVFFLAT:
				if opt.Day < 0 {
					return nil, moerr.NewInternalErrorf(
						ctx.GetContext(),
						"day should be >= 0.",
					)
				}
				if opt.Hour < 0 || opt.Hour > 23 {
					return nil, moerr.NewInternalErrorf(
						ctx.GetContext(),
						"hour should be between 0 and 23.",
					)
				}
				alterTableAutoUpdate.AutoUpdate = opt.AutoUpdate
				alterTableAutoUpdate.Day = opt.Day
				alterTableAutoUpdate.Hour = opt.Hour
			default:
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					unsupportedErrFmt,
					opt.KeyType.ToString(),
				)
			}

			name_not_found := true
			// check index
			for _, indexdef := range tableDef.Indexes {
				if constraintName == indexdef.IndexName {
					name_not_found = false
					break
				}
			}
			if name_not_found {
				return nil, moerr.NewInternalErrorf(
					ctx.GetContext(),
					"Can't REINDEX '%s'; check that column/key exists",
					constraintName,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterAutoUpdate{
					AlterAutoUpdate: alterTableAutoUpdate,
				},
			}

		case *tree.TableOptionComment:
			if getNumOfCharacters(opt.Comment) > maxLengthOfTableComment {
				return nil, moerr.NewInvalidInputf(
					ctx.GetContext(),
					"comment for field '%s' is too long",
					alterTable.TableDef.Name,
				)
			}
			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterComment{
					AlterComment: &plan.AlterTableComment{
						NewComment: opt.Comment,
					},
				},
			}

		case *tree.AlterOptionTableName:
			oldName := tableDef.Name
			newName := string(opt.Name.ToTableName().ObjectName)
			if oldName == newName {
				continue
			}

			// TODO ONLY Check
			_, tableDef, err := ctx.Resolve(databaseName, newName, nil)
			if err != nil {
				return nil, err
			}
			if tableDef != nil {
				return nil, moerr.NewTableAlreadyExists(ctx.GetContext(), newName)
			}

			alterTable.Actions[i] = &plan.AlterTable_Action{
				Action: &plan.AlterTable_Action_AlterName{
					AlterName: &plan.AlterTableName{
						OldName: oldName,
						NewName: newName,
					},
				},
			}

			updateSqls = append(
				updateSqls,
				getSqlForRenameTable(databaseName, oldName, newName)...,
			)
		case *tree.AlterOptionAlterCheck, *tree.TableOptionCharset:
			continue

		case *tree.AlterTableModifyColumnClause:
			// defensively check again
			ok, _ := isInplaceModifyColumn(ctx.GetContext(), opt, tableDef)
			if !ok {
				return nil, moerr.NewInvalidInputf(
					ctx.GetContext(),
					"failed inplace check: %s",
					formatTreeNode(opt),
				)
			}

			if alterTable.CopyTableDef == nil {
				alterTable.CopyTableDef = DeepCopyTableDef(tableDef, true)
			}

			// update new column info to copy_table_def
			_, err := updateNewColumnInTableDef(
				ctx,
				alterTable.CopyTableDef,
				FindColumn(tableDef.Cols, opt.NewColumn.Name.ColName()),
				opt.NewColumn,
				opt.Position,
			)
			if err != nil {
				return nil, err
			}
		case *tree.AlterTableRenameColumnClause:
			if err := checkTableType(ctx.GetContext(), tableDef, ""); err != nil {
				return nil, err
			}

			if alterTable.CopyTableDef == nil {
				alterTable.CopyTableDef = DeepCopyTableDef(tableDef, true)
			}

			col := FindColumn(
				alterTable.CopyTableDef.Cols,
				opt.OldColumnName.ColName(),
			)
			if col == nil {
				return nil, moerr.NewBadFieldError(
					ctx.GetContext(),
					opt.OldColumnName.ColNameOrigin(),
					alterTable.TableDef.Name,
				)
			}
			oldColNameOrigin := col.OriginName
			newColNameOrigin := opt.NewColumnName.ColNameOrigin()

			if oldColNameOrigin == newColNameOrigin {
				continue
			}

			sqls, err := updateRenameColumnInTableDef(
				ctx,
				col,
				alterTable.CopyTableDef,
				opt,
			)
			if err != nil {
				return nil, err
			}

			updateSqls = append(updateSqls,
				getSqlForRenameColumn(tableDef.DbName,
					alterTable.TableDef.Name,
					oldColNameOrigin,
					newColNameOrigin)...)

			updateSqls = append(updateSqls, sqls...)

			alterTable.Actions = append(
				alterTable.Actions,
				&plan.AlterTable_Action{
					Action: &plan.AlterTable_Action_AlterRenameColumn{
						AlterRenameColumn: &plan.AlterRenameColumn{
							OldName:     oldColNameOrigin,
							NewName:     newColNameOrigin,
							SequenceNum: int32(col.Seqnum),
						},
					},
				},
			)

		default:
			return nil, moerr.NewInvalidInputf(
				ctx.GetContext(),
				unsupportedErrFmt,
				formatTreeNode(opt),
			)
		}
	}

	if alterTable.CopyTableDef != nil {
		alterTable.Actions = append(alterTable.Actions, &plan.AlterTable_Action{
			Action: &plan.AlterTable_Action_AlterReplaceDef{
				AlterReplaceDef: &plan.AlterReplaceDef{},
			},
		})
	}

	if stmt.PartitionOption != nil {
		alterTable.AlterPartition = &plan.AlterPartitionOption{}

		switch p := stmt.PartitionOption.(type) {
		case *tree.AlterPartitionAddPartitionClause:
			alterTable.AlterPartition.AlterType = plan.AlterPartitionType_AddPartitionTables
			defs, err := constructAddedPartitionDefs(ctx, tableDef, p)
			if err != nil {
				return nil, err
			}
			alterTable.AlterPartition.PartitionDefs = defs
		case *tree.AlterPartitionDropPartitionClause:
			alterTable.AlterPartition.AlterType = plan.AlterPartitionType_DropPartitionTables
		case *tree.AlterPartitionTruncatePartitionClause:
			alterTable.AlterPartition.AlterType = plan.AlterPartitionType_TruncatePartitionTables
		case *tree.AlterPartitionRedefinePartitionClause:
			alterTable.AlterPartition.AlterType = plan.AlterPartitionType_RedefinePartitionTables
		default:
			return nil, moerr.NewNotSupportedf(
				ctx.GetContext(),
				unsupportedErrFmt,
				formatTreeNode(stmt.PartitionOption),
			)
		}
	}

	// check Constraint Name (include index/ unique)
	if err := checkConstraintNames(
		uniqueIndexInfos,
		secondaryIndexInfos,
		ctx.GetContext(),
	); err != nil {
		return nil, err
	}

	alterTable.DetectSqls = detectSqls
	alterTable.UpdateFkSqls = updateSqls
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_TABLE,
				Definition: &plan.DataDefinition_AlterTable{
					AlterTable: alterTable,
				},
			},
		},
	}, nil
}

func buildLockTables(stmt *tree.LockTableStmt, ctx CompilerContext) (*Plan, error) {
	lockTables := make([]*plan.TableLockInfo, 0, len(stmt.TableLocks))
	uniqueTableName := make(map[string]bool)

	// Check table locks
	for _, tableLock := range stmt.TableLocks {
		tb := tableLock.Table

		// get table name
		tblName := string(tb.ObjectName)

		// get database name
		var schemaName string
		if len(tb.SchemaName) == 0 {
			schemaName = ctx.DefaultDatabase()
		} else {
			schemaName = string(tb.SchemaName)
		}

		// check table whether exist
		obj, tableDef, err := ctx.Resolve(schemaName, tblName, nil)
		if err != nil {
			return nil, err
		}
		if tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tblName)
		}

		if obj.PubInfo != nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot lock table in subscription database")
		}

		// check the stmt whether locks the same table
		if _, ok := uniqueTableName[tblName]; ok {
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Not unique table %s", tblName)
		}

		uniqueTableName[tblName] = true

		tableLockInfo := &plan.TableLockInfo{
			LockType: plan.TableLockType(tableLock.LockType),
			TableDef: tableDef,
		}
		lockTables = append(lockTables, tableLockInfo)
	}

	LockTables := &plan.LockTables{
		TableLocks: lockTables,
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_LOCK_TABLES,
				Definition: &plan.DataDefinition_LockTables{
					LockTables: LockTables,
				},
			},
		},
	}, nil
}

func buildUnLockTables(stmt *tree.UnLockTableStmt, ctx CompilerContext) (*Plan, error) {
	unLockTables := &plan.UnLockTables{}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_UNLOCK_TABLES,
				Definition: &plan.DataDefinition_UnlockTables{
					UnlockTables: unLockTables,
				},
			},
		},
	}, nil
}

type FkData struct {
	// fk reference to itself
	IsSelfRefer bool
	// the database that the fk refers to
	ParentDbName string
	// the table that the fk refers to
	ParentTableName string
	// the columns in foreign key
	Cols *plan.FkColName
	// the columns referred
	ColsReferred *plan.FkColName
	// fk definition
	Def *plan.ForeignKeyDef
	// the column typs in foreign key
	ColTyps map[int]*plan.Type
	// update foreign keys relations
	UpdateSql string
	// forward reference
	ForwardRefer bool
}

// getForeignKeyData prepares the foreign key data.
// for fk refer except the self refer, it is same as the previous one.
// but for fk self refer, it is different in not checking fk self refer instantly.
// because it is not ready. It should be checked after the pk,uk has been ready.
func getForeignKeyData(ctx CompilerContext, dbName string, tableDef *TableDef, def *tree.ForeignKey) (*FkData, error) {
	refer := def.Refer
	fkData := FkData{
		Def: &plan.ForeignKeyDef{
			Name:        def.ConstraintSymbol,
			Cols:        make([]uint64, len(def.KeyParts)),
			OnDelete:    getRefAction(refer.OnDelete),
			OnUpdate:    getRefAction(refer.OnUpdate),
			ForeignCols: make([]uint64, len(refer.KeyParts)),
		},
	}

	// get fk columns of create table
	fkData.Cols = &plan.FkColName{
		Cols: make([]string, len(def.KeyParts)),
	}
	fkData.ColTyps = make(map[int]*plan.Type)
	name2ColDef := make(map[string]*ColDef)
	for _, colDef := range tableDef.Cols {
		name2ColDef[colDef.Name] = colDef
	}
	// get the column (id,name,type) from tableDef for the foreign key
	for i, keyPart := range def.KeyParts {
		colName := keyPart.ColName.ColName()
		if colDef, has := name2ColDef[colName]; has {
			// column id from tableDef
			fkData.Def.Cols[i] = colDef.ColId
			// column name from tableDef
			fkData.Cols.Cols[i] = colDef.Name
			// column type from tableDef
			fkData.ColTyps[i] = &colDef.Typ
		} else {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "column '%v' no exists in the creating table '%v'", keyPart.ColName.ColNameOrigin(), tableDef.Name)
		}
	}

	fkData.ColsReferred = &plan.FkColName{
		Cols: make([]string, len(refer.KeyParts)),
	}
	for i, part := range refer.KeyParts {
		fkData.ColsReferred.Cols[i] = part.ColName.ColName()
	}

	// get foreign table & their columns
	parentTableName := string(refer.TableName.ObjectName)
	parentDbName := string(refer.TableName.SchemaName)
	if parentDbName == "" {
		parentDbName = ctx.DefaultDatabase()
	}

	if IsFkBannedDatabase(parentDbName) {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "can not refer foreign keys in %s", parentDbName)
	}

	// foreign key reference to itself
	if IsFkSelfRefer(parentDbName, parentTableName, dbName, tableDef.Name) {
		// should be handled later for fk self reference
		// PK and unique key may not be processed now
		// check fk columns can not reference to themselves
		// In self refer, the parent table is the table itself
		parentColumnsMap := make(map[string]int8)
		for _, part := range refer.KeyParts {
			parentColumnsMap[part.ColName.ColName()] = 0
		}
		for _, name := range fkData.Cols.Cols {
			if _, ok := parentColumnsMap[name]; ok {
				return nil, moerr.NewInternalErrorf(ctx.GetContext(), "foreign key %s can not reference to itself", name)
			}
		}
		// for fk self refer. column id may be not ready.
		fkData.IsSelfRefer = true
		fkData.ParentDbName = parentDbName
		fkData.ParentTableName = parentTableName
		fkData.Def.ForeignTbl = 0
		fkData.UpdateSql = getSqlForAddFk(dbName, tableDef.Name, &fkData)
		return &fkData, nil
	}

	fkData.ParentDbName = parentDbName
	fkData.ParentTableName = parentTableName

	// make insert mo_foreign_keys
	fkData.UpdateSql = getSqlForAddFk(dbName, tableDef.Name, &fkData)

	_, parentTableDef, err := ctx.Resolve(parentDbName, parentTableName, nil)
	if err != nil {
		return nil, err
	}
	if parentTableDef == nil {
		enabled, err := IsForeignKeyChecksEnabled(ctx)
		if err != nil {
			return nil, err
		}
		if !enabled {
			fkData.ForwardRefer = true
			return &fkData, nil
		}
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), ctx.DefaultDatabase(), parentTableName)
	}

	if parentTableDef.IsTemporary {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "add foreign key for temporary table")
	}

	fkData.Def.ForeignTbl = parentTableDef.TblId

	// separate the rest of the logic in previous version
	// into an independent function checkFkColsAreValid
	// for reusing it in fk self refer that checks the
	// columns in fk definition are valid or not.
	if err := checkFkColsAreValid(ctx, &fkData, parentTableDef); err != nil {
		return nil, err
	}

	return &fkData, nil
}

/*
checkFkColsAreValid check foreign key columns is valid or not, then it saves them.
the columns referred by the foreign key in the children table must appear in the unique keys or primary key
in the parent table.

For instance:
create table f1 (a int ,b int, c int ,d int ,e int,

	primary key(a,b),  unique key(c,d), unique key (e))

Case 1:

	single column like "a" ,"b", "c", "d", "e" can be used as the column in foreign key of the child table
	due to they are the member of the primary key or some Unique key.

Case 2:

	"a, b" can be used as the columns in the foreign key of the child table
	due to they are the member of the primary key.

	"c, d" can be used as the columns in the foreign key of the child table
	due to they are the member of some unique key.

Case 3:

	"a, c" can not be used due to they belong to the different primary key / unique key
*/
func checkFkColsAreValid(ctx CompilerContext, fkData *FkData, parentTableDef *TableDef) error {
	// colId in parent table-> position in parent table
	columnIdPos := make(map[uint64]int)
	// columnName in parent table -> position in parent table
	columnNamePos := make(map[string]int)
	// columnName of index and pk of parent table -> colId in parent table
	uniqueColumns := make([]map[string]uint64, 0, len(parentTableDef.Cols))

	// 1. collect parent column info
	for i, col := range parentTableDef.Cols {
		columnIdPos[col.ColId] = i
		columnNamePos[col.Name] = i
	}

	// 2. check if the referred column does not exist in the parent table
	for _, colName := range fkData.ColsReferred.Cols {
		if _, exists := columnNamePos[colName]; !exists { // column exists in parent table
			return moerr.NewInternalErrorf(ctx.GetContext(), "column '%v' no exists in table '%v'", colName, fkData.ParentTableName)
		}
	}
	if err := checkFkVirtualGeneratedColumns(ctx.GetContext(), parentTableDef, fkData.ColsReferred.Cols); err != nil {
		return err
	}

	// columnName in uk or pk -> its colId in the parent table
	collectIndexColumn := func(names []string) {
		ret := make(map[string]uint64)
		// columnName -> its colId in the parent table
		for _, colName := range names {
			ret[colName] = parentTableDef.Cols[columnNamePos[colName]].ColId
		}
		uniqueColumns = append(uniqueColumns, ret)
	}

	// 3. collect pk column info of the parent table
	if parentTableDef.Pkey != nil {
		collectIndexColumn(parentTableDef.Pkey.Names)
	}

	// 4. collect index column info of the parent table
	// secondary key?
	// now tableRef.Indices are empty, you can not test it
	for _, index := range parentTableDef.Indexes {
		if index.Unique {
			collectIndexColumn(index.Parts)
		}
	}

	// 5. check if there is at least one unique key or primary key should have
	// the columns referenced by the foreign keys in the children tables.
	matchCol := make([]uint64, 0, len(fkData.ColsReferred.Cols))
	// iterate on every pk or uk
	for _, uniqueColumn := range uniqueColumns {
		// iterate on the referred column of fk
		for i, colName := range fkData.ColsReferred.Cols {
			// check if the referred column exists in this pk or uk
			if colId, ok := uniqueColumn[colName]; ok {
				// check column type
				// left part of expr: column type in parent table
				// right part of expr: column type in child table
				if parentTableDef.Cols[columnIdPos[colId]].Typ.Id != fkData.ColTyps[i].Id {
					return moerr.NewInternalErrorf(ctx.GetContext(), "type of reference column '%v' is not match for column '%v'", colName, fkData.Cols.Cols[i])
				}
				matchCol = append(matchCol, colId)
			} else {
				// column in fk does not exist in this pk or uk
				matchCol = matchCol[:0]
				break
			}
		}

		if len(matchCol) > 0 {
			break
		}
	}

	if len(matchCol) == 0 {
		return moerr.NewInternalError(ctx.GetContext(), "failed to add the foreign key constraint")
	} else {
		fkData.Def.ForeignCols = matchCol
	}
	return nil
}

func checkFkVirtualGeneratedColumns(ctx context.Context, parentTableDef *TableDef, referredCols []string) error {
	for _, colName := range referredCols {
		colDef := FindColumn(parentTableDef.Cols, colName)
		if colDef == nil || colDef.GeneratedCol == nil || colDef.GeneratedCol.IsStored {
			continue
		}
		return moerr.NewInvalidInputf(ctx,
			"foreign key cannot reference virtual generated column '%s'",
			colDef.GetOriginCaseName())
	}
	return nil
}

// buildFkDataOfForwardRefer rebuilds the fk relationships based on
// the mo_catalog.mo_foreign_keys.
func buildFkDataOfForwardRefer(ctx CompilerContext,
	constraintName string,
	fkDefs []*FkReferDef,
	createTable *plan.CreateTable) (*FkData, error) {
	fkData := FkData{
		Def: &plan.ForeignKeyDef{
			Name:        constraintName,
			Cols:        make([]uint64, len(fkDefs)),
			OnDelete:    convertIntoReferAction(fkDefs[0].OnDelete),
			OnUpdate:    convertIntoReferAction(fkDefs[0].OnUpdate),
			ForeignCols: make([]uint64, len(fkDefs)),
		},
	}
	// 1. get tableDef of the child table
	_, childTableDef, err := ctx.Resolve(fkDefs[0].Db, fkDefs[0].Tbl, nil)
	if err != nil {
		return nil, err
	}
	if childTableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), fkDefs[0].Db, fkDefs[0].Tbl)
	}
	// 2. fill fkdata
	fkData.Cols = &plan.FkColName{
		Cols: make([]string, len(fkDefs)),
	}
	fkData.ColTyps = make(map[int]*plan.Type)

	name2ColDef := make(map[string]*ColDef)
	for _, def := range childTableDef.Cols {
		name2ColDef[def.Name] = def
	}
	for i, fkDef := range fkDefs {
		if colDef, has := name2ColDef[fkDef.Col]; has {
			// column id from tableDef
			fkData.Def.Cols[i] = colDef.ColId
			// column name from tableDef
			fkData.Cols.Cols[i] = colDef.Name
			// column type from tableDef
			fkData.ColTyps[i] = &colDef.Typ
		} else {
			return nil, moerr.NewInternalErrorf(ctx.GetContext(), "column '%v' no exists in table '%v'", fkDef.Col, fkDefs[0].Tbl)
		}
	}

	fkData.ColsReferred = &plan.FkColName{
		Cols: make([]string, len(fkDefs)),
	}
	for i, def := range fkDefs {
		fkData.ColsReferred.Cols[i] = def.ReferCol
	}

	// 3. check fk valid or not
	if err := checkFkColsAreValid(ctx, &fkData, createTable.TableDef); err != nil {
		return nil, err
	}
	return &fkData, nil
}

func getAutoIncrementOffsetFromVariables(ctx CompilerContext) (uint64, bool) {
	v, err := ctx.ResolveVariable("auto_increment_offset", true, false)
	if err == nil {
		if offset, ok := v.(int64); ok && offset > 1 {
			return uint64(offset - 1), true
		}
	}
	return 0, false
}

var unitDurations = map[string]time.Duration{
	"second": time.Second,
	"minute": time.Minute,
	"hour":   time.Hour,
	"day":    time.Hour * 24,
	"week":   time.Hour * 24 * 7,
	"month":  time.Hour * 24 * 30,
}

func parseDuration(ctx context.Context, period uint64, unit string) (time.Duration, error) {
	unitDuration, ok := unitDurations[strings.ToLower(unit)]
	if !ok {
		return 0, moerr.NewInvalidArg(ctx, "time unit", unit)
	}
	seconds := period * uint64(unitDuration)
	return time.Duration(seconds), nil
}

func buildCreatePitr(stmt *tree.CreatePitr, ctx CompilerContext) (*Plan, error) {
	// only sys can create cluster level pitr
	currentAccount := ctx.GetAccountName()
	currentAccountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if stmt.Level == tree.PITRLEVELCLUSTER && currentAccount != "sys" {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only sys tenant can create cluster level pitr")
	}

	// only sys can create tenant level pitr for other tenant
	if stmt.Level == tree.PITRLEVELACCOUNT {
		if len(stmt.AccountName) > 0 && currentAccount != "sys" {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only sys tenant can create tenant level pitr for other tenant")
		}
	}

	// Check PITR value range
	pitrVal := stmt.PitrValue
	if pitrVal <= 0 || pitrVal > 100 {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "invalid pitr value %d", pitrVal)
	}

	// Check if PITR unit is valid
	pitrUnit := strings.ToLower(stmt.PitrUnit)
	if pitrUnit != "h" && pitrUnit != "d" && pitrUnit != "mo" && pitrUnit != "y" {
		return nil, moerr.NewInternalErrorf(ctx.GetContext(), "invalid pitr unit %s", pitrUnit)
	}

	// check pitr exists or not
	if string(stmt.Name) == SYSMOCATALOGPITR {
		return nil, moerr.NewInternalError(ctx.GetContext(), "pitr name is reserved")
	}

	// Validate related objects according to PITR level
	var databaseId uint64
	var tableId uint64
	accountId := currentAccountId
	accountName := currentAccount
	switch stmt.Level {
	case tree.PITRLEVELACCOUNT:
		if len(stmt.AccountName) > 0 {
			accountIds, err := ctx.ResolveAccountIds([]string{string(stmt.AccountName)})
			if err != nil {
				return nil, err
			}
			if len(accountIds) == 0 {
				return nil, moerr.NewInternalError(ctx.GetContext(), "account "+string(stmt.AccountName)+" does not exist")
			}
			accountId = accountIds[len(accountIds)-1]
			accountName = string(stmt.AccountName)
		}
	case tree.PITRLEVELDATABASE:
		if !ctx.DatabaseExists(string(stmt.DatabaseName), nil) {
			return nil, moerr.NewInternalError(ctx.GetContext(), "database "+string(stmt.DatabaseName)+" does not exist")
		}
		databaseId, err = ctx.GetDatabaseId(string(stmt.DatabaseName), nil)
		if err != nil {
			return nil, err
		}
	case tree.PITRLEVELTABLE:
		if !ctx.DatabaseExists(string(stmt.DatabaseName), nil) {
			return nil, moerr.NewInternalError(ctx.GetContext(), "database "+string(stmt.DatabaseName)+" does not exist")
		}
		objRef, tableDef, err := ctx.Resolve(string(stmt.DatabaseName), string(stmt.TableName), nil)
		if err != nil {
			return nil, err
		}
		if objRef == nil || tableDef == nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "table "+string(stmt.DatabaseName)+"."+string(stmt.TableName)+" does not exist")
		}
		tableId = tableDef.TblId
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_PITR,
				Definition: &plan.DataDefinition_CreatePitr{
					CreatePitr: &plan.CreatePitr{
						IfNotExists:       stmt.IfNotExists,
						Name:              string(stmt.Name),
						Level:             int32(stmt.Level),
						AccountName:       accountName,
						DatabaseName:      string(stmt.DatabaseName),
						TableName:         string(stmt.TableName),
						PitrValue:         stmt.PitrValue,
						PitrUnit:          stmt.PitrUnit,
						DatabaseId:        databaseId,
						TableId:           tableId,
						AccountId:         accountId,
						CurrentAccountId:  currentAccountId,
						CurrentAccount:    currentAccount,
						OriginAccountName: len(stmt.AccountName) > 0,
					},
				},
			},
		},
	}, nil
}

func buildDropPitr(stmt *tree.DropPitr, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_DROP_PITR
	// Remove privilege check, no account ID validation

	// Build drop pitr plan
	dropPitr := &plan.DropPitr{
		IfExists: stmt.IfExists,
		Name:     string(stmt.Name),
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: ddlType,
				Definition: &plan.DataDefinition_DropPitr{
					DropPitr: dropPitr,
				},
			},
		},
	}, nil
}

func buildCreateCDC(stmt *tree.CreateCDC, ctx CompilerContext) (*Plan, error) {
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_CDC,
				Definition: &plan.DataDefinition_CreateCdc{
					CreateCdc: &plan.CreateCDC{
						IfNotExists: stmt.IfNotExists,
						TaskName:    string(stmt.TaskName),
						SourceUri:   stmt.SourceUri,
						SinkType:    stmt.SinkType,
						SinkUri:     stmt.SinkUri,
						Tables:      stmt.Tables,
						Option:      stmt.Option,
						UserName:    ctx.GetUserName(),
						AccountName: ctx.GetAccountName(),
						AccountId:   accountId,
					},
				},
			},
		},
	}, nil
}

func buildDropCDC(stmt *tree.DropCDC, ctx CompilerContext) (*Plan, error) {
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_CDC,
				Definition: &plan.DataDefinition_DropCdc{
					DropCdc: &plan.DropCDC{
						IfExists:  stmt.IfExists,
						AccountId: accountId,
						All:       stmt.Option.All,
						TaskName:  string(stmt.Option.TaskName),
					},
				},
			},
		},
	}, nil
}

func constructAddedPartitionDefs(
	ctx CompilerContext,
	tableDef *plan.TableDef,
	clause *tree.AlterPartitionAddPartitionClause,
) ([]*plan.PartitionDef, error) {
	originTableStmt, err := parsers.ParseOne(
		ctx.GetContext(),
		dialect.MYSQL,
		tableDef.Createsql,
		ctx.GetLowerCaseTableNames(),
	)
	if err != nil {
		return nil, err
	}

	ct, ok := originTableStmt.(*tree.CreateTable)
	if !ok {
		return nil, moerr.NewNotSupportedNoCtx("unsupported ADD PARTITION not in create table")
	}
	if ct == nil || ct.PartitionOption == nil || ct.PartitionOption.PartBy == nil {
		return nil, moerr.NewNotSupportedNoCtx("Partition management on a not partitioned table is not possible")
	}

	switch ct.PartitionOption.PartBy.PType.(type) {
	case *tree.RangeType, *tree.ListType:
		originParts := ct.PartitionOption.Partitions
		newParts := clause.Partitions
		if len(newParts) == 0 {
			return nil, nil
		}

		merged := make([]*tree.Partition, 0, len(originParts)+len(newParts))
		merged = append(merged, originParts...)
		merged = append(merged, newParts...)

		combined := tree.NewPartitionOption(
			ct.PartitionOption.PartBy,
			ct.PartitionOption.SubPartBy,
			merged,
		)

		partBuilder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
		partBindCtx := NewBindContext(partBuilder, nil)
		nodeID := partBuilder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			Stats:       nil,
			ObjRef:      nil,
			TableDef:    tableDef,
			BindingTags: []int32{partBuilder.genNewBindTag()},
		}, partBindCtx)
		if err := partBuilder.addBinding(nodeID, tree.AliasClause{}, partBindCtx); err != nil {
			return nil, err
		}
		partitionBinder := NewPartitionBinder(partBuilder, partBindCtx)
		allDefs, err := partitionBinder.buildPartitionDefs(ctx.GetContext(), combined)
		if err != nil {
			return nil, err
		}
		originCnt := len(originParts)
		if originCnt > len(allDefs.PartitionDefs) {
			return nil, moerr.NewInternalError(ctx.GetContext(), "invalid partition definition state")
		}
		return allDefs.PartitionDefs[originCnt:], nil
	default:
		return nil, moerr.NewNotSupportedNoCtx("unsupported partition method in ADD PARTITION")
	}
}

// validateWriteFilePattern validates the WRITE_FILE_PATTERN option that makes an
// external table writable, plus the column restrictions writability implies.
// No-op for read-only external tables (option absent). tableDef may be nil when
// only the param-level options need checking.
// effectiveWriteCompression mirrors crt.GetCompressType's decision (inlined to
// avoid the plan<-crt import cycle): an explicit non-auto compression wins,
// otherwise the type is auto-detected from any of the given file paths'
// suffixes. Returns the effective type and whether it is compressed.
func effectiveWriteCompression(comp string, paths ...string) (string, bool) {
	comp = strings.ToLower(strings.TrimSpace(comp))
	if comp != "" && comp != tree.AUTO {
		return comp, comp != tree.NOCOMPRESS
	}
	suffixes := []string{".tar.gz", ".tar.gzip", ".tar.bz2", ".tar.bzip2", ".gz", ".gzip", ".bz2", ".bzip2", ".lz4"}
	for _, p := range paths {
		p = strings.ToLower(p)
		for _, suf := range suffixes {
			if strings.HasSuffix(p, suf) {
				return strings.TrimPrefix(suf, "."), true
			}
		}
	}
	return tree.NOCOMPRESS, false
}

func validateWriteFilePattern(ctx context.Context, param *tree.ExternParam, tableDef *TableDef) error {
	pattern, ok := GetWriteFilePattern(param)
	if !ok {
		return nil
	}
	if !strings.HasPrefix(pattern, "stage://") {
		return moerr.NewBadConfigf(ctx, "WRITE_FILE_PATTERN must be a stage:// path, got '%s'", pattern)
	}
	// Duplicate option keys would let validation inspect a different value than
	// the one the read-side init later keeps (it walks the whole slice, last
	// wins) — so a table could validate as csv and run as parquet.
	if err := rejectDuplicateKeys(ctx, param.Option,
		[]string{"format", "jsondata", "compression", "filepath", ExternalWriteFilePatternKey}); err != nil {
		return err
	}
	// The writer streams plain bytes; the read path decompresses based on the
	// COMPRESSION option (or, when unset/auto, the file suffix), so any
	// effective compression — from the option, the read FILEPATH glob, or the
	// write pattern itself — would make the produced files unreadable.
	comp := param.CompressType
	if comp == "" {
		comp = getRawOption(param.Option, "compression")
	}
	if eff, compressed := effectiveWriteCompression(comp, getRawOption(param.Option, "filepath"), pattern); compressed {
		return moerr.NewBadConfigf(ctx, "writable external table does not support compression (effective '%s'); the writer emits uncompressed files", eff)
	}
	format := strings.ToLower(param.Format)
	if format == "" {
		format = strings.ToLower(getRawOption(param.Option, "format"))
	}
	if format != tree.CSV && format != tree.JSONLINE {
		return moerr.NewBadConfigf(ctx, "writable external table only supports csv and jsonline formats, got '%s'", format)
	}
	if format == tree.JSONLINE {
		jsondata := strings.ToLower(param.JsonData)
		if jsondata == "" {
			jsondata = strings.ToLower(getRawOption(param.Option, "jsondata"))
		}
		// The writer emits one JSON object per line; jsondata='array' tables would
		// not be able to read back their own output.
		if jsondata == tree.ARRAY {
			return moerr.NewBadConfig(ctx, "writable external table does not support jsondata 'array', use 'object'")
		}
		// JSON strings have no enclosure mechanism: a printable line terminator
		// occurring inside a value would split the record on readback. \n and
		// \r\n are safe because the JSON encoder \u-escapes control characters.
		if param.Tail != nil && param.Tail.Lines != nil && param.Tail.Lines.TerminatedBy != nil {
			if v := param.Tail.Lines.TerminatedBy.Value; v != "" && v != "\n" && v != "\r\n" {
				return moerr.NewBadConfig(ctx, "writable external table with format 'jsonline' only supports LINES TERMINATED BY '\\n' or '\\r\\n'")
			}
		}
		// COMMENT is unsafe for jsonline: the reader matches the marker against the
		// raw line prefix before LINES STARTING BY is consumed, but every jsonline
		// record deterministically begins with LINES STARTING BY + '{' and JSON has
		// no enclosure mechanism to hide it. A marker such as '{' would skip every
		// row the writer produced, so reject COMMENT on writable jsonline tables.
		if GetCSVComment(param) != "" {
			return moerr.NewBadConfig(ctx, "writable external table with format 'jsonline' does not support the COMMENT option")
		}
	}
	if format == tree.CSV {
		if err := validateWritableComment(ctx, param); err != nil {
			return err
		}
	}
	// Dry-run the pattern against a fixed timestamp to reject bad directives at DDL time.
	if _, err := externalwrite.ExpandFilePattern(pattern, time.Unix(0, 0).UTC()); err != nil {
		return err
	}
	// Every parallel pipeline owns one writer and expands the pattern against the
	// same statement timestamp, so without a per-writer-unique directive all
	// pipelines would open the identical path and clobber each other.
	if !externalwrite.PatternHasUniqueDirective(pattern) {
		return moerr.NewBadConfigf(ctx, "WRITE_FILE_PATTERN must contain a %%U or %%<n>N directive so parallel writers produce distinct files, got '%s'", pattern)
	}
	// Reject FIELDS/LINES combinations the writer cannot make round-trip.
	if param.Tail != nil {
		if err := validateWritableEscape(ctx, param.Tail); err != nil {
			return err
		}
		// The reader skips IGNORE N LINES per file, but the writer emits no
		// header lines, so real data rows would be discarded on readback.
		if param.Tail.IgnoredLines > 0 {
			return moerr.NewBadConfig(ctx, "writable external table does not support IGNORE ... LINES")
		}
	}
	if tableDef != nil {
		for _, col := range tableDef.Cols {
			// Hidden/synthetic columns (e.g. the fake-PK column added to tables
			// without a primary key) are never written to the output file.
			if col.Hidden {
				continue
			}
			// AUTO_INCREMENT values are generated by the PreInsert operator, which
			// the minimal external-insert plan does not run.
			if col.Typ.AutoIncr {
				return moerr.NewBadConfigf(ctx, "writable external table does not support AUTO_INCREMENT column '%s'", col.Name)
			}
			// Generated columns are recomputed (and explicit writes rejected) only
			// by the normal insert/load binders. The external INSERT/LOAD path uses
			// the minimal legacy builder, which neither filters nor recomputes them,
			// so a generated column would store arbitrary or NULL/default values.
			if col.GeneratedCol != nil {
				return moerr.NewBadConfigf(ctx, "writable external table does not support generated column '%s'", col.Name)
			}
			// Binary payloads cannot round-trip through JSON strings: bit bytes
			// >= 0x80 are invalid UTF-8, and binary/varbinary/blob would need a
			// base64 encoding the jsonline READER does not decode.
			if format == tree.JSONLINE {
				switch types.T(col.Typ.Id) {
				case types.T_bit, types.T_binary, types.T_varbinary, types.T_blob:
					return moerr.NewBadConfigf(ctx, "writable external table with format 'jsonline' does not support %s column '%s'",
						strings.ToLower(types.T(col.Typ.Id).String()), col.Name)
				}
			}
		}
	}
	return nil
}

// validateWritableComment rejects CSV COMMENT markers the writer cannot make
// round-trip. The reader skips a line whose RAW prefix (before unquoting)
// matches the marker, so the writer must never produce such a line. The writer
// guards a non-NULL, unenclosed first field by enclosing it (the line then
// begins with the enclosure byte), but three structural cases cannot be guarded
// that way and are rejected here:
//
//  1. LINES STARTING BY: the marker is matched on the raw prefix BEFORE the
//     starting-by prefix is consumed, so a marker contained in that fixed prefix
//     (e.g. COMMENT 'REM' with LINES STARTING BY 'REM:') skips every row. COMMENT
//     and LINES STARTING BY are therefore mutually exclusive for writable tables.
//  2. The enclosure byte: a first field that must be enclosed (or the writer's
//     own guard) makes the line begin with the enclosure byte, so a marker that
//     begins with it would skip those rows. Enclosing cannot help — it is the
//     collision. Reject a marker whose first byte is the enclosure byte.
//  3. The escape byte: the writer escapes the (unenclosed) first field, so a
//     value can be written starting with the escape byte (e.g. a doubled escape),
//     which a marker beginning with that byte would skip. Reject it too.
//  4. The field terminator: an empty first field makes the line begin with the
//     terminator, so a marker beginning with it would skip such rows. Reject it.
//  5. The NULL sentinel: a NULL first column is written verbatim as `\N` (it
//     cannot be enclosed without reading back as the string), so a marker that is
//     a prefix of `\N` (or vice-versa) skips every row with a leading NULL. The
//     sentinel uses a literal backslash regardless of the configured escape, so
//     this is checked independently of rule 3.
func validateWritableComment(ctx context.Context, param *tree.ExternParam) error {
	comment := GetCSVComment(param)
	if comment == "" {
		return nil
	}
	var fields *tree.Fields
	if param.Tail != nil {
		if param.Tail.Lines != nil && param.Tail.Lines.StartingBy != "" {
			return moerr.NewBadConfig(ctx, "writable external table does not support COMMENT together with LINES STARTING BY")
		}
		fields = param.Tail.Fields
	}
	enclosed := byte('"')
	if fields != nil && fields.EnclosedBy != nil && fields.EnclosedBy.Value != 0 {
		enclosed = fields.EnclosedBy.Value
	}
	if comment[0] == enclosed {
		return moerr.NewBadConfigf(ctx, "writable external table COMMENT must not start with the enclosure byte '%c'", enclosed)
	}
	// Escape byte: default '\\'; an explicit empty FIELDS ESCAPED BY disables it.
	escape := byte('\\')
	if fields != nil && fields.EscapedBy != nil {
		escape = fields.EscapedBy.Value // 0 means escaping disabled
	}
	if escape != 0 && comment[0] == escape {
		return moerr.NewBadConfigf(ctx, "writable external table COMMENT must not start with the escape byte '%c'", escape)
	}
	// Field terminator: default ','; an empty first field starts the line with it.
	fieldTerm := ","
	if fields != nil && fields.Terminated != nil && fields.Terminated.Value != "" {
		fieldTerm = fields.Terminated.Value
	}
	if comment[0] == fieldTerm[0] {
		return moerr.NewBadConfigf(ctx, "writable external table COMMENT must not start with the field terminator byte '%c'", fieldTerm[0])
	}
	csvNull := `\N`
	if strings.HasPrefix(comment, csvNull) || strings.HasPrefix(csvNull, comment) {
		return moerr.NewBadConfig(ctx, "writable external table COMMENT must not collide with the NULL sentinel \\N")
	}
	return nil
}

// validateWritableEscape checks that the FIELDS/LINES configuration can
// round-trip through the writer + reader pair.
//
// Escape: the writer escapes by doubling the character, and the reader
// unescapes E-sequences in BOTH quoted and unquoted fields, so a custom escape
// must not collide with bytes the reader treats specially. An empty FIELDS
// ESCAPED BY
// (escaping disabled) is allowed; the writer disables escaping too. Note: with
// any non-'\\' escape (including disabled), a string whose content is exactly
// `\N` reads back as NULL — the reader matches the null sentinel after
// unescaping and only exempts it for the default backslash.
//
// Enclosure: values containing structural bytes are written enclosed
// (OPTIONALLY ENCLOSED semantics), which requires the enclosure byte itself
// to be distinguishable from the terminators — no quoting discipline can fix
// an enclosure byte that also begins a field or record boundary.
func validateWritableEscape(ctx context.Context, tail *tree.TailParameter) error {
	f := tail.Fields

	fieldTerm := ","
	enclosed := byte('"')
	var esc byte = '\\'
	if f != nil {
		if f.Terminated != nil && f.Terminated.Value != "" {
			fieldTerm = f.Terminated.Value
		}
		if f.EnclosedBy != nil && f.EnclosedBy.Value != 0 {
			enclosed = f.EnclosedBy.Value
		}
		if f.EscapedBy != nil {
			esc = f.EscapedBy.Value // 0 = escaping disabled
		}
	}
	lineTerm := "\n"
	startingBy := ""
	if l := tail.Lines; l != nil {
		if l.TerminatedBy != nil && l.TerminatedBy.Value != "" {
			lineTerm = l.TerminatedBy.Value
		}
		startingBy = l.StartingBy
	}

	// The CSV reader rejects a field terminator whose first byte is a quote,
	// CR, LF or NUL (csvparser.validDelim / NewCSVParser), so such a table
	// could be created and written but never read back.
	if b := fieldTerm[0]; b == 0 || b == '"' || b == '\r' || b == '\n' {
		return moerr.NewBadConfig(ctx, "writable external table FIELDS TERMINATED BY cannot start with a quote, CR, LF or NUL byte")
	}

	// The escape/enclosure conflict applies to the DEFAULT backslash escape
	// too: ENCLOSED BY '\\' with the default escape makes the tokenizer's
	// doubled-delimiter collapse and the unescaper both consume the same
	// bytes, corrupting values on readback.
	if esc != 0 && esc == enclosed {
		return moerr.NewBadConfigf(ctx, "writable external table cannot use FIELDS ESCAPED BY '%c': it conflicts with the enclosure character", esc)
	}
	if esc != 0 && esc != '\\' {
		// The reader's unescaper maps E+{0,b,n,r,t,Z} to control characters, so a
		// doubled escape (E E) would decode to a control char instead of E itself.
		if strings.IndexByte("0bnrtZ", esc) >= 0 {
			return moerr.NewBadConfigf(ctx, "writable external table cannot use FIELDS ESCAPED BY '%c': the reader maps '%c'-sequences to control characters", esc, esc)
		}
		// Control bytes as the escape would collide with the writer's own
		// E+'r' CR encoding and the reader's record handling.
		if esc < 0x20 || esc == 0x7f {
			return moerr.NewBadConfig(ctx, "writable external table cannot use a control character as FIELDS ESCAPED BY")
		}
		for _, s := range []string{fieldTerm, lineTerm, startingBy} {
			if strings.IndexByte(s, esc) >= 0 {
				return moerr.NewBadConfigf(ctx, "writable external table cannot use FIELDS ESCAPED BY '%c': it occurs in a field/line terminator or LINES STARTING BY", esc)
			}
		}
	}

	// The writer encloses values containing structural bytes; the enclosure
	// byte must not itself be part of a terminator or the record prefix.
	for _, s := range []string{fieldTerm, lineTerm, startingBy} {
		if strings.IndexByte(s, enclosed) >= 0 {
			return moerr.NewBadConfigf(ctx, "writable external table cannot use ENCLOSED BY '%c': it occurs in a field/line terminator or LINES STARTING BY", enclosed)
		}
	}
	return nil
}

// validateAndSetHivePartitionOptions parses and validates hive_partitioning options from the DDL,
// normalizes partition column names, extracts column types, and strips hive keys from Option[].
func validateAndSetHivePartitionOptions(ctx context.Context, stmt *tree.CreateTable, createTable *plan.CreateTable) error {
	raw := stmt.Param.Option

	if err := rejectDuplicateKeys(ctx, raw, []string{"hive_partitioning", "hive_partition_columns"}); err != nil {
		return err
	}

	hiveEnabled, hiveCols, err := parseHiveOptionsFromRawOptions(ctx, raw)
	if err != nil {
		return err
	}
	if !hiveEnabled {
		return nil
	}

	if err := rejectDuplicateKeys(ctx, raw, []string{"format", "filepath"}); err != nil {
		return err
	}

	rawFormat := strings.ToLower(getRawOption(raw, "format"))
	if rawFormat != "parquet" {
		return moerr.NewBadConfigf(ctx, "hive_partitioning currently only supports format='parquet', got '%s'", rawFormat)
	}

	rawFilepath := getRawOption(raw, "filepath")
	if len(stmt.Param.StageName) != 0 || strings.HasPrefix(rawFilepath, "stage://") {
		return moerr.NewBadConfig(ctx, "hive_partitioning does not support stage external tables")
	}

	if len(hiveCols) == 0 || (len(hiveCols) == 1 && strings.EqualFold(strings.TrimSpace(hiveCols[0]), "auto")) {
		prepareHiveInferenceParam(stmt.Param, raw)
		hiveCols, err = inferHivePartitionColumns(ctx, stmt.Param)
		if err != nil {
			return err
		}
	}

	normalized := make([]string, 0, len(hiveCols))
	colTypes := make([]tree.HivePartColType, 0, len(hiveCols))
	seen := make(map[string]bool)
	for _, pc := range hiveCols {
		col := findColInTableDefCaseInsensitive(createTable.TableDef.Cols, pc)
		if col == nil {
			return moerr.NewBadConfigf(ctx, "partition column '%s' not found in table columns", pc)
		}
		if col.Hidden {
			return moerr.NewBadConfigf(ctx, "partition column '%s' cannot be a hidden column", pc)
		}
		if col.GeneratedCol != nil {
			return moerr.NewBadConfigf(ctx, "partition column '%s' cannot be a generated column", pc)
		}
		typId := types.T(col.Typ.Id)
		if typId == types.T_array_float32 || typId == types.T_array_float64 {
			return moerr.NewBadConfigf(ctx, "partition column '%s' cannot be a VECTOR type", pc)
		}
		canonical := strings.ToLower(col.Name)
		if seen[canonical] {
			return moerr.NewBadConfigf(ctx, "duplicate partition column '%s'", pc)
		}
		seen[canonical] = true
		normalized = append(normalized, canonical)

		nullable := true
		if col.Default != nil {
			nullable = col.Default.NullAbility
		}
		colTypes = append(colTypes, tree.HivePartColType{
			Id:          col.Typ.Id,
			Width:       col.Typ.Width,
			Scale:       col.Typ.Scale,
			Enumvalues:  col.Typ.Enumvalues,
			NullAbility: nullable,
		})
	}

	stmt.Param.HivePartitioning = true
	stmt.Param.HivePartitionCols = normalized
	stmt.Param.HivePartitionColTypes = colTypes
	stmt.Param.Option = stripHiveOptionKeys(stmt.Param.Option)
	return nil
}

func prepareHiveInferenceParam(param *tree.ExternParam, options []string) {
	if param.Filepath == "" {
		param.Filepath = getRawOption(options, "filepath")
	}
	if param.Format == "" {
		param.Format = strings.ToLower(getRawOption(options, "format"))
	}
	if param.ScanType != tree.S3 {
		return
	}
	if param.S3Param == nil {
		param.S3Param = &tree.S3Parameter{}
	}
	if param.S3Param.Endpoint == "" {
		param.S3Param.Endpoint = getRawOption(options, "endpoint")
	}
	if param.S3Param.Region == "" {
		param.S3Param.Region = getRawOption(options, "region")
	}
	if param.S3Param.APIKey == "" {
		param.S3Param.APIKey = getRawOption(options, "access_key_id")
	}
	if param.S3Param.APISecret == "" {
		param.S3Param.APISecret = getRawOption(options, "secret_access_key")
	}
	if param.S3Param.Bucket == "" {
		param.S3Param.Bucket = getRawOption(options, "bucket")
	}
	if param.S3Param.Provider == "" {
		param.S3Param.Provider = getRawOption(options, "provider")
	}
	if param.S3Param.RoleArn == "" {
		param.S3Param.RoleArn = getRawOption(options, "role_arn")
	}
	if param.S3Param.ExternalId == "" {
		param.S3Param.ExternalId = getRawOption(options, "external_id")
	}
}

const (
	hivePartitionInferMaxDepth      = 16
	hivePartitionInferMaxListCalls  = 64
	hivePartitionInferMaxSampleDirs = 64
)

func inferHivePartitionColumns(ctx context.Context, param *tree.ExternParam) ([]string, error) {
	basePath := normalizeHiveInferPath(param.Filepath)
	listDir, err := newHiveInferListDir(param, basePath)
	if err != nil {
		return nil, err
	}

	currentPrefixes := []string{basePath}
	inferred := make([]string, 0)
	listCalls := 0
	for depth := 0; depth < hivePartitionInferMaxDepth && len(currentPrefixes) > 0; depth++ {
		var levelKey string
		nextPrefixes := make([]string, 0)
		for _, prefix := range currentPrefixes {
			listCalls++
			if listCalls > hivePartitionInferMaxListCalls {
				return nil, moerr.NewBadConfigf(ctx,
					"hive partition auto inference exceeded %d List calls; specify hive_partition_columns explicitly",
					hivePartitionInferMaxListCalls)
			}
			for entry, err := range listDir(ctx, prefix) {
				if err != nil {
					return nil, moerr.NewBadConfigf(ctx,
						"hive partition auto inference failed to list '%s': %v; specify hive_partition_columns explicitly",
						prefix, err)
				}
				if entry == nil || !entry.IsDir || isHiveInferHidden(entry.Name) {
					continue
				}
				key, isHive, err := parseHiveInferSegmentKey(entry.Name)
				if err != nil {
					return nil, err
				}
				if !isHive {
					continue
				}
				if levelKey == "" {
					levelKey = key
				} else if levelKey != key {
					return nil, moerr.NewBadConfigf(ctx,
						"hive partition auto inference found mixed keys '%s' and '%s' at the same level; specify hive_partition_columns explicitly",
						levelKey, key)
				}
				if len(nextPrefixes) < hivePartitionInferMaxSampleDirs {
					nextPrefixes = append(nextPrefixes, path.Join(prefix, entry.Name))
				}
			}
			if len(nextPrefixes) >= hivePartitionInferMaxSampleDirs {
				break
			}
		}
		if levelKey == "" {
			break
		}
		inferred = append(inferred, levelKey)
		currentPrefixes = nextPrefixes
	}
	if len(inferred) == 0 {
		return nil, moerr.NewBadConfig(ctx,
			"hive partition auto inference found no hive-style partition directories; specify hive_partition_columns explicitly")
	}
	return inferred, nil
}

type hiveInferListDirFunc func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error]

func newHiveInferListDir(param *tree.ExternParam, basePath string) (hiveInferListDirFunc, error) {
	if param.ScanType == tree.S3 {
		fs, baseReadPath, err := GetForETLWithType(param, basePath)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
			return fs.List(ctx, deriveHiveInferReadPath(basePath, baseReadPath, prefix))
		}, nil
	}
	return func(ctx context.Context, prefix string) iter.Seq2[*fileservice.DirEntry, error] {
		fs, readPath, err := GetForETLWithType(param, prefix)
		if err != nil {
			return func(yield func(*fileservice.DirEntry, error) bool) {
				yield(nil, err)
			}
		}
		return fs.List(ctx, readPath)
	}, nil
}

func normalizeHiveInferPath(p string) string {
	p = strings.TrimSpace(p)
	if strings.HasPrefix(p, "etl:") {
		return path.Clean(p)
	}
	if strings.Contains(p, fileservice.ServiceNameSeparator) {
		return path.Clean(p)
	}
	return path.Clean("/" + p)
}

func deriveHiveInferReadPath(basePath, baseReadPath, prefix string) string {
	prefix = normalizeHiveInferPath(prefix)
	if prefix == basePath {
		return baseReadPath
	}
	if !strings.HasPrefix(prefix, basePath+"/") {
		return prefix
	}
	rel := strings.TrimPrefix(prefix, basePath+"/")
	if rel == "" {
		return baseReadPath
	}
	if baseReadPath == "" || baseReadPath == "." {
		return rel
	}
	return path.Join(baseReadPath, rel)
}

func parseHiveInferSegmentKey(segment string) (string, bool, error) {
	idx := strings.IndexByte(segment, '=')
	if idx <= 0 {
		return "", false, nil
	}
	key := segment[:idx]
	if key == "." || key == ".." {
		return "", true, moerr.NewBadConfigf(context.Background(),
			"invalid hive partition key '%s' during auto inference", key)
	}
	for _, r := range key {
		if r != '_' && (r < '0' || r > '9') && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return "", true, moerr.NewBadConfigf(context.Background(),
				"invalid hive partition key '%s' during auto inference", key)
		}
	}
	return strings.ToLower(key), true, nil
}

func isHiveInferHidden(name string) bool {
	return len(name) > 0 && (name[0] == '.' || name[0] == '_')
}

func parseHiveOptionsFromRawOptions(ctx context.Context, options []string) (enabled bool, cols []string, err error) {
	var hiveVal string
	var colsVal string
	for i := 0; i < len(options); i += 2 {
		key := strings.ToLower(options[i])
		switch key {
		case "hive_partitioning":
			hiveVal = strings.ToLower(options[i+1])
		case "hive_partition_columns":
			colsVal = options[i+1]
		}
	}
	if hiveVal == "" {
		if strings.TrimSpace(colsVal) != "" {
			return false, nil, moerr.NewBadConfig(ctx, "hive_partition_columns requires hive_partitioning='true'")
		}
		return false, nil, nil
	}
	if hiveVal != "true" && hiveVal != "false" {
		return false, nil, moerr.NewBadConfigf(ctx, "hive_partitioning must be 'true' or 'false', got '%s'", hiveVal)
	}
	if hiveVal == "false" {
		if strings.TrimSpace(colsVal) != "" {
			return false, nil, moerr.NewBadConfig(ctx, "hive_partition_columns requires hive_partitioning='true'")
		}
		return false, nil, nil
	}
	if colsVal == "" {
		return true, nil, nil
	}
	parts := strings.Split(colsVal, ",")
	cols = make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			cols = append(cols, p)
		}
	}
	return true, cols, nil
}

func rejectDuplicateKeys(ctx context.Context, options []string, keys []string) error {
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}
	seen := make(map[string]bool)
	for i := 0; i < len(options); i += 2 {
		key := strings.ToLower(options[i])
		if !keySet[key] {
			continue
		}
		if seen[key] {
			return moerr.NewBadConfigf(ctx, "duplicate option key '%s'", key)
		}
		seen[key] = true
	}
	return nil
}

func getRawOption(options []string, key string) string {
	for i := 0; i < len(options); i += 2 {
		if strings.ToLower(options[i]) == key {
			return options[i+1]
		}
	}
	return ""
}

func stripHiveOptionKeys(opt []string) []string {
	out := make([]string, 0, len(opt))
	for i := 0; i < len(opt); i += 2 {
		key := strings.ToLower(opt[i])
		if key == "hive_partitioning" || key == "hive_partition_columns" {
			continue
		}
		out = append(out, opt[i], opt[i+1])
	}
	return out
}

func findColInTableDefCaseInsensitive(cols []*plan.ColDef, name string) *plan.ColDef {
	lower := strings.ToLower(name)
	for _, col := range cols {
		if strings.ToLower(col.Name) == lower {
			return col
		}
	}
	return nil
}
