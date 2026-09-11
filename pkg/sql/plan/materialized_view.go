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
	"context"
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/catalog/mvdefinition"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// Direct FROM sources are the complete scheduling and snapshot boundary.
// Nested queries and session inputs cannot participate until their dependencies
// are represented too. Inspect the AST before binding can fold volatile inputs.
func validateMaterializedViewQuery(ctx context.Context, stmt *tree.Select) error {
	unsupported := func() error {
		return moerr.NewNotSupported(ctx, "materialized view requires deterministic expressions without subqueries, CTEs or session variables")
	}
	if stmt == nil || stmt.With != nil {
		return unsupported()
	}
	var err error
	walkASTExpressions(stmt, func(expr tree.Expr) bool {
		if err != nil {
			return false
		}
		switch node := expr.(type) {
		case *tree.Subquery, *tree.VarExpr, *tree.ParamExpr:
			err = unsupported()
		case *tree.FuncExpr:
			if function.GetFunctionIsVolatileOrRealTimeRelatedByName(materializedViewIncrementalFunctionName(node)) {
				err = unsupported()
			}
		}
		return err == nil
	})
	return err
}

func buildMaterializedViewDefinition(ctx CompilerContext, stmt *tree.CreateView, createView *plan.CreateView) error {
	if stmt.RefreshTiming == tree.MaterializedViewRefreshOnDemand && stmt.RefreshMethod != tree.MaterializedViewRefreshComplete {
		return moerr.NewNotSupported(ctx.GetContext(), "materialized view ON DEMAND requires COMPLETE refresh")
	}
	sources, ok := materializedViewDefinitionSources(stmt.AsSource, ctx.DefaultDatabase())
	if !ok || len(sources) == 0 || len(sources) > mvdefinition.MaxSources {
		return moerr.NewNotSupported(ctx.GetContext(), "materialized view requires 1 to 16 direct base sources or UNION ALL branches")
	}
	accountID, err := ctx.GetAccountId()
	if err != nil {
		return err
	}
	target := createView.TableDef
	d := &mvdefinition.Definition{Format: mvdefinition.Format, RequiredCapability: mvdefinition.RequiredCapability, AccountID: accountID,
		Target: mvdefinition.Relation{Database: createView.Database, Name: target.Name}, Generation: 1,
		CreateSQL: tree.String(stmt, dialect.MYSQL), RefreshSQL: materializedViewRefreshSQL(stmt.AsSource),
		Method: materializedViewRefreshMethodName(stmt.RefreshMethod), Timing: materializedViewRefreshTimingName(stmt.RefreshTiming)}
	for _, column := range target.Cols {
		if !column.Hidden {
			d.Columns = append(d.Columns, column.Name)
		}
	}
	for _, source := range sources {
		database := string(source.SchemaName)
		if database == "" {
			database = ctx.DefaultDatabase()
		}
		name := string(source.ObjectName)
		if err := validateMaterializedViewSourceTable(ctx, database, name); err != nil {
			return err
		}
		_, def, err := ctx.Resolve(database, name, ctx.GetSnapshot())
		if err != nil {
			return err
		}
		version := def.Version
		d.Sources = append(d.Sources, mvdefinition.Source{Relation: mvdefinition.Relation{Database: database, Name: name, DatabaseID: def.DbId, ID: def.TblId}, Version: &version})
	}
	incrementalSpec := ""
	if stmt.RefreshTiming == tree.MaterializedViewRefreshOnChange && stmt.RefreshMethod != tree.MaterializedViewRefreshComplete {
		var stateCols []*ColDef
		var refreshSQL string
		incrementalSpec, stateCols, refreshSQL = buildMaterializedViewIncrementalPlanForDatabase(stmt.AsSource, target.Cols, ctx.DefaultDatabase(), materializedViewStateTableName(createView.Database, target.Name))
		if incrementalSpec != "" {
			for _, col := range stateCols {
				col.ColId = uint64(len(target.Cols))
				target.Cols = append(target.Cols, col)
			}
			d.RefreshSQL = refreshSQL
			d.Incremental = incrementalSpec
			desc, err := mvdefinition.DecodeIncremental(incrementalSpec)
			if err != nil {
				return err
			}
			if desc.StateTable != "" {
				d.State = &mvdefinition.Relation{Database: createView.Database, Name: desc.StateTable}
			}
		}
	}
	if stmt.RefreshMethod == tree.MaterializedViewRefreshFast && incrementalSpec == "" {
		return moerr.NewNotSupportedf(ctx.GetContext(), "materialized view FAST refresh is not supported: %s", materializedViewIncrementalUnsupportedReason(stmt.AsSource))
	}
	if primaryKeys := materializedViewIncrementalPrimaryKey(incrementalSpec); len(primaryKeys) > 0 {
		for _, col := range target.Cols {
			if strings.EqualFold(col.Name, primaryKeys[0]) {
				col.Primary = true
				col.NotNull = true
				col.Typ.NotNullable = true
				if col.Default != nil {
					col.Default.NullAbility = false
				}
				break
			}
		}
		target.Pkey = &PrimaryKeyDef{Names: primaryKeys, PkeyColName: primaryKeys[0]}
	} else {
		// Full-refresh-only and legacy incremental definitions retain the
		// ordinary fake key used by existing materialized views.
		fakePK := &ColDef{
			ColId: uint64(len(target.Cols)), Name: catalog.FakePrimaryKeyColName, Hidden: true,
			Typ: Type{Id: int32(types.T_uint64), AutoIncr: true}, Default: &plan.Default{NullAbility: false},
			NotNull: true, Primary: true, Comment: materializedViewMarkerComment,
		}
		target.Cols = append(target.Cols, fakePK)
		target.Pkey = &PrimaryKeyDef{
			Names: []string{catalog.FakePrimaryKeyColName}, PkeyColName: catalog.FakePrimaryKeyColName,
		}
	}
	// The durable view kind/ViewSql is a predecessor read/write rejection barrier.
	// New catalog readers project this authenticated physical object for scanning.
	var guard ViewData
	if target.ViewSql == nil || json.Unmarshal([]byte(target.ViewSql.View), &guard) != nil {
		return mvdefinition.Invalid("missing view syntax barrier")
	}
	guard.Stmt = d.CreateSQL
	guardSQL, err := json.Marshal(guard)
	if err != nil {
		return err
	}
	target.ViewSql = &plan.ViewDef{View: string(guardSQL)}
	target.TableType = catalog.SystemViewRel
	for _, item := range target.Defs {
		if props := item.GetProperties(); props != nil {
			for _, p := range props.Properties {
				if p.Key == catalog.SystemRelAttr_Kind {
					p.Value = catalog.SystemViewRel
				}
			}
		}
	}
	if err := d.Validate(false); err != nil {
		return err
	}
	encoded, err := mvdefinition.Encode(d)
	if err != nil {
		return err
	}
	target.Defs = append(target.Defs, &plan.TableDef_DefType{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: mvdefinition.Property, Value: encoded}}}}})
	return nil
}
