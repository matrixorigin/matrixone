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
		case *tree.CastExpr:
			// The AST does not retain the source column type here. A cast from
			// TIMESTAMP to DATE/DATETIME/string can therefore be timezone
			// sensitive while looking identical to a numeric cast. Refresh
			// workers do not inherit the defining session's timezone.
			err = unsupported()
		case *tree.IntervalExpr:
			// TIMESTAMP +/- INTERVAL uses the session timezone for calendar
			// arithmetic and must not run in a differently configured worker.
			err = unsupported()
		case *tree.FuncExpr:
			name := materializedViewIncrementalFunctionName(node)
			if materializedViewTimezoneSensitiveFunction(name) || function.GetFunctionIsVolatileOrRealTimeRelatedByName(name) {
				err = unsupported()
			}
		}
		return err == nil
	})
	return err
}

func materializedViewTimezoneSensitiveFunction(name string) bool {
	switch name {
	case "date_trunc", "date_format", "to_date", "str_to_date",
		"unix_timestamp", "from_unixtime", "timestampadd", "timestampdiff",
		"date_add", "date_sub", "adddate", "subdate", "addtime", "subtime",
		"extract", "date", "year", "yearweek", "quarter", "month", "week",
		"day", "dayofmonth", "dayofweek", "dayofyear", "hour", "minute",
		"second", "microsecond", "time", "ts_to_time", "convert_tz",
		"weekday", "weekofyear", "dayname", "monthname", "last_day", "interval":
		return true
	default:
		return false
	}
}

// validateMaterializedViewTimezoneColumns closes the gap where binding inserts
// a TIMESTAMP conversion into an otherwise ordinary comparison. The persisted
// MV definition does not carry the defining session timezone, so any reference
// to a TIMESTAMP source column is unsafe for a background refresh.
func validateMaterializedViewTimezoneColumns(ctx CompilerContext, stmt *tree.Select) error {
	sources, ok := materializedViewDefinitionSources(stmt, ctx.DefaultDatabase())
	if !ok {
		return nil
	}
	timestampColumns := make(map[string]struct{})
	for _, source := range sources {
		dbName := string(source.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		_, def, err := ctx.Resolve(dbName, string(source.ObjectName), ctx.GetSnapshot())
		if err != nil {
			return err
		}
		if def == nil {
			continue
		}
		for _, col := range def.Cols {
			if col != nil && types.T(col.Typ.Id) == types.T_timestamp {
				timestampColumns[strings.ToLower(col.Name)] = struct{}{}
			}
		}
	}
	var err error
	if materializedViewHasTimezoneSensitiveJoin(stmt, timestampColumns) {
		return moerr.NewNotSupported(ctx.GetContext(), "materialized view requires deterministic expressions without timezone-dependent TIMESTAMP conversions")
	}
	walkASTExpressions(stmt, func(expr tree.Expr) bool {
		name, ok := expr.(*tree.UnresolvedName)
		if ok && !name.Star {
			if _, exists := timestampColumns[strings.ToLower(name.ColName())]; exists {
				err = moerr.NewNotSupported(ctx.GetContext(), "materialized view requires deterministic expressions without timezone-dependent TIMESTAMP conversions")
				return false
			}
		}
		return true
	})
	return err
}

// UsingJoinCond stores its identifiers outside tree.Expr, and NATURAL JOIN
// stores no identifiers at all. The ordinary expression walker therefore
// cannot see the comparison that binding synthesizes for either form. Reject
// a USING comparison that names a TIMESTAMP column, and conservatively reject
// NATURAL JOIN when one of the direct sources contains TIMESTAMP: the binder
// chooses the common columns after this AST-level admission check.
func materializedViewHasTimezoneSensitiveJoin(stmt *tree.Select, timestampColumns map[string]struct{}) bool {
	if stmt == nil || len(timestampColumns) == 0 {
		return false
	}
	clauses, _ := materializedViewUnionAllClauses(stmt.Select)
	var visit func(tree.TableExpr) bool
	visit = func(expr tree.TableExpr) bool {
		join, ok := expr.(*tree.JoinTableExpr)
		if !ok {
			return false
		}
		if strings.Contains(join.JoinType, "NATURAL") {
			return true
		}
		switch cond := join.Cond.(type) {
		case *tree.UsingJoinCond:
			for _, col := range cond.Cols {
				if _, ok := timestampColumns[strings.ToLower(string(col))]; ok {
					return true
				}
			}
		}
		return visit(join.Left) || visit(join.Right)
	}
	for _, clause := range clauses {
		if clause == nil || clause.From == nil {
			continue
		}
		for _, expr := range clause.From.Tables {
			if visit(expr) {
				return true
			}
		}
	}
	return false
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
		CreateSQL: tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithSingleQuoteString(), tree.WithQuoteIdentifier()), RefreshSQL: materializedViewRefreshSQL(stmt.AsSource),
		Method: materializedViewRefreshMethodName(stmt.RefreshMethod), Timing: materializedViewRefreshTimingName(stmt.RefreshTiming)}
	for _, column := range target.Cols {
		if !column.Hidden {
			d.Columns = append(d.Columns, column.Name)
		}
	}
	resolvedSources := make(map[string]*TableDef, len(sources))
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
		resolvedSources[materializedViewSourceKey(database, name)] = def
		version := def.Version
		d.Sources = append(d.Sources, mvdefinition.Source{Relation: mvdefinition.Relation{Database: database, Name: name, DatabaseID: def.DbId, ID: def.TblId}, Version: &version})
	}
	// Source identities are deduplicated above, but every FROM/JOIN occurrence
	// needs the database captured by CREATE, including aliases of the same table.
	references, _ := materializedViewDefinitionSourceReferences(stmt.AsSource)
	for _, source := range references {
		if source.SchemaName == "" {
			source.SchemaName = tree.Identifier(ctx.DefaultDatabase())
		}
		source.ExplicitSchema = true
	}
	// Re-render after source resolution so complete-refresh workers consume the
	// same database binding accepted by CREATE, including cross-database joins.
	d.RefreshSQL = materializedViewRefreshSQL(stmt.AsSource)
	incrementalSpec := ""
	unsupportedSourceReason := ""
	if stmt.RefreshTiming == tree.MaterializedViewRefreshOnChange && stmt.RefreshMethod != tree.MaterializedViewRefreshComplete {
		var stateCols []*ColDef
		var refreshSQL string
		incrementalSpec, stateCols, refreshSQL = buildMaterializedViewIncrementalPlanForDatabase(stmt.AsSource, target.Cols, ctx.DefaultDatabase(), materializedViewStateTableName(createView.Database, target.Name))
		if incrementalSpec != "" {
			desc, err := mvdefinition.DecodeIncremental(incrementalSpec)
			if err != nil {
				return err
			}
			if reason := materializedViewIncrementalUnsupportedSourceReason(desc, resolvedSources); reason != "" {
				incrementalSpec = ""
				unsupportedSourceReason = reason
			} else {
				for _, col := range stateCols {
					col.ColId = uint64(len(target.Cols))
					target.Cols = append(target.Cols, col)
				}
				d.RefreshSQL = refreshSQL
				d.Incremental = incrementalSpec
				if desc.StateTable != "" {
					d.State = &mvdefinition.Relation{Database: createView.Database, Name: desc.StateTable}
				}
			}
		}
	}
	if stmt.RefreshMethod == tree.MaterializedViewRefreshFast && incrementalSpec == "" {
		if unsupportedSourceReason != "" {
			return moerr.NewNotSupportedf(ctx.GetContext(), "materialized view FAST refresh is not supported: %s", unsupportedSourceReason)
		}
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

func materializedViewSourceKey(database, table string) string {
	return strings.ToLower(database) + "\x00" + strings.ToLower(table)
}

// Delta VALUES transport and serial_full currently use raw bytes. A source
// column with collation-aware equality can change comparisons or state keys;
// a floating DISTINCT value can compare equal in SQL despite a different
// signed-zero byte representation.
func materializedViewIncrementalUnsupportedSourceReason(
	desc *mvdefinition.Incremental,
	sources map[string]*TableDef,
) string {
	if desc == nil {
		return "MV_FAST_UNSUPPORTED_SOURCE_TYPE"
	}
	if len(desc.Branches) > 0 {
		for _, branch := range desc.Branches {
			if reason := materializedViewIncrementalUnsupportedSourceReason(branch.Description, sources); reason != "" {
				return reason
			}
		}
		return ""
	}
	def := sources[materializedViewSourceKey(desc.SourceDatabase, desc.SourceTable)]
	if def == nil {
		return "MV_FAST_UNSUPPORTED_SOURCE_TYPE"
	}
	hasDistinct := false
	for _, agg := range desc.Aggregates {
		if agg.Kind == "count_distinct" || agg.Kind == "sum_distinct" || agg.Kind == "avg_distinct" {
			hasDistinct = true
			break
		}
	}
	for _, name := range desc.SourceColumns {
		found := false
		for _, col := range def.Cols {
			if col != nil && strings.EqualFold(col.Name, name) {
				if hasDistinct && (types.T(col.Typ.Id) == types.T_float32 || types.T(col.Typ.Id) == types.T_float64) {
					return "MV_FAST_UNSUPPORTED_FLOAT_DISTINCT_KEY"
				}
				if !materializedViewIncrementalGroupTypeSupported(col.Typ) &&
					types.T(col.Typ.Id) != types.T_float32 && types.T(col.Typ.Id) != types.T_float64 {
					return "MV_FAST_UNSUPPORTED_SOURCE_COLLATION"
				}
				found = true
				break
			}
		}
		if !found {
			return "MV_FAST_UNSUPPORTED_SOURCE_TYPE"
		}
	}
	return ""
}
