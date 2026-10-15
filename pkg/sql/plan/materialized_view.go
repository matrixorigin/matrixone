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
	"reflect"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// ValidateMaterializedViewSelect is a bounded definition-time AST check. This
// deliberately admits a smaller language than ordinary SELECT in the first release.
func ValidateMaterializedViewSelect(ctx CompilerContext, stmt *tree.Select) error {
	if stmt == nil || stmt.With != nil || stmt.Limit != nil || len(stmt.OrderBy) != 0 || stmt.RankOption != nil || stmt.TimeWindow != nil || stmt.SelectLockInfo != nil || stmt.Ep != nil || len(stmt.IntoVars) != 0 {
		return moerr.NewNotSupported(ctx.GetContext(), "materialized view requires a deterministic native-table SELECT without CTE, ORDER BY, LIMIT, window, snapshot or locking clauses")
	}
	clause, ok := stmt.Select.(*tree.SelectClause)
	if !ok || clause.Distinct || len(clause.Windows) != 0 || clause.IntoExport != nil || len(clause.IntoVars) != 0 {
		return moerr.NewNotSupported(ctx.GetContext(), "materialized view query shape")
	}
	count := 0
	var walk func(reflect.Value, int) error
	walk = func(v reflect.Value, depth int) error {
		if !v.IsValid() {
			return nil
		}
		if depth > 128 || count > 4096 {
			return moerr.NewInvalidInput(ctx.GetContext(), "materialized view definition exceeds binding budget")
		}
		count++
		if v.Kind() == reflect.Interface {
			if v.IsNil() {
				return nil
			}
			return walk(v.Elem(), depth+1)
		}
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return nil
			}
			if v.CanInterface() {
				switch n := v.Interface().(type) {
				case *tree.VarExpr, *tree.Subquery, *tree.Select, *tree.SampleExpr, *tree.ParamExpr, *tree.DefaultVal:
					return moerr.NewNotSupported(ctx.GetContext(), "materialized view variable or subquery")
				case *tree.JoinTableExpr:
					if n.Right != nil && n.JoinType != tree.JOIN_TYPE_INNER && n.JoinType != "" {
						return moerr.NewNotSupported(ctx.GetContext(), "materialized view supports inner joins only")
					}
				case *tree.GroupByClause:
					if n.Cube || n.Rollup || n.GroupingSets {
						return moerr.NewNotSupported(ctx.GetContext(), "materialized view grouping sets")
					}
				case *tree.TableName:
					if n.AtTsExpr != nil {
						return moerr.NewNotSupported(ctx.GetContext(), "materialized view historical source")
					}
				case *tree.FuncExpr:
					if n.WindowSpec != nil || n.Type == tree.FUNC_TYPE_DISTINCT || n.FuncName == nil {
						return moerr.NewNotSupported(ctx.GetContext(), "materialized view aggregate/function shape")
					}
					name := strings.ToLower(n.FuncName.Compare())
					ref, ok := n.Func.FunctionReference.(*tree.UnresolvedName)
					if !ok || ref.NumParts != 1 || n.IsGeneric && mysql.IsSQLModeSensitiveFunctionName(name) {
						return moerr.NewNotSupported(ctx.GetContext(), "materialized view qualified or user-defined function")
					}
					switch name {
					case "sum", "count", "min", "max", "avg", "cast", "abs", "coalesce", "ifnull", "if", "year", "month", "date", "date_trunc", "concat", "concat_ws":
					default:
						return moerr.NewNotSupportedf(ctx.GetContext(), "materialized view function %s", name)
					}
				}
			}
			return walk(v.Elem(), depth+1)
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if v.Type().Field(i).PkgPath == "" {
					if err := walk(v.Field(i), depth+1); err != nil {
						return err
					}
				}
			}
		case reflect.Slice:
			for i := 0; i < v.Len(); i++ {
				if err := walk(v.Index(i), depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(reflect.ValueOf(clause), 0)
}

func buildCreateMaterializedView(stmt *tree.CreateMaterializedView, ctx CompilerContext, isPrepare bool) (*Plan, error) {
	enabled, enableErr := ctx.ResolveVariable("experimental_materialized_view", true, false)
	if enableErr != nil {
		return nil, enableErr
	}
	if enabled != int8(1) && enabled != int64(1) && enabled != uint64(1) && enabled != true {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "enable experimental_materialized_view before creating a materialized view")
	}
	if err := RequirePersistedProtocolVersionForAuthoring(ctx.GetContext(), ctx.GetProcess(), defines.MORPCVersion115); err != nil {
		return nil, err
	}
	if stmt.IntervalSeconds != 0 && stmt.IntervalSeconds < 60 {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "materialized view refresh interval must be at least 60 seconds")
	}
	if err := ValidateMaterializedViewSelect(ctx, stmt.AsSource); err != nil {
		return nil, err
	}
	db := string(stmt.Name.SchemaName)
	if db == "" {
		db = ctx.DefaultDatabase()
	}
	if !catalog.IsMaterializedViewDatabase(db) {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "materialized view requires a user database")
	}
	account, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	inferred, err := inferViewColumns(&materializedViewSourceContext{CompilerContext: ctx, database: db}, stmt.AsSource, nil, db, string(stmt.Name.ObjectName), true)
	if err != nil {
		return nil, err
	}
	if len(inferred.dependencies) == 0 || len(inferred.dependencies) > 16 || len(inferred.columns) > 256 {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "materialized view requires 1..16 native sources and at most 256 columns")
	}
	names := make(map[string]struct{}, len(inferred.columns))
	for _, column := range inferred.columns {
		name := strings.ToLower(column.Name)
		if _, duplicate := names[name]; duplicate {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "materialized view output names must be unique; use explicit aliases")
		}
		names[name] = struct{}{}
	}
	definition := catalog.MVDefinition{FormatVersion: 1, Database: db, IntervalSeconds: stmt.IntervalSeconds, LowerCaseTableNames: ctx.GetLowerCaseTableNames(), OwnerUser: defines.GetUserId(ctx.GetContext()), OwnerRole: defines.GetRoleId(ctx.GetContext()), OwnerName: materializedViewOwnerName(ctx.GetUserName())}
	if mode := parserSQLModeFromContext(ctx); mode != nil {
		definition.SQLMode = *mode
	}
	if proc := ctx.GetProcess(); proc != nil && proc.GetSessionInfo().TimeZone != nil {
		definition.TimeZone = proc.GetSessionInfo().TimeZone.String()
		_, offset := time.Now().In(proc.GetSessionInfo().TimeZone).Zone()
		definition.TimeZoneOffset = int32(offset)
	}
	for _, d := range inferred.dependencies {
		_, sourceDef, resolveErr := ctx.Resolve(d.DatabaseName, d.RelationName, nil)
		if resolveErr != nil {
			return nil, resolveErr
		}
		if sourceDef == nil || sourceDef.IsTemporary {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "materialized view temporary source")
		}
		if d.AccountID != account || d.DatabaseName != db || d.RelationKind != catalog.SystemOrdinaryRel || d.SubscriptionName != "" || IsSnapshotValid(d.Snapshot) {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "materialized view sources must be persistent native tables in the same account/database")
		}
		definition.Sources = append(definition.Sources, catalog.MVSource{AccountID: d.AccountID, DatabaseID: d.DatabaseID, TableID: d.RelationID, LogicalID: d.LogicalID, Version: d.Version, Database: d.DatabaseName, Table: d.RelationName})
	}
	source := stmt.AsSource
	if stable, ok := viewSelectWithExpandedStars(source, inferred.expandedSelectLists); ok {
		source = stable
	}
	definition.Query = tree.StringWithOpts(source, dialect.MYSQL, tree.WithQuoteIdentifier(), tree.WithSingleQuoteString(), tree.WithModeIndependentStringLiterals())
	stableStatement := *stmt
	stableStatement.AsSource = source
	definition.CreateSQL = tree.StringWithOpts(&stableStatement, dialect.MYSQL, tree.WithQuoteIdentifier(), tree.WithSingleQuoteString(), tree.WithModeIndependentStringLiterals())
	if len(definition.Query) > 65536 {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "materialized view query exceeds 64 KiB")
	}
	ctas := &tree.CreateTable{Table: *stmt.Name, IfNotExists: stmt.IfNotExists, IsAsSelect: true, AsSource: source}
	p, err := buildCreateTable(ctx, ctas, nil, isPrepare)
	if err != nil {
		return nil, err
	}
	c := p.GetDdl().GetCreateTable()
	c.CreateAsSelectSql = ""
	p.GetDdl().Query = nil
	c.TableDef.TableType = catalog.SystemMaterializedViewRel
	data, err := json.Marshal(definition)
	if err != nil {
		return nil, err
	}
	for _, d := range c.TableDef.Defs {
		if props := d.GetProperties(); props != nil {
			for _, pr := range props.Properties {
				if pr.Key == catalog.SystemRelAttr_Kind {
					pr.Value = catalog.SystemMaterializedViewRel
				}
				if pr.Key == catalog.SystemRelAttr_CreateSQL {
					pr.Value = definition.CreateSQL
				}
			}
		}
	}
	c.TableDef.Defs = append(c.TableDef.Defs, &pb.TableDef_DefType{Def: &pb.TableDef_DefType_Properties{Properties: &pb.PropertiesDef{Properties: []*pb.Property{{Key: catalog.MaterializedViewProperty, Value: string(data)}}}}})
	return p, nil
}

// Reject unsupported source kinds at resolution, before view expansion or an
// external connector can run. Capturing dependencies after expansion alone is
// too late to enforce the P1 native-source contract.
type materializedViewSourceContext struct {
	CompilerContext
	database string
}

func (c *materializedViewSourceContext) Resolve(database, table string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	if database == "" {
		database = c.DefaultDatabase()
	}
	if database != c.database || IsSnapshotValid(snapshot) {
		return nil, nil, moerr.NewNotSupported(c.GetContext(), "materialized view sources require the current native database")
	}
	ref, def, err := c.CompilerContext.Resolve(database, table, snapshot)
	if err != nil || def == nil {
		return ref, def, err
	}
	if def.TableType != catalog.SystemOrdinaryRel || def.IsTemporary || ref != nil && (ref.PubInfo != nil || ref.SubscriptionName != "") {
		return nil, nil, moerr.NewNotSupported(c.GetContext(), "materialized view sources must be persistent native tables")
	}
	return ref, def, nil
}

// The wire username may include account and role, unlike mo_user.user_name.
func materializedViewOwnerName(wire string) string {
	parts := strings.Split(wire, ":")
	if len(parts) >= 2 {
		return parts[1]
	}
	return wire
}
