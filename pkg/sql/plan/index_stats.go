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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// IsSourceResolvingTableFunction reports scans that require SELECT on their
// resolved source relation, including when rewrites discard their output.
func IsSourceResolvingTableFunction(node *plan.Node) bool {
	if node.GetNodeType() != plan.Node_FUNCTION_SCAN {
		return false
	}
	switch node.GetTableDef().GetTblFunc().GetName() {
	case "table_changes", indexstats.FunctionName:
		return true
	}
	return false
}

func (builder *QueryBuilder) buildIndexStats(_ *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr) (int32, error) {
	if len(exprs) != 3 && len(exprs) != 4 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "mo_index_stats requires 3 or 4 arguments (database, table, index [, options])")
	}
	var names [3]string
	for i, expr := range exprs[:3] {
		name, ok := stringLiteral(expr)
		if !ok || strings.TrimSpace(name) == "" {
			return 0, moerr.NewInvalidInput(builder.GetContext(), "mo_index_stats requires non-empty string literals (database, table, index)")
		}
		names[i] = name
	}
	if lower := builder.compCtx.GetLowerCaseTableNames(); lower == 1 {
		names[0] = tree.NewCStr(names[0], lower).Compare()
		names[1] = tree.NewCStr(names[1], lower).Compare()
	}
	obj, source, err := builder.compCtx.Resolve(names[0], names[1], ctx.snapshot, nil)
	if err != nil {
		return 0, err
	}
	if obj == nil || source == nil {
		return 0, moerr.NewInvalidInputf(builder.GetContext(), "table '%s.%s' does not exist", names[0], names[1])
	}
	// Resolve must not redirect the runtime's literal source to another tenant.
	if obj.PubInfo != nil || source.TableType != catalog.SystemOrdinaryRel || source.IsTemporary || source.Partition != nil {
		return 0, moerr.NewNotSupported(builder.GetContext(), "mo_index_stats supports local, non-temporary, non-partitioned ordinary tables only")
	}
	cfg := indexstats.Config{Database: obj.SchemaName, Table: source.Name, Index: names[2], TableID: source.TblId}
	if len(exprs) == 4 {
		raw, ok := stringLiteral(exprs[3])
		if !ok {
			return 0, moerr.NewInvalidInput(builder.GetContext(), "index analysis options must be a JSON string literal")
		}
		cfg.Analyze, err = indexstats.ParseAnalyzeOptions(builder.GetContext(), raw)
		if err != nil {
			return 0, err
		}
	}

	// Authorization runs on the finished plan. Keep the source scan even when
	// index-specific validation fails, so errors cannot disclose index metadata
	// before the caller's SELECT privilege is checked.
	if configErr := configureIndexStats(builder.GetContext(), &cfg, source); configErr != nil {
		plannedErr := moerr.ConvertGoError(builder.GetContext(), configErr).(*moerr.Error)
		cfg.ValidationError, err = plannedErr.MarshalBinary()
		if err != nil {
			return 0, err
		}
	}
	if source.Pkey != nil {
		cfg.PrimaryKey = source.Pkey.PkeyColName
		for _, col := range source.Cols {
			if col.Name == cfg.PrimaryKey {
				// SERIAL receives an ENUM label from the expression binder.
				// Decode that label so the comparison uses the same collation.
				cfg.SerializedKeyType = FormatColType(col.Typ)
				if types.T(col.Typ.Id) == types.T_enum {
					cfg.SerializedKeyType = "VARCHAR"
				}
				break
			}
		}
	}
	param, err := json.Marshal(cfg)
	if err != nil {
		return 0, err
	}
	if builder.isReusablePlan() {
		builder.qry.CatalogDependencies = appendPrepareSchemas(builder.qry.CatalogDependencies, prepareSchemaRefWithSnapshot(obj, source, ctx.snapshot))
	}
	columns := []struct {
		name string
		typ  types.T
	}{
		{"index_type", types.T_varchar}, {"row_type", types.T_varchar},
		{"object_id", types.T_varchar}, {"metrics", types.T_json},
	}
	defs := make([]*plan.ColDef, len(columns))
	for i, col := range columns {
		tp := col.typ.ToType()
		defs[i] = &plan.ColDef{Name: col.name, Typ: makePlan2Type(&tp)}
	}
	return builder.appendNode(&plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN, Stats: &plan.Stats{}, ObjRef: obj,
		TableDef: &plan.TableDef{TableType: "func_table", Cols: defs,
			TblFunc: &plan.TableFunction{Name: indexstats.FunctionName, Param: param, IsSingle: true}},
		BindingTags: []int32{builder.genNewBindTag()}, TblFuncExprList: exprs,
		ScanSnapshot: DeepCopySnapshot(ctx.snapshot),
	}, ctx), nil
}

func configureIndexStats(ctx context.Context, cfg *indexstats.Config, source *plan.TableDef) error {
	if strings.EqualFold(cfg.Index, "PRIMARY") {
		if source.Pkey == nil || source.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
			return moerr.NewInvalidInputf(ctx, "index 'PRIMARY' does not exist on '%s.%s'", cfg.Database, cfg.Table)
		}
		configurePrimaryIndexStats(cfg, source)
	}
	if cfg.KeyKind != "primary" {
		var defs []*plan.IndexDef
		for _, idx := range source.Indexes {
			if strings.EqualFold(idx.IndexName, cfg.Index) {
				defs = append(defs, idx)
			}
		}
		if len(defs) == 0 {
			return moerr.NewInvalidInputf(ctx, "index '%s' does not exist on '%s.%s'", cfg.Index, cfg.Database, cfg.Table)
		}
		idx := defs[0]
		cfg.Index, cfg.Algorithm = idx.IndexName, strings.ToLower(idx.IndexAlgo)
		if catalog.IsNullIndexAlgo(idx.IndexAlgo) || cfg.Algorithm == catalog.MoIndexBTreeAlgo.ToString() {
			if err := configureKeyIndexStats(ctx, cfg, source, idx); err != nil {
				return err
			}
		} else {
			hooks, err := indexplugin.IndexStats(ctx, cfg.Algorithm)
			if err != nil {
				return err
			}
			if err := hooks.ConfigureIndexStats(ctx, source, defs, cfg); err != nil {
				return err
			}
		}
	}
	if cfg.Analyze != nil && !cfg.SupportsAnalysis {
		return moerr.NewNotSupported(ctx, "index type does not support deep analysis")
	}
	return nil
}
