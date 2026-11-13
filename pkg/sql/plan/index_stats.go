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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) buildIndexStats(_ *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr) (int32, error) {
	if len(exprs) != 3 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "mo_index_stats requires 3 arguments (database, table, index)")
	}
	var names [3]string
	for i, expr := range exprs {
		name, ok := stringLiteral(expr)
		if !ok || strings.TrimSpace(name) == "" {
			return 0, moerr.NewInvalidInput(builder.GetContext(), "mo_index_stats requires non-empty string literals (database, table, index)")
		}
		names[i] = name
	}
	obj, source, err := builder.compCtx.Resolve(names[0], names[1], ctx.snapshot, nil)
	if err != nil {
		return 0, err
	}
	if obj == nil || source == nil {
		return 0, moerr.NewInvalidInputf(builder.GetContext(), "table '%s.%s' does not exist", names[0], names[1])
	}
	// Resolve must not redirect the runtime's literal source to another tenant.
	if obj.PubInfo != nil || source.TableType != catalog.SystemOrdinaryRel {
		return 0, moerr.NewNotSupported(builder.GetContext(), "mo_index_stats currently supports local ordinary tables only")
	}
	cfg := indexstats.Config{Database: names[0], Table: names[1], Index: names[2], TableID: source.TblId, Lists: 1}
	if strings.EqualFold(names[2], "PRIMARY") {
		if source.Pkey == nil || source.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
			return 0, moerr.NewInvalidInputf(builder.GetContext(), "index 'PRIMARY' does not exist on '%s.%s'", names[0], names[1])
		}
		configurePrimaryIndexStats(&cfg, source)
	}
	for _, idx := range source.Indexes {
		if cfg.KeyKind == "primary" {
			break
		}
		if !strings.EqualFold(idx.IndexName, names[2]) {
			continue
		}
		cfg.Index = idx.IndexName
		cfg.Algorithm = strings.ToLower(idx.IndexAlgo)
		if catalog.IsNullIndexAlgo(idx.IndexAlgo) || cfg.Algorithm == catalog.MoIndexBTreeAlgo.ToString() {
			if err := configureKeyIndexStats(builder.GetContext(), &cfg, source, idx); err != nil {
				return 0, err
			}
			break
		}
		isFullText := cfg.Algorithm == catalog.MOIndexFullTextAlgo.ToString() || catalog.IsFullText2IndexAlgo(cfg.Algorithm)
		if cfg.Algorithm != catalog.MoIndexIvfFlatAlgo.ToString() && !isFullText {
			return 0, moerr.NewNotSupportedf(builder.GetContext(), "mo_index_stats: index type '%s' is not supported", idx.IndexAlgo)
		}
		if idx.IndexTableName != "" {
			cfg.Tables = append(cfg.Tables, indexstats.IndexTable{Kind: idx.IndexAlgoTableType, Name: idx.IndexTableName})
		}
		if isFullText {
			if idx.IndexAlgoTableType == catalog.IndexIgnored_TblType {
				continue
			}
			rawParams := idx.IndexAlgoParams
			if rawParams == "" {
				rawParams = "{}"
			}
			params, parseErr := catalog.IndexParamsStringToMap(rawParams)
			if parseErr != nil {
				return 0, parseErr
			}
			cfg.Options = params
			cfg.Columns = append([]string(nil), idx.Parts...)
			cfg.Parser = strings.ToLower(params["parser"])
			if cfg.Parser == "" || cfg.Parser == "default" {
				cfg.Parser = "ngram"
			}
			cfg.FullTextVersion = catalog.IndexAlgoParamVersionOf(idx.IndexAlgoParams)
			cfg.Async, err = catalog.IndexParamAsync(idx.IndexAlgoParams)
			if err != nil {
				return 0, err
			}
			switch idx.IndexAlgoTableType {
			case catalog.FullText2Index_TblType_Metadata:
				cfg.Metadata = idx.IndexTableName
				cfg.FullTextVersion = 2
			case catalog.FullText2Index_TblType_Storage:
				cfg.Entries = idx.IndexTableName
				cfg.FullTextVersion = 2
			case "", catalog.FullTextIndex_TblType:
				cfg.Entries = idx.IndexTableName
			}
			if catalog.IsFullText2IndexAlgo(cfg.Algorithm) || cfg.FullTextVersion == 2 {
				cfg.FullTextVersion = 2
				cfg.Async = true
			}
			continue
		}
		switch idx.IndexAlgoTableType {
		case catalog.SystemSI_IVFFLAT_TblType_Metadata:
			cfg.Metadata = idx.IndexTableName
			if len(idx.Parts) != 1 {
				return 0, moerr.NewInvalidInput(builder.GetContext(), "mo_index_stats: invalid IVF_FLAT index column")
			}
			cfg.Column = idx.Parts[0]
			params, parseErr := catalog.IndexParamsStringToMap(idx.IndexAlgoParams)
			if parseErr != nil {
				return 0, parseErr
			}
			cfg.OpType = params[catalog.IndexAlgoParamOpType]
			if cfg.OpType == "" {
				cfg.OpType = "vector_l2_ops"
			}
			cfg.Quantization = params[catalog.Quantization]
			if cfg.Quantization == "" {
				cfg.Quantization = "none"
			}
			cfg.Options = params
			if value, ok := params[catalog.IndexAlgoParamLists]; ok {
				cfg.Lists, err = strconv.ParseInt(value, 10, 64)
				if err != nil || cfg.Lists <= 0 {
					return 0, moerr.NewInvalidInput(builder.GetContext(), "mo_index_stats: invalid IVF_FLAT lists option")
				}
			}
			cfg.Async, err = catalog.IndexParamAsync(idx.IndexAlgoParams)
			if err != nil {
				return 0, err
			}
		case catalog.SystemSI_IVFFLAT_TblType_Centroids:
			cfg.Centroids = idx.IndexTableName
		case catalog.SystemSI_IVFFLAT_TblType_Entries:
			cfg.Entries = idx.IndexTableName
		}
	}
	if cfg.KeyKind == "" && cfg.Metadata == "" && cfg.Centroids == "" && cfg.Entries == "" {
		return 0, moerr.NewInvalidInputf(builder.GetContext(), "index '%s' does not exist on '%s.%s'", names[2], names[0], names[1])
	}
	isFullText := cfg.Algorithm == catalog.MOIndexFullTextAlgo.ToString() || catalog.IsFullText2IndexAlgo(cfg.Algorithm)
	if cfg.KeyKind != "" {
		if len(cfg.KeyColumns) == 0 || source.Pkey == nil || cfg.Entries == "" {
			return 0, moerr.NewInvalidInput(builder.GetContext(), "mo_index_stats: incomplete key index definition")
		}
	} else if isFullText {
		if cfg.Entries == "" || len(cfg.Columns) == 0 || source.Pkey == nil || cfg.FullTextVersion == 2 && cfg.Metadata == "" {
			return 0, moerr.NewInvalidInput(builder.GetContext(), "mo_index_stats: incomplete fulltext index definition")
		}
	} else if cfg.Metadata == "" || cfg.Centroids == "" || cfg.Entries == "" || source.Pkey == nil {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "mo_index_stats: incomplete IVF_FLAT index definition")
	}
	cfg.PrimaryKey = source.Pkey.PkeyColName
	param, err := json.Marshal(cfg)
	if err != nil {
		return 0, err
	}
	if builder.isReusablePlan() {
		builder.qry.CatalogDependencies = appendPrepareSchemas(builder.qry.CatalogDependencies, prepareSchemaRef(obj, source))
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
