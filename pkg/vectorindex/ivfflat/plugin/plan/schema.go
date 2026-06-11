// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	ivfflatrt "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/runtime"
)

// ivfflatCatalogHooks is the shared (stateless) catalog-hooks instance used for
// plugin-declared type validation (see pkg/indexplugin/catalog).
var ivfflatCatalogHooks = ivfflatrt.CatalogHooks{}

// BuildSecondaryIndexDefs builds the three hidden tables IVF-FLAT needs:
// metadata (key/val for version + clustering timestamps), centroids
// (version + id + centroid + composite PK), entries (version + id +
// origin_pk + entry + composite PK).
//
// Lifted verbatim from pkg/sql/plan/build_ddl.go:2480
// (buildIvfFlatSecondaryIndexDef, now deleted).
func (Hooks) BuildSecondaryIndexDefs(
	ctx planplugin.CompilerContext,
	indexInfo *tree.Index,
	colMap map[string]*plan.ColDef,
	existedIndexes []*plan.IndexDef,
	pkeyName string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {

	indexParts := make([]string, 1)

	// 0. Validate: single VECF32/VECF64 column, no duplicate IVFFLAT on it.
	if len(indexInfo.KeyParts) != 1 {
		return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "don't support multi column  IVF vector index")
	}
	name := indexInfo.KeyParts[0].ColName.ColName()
	indexParts[0] = name
	if _, ok := colMap[name]; !ok {
		return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", indexInfo.KeyParts[0].ColName.ColNameOrigin())
	}
	if !catalogplugin.SupportsVectorType(ivfflatCatalogHooks, types.T(colMap[name].Typ.Id)) {
		return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "IVFFLAT only supports VECFXX column types")
	}
	for _, existedIndex := range existedIndexes {
		if existedIndex.IndexAlgo == catalog.MoIndexIvfFlatAlgo.ToString() && existedIndex.Parts[0] == name {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "Multiple IVFFLAT indexes are not allowed to use the same column")
		}
	}

	indexDefs := make([]*plan.IndexDef, 3)
	tableDefs := make([]*plan.TableDef, 3)

	// 1. metadata table: ( key VARCHAR PRIMARY KEY, val VARCHAR )
	{
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[0] = &plan.TableDef{
			Name:      indexTableName,
			TableType: catalog.SystemSI_IVFFLAT_TblType_Metadata,
			Cols:      make([]*plan.ColDef, 2),
		}
		indexDefs[0], err = planplugin.CreateIndexDef(ctx, indexInfo, indexTableName, catalog.SystemSI_IVFFLAT_TblType_Metadata, indexParts, false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[0].Cols[0] = &plan.ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: types.MaxVarcharLen,
			},
			Primary: true,
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[0].Cols[1] = &plan.ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: types.MaxVarcharLen,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[0].Pkey = &plan.PrimaryKeyDef{
			Names:       []string{catalog.SystemSI_IVFFLAT_TblCol_Metadata_key},
			PkeyColName: catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
		}
		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSI_IVFFLAT_TblType_Metadata},
		}
		tableDefs[0].Defs = append(tableDefs[0].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	}

	colName := indexInfo.KeyParts[0].ColName.ColName()

	// 2. centroids table: ( version INT64, id INT64, centroid VECFXX,
	//    PRIMARY KEY (version,id) )
	{
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[1] = &plan.TableDef{
			Name:      indexTableName,
			TableType: catalog.SystemSI_IVFFLAT_TblType_Centroids,
			Cols:      make([]*plan.ColDef, 4),
		}
		indexDefs[1], err = planplugin.CreateIndexDef(ctx, indexInfo, indexTableName, catalog.SystemSI_IVFFLAT_TblType_Centroids, indexParts, false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[1].Cols[0] = &plan.ColDef{
			Name:    catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
			Alg:     plan.CompressType_Lz4,
			Typ:     plan.Type{Id: int32(types.T_int64)},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[1] = &plan.ColDef{
			Name:    catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
			Alg:     plan.CompressType_Lz4,
			Typ:     plan.Type{Id: int32(types.T_int64)},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		// Centroid type is decoupled from the entry type. Centroids are f32 whenever
		// the entries are NOT a plain f32/f64 column: i.e. for a narrow base
		// (bf16/f16/int8) or for ANY base under QUANTIZATION (incl. f64). f32 gives
		// accurate assignment, fast f32 search, and tiny RAM for the few centroids;
		// the entries carry the memory win. A plain f32/f64 column keeps its type.
		centroidTyp := plan.Type{
			Id:    colMap[colName].Typ.Id,
			Width: colMap[colName].Typ.Width,
			Scale: colMap[colName].Typ.Scale,
		}
		quantized := indexInfo.IndexOption != nil && indexInfo.IndexOption.Quantization != ""
		switch types.T(centroidTyp.Id) {
		case types.T_array_bf16, types.T_array_float16, types.T_array_int8:
			centroidTyp.Id = int32(types.T_array_float32)
			centroidTyp.Scale = 0
		default:
			if quantized {
				centroidTyp.Id = int32(types.T_array_float32)
				centroidTyp.Scale = 0
			}
		}
		tableDefs[1].Cols[2] = &plan.ColDef{
			Name:    catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
			Alg:     plan.CompressType_Lz4,
			Typ:     centroidTyp,
			Default: &plan.Default{NullAbility: true, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[3] = planplugin.MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
		tableDefs[1].Cols[3].Alg = plan.CompressType_Lz4
		tableDefs[1].Cols[3].Primary = true

		tableDefs[1].Pkey = &plan.PrimaryKeyDef{
			Names: []string{
				catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
				catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
			},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: tableDefs[1].Cols[3],
		}
		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSI_IVFFLAT_TblType_Centroids},
		}
		tableDefs[1].Defs = append(tableDefs[1].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	}

	// 3. entries table: ( version INT64, id INT64, origin_pk <pkType>,
	//    entry VECFXX, PRIMARY KEY (version,id,origin_pk) )
	{
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[2] = &plan.TableDef{
			Name:      indexTableName,
			TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
			Cols:      make([]*plan.ColDef, 5),
		}
		indexDefs[2], err = planplugin.CreateIndexDef(ctx, indexInfo, indexTableName, catalog.SystemSI_IVFFLAT_TblType_Entries, indexParts, false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[2].Cols[0] = &plan.ColDef{
			Name:    catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
			Alg:     plan.CompressType_Lz4,
			Typ:     plan.Type{Id: int32(types.T_int64)},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[2].Cols[1] = &plan.ColDef{
			Name:    catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
			Alg:     plan.CompressType_Lz4,
			Typ:     plan.Type{Id: int32(types.T_int64)},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[2].Cols[2] = &plan.ColDef{
			Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				// Don't copy original PK Type wholesale — would inherit
				// AutoIncrement and break entries-table INSERTs.
				Id:    colMap[pkeyName].Typ.Id,
				Width: colMap[pkeyName].Typ.Width,
				Scale: colMap[pkeyName].Typ.Scale,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		// Entry type follows the QUANTIZATION option: CREATE INDEX ... USING
		// ivfflat ... QUANTIZATION='int8' stores entries as vecint8 (quantized from
		// the base vectors), while the base column and the f32 centroids are
		// unchanged. Without QUANTIZATION the entries keep the base column type.
		entryTyp := plan.Type{
			Id:    colMap[colName].Typ.Id,
			Width: colMap[colName].Typ.Width,
			Scale: colMap[colName].Typ.Scale,
		}
		if indexInfo.IndexOption != nil && indexInfo.IndexOption.Quantization != "" {
			if qt, ok := vectorindex.QuantizationToVectorType(indexInfo.IndexOption.Quantization); ok {
				entryTyp.Id = int32(qt)
				entryTyp.Scale = 0
			}
		}
		tableDefs[2].Cols[3] = &plan.ColDef{
			Name:    catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			Alg:     plan.CompressType_Lz4,
			Typ:     entryTyp,
			Default: &plan.Default{NullAbility: true, Expr: nil, OriginString: ""},
		}
		tableDefs[2].Cols[4] = planplugin.MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
		tableDefs[2].Cols[4].Alg = plan.CompressType_Lz4
		tableDefs[2].Cols[4].Primary = true

		tableDefs[2].Pkey = &plan.PrimaryKeyDef{
			Names: []string{
				catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
				catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
				catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: tableDefs[2].Cols[4],
		}
		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSI_IVFFLAT_TblType_Entries},
		}
		tableDefs[2].Defs = append(tableDefs[2].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	}

	return indexDefs, tableDefs, nil
}

// BuildFullTextIndexDefs is unreachable for ivfflat — the plan-build
// dispatch only routes *tree.FullTextIndex parse trees to the fulltext
// plugin. Returning an error here makes any misrouting visible.
func (Hooks) BuildFullTextIndexDefs(
	_ planplugin.CompilerContext,
	_ *tree.FullTextIndex,
	_ map[string]*plan.ColDef,
	_ []*plan.IndexDef,
	_ string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {
	return nil, nil, moerr.NewNotSupportedNoCtx("ivfflat plugin does not build fulltext indexes")
}
