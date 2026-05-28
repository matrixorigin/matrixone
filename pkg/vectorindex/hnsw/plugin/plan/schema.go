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
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// BuildSecondaryIndexDefs constructs the IndexDef + TableDef pair for the
// two hidden tables HNSW requires (metadata + storage). Lifted from
// pkg/sql/plan/build_ddl.go:2810 (buildHnswSecondaryIndexDef, now deleted).
//
// Differs from CAGRA/IVF-PQ:
//   - HNSW supports both T_array_float32 and T_array_float64 (CAGRA/IVF-PQ
//     are float32 only).
//   - No INCLUDE columns (no validateIncludeColumns call).
//   - The composite-PK column references Cols[3] (Tag), matching the
//     pre-lift behaviour at build_ddl.go:3034.
func (Hooks) BuildSecondaryIndexDefs(
	ctx planplugin.CompilerContext,
	indexInfo *tree.Index,
	colMap map[string]*plan.ColDef,
	existedIndexes []*plan.IndexDef,
	pkeyName string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {

	if pkeyName == "" || pkeyName == catalog.FakePrimaryKeyColName {
		return nil, nil, moerr.NewInternalErrorNoCtx("primary key cannot be empty for hnsw index")
	}
	if colMap[pkeyName].Typ.Id != int32(types.T_int64) {
		return nil, nil, moerr.NewInternalErrorNoCtx("type of primary key must be bigint")
	}

	indexParts := make([]string, 1)
	{
		if len(indexInfo.KeyParts) != 1 {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "don't support multi column  HNSW vector index")
		}
		name := indexInfo.KeyParts[0].ColName.ColName()
		indexParts[0] = name
		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", indexInfo.KeyParts[0].ColName.ColNameOrigin())
		}
		if colMap[name].Typ.Id != int32(types.T_array_float32) && colMap[name].Typ.Id != int32(types.T_array_float64) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "HNSW only supports VECF32 and VECF64 column types")
		}
		for _, existedIndex := range existedIndexes {
			if existedIndex.IndexAlgo == catalog.MoIndexHnswAlgo.ToString() && existedIndex.Parts[0] == name {
				return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "Multiple HNSW indexes are not allowed to use the same column")
			}
		}
	}

	indexDefs := make([]*plan.IndexDef, 2)
	tableDefs := make([]*plan.TableDef, 2)

	// 1. metadata table
	{
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[0] = &plan.TableDef{
			Name:      indexTableName,
			TableType: catalog.Hnsw_TblType_Metadata,
			Cols:      make([]*plan.ColDef, 4),
		}
		indexDefs[0], err = planplugin.CreateIndexDef(indexInfo, indexTableName, catalog.Hnsw_TblType_Metadata, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		tableDefs[0].Cols[0] = &plan.ColDef{
			Name: catalog.Hnsw_TblCol_Metadata_Index_Id,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 128,
				Scale: 0,
			},
			Primary: true,
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[0].Cols[1] = &plan.ColDef{
			Name: catalog.Hnsw_TblCol_Metadata_Checksum,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: types.MaxVarcharLen,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[0].Cols[2] = &plan.ColDef{
			Name: catalog.Hnsw_TblCol_Metadata_Timestamp,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[0].Cols[3] = &plan.ColDef{
			Name: catalog.Hnsw_TblCol_Metadata_Filesize,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}

		tableDefs[0].Pkey = &plan.PrimaryKeyDef{
			Names:       []string{catalog.Hnsw_TblCol_Metadata_Index_Id},
			PkeyColName: catalog.Hnsw_TblCol_Metadata_Index_Id,
		}

		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.Hnsw_TblType_Metadata},
		}
		tableDefs[0].Defs = append(tableDefs[0].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	}

	// 2. storage table
	{
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
		if err != nil {
			return nil, nil, err
		}
		tableDefs[1] = &plan.TableDef{
			Name:      indexTableName,
			TableType: catalog.Hnsw_TblType_Storage,
			Cols:      make([]*plan.ColDef, 5),
		}
		indexDefs[1], err = planplugin.CreateIndexDef(indexInfo, indexTableName, catalog.Hnsw_TblType_Storage, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		tableDefs[1].Cols[0] = &plan.ColDef{
			Name: catalog.Hnsw_TblCol_Storage_Index_Id,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 128,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[1] = &plan.ColDef{
			Name: catalog.Hnsw_TblCol_Storage_Chunk_Id,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[2] = &plan.ColDef{
			Name: catalog.Hnsw_TblCol_Storage_Data,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_blob),
				Width: 65536,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[3] = &plan.ColDef{
			Name: catalog.Hnsw_TblCol_Storage_Tag,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[4] = planplugin.MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
		tableDefs[1].Cols[4].Alg = plan.CompressType_Lz4
		tableDefs[1].Cols[4].Primary = true

		tableDefs[1].Pkey = &plan.PrimaryKeyDef{
			Names: []string{
				catalog.Hnsw_TblCol_Storage_Index_Id,
				catalog.Hnsw_TblCol_Storage_Chunk_Id,
			},
			PkeyColName: catalog.CPrimaryKeyColName,
			// Matches the pre-lift behaviour at build_ddl.go:3034 — Cols[3]
			// (Tag), not Cols[4] (the hidden composite-PK placeholder).
			CompPkeyCol: tableDefs[1].Cols[3],
		}

		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.Hnsw_TblType_Storage},
		}
		tableDefs[1].Defs = append(tableDefs[1].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	}
	return indexDefs, tableDefs, nil
}

// BuildFullTextIndexDefs is unreachable for hnsw — the plan-build
// dispatch only routes *tree.FullTextIndex parse trees to the fulltext
// plugin. Returning an error here makes any misrouting visible.
func (Hooks) BuildFullTextIndexDefs(
	_ planplugin.CompilerContext,
	_ *tree.FullTextIndex,
	_ map[string]*plan.ColDef,
	_ []*plan.IndexDef,
	_ string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {
	return nil, nil, moerr.NewNotSupportedNoCtx("hnsw plugin does not build fulltext indexes")
}
