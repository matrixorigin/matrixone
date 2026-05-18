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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// BuildSecondaryIndexDefs constructs the IndexDef + TableDef pair for the
// two hidden tables CAGRA requires (metadata + storage). Lifted from
// pkg/sql/plan/build_ddl.go:3147 (buildCagraSecondaryIndexDef, now deleted).
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
		return nil, nil, moerr.NewInternalErrorNoCtx("type of primary key must be int64")
	}

	indexParts := make([]string, 1)
	{
		if len(indexInfo.KeyParts) != 1 {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "don't support multi column  CAGRA vector index")
		}
		name := indexInfo.KeyParts[0].ColName.ColName()
		indexParts[0] = name
		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", indexInfo.KeyParts[0].ColName.ColNameOrigin())
		}
		if colMap[name].Typ.Id != int32(types.T_array_float32) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "Cagra only supports VECF32 column types")
		}
		for _, existedIndex := range existedIndexes {
			if existedIndex.IndexAlgo == "cagra" && existedIndex.Parts[0] == name {
				return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "Multiple CAGRA indexes are not allowed to use the same column")
			}
		}
	}

	if indexInfo.IndexOption != nil {
		if err := planplugin.ValidateIncludeColumns(ctx, indexInfo.IndexOption.IncludeColumns, colMap, indexParts[0], pkeyName); err != nil {
			return nil, nil, err
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
			TableType: catalog.Cagra_TblType_Metadata,
			Cols:      make([]*plan.ColDef, 4),
		}
		indexDefs[0], err = planplugin.CreateIndexDef(indexInfo, indexTableName, catalog.Cagra_TblType_Metadata, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		tableDefs[0].Cols[0] = &plan.ColDef{
			Name: catalog.Cagra_TblCol_Metadata_Index_Id,
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
			Name: catalog.Cagra_TblCol_Metadata_Checksum,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: types.MaxVarcharLen,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[0].Cols[2] = &plan.ColDef{
			Name: catalog.Cagra_TblCol_Metadata_Timestamp,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[0].Cols[3] = &plan.ColDef{
			Name: catalog.Cagra_TblCol_Metadata_Filesize,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}

		tableDefs[0].Pkey = &plan.PrimaryKeyDef{
			Names:       []string{catalog.Cagra_TblCol_Metadata_Index_Id},
			PkeyColName: catalog.Cagra_TblCol_Metadata_Index_Id,
		}

		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.Cagra_TblType_Metadata},
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
			TableType: catalog.Cagra_TblType_Storage,
			Cols:      make([]*plan.ColDef, 5),
		}
		indexDefs[1], err = planplugin.CreateIndexDef(indexInfo, indexTableName, catalog.Cagra_TblType_Storage, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		tableDefs[1].Cols[0] = &plan.ColDef{
			Name: catalog.Cagra_TblCol_Storage_Index_Id,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 128,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[1] = &plan.ColDef{
			Name: catalog.Cagra_TblCol_Storage_Chunk_Id,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[2] = &plan.ColDef{
			Name: catalog.Cagra_TblCol_Storage_Data,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_blob),
				Width: 65536,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[3] = &plan.ColDef{
			Name: catalog.Cagra_TblCol_Storage_Tag,
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
				catalog.Cagra_TblCol_Storage_Index_Id,
				catalog.Cagra_TblCol_Storage_Chunk_Id,
				catalog.Cagra_TblCol_Storage_Tag,
			},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: tableDefs[1].Cols[4],
		}

		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.Cagra_TblType_Storage},
		}
		tableDefs[1].Defs = append(tableDefs[1].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	}
	return indexDefs, tableDefs, nil
}
