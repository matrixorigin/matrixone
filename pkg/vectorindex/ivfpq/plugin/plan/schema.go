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
	ivfpqrt "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/runtime"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
)

// ivfpqCatalogHooks is the shared (stateless) catalog-hooks instance used for
// plugin-declared type validation (see pkg/indexplugin/catalog).
var ivfpqCatalogHooks = ivfpqrt.CatalogHooks{}

// BuildSecondaryIndexDefs runs during plan-tree construction for
// CREATE INDEX (pkg/sql/plan/build_ddl.go:2081 dispatch). It returns the
// per-hidden-table IndexDef and TableDef pair that pkg/sql/compile will
// later create on disk.
//
// The IndexDef list and TableDef list must align positionally — one entry
// per hidden table. For IVF-PQ that is two: a small metadata table
// (mo_secondary_metadata_xxx — index_id, checksum, timestamp, filesize),
// and a chunked storage table (mo_secondary_index_xxx — the serialized
// index payload split into blob chunks). The TableType field is what
// CatalogHooks.HiddenTableTypes() returned earlier; the framework keys
// downstream maps by this.
//
// Helpers available from planplugin (populated by pkg/sql/plan's init):
//
//	planplugin.CreateIndexDef         — constructs the *plan.IndexDef
//	                                    (serializes algo params from
//	                                    indexInfo.IndexOption into JSON)
//	planplugin.MakeHiddenColDefByName — builds a hidden composite-PK
//	                                    placeholder column (used for the
//	                                    storage table's compound PK)
//	planplugin.ValidateIncludeColumns — validates INCLUDE column list
//	                                    against colMap + pk constraints
//
// Lifted from pkg/sql/plan/build_ddl.go:3114-3353 (the deleted
// buildIvfpqSecondaryIndexDef).
func (Hooks) BuildSecondaryIndexDefs(
	ctx planplugin.CompilerContext,
	indexInfo *tree.Index,
	colMap map[string]*plan.ColDef,
	existedIndexes []*plan.IndexDef,
	pkeyName string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {

	if pkeyName == "" || pkeyName == catalog.FakePrimaryKeyColName {
		return nil, nil, moerr.NewInternalErrorNoCtx("primary key cannot be empty for ivfpq index")
	}
	pk, ok := colMap[pkeyName]
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("primary key column not found for ivfpq index")
	}
	if !catalogplugin.SupportsPrimaryKeyType(ivfpqCatalogHooks, types.T(pk.Typ.Id)) {
		return nil, nil, moerr.NewInternalErrorNoCtx("type of primary key must be int64")
	}

	indexParts := make([]string, 1)

	// Validate: only 1 column of VECF32
	{
		if len(indexInfo.KeyParts) != 1 {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "don't support multi column IVFPQ vector index")
		}
		name := indexInfo.KeyParts[0].ColName.ColName()
		indexParts[0] = name
		if _, ok := colMap[name]; !ok {
			return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", indexInfo.KeyParts[0].ColName.ColNameOrigin())
		}
		if !catalogplugin.SupportsVectorType(ivfpqCatalogHooks, types.T(colMap[name].Typ.Id)) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "IvfPQ only supports VECF32 / VECF16 base column types")
		}
		// QUANTIZATION is downcast-only: the storage element must be the same width
		// or narrower than the base column (f16 base -> int8/uint8 OK; f16 base ->
		// float32 is an upcast and rejected). Mirrors ivfflat's guard.
		if indexInfo.IndexOption != nil && indexInfo.IndexOption.Quantization != "" {
			if qt, ok := quantizer.ToVectorType(indexInfo.IndexOption.Quantization); ok {
				baseSize := types.Type{Oid: types.T(colMap[name].Typ.Id)}.GetArrayElementSize()
				quantSize := types.Type{Oid: qt}.GetArrayElementSize()
				if quantSize > baseSize {
					return nil, nil, moerr.NewNotSupportedf(ctx.GetContext(),
						"IvfPQ QUANTIZATION '%s' (%d bytes/element) cannot upcast base column %s (%d bytes/element); use a quantization of equal or smaller width, or omit it to keep the base type",
						indexInfo.IndexOption.Quantization, quantSize,
						types.T(colMap[name].Typ.Id).String(), baseSize)
				}
			}
		}
		for _, existedIndex := range existedIndexes {
			if existedIndex.IndexAlgo == catalog.MoIndexIvfpqAlgo.ToString() && existedIndex.Parts[0] == name {
				return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "Multiple IVFPQ indexes are not allowed to use the same column")
			}
		}
	}

	if indexInfo.IndexOption != nil {
		if err := planplugin.ValidateIncludeColumns(ctx, indexInfo.IndexOption.IncludeColumns, colMap, indexParts[0], pkeyName, ivfpqCatalogHooks.SupportedIncludeColumnTypes()); err != nil {
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
			TableType: catalog.Ivfpq_TblType_Metadata,
			Cols:      make([]*plan.ColDef, 4),
		}
		indexDefs[0], err = planplugin.CreateIndexDef(ctx, indexInfo, indexTableName, catalog.Ivfpq_TblType_Metadata, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		tableDefs[0].Cols[0] = &plan.ColDef{
			Name: catalog.Ivfpq_TblCol_Metadata_Index_Id,
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
			Name: catalog.Ivfpq_TblCol_Metadata_Checksum,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: types.MaxVarcharLen,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[0].Cols[2] = &plan.ColDef{
			Name: catalog.Ivfpq_TblCol_Metadata_Timestamp,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[0].Cols[3] = &plan.ColDef{
			Name: catalog.Ivfpq_TblCol_Metadata_Filesize,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}

		tableDefs[0].Pkey = &plan.PrimaryKeyDef{
			Names:       []string{catalog.Ivfpq_TblCol_Metadata_Index_Id},
			PkeyColName: catalog.Ivfpq_TblCol_Metadata_Index_Id,
		}

		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.Ivfpq_TblType_Metadata},
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
			TableType: catalog.Ivfpq_TblType_Storage,
			Cols:      make([]*plan.ColDef, 5),
		}
		indexDefs[1], err = planplugin.CreateIndexDef(ctx, indexInfo, indexTableName, catalog.Ivfpq_TblType_Storage, indexParts, false)
		if err != nil {
			return nil, nil, err
		}

		tableDefs[1].Cols[0] = &plan.ColDef{
			Name: catalog.Ivfpq_TblCol_Storage_Index_Id,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 128,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[1] = &plan.ColDef{
			Name: catalog.Ivfpq_TblCol_Storage_Chunk_Id,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_int64),
				Width: 0,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[2] = &plan.ColDef{
			Name: catalog.Ivfpq_TblCol_Storage_Data,
			Alg:  plan.CompressType_Lz4,
			Typ: plan.Type{
				Id:    int32(types.T_blob),
				Width: 65536,
				Scale: 0,
			},
			Default: &plan.Default{NullAbility: false, Expr: nil, OriginString: ""},
		}
		tableDefs[1].Cols[3] = &plan.ColDef{
			Name: catalog.Ivfpq_TblCol_Storage_Tag,
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
				catalog.Ivfpq_TblCol_Storage_Index_Id,
				catalog.Ivfpq_TblCol_Storage_Chunk_Id,
				catalog.Ivfpq_TblCol_Storage_Tag,
			},
			PkeyColName: catalog.CPrimaryKeyColName,
			CompPkeyCol: tableDefs[1].Cols[4],
		}

		properties := []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.Ivfpq_TblType_Storage},
		}
		tableDefs[1].Defs = append(tableDefs[1].Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: properties},
			},
		})
	}
	return indexDefs, tableDefs, nil
}

// BuildFullTextIndexDefs is unreachable for ivfpq — the plan-build
// dispatch only routes *tree.FullTextIndex parse trees to the fulltext
// plugin. Returning an error here makes any misrouting visible.
func (Hooks) BuildFullTextIndexDefs(
	_ planplugin.CompilerContext,
	_ *tree.FullTextIndex,
	_ map[string]*plan.ColDef,
	_ []*plan.IndexDef,
	_ string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {
	return nil, nil, moerr.NewNotSupportedNoCtx("ivfpq plugin does not build fulltext indexes")
}
