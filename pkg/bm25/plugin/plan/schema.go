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

// bm25TextColumn reports whether a column type can be bm25-indexed
// (text-ish types, same set the classic fulltext index accepts).
func bm25TextColumn(id int32) bool {
	return id == int32(types.T_text) || id == int32(types.T_char) ||
		id == int32(types.T_varchar) || id == int32(types.T_json) ||
		id == int32(types.T_datalink)
}

// bm25SupportedPkType reports whether a single-column primary key of this type can
// be encoded/decoded by the WAND engine (wand.encodePk / decodePk). It MUST mirror
// encodePk's switch exactly — a pk type accepted at CREATE but not by encodePk would
// silently abort the CDC sink later (the index would stop updating with no visible
// error). A composite primary key is delivered as the packed CPrimaryKey varchar,
// which is covered by the varlena case, so composite pks are always supported.
func bm25SupportedPkType(id int32) bool {
	switch types.T(id) {
	case types.T_int64, types.T_uint64, types.T_int32, types.T_uint32,
		types.T_varchar, types.T_char, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_json,
		types.T_uuid,
		types.T_date, types.T_datetime, types.T_time, types.T_timestamp,
		types.T_decimal64, types.T_decimal128:
		return true
	default:
		return false
	}
}

// BuildSecondaryIndexDefs constructs the bm25 index def + its two hidden tables
// (storage + metadata) from CREATE INDEX ... USING bm25. bm25 parses to
// *tree.Index and is dispatched here (the vector-plugin path). The two tables
// mirror the HNSW storage/metadata layout: the storage table holds the chunked
// binary (WAND) index blobs, the metadata table one row per sub-index. There is
// no postings table — bm25 builds directly from the source rows.
func (Hooks) BuildSecondaryIndexDefs(
	ctx planplugin.CompilerContext,
	indexInfo *tree.Index,
	colMap map[string]*plan.ColDef,
	existedIndexes []*plan.IndexDef,
	pkeyName string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {

	// 0. Validate: single text/varchar column, no duplicate bm25 on it.
	if len(indexInfo.KeyParts) != 1 {
		return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "bm25 index does not support multiple columns")
	}
	name := indexInfo.KeyParts[0].ColName.ColName()
	indexParts := []string{name}
	col, ok := colMap[name]
	if !ok {
		return nil, nil, moerr.NewInvalidInputf(ctx.GetContext(), "column '%s' is not exist", indexInfo.KeyParts[0].ColName.ColNameOrigin())
	}
	if !bm25TextColumn(col.Typ.Id) {
		return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "bm25 index only supports CHAR/VARCHAR/TEXT/JSON/DATALINK columns")
	}
	for _, existed := range existedIndexes {
		if existed.IndexAlgo == catalog.MoIndexBm25Algo.ToString() && len(existed.Parts) > 0 && existed.Parts[0] == name {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "Multiple bm25 indexes are not allowed to use the same column")
		}
	}
	// Reject a primary key whose type the WAND engine cannot encode, at CREATE time
	// rather than letting the CDC sink silently abort later. A single-column pk is in
	// colMap; a composite pk uses the packed CPrimaryKey varchar (not in colMap, and
	// always encodable), so only validate when the pk column is present here.
	if pkCol, ok := colMap[pkeyName]; ok && !bm25SupportedPkType(pkCol.Typ.Id) {
		return nil, nil, moerr.NewNotSupportedf(ctx.GetContext(),
			"bm25 index does not support a primary key of type %s; use an integer, decimal, string, uuid, date/time, or timestamp primary key",
			types.T(pkCol.Typ.Id).String())
	}

	// 1. storage (chunk) table: ( index_id VARCHAR, chunk_id INT64, data BLOB,
	//    tag INT64, PRIMARY KEY (index_id, chunk_id) )
	storeName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	storeIdx, err := planplugin.CreateIndexDef(ctx, indexInfo, storeName, catalog.Bm25Index_TblType_Storage, indexParts, false)
	if err != nil {
		return nil, nil, err
	}
	storeTbl := &plan.TableDef{
		Name:      storeName,
		TableType: catalog.Bm25Index_TblType_Storage,
		Cols: []*plan.ColDef{
			{Name: catalog.Bm25Index_TblCol_Storage_Index_Id, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_varchar), Width: 128}, Default: &plan.Default{}},
			{Name: catalog.Bm25Index_TblCol_Storage_Chunk_Id, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
			{Name: catalog.Bm25Index_TblCol_Storage_Data, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_blob), Width: 65536}, Default: &plan.Default{}},
			{Name: catalog.Bm25Index_TblCol_Storage_Tag, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
		},
	}
	storePk := planplugin.MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
	storePk.Alg = plan.CompressType_Lz4
	storePk.Primary = true
	storeTbl.Cols = append(storeTbl.Cols, storePk)
	storeTbl.Pkey = &plan.PrimaryKeyDef{
		Names:       []string{catalog.Bm25Index_TblCol_Storage_Index_Id, catalog.Bm25Index_TblCol_Storage_Chunk_Id},
		PkeyColName: catalog.CPrimaryKeyColName,
		CompPkeyCol: storeTbl.Cols[3], // tag col, mirrors HNSW storage layout
	}
	storeTbl.Defs = append(storeTbl.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.Bm25Index_TblType_Storage},
		}}},
	})

	// 2. metadata table: one row per sub-index.
	metaName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	metaIdx, err := planplugin.CreateIndexDef(ctx, indexInfo, metaName, catalog.Bm25Index_TblType_Metadata, indexParts, false)
	if err != nil {
		return nil, nil, err
	}
	metaTbl := &plan.TableDef{
		Name:      metaName,
		TableType: catalog.Bm25Index_TblType_Metadata,
		Cols: []*plan.ColDef{
			{Name: catalog.Bm25Index_TblCol_Metadata_Index_Id, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_varchar), Width: 128}, Primary: true, Default: &plan.Default{}},
			{Name: catalog.Bm25Index_TblCol_Metadata_Timestamp, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
			{Name: catalog.Bm25Index_TblCol_Metadata_Checksum, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, Default: &plan.Default{}},
			{Name: catalog.Bm25Index_TblCol_Metadata_Filesize, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
			{Name: catalog.Bm25Index_TblCol_Metadata_Recency, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
			{Name: catalog.Bm25Index_TblCol_Metadata_Nrow, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
		},
	}
	metaTbl.Pkey = &plan.PrimaryKeyDef{
		Names:       []string{catalog.Bm25Index_TblCol_Metadata_Index_Id},
		PkeyColName: catalog.Bm25Index_TblCol_Metadata_Index_Id,
	}
	metaTbl.Defs = append(metaTbl.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.Bm25Index_TblType_Metadata},
		}}},
	})

	return []*plan.IndexDef{storeIdx, metaIdx}, []*plan.TableDef{storeTbl, metaTbl}, nil
}

// BuildFullTextIndexDefs — bm25 is not reached via CREATE FULLTEXT INDEX
// (*tree.FullTextIndex); it uses BuildSecondaryIndexDefs instead.
func (Hooks) BuildFullTextIndexDefs(
	_ planplugin.CompilerContext,
	_ *tree.FullTextIndex,
	_ map[string]*plan.ColDef,
	_ []*plan.IndexDef,
	_ string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {
	return nil, nil, moerr.NewNotSupportedNoCtx("bm25 plugin does not build fulltext indexes")
}
