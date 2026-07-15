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
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// BuildFullTextIndexDefs constructs a fulltext2 index's two hidden tables — a
// chunked segment store + a metadata table (bm25 layout) — from CREATE FULLTEXT2
// INDEX. Both defs are stamped algo="fulltext2" (told apart by IndexAlgoTableType)
// so every downstream hook dispatches to this plugin.
func (Hooks) BuildFullTextIndexDefs(
	ctx planplugin.CompilerContext,
	indexInfo *tree.FullTextIndex,
	colMap map[string]*plan.ColDef,
	existedIndexes []*plan.IndexDef,
	pkeyName string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {
	if pkeyName == "" || pkeyName == catalog.FakePrimaryKeyColName {
		return nil, nil, moerr.NewInternalErrorNoCtx("primary key cannot be empty for fulltext2 index")
	}

	// Reject a duplicate fulltext2 index on the same column set.
	for _, existed := range existedIndexes {
		if existed.IndexAlgo != tree.INDEX_TYPE_FULLTEXT2.ToString() || len(indexInfo.KeyParts) != len(existed.Parts) {
			continue
		}
		n := 0
		for _, keyPart := range indexInfo.KeyParts {
			for _, ePart := range existed.Parts {
				if ePart == keyPart.ColName.ColName() {
					n++
					break
				}
			}
		}
		if n == len(indexInfo.KeyParts) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "fulltext2 index are not allowed to use the same column")
		}
	}

	// Validate column types (char/varchar/text/json/datalink).
	for _, keyPart := range indexInfo.KeyParts {
		name := keyPart.ColName.ColName()
		col, ok := colMap[name]
		if !ok {
			return nil, nil, moerr.NewInvalidInput(ctx.GetContext(), fmt.Sprintf("column '%s' does not exist", keyPart.ColName.ColNameOrigin()))
		}
		typid := col.Typ.Id
		if !(typid == int32(types.T_text) || typid == int32(types.T_char) ||
			typid == int32(types.T_varchar) || typid == int32(types.T_json) || typid == int32(types.T_datalink)) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "fulltext2 index only support char, varchar, text, datalink and json")
		}
	}
	// Validate parser.
	if indexInfo.IndexOption != nil && indexInfo.IndexOption.ParserName != "" {
		p := strings.ToLower(indexInfo.IndexOption.ParserName)
		if p != "ngram" && p != "default" && p != "json" && p != "json_value" && p != "gojieba" {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("fulltext2 parser %s not supported", p))
		}
	}

	params, err := buildFullText2Params(indexInfo)
	if err != nil {
		return nil, nil, err
	}
	indexParts := make([]string, 0, len(indexInfo.KeyParts))
	for _, keyPart := range indexInfo.KeyParts {
		indexParts = append(indexParts, keyPart.ColName.ColName())
	}
	var option *plan.IndexOption
	var comment string
	if indexInfo.IndexOption != nil {
		if indexInfo.IndexOption.ParserName != "" {
			option = &plan.IndexOption{ParserName: indexInfo.IndexOption.ParserName, NgramTokenSize: int32(3)}
		}
		comment = indexInfo.IndexOption.Comment
	}
	algo := tree.INDEX_TYPE_FULLTEXT2.ToString()
	mkIdx := func(tblName, tblType string) *plan.IndexDef {
		return &plan.IndexDef{
			Unique:             false,
			IndexName:          indexInfo.Name,
			IndexTableName:     tblName,
			IndexAlgo:          algo,
			IndexAlgoTableType: tblType,
			IndexAlgoParams:    params,
			Parts:              indexParts,
			TableExist:         true,
			Option:             option,
			Comment:            comment,
		}
	}

	// storage (chunk) table.
	storeName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	storeTbl := &plan.TableDef{
		Name:      storeName,
		TableType: catalog.FullText2Index_TblType_Storage,
		Cols: []*plan.ColDef{
			{Name: catalog.FullText2Index_TblCol_Storage_Index_Id, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_varchar), Width: 128}, Default: &plan.Default{}},
			{Name: catalog.FullText2Index_TblCol_Storage_Chunk_Id, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
			{Name: catalog.FullText2Index_TblCol_Storage_Data, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_blob), Width: 65536}, Default: &plan.Default{}},
			{Name: catalog.FullText2Index_TblCol_Storage_Tag, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
		},
	}
	storePk := planplugin.MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
	storePk.Alg = plan.CompressType_Lz4
	storePk.Primary = true
	storeTbl.Cols = append(storeTbl.Cols, storePk)
	storeTbl.Pkey = &plan.PrimaryKeyDef{
		Names:       []string{catalog.FullText2Index_TblCol_Storage_Index_Id, catalog.FullText2Index_TblCol_Storage_Chunk_Id},
		PkeyColName: catalog.CPrimaryKeyColName,
		CompPkeyCol: storeTbl.Cols[3],
	}
	storeTbl.Defs = append(storeTbl.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.FullText2Index_TblType_Storage},
		}}},
	})

	// metadata table.
	metaName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}
	metaTbl := &plan.TableDef{
		Name:      metaName,
		TableType: catalog.FullText2Index_TblType_Metadata,
		Cols: []*plan.ColDef{
			{Name: catalog.FullText2Index_TblCol_Metadata_Index_Id, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_varchar), Width: 128}, Primary: true, Default: &plan.Default{}},
			{Name: catalog.FullText2Index_TblCol_Metadata_Timestamp, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
			{Name: catalog.FullText2Index_TblCol_Metadata_Checksum, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, Default: &plan.Default{}},
			{Name: catalog.FullText2Index_TblCol_Metadata_Filesize, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
			{Name: catalog.FullText2Index_TblCol_Metadata_Recency, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
			{Name: catalog.FullText2Index_TblCol_Metadata_Nrow, Alg: plan.CompressType_Lz4, Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}},
		},
	}
	metaTbl.Pkey = &plan.PrimaryKeyDef{
		Names:       []string{catalog.FullText2Index_TblCol_Metadata_Index_Id},
		PkeyColName: catalog.FullText2Index_TblCol_Metadata_Index_Id,
	}
	metaTbl.Defs = append(metaTbl.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.FullText2Index_TblType_Metadata},
		}}},
	})

	return []*plan.IndexDef{
		mkIdx(storeName, catalog.FullText2Index_TblType_Storage),
		mkIdx(metaName, catalog.FullText2Index_TblType_Metadata),
	}, []*plan.TableDef{storeTbl, metaTbl}, nil
}

// buildFullText2Params records the parser name and max_index_capacity in
// algo_params (fulltext2 is always-async by algo, so no async/version params).
func buildFullText2Params(idx *tree.FullTextIndex) (string, error) {
	res := make(map[string]string)
	// Always record the parser (default "ngram" when unspecified), so algo_params
	// is never an empty string — every consumer IndexParamsStringToMaps it (incl.
	// the ALTER REINDEX flow in ddl.go, which errors on ""), and it self-documents
	// the parser. Mirrors classic fulltext storing "parser" when set.
	parser := "ngram"
	if idx.IndexOption != nil && idx.IndexOption.ParserName != "" {
		parser = strings.ToLower(idx.IndexOption.ParserName)
	}
	res["parser"] = parser
	if idx.IndexOption != nil && idx.IndexOption.MaxIndexCapacity > 0 {
		res[catalog.IndexAlgoParamMaxIndexCapacity] = strconv.FormatInt(idx.IndexOption.MaxIndexCapacity, 10)
	}
	return catalog.IndexParamsMapToJsonString(res)
}
