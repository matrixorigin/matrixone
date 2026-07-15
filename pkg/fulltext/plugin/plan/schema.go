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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// buildFullTextParams builds the algo_params JSON for a fulltext index from its
// parsed options (parser name + async). Moved from the former
// catalog.fullTextIndexParamsToMap so fulltext owns its own param parsing, like
// the vector plugins' BuildIndexParams hooks.
func buildFullTextParams(idx *tree.FullTextIndex) (string, error) {
	res := make(map[string]string)
	if idx.IndexOption != nil {
		parsername := strings.ToLower(idx.IndexOption.ParserName)
		if len(parsername) > 0 {
			switch parsername {
			case "ngram", "default", "json", "json_value", "gojieba":
			default:
				return "", moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid parser %s", parsername))
			}
			res["parser"] = parsername
		}
		if idx.IndexOption.Async {
			res[catalog.Async] = "true"
		}
	}
	if len(res) == 0 {
		return "", nil
	}
	return catalog.IndexParamsMapToJsonString(res)
}

// BuildFullTextIndexDefs constructs the IndexDef + TableDef for one
// fulltext index. Lifted from
// pkg/sql/plan/build_ddl.go::buildFullTextIndexTable, but per-index
// (the legacy function batched a slice; the plan-layer caller now
// loops and dispatches per-info).
//
// Hidden-table schema: (doc_id, pos, word, __mo_pk_rowid) clustered by word.
func (Hooks) BuildFullTextIndexDefs(
	ctx planplugin.CompilerContext,
	indexInfo *tree.FullTextIndex,
	colMap map[string]*plan.ColDef,
	existedIndexes []*plan.IndexDef,
	pkeyName string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {

	if pkeyName == "" || pkeyName == catalog.FakePrimaryKeyColName {
		return nil, nil, moerr.NewInternalErrorNoCtx("primary key cannot be empty for fulltext index")
	}

	// 1. Reject if an existing fulltext index already covers the same
	// columns. Matches the legacy outer "for existedIndexes" loop.
	for _, existed := range existedIndexes {
		if existed.IndexAlgo != catalog.MOIndexFullTextAlgo.ToString() {
			continue
		}
		if len(indexInfo.KeyParts) != len(existed.Parts) {
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
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "Fulltext index are not allowed to use the same column")
		}
	}

	// 2. Validate column types — fulltext only supports char/varchar/
	// text/json/datalink.
	for _, keyPart := range indexInfo.KeyParts {
		nameOrigin := keyPart.ColName.ColNameOrigin()
		name := keyPart.ColName.ColName()
		col, ok := colMap[name]
		if !ok {
			return nil, nil, moerr.NewInvalidInput(ctx.GetContext(), fmt.Sprintf("column '%s' does not exist", nameOrigin))
		}
		typid := col.Typ.Id
		if !(typid == int32(types.T_text) || typid == int32(types.T_char) ||
			typid == int32(types.T_varchar) || typid == int32(types.T_json) || typid == int32(types.T_datalink)) {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), "fulltext index only support char, varchar, text, datalink and json")
		}
	}

	// 3. Validate parser name (if explicitly set).
	if indexInfo.IndexOption != nil && indexInfo.IndexOption.ParserName != "" {
		parsername := strings.ToLower(indexInfo.IndexOption.ParserName)
		if parsername != "ngram" && parsername != "default" && parsername != "json" && parsername != "json_value" && parsername != "gojieba" {
			return nil, nil, moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("Fulltext parser %s not supported", parsername))
		}
	}

	// 4. Build the IndexDef.
	indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), false)
	if err != nil {
		return nil, nil, err
	}

	indexParts := make([]string, 0, len(indexInfo.KeyParts))
	for _, keyPart := range indexInfo.KeyParts {
		indexParts = append(indexParts, keyPart.ColName.ColName())
	}

	indexDef := &plan.IndexDef{
		Unique:             false,
		IndexName:          indexInfo.Name,
		IndexTableName:     indexTableName,
		IndexAlgo:          tree.INDEX_TYPE_FULLTEXT.ToString(),
		IndexAlgoTableType: "",
		Parts:              indexParts,
		TableExist:         true,
	}
	if indexInfo.IndexOption != nil {
		if indexInfo.IndexOption.ParserName != "" {
			indexDef.Option = &plan.IndexOption{ParserName: indexInfo.IndexOption.ParserName, NgramTokenSize: int32(3)}
		}
		indexDef.IndexAlgoParams, err = buildFullTextParams(indexInfo)
		if err != nil {
			return nil, nil, err
		}
		if indexInfo.IndexOption.Comment != "" {
			indexDef.Comment = indexInfo.IndexOption.Comment
		}
	}

	// 5. Build the hidden TableDef: (doc_id, pos, word, __mo_pk_rowid).
	tableDef := &plan.TableDef{
		Name:      indexTableName,
		TableType: catalog.FullTextIndex_TblType,
	}

	// 5a. foreign primary key column (matches source table's PK type).
	pkSrc, ok := colMap[pkeyName]
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("primary key column not found for fulltext index")
	}
	tableDef.Cols = append(tableDef.Cols, &plan.ColDef{
		Name: catalog.FullTextIndex_TabCol_Id,
		Alg:  plan.CompressType_Lz4,
		Typ: plan.Type{
			Id:    pkSrc.Typ.Id,
			Width: pkSrc.Typ.Width,
			Scale: pkSrc.Typ.Scale,
		},
		Default: &plan.Default{},
	})

	// 5b. position (int32).
	tableDef.Cols = append(tableDef.Cols, &plan.ColDef{
		Name: catalog.FullTextIndex_TabCol_Position,
		Alg:  plan.CompressType_Lz4,
		Typ: plan.Type{
			Id:    int32(types.T_int32),
			Width: 32,
			Scale: -1,
		},
		Default: &plan.Default{},
	})

	// 5c. word (varchar).
	tableDef.Cols = append(tableDef.Cols, &plan.ColDef{
		Name: catalog.FullTextIndex_TabCol_Word,
		Alg:  plan.CompressType_Lz4,
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		},
		Default: &plan.Default{},
	})

	// 5d. hidden auto-increment primary key.
	tableDef.Cols = append(tableDef.Cols, &plan.ColDef{
		Name:   catalog.FakePrimaryKeyColName,
		Hidden: true,
		Alg:    plan.CompressType_Lz4,
		Typ: plan.Type{
			Id:       int32(types.T_uint64),
			AutoIncr: true,
		},
		Default: &plan.Default{},
		NotNull: true,
		Primary: true,
	})

	tableDef.Pkey = &plan.PrimaryKeyDef{
		Names:       []string{catalog.FakePrimaryKeyColName},
		PkeyColName: catalog.FakePrimaryKeyColName,
	}
	tableDef.ClusterBy = &plan.ClusterByDef{Name: "word"}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: []*plan.Property{{
					Key:   catalog.SystemRelAttr_Kind,
					Value: catalog.FullTextIndex_TblType,
				}},
			},
		},
	})

	return []*plan.IndexDef{indexDef}, []*plan.TableDef{tableDef}, nil
}
