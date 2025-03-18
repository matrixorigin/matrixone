// Copyright 2023 Matrix Origin
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

package catalog

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// Index Algorithm names
const (
	MoIndexDefaultAlgo  = tree.INDEX_TYPE_INVALID  // used by UniqueIndex or default SecondaryIndex
	MoIndexBTreeAlgo    = tree.INDEX_TYPE_BTREE    // used for Mocking MySQL behaviour.
	MoIndexIvfFlatAlgo  = tree.INDEX_TYPE_IVFFLAT  // used for IVF flat index on Vector/Array columns
	MOIndexMasterAlgo   = tree.INDEX_TYPE_MASTER   // used for Master Index on VARCHAR columns
	MOIndexFullTextAlgo = tree.INDEX_TYPE_FULLTEXT // used for Fulltext Index on VARCHAR columns
	MoIndexHnswAlgo     = tree.INDEX_TYPE_HNSW     // used for HNSW Index on Vector/Array columns
)

// ToLower is used for before comparing AlgoType and IndexAlgoParamOpType. Reason why they are strings
//  1. Changing AlgoType from string to Enum will break the backward compatibility.
//     "panic: Unable to find target column from predefined table columns"
//  2. IndexAlgoParamOpType is serialized and stored in the mo_indexes as JSON string.
func ToLower(str string) string {
	return strings.ToLower(strings.TrimSpace(str))
}

// IsNullIndexAlgo is used to skip printing the default "" index algo in the restoreDDL and buildShowCreateTable
func IsNullIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexDefaultAlgo.ToString()
}

// IsRegularIndexAlgo are indexes which will be handled by regular index flow, ie the one where
// we have one hidden table.
func IsRegularIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexDefaultAlgo.ToString() || _algo == MoIndexBTreeAlgo.ToString()
}

func IsIvfIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexIvfFlatAlgo.ToString()
}

func IsMasterIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MOIndexMasterAlgo.ToString()
}

func IsFullTextIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MOIndexFullTextAlgo.ToString()
}

func IsHnswIndexAlgo(algo string) bool {
	_algo := ToLower(algo)
	return _algo == MoIndexHnswAlgo.ToString()
}

// ------------------------[START] IndexAlgoParams------------------------
const (
	IndexAlgoParamLists  = "lists"
	IndexAlgoParamOpType = "op_type"
	HnswM                = "m"
	HnswEfConstruction   = "ef_construction"
	HnswQuantization     = "quantization"
	HnswEfSearch         = "ef_search"
)

/* 1. ToString Functions */

// IndexParamsToStringList used by buildShowCreateTable and restoreDDL
// Eg:- "LIST = 10 op_type 'vector_l2_ops'"
// NOTE: don't set default values here as it is used by SHOW and RESTORE DDL.
func IndexParamsToStringList(indexParams string) (string, error) {
	result, err := IndexParamsStringToMap(indexParams)
	if err != nil {
		return "", err
	}

	res := ""
	if val, ok := result[IndexAlgoParamLists]; ok {
		res += fmt.Sprintf(" %s = %s ", IndexAlgoParamLists, val)
	}

	if val, ok := result[HnswM]; ok {
		res += fmt.Sprintf(" %s = %s ", HnswM, val)
	}

	if val, ok := result[HnswEfConstruction]; ok {
		res += fmt.Sprintf(" %s = %s ", HnswEfConstruction, val)
	}

	if val, ok := result[HnswEfSearch]; ok {
		res += fmt.Sprintf(" %s = %s ", HnswEfSearch, val)
	}

	if val, ok := result[HnswQuantization]; ok {
		val = ToLower(val)
		_, ok := vectorindex.QuantizationValid(val)
		if !ok {
			return "", moerr.NewInternalErrorNoCtxf("invalid quantization '%s'", val)
		}
		res += fmt.Sprintf(" %s '%s' ", HnswQuantization, val)
	}

	if opType, ok := result[IndexAlgoParamOpType]; ok {
		opType = ToLower(opType)
		if _, ok := metric.OpTypeToIvfMetric[opType]; !ok {
			return "", moerr.NewInternalErrorNoCtxf("invalid op_type: '%s'", opType)
		}

		res += fmt.Sprintf(" %s '%s' ", IndexAlgoParamOpType, opType)
	}

	return res, nil
}

// IndexParamsToJsonString used by buildSecondaryIndexDef
// Eg:- {"lists":"10","op_type":"vector_l2_ops"}
func IndexParamsToJsonString(def interface{}) (string, error) {

	res, err := indexParamsToMap(def)
	if err != nil {
		return "", err
	}

	if len(res) == 0 {
		return "", nil // don't return empty json "{}" string
	}

	return IndexParamsMapToJsonString(res)
}

// IndexParamsMapToJsonString used by AlterTableInPlace and CreateIndexDef
func IndexParamsMapToJsonString(res map[string]string) (string, error) {
	str, err := json.Marshal(res)
	if err != nil {
		return "", err
	}
	return string(str), nil
}

/* 2. ToMap Functions */

// IndexParamsStringToMap used by buildShowCreateTable and restoreDDL
func IndexParamsStringToMap(indexParams string) (map[string]string, error) {
	var result map[string]string
	err := json.Unmarshal([]byte(indexParams), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func fullTextIndexParamsToMap(def *tree.FullTextIndex) (map[string]string, error) {
	res := make(map[string]string)

	// fulltext index here
	if def.IndexOption != nil {
		parsername := strings.ToLower(def.IndexOption.ParserName)
		if parsername != "ngram" && parsername != "default" && parsername != "json" && parsername != "json_value" {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid parser %s", parsername))
		}
		res["parser"] = parsername
	}
	return res, nil
}

func indexParamsToMap(def interface{}) (map[string]string, error) {
	res := make(map[string]string)

	if ftidx, ok := def.(*tree.FullTextIndex); ok {
		return fullTextIndexParamsToMap(ftidx)
	}

	if idx, ok := def.(*tree.Index); ok {

		switch idx.KeyType {
		case tree.INDEX_TYPE_BTREE, tree.INDEX_TYPE_INVALID:
			// do nothing
		case tree.INDEX_TYPE_MASTER:
			// do nothing
		case tree.INDEX_TYPE_IVFFLAT:
			if idx.IndexOption.AlgoParamList == 0 {
				// NOTE:
				// 1. In the parser, we added the failure check for list=0 scenario. So if user tries to explicit
				// set list=0, it will fail.
				// 2. However, if user didn't use the list option (we will get it as 0 here), then we will
				// set the default value as 1.
				res[IndexAlgoParamLists] = strconv.FormatInt(1, 10)
			} else if idx.IndexOption.AlgoParamList > 0 {
				res[IndexAlgoParamLists] = strconv.FormatInt(idx.IndexOption.AlgoParamList, 10)
			} else {
				return nil, moerr.NewInternalErrorNoCtx("invalid list. list must be > 0")
			}

			if len(idx.IndexOption.AlgoParamVectorOpType) > 0 {
				opType := ToLower(idx.IndexOption.AlgoParamVectorOpType)
				if _, ok := metric.OpTypeToIvfMetric[opType]; !ok {
					return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid op_type: '%s'", opType))
				}
				res[IndexAlgoParamOpType] = idx.IndexOption.AlgoParamVectorOpType
			} else {
				res[IndexAlgoParamOpType] = metric.OpType_L2Distance // set l2 as default
			}
		case tree.INDEX_TYPE_HNSW:
			if idx.IndexOption.HnswM < 0 {
				return nil, moerr.NewInternalErrorNoCtx("invalid M. hnsw.M must be > 0")
			}
			if idx.IndexOption.HnswEfConstruction < 0 {
				return nil, moerr.NewInternalErrorNoCtx("invalid ef_construction. hnsw.ef_construction must be > 0")
			}
			if idx.IndexOption.HnswEfSearch < 0 {
				return nil, moerr.NewInternalErrorNoCtx("invalid ef_search. hnsw.ef_search must be > 0")
			}
			if len(idx.IndexOption.HnswQuantization) > 0 {
				_, ok := vectorindex.QuantizationValid(idx.IndexOption.HnswQuantization)
				if !ok {
					return nil, moerr.NewInternalErrorNoCtx("invalid hnsw quantization.")
				}
			}

			// hnswM or HnswEfConstruction == 0, use usearch default value
			if idx.IndexOption.HnswM > 0 {
				res[HnswM] = strconv.FormatInt(idx.IndexOption.HnswM, 10)
			}
			if idx.IndexOption.HnswEfConstruction > 0 {
				res[HnswEfConstruction] = strconv.FormatInt(idx.IndexOption.HnswEfConstruction, 10)
			}
			if idx.IndexOption.HnswEfSearch > 0 {
				res[HnswEfSearch] = strconv.FormatInt(idx.IndexOption.HnswEfSearch, 10)
			}

			if len(idx.IndexOption.HnswQuantization) > 0 {
				res[HnswQuantization] = idx.IndexOption.HnswQuantization
			}

			if len(idx.IndexOption.AlgoParamVectorOpType) > 0 {
				opType := ToLower(idx.IndexOption.AlgoParamVectorOpType)
				if _, ok := metric.OpTypeToUsearchMetric[opType]; !ok {
					return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid op_type. '%s'", opType))
				}
				res[IndexAlgoParamOpType] = idx.IndexOption.AlgoParamVectorOpType
			} else {
				res[IndexAlgoParamOpType] = metric.OpType_L2Distance // set l2 as default
			}
		default:
			return nil, moerr.NewInternalErrorNoCtx("invalid index alogorithm type")
		}

		return res, nil
	}
	return res, moerr.NewInternalErrorNoCtx("indexParamsToMap: invalid index type")
}

func DefaultIvfIndexAlgoOptions() map[string]string {
	res := make(map[string]string)
	res[IndexAlgoParamLists] = "1"                       // set lists = 1 as default
	res[IndexAlgoParamOpType] = metric.OpType_L2Distance // set l2 as default
	return res
}

//------------------------[END] IndexAlgoParams------------------------

// ------------------------[START] Aliaser------------------------

// This code is used by "secondary index" to resolve the "programmatically generated PK" appended to the
// end of the index key "__mo_index_idx_col".

const (
	AliasPrefix = "__mo_alias_"
)

func CreateAlias(column string) string {
	return fmt.Sprintf("%s%s", AliasPrefix, column)
}

func ResolveAlias(alias string) string {
	return strings.TrimPrefix(alias, AliasPrefix)
}

func IsAlias(column string) bool {
	return strings.HasPrefix(column, AliasPrefix)
}

// ------------------------[END] Aliaser------------------------
