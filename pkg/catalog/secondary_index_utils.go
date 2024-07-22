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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strconv"
	"strings"
)

// Index Algorithm names
const (
	MoIndexDefaultAlgo = tree.INDEX_TYPE_INVALID // used by UniqueIndex or default SecondaryIndex
	MoIndexBTreeAlgo   = tree.INDEX_TYPE_BTREE   // used for Mocking MySQL behaviour.
	MoIndexIvfFlatAlgo = tree.INDEX_TYPE_IVFFLAT // used for IVF flat index on Vector/Array columns
	MOIndexMasterAlgo  = tree.INDEX_TYPE_MASTER  // used for Master Index on VARCHAR columns
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

// ------------------------[START] IndexAlgoParams------------------------
const (
	IndexAlgoParamLists     = "lists"
	IndexAlgoParamOpType    = "op_type"
	IndexAlgoParamOpType_l2 = "vector_l2_ops"
	//IndexAlgoParamOpType_ip  = "vector_ip_ops"
	//IndexAlgoParamOpType_cos = "vector_cosine_ops"
)

const (
	KmeansSamplePerList = 50
	MaxSampleCount      = 10_000
)

// CalcSampleCount is used to calculate the sample count for Kmeans index.
func CalcSampleCount(lists, totalCnt int64) (sampleCnt int64) {

	if totalCnt > lists*KmeansSamplePerList {
		sampleCnt = lists * KmeansSamplePerList
	} else {
		sampleCnt = totalCnt
	}

	if totalCnt > MaxSampleCount && sampleCnt < MaxSampleCount {
		sampleCnt = MaxSampleCount
	}

	if sampleCnt > MaxSampleCount {
		sampleCnt = MaxSampleCount
	}

	return sampleCnt
}

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

	if opType, ok := result[IndexAlgoParamOpType]; ok {
		opType = ToLower(opType)
		if opType != IndexAlgoParamOpType_l2 {
			//	opType != IndexAlgoParamOpType_ip &&
			//	opType != IndexAlgoParamOpType_cos
			return "", moerr.NewInternalErrorNoCtx("invalid op_type. not of type '%s'", IndexAlgoParamOpType_l2)
			//IndexAlgoParamOpType_ip, , IndexAlgoParamOpType_cos)

		}

		res += fmt.Sprintf(" %s '%s' ", IndexAlgoParamOpType, opType)
	}

	return res, nil
}

// IndexParamsToJsonString used by buildSecondaryIndexDef
// Eg:- {"lists":"10","op_type":"vector_l2_ops"}
func IndexParamsToJsonString(def *tree.Index) (string, error) {

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

func indexParamsToMap(def *tree.Index) (map[string]string, error) {
	res := make(map[string]string)

	switch def.KeyType {
	case tree.INDEX_TYPE_BTREE, tree.INDEX_TYPE_INVALID:
		// do nothing
	case tree.INDEX_TYPE_MASTER:
		// do nothing
	case tree.INDEX_TYPE_IVFFLAT:
		if def.IndexOption.AlgoParamList == 0 {
			// NOTE:
			// 1. In the parser, we added the failure check for list=0 scenario. So if user tries to explicit
			// set list=0, it will fail.
			// 2. However, if user didn't use the list option (we will get it as 0 here), then we will
			// set the default value as 1.
			res[IndexAlgoParamLists] = strconv.FormatInt(1, 10)
		} else if def.IndexOption.AlgoParamList > 0 {
			res[IndexAlgoParamLists] = strconv.FormatInt(def.IndexOption.AlgoParamList, 10)
		} else {
			return nil, moerr.NewInternalErrorNoCtx("invalid list. list must be > 0")
		}

		if len(def.IndexOption.AlgoParamVectorOpType) > 0 {
			opType := ToLower(def.IndexOption.AlgoParamVectorOpType)
			if opType != IndexAlgoParamOpType_l2 {
				//opType != IndexAlgoParamOpType_ip &&
				//opType != IndexAlgoParamOpType_cos &&

				return nil, moerr.NewInternalErrorNoCtx("invalid op_type. not of type '%s'",
					IndexAlgoParamOpType_l2,
					//IndexAlgoParamOpType_ip, IndexAlgoParamOpType_cos,
				)
			}
			res[IndexAlgoParamOpType] = def.IndexOption.AlgoParamVectorOpType
		} else {
			res[IndexAlgoParamOpType] = IndexAlgoParamOpType_l2 // set l2 as default
		}
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid index type")
	}
	return res, nil
}

func DefaultIvfIndexAlgoOptions() map[string]string {
	res := make(map[string]string)
	res[IndexAlgoParamLists] = "1"                      // set lists = 1 as default
	res[IndexAlgoParamOpType] = IndexAlgoParamOpType_l2 // set l2 as default
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
