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

// ------------------------[START] IndexAlgoParams------------------------
const (
	IndexAlgoParamLists      = "lists"
	IndexAlgoParamOpType     = "op_type"
	IndexAlgoParamOpType_ip  = "vector_ip_ops"
	IndexAlgoParamOpType_l2  = "vector_l2_ops"
	IndexAlgoParamOpType_cos = "vector_cosine_ops"
)

// IndexParamsToStringList used by buildShowCreateTable and restoreDDL
// Eg:- "LIST = 10 op_type 'vector_l2_ops'"
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
		if opType == IndexAlgoParamOpType_ip ||
			opType == IndexAlgoParamOpType_l2 ||
			opType == IndexAlgoParamOpType_cos {
			res += fmt.Sprintf(" %s '%s' ", IndexAlgoParamOpType, opType)
		} else {
			return "", moerr.NewInternalErrorNoCtx("invalid op_type. not of type '%s', '%s', '%s'",
				IndexAlgoParamOpType_ip, IndexAlgoParamOpType_l2, IndexAlgoParamOpType_cos)
		}
	}

	return res, nil
}

// IndexParamsToJsonString used by buildSecondaryIndexDef
// Eg:- {"lists":"10","op_type":"vector_l2_ops"}
func IndexParamsToJsonString(def *tree.Index) (string, error) {

	res, err := IndexParamsToMap(def)
	if err != nil {
		return "", err
	}

	if len(res) == 0 {
		return "", nil // don't return empty json "{}" string
	}

	return IndexParamsMapToJsonString(res)
}

func IndexParamsMapToJsonString(res map[string]string) (string, error) {
	str, err := json.Marshal(res)
	if err != nil {
		return "", err
	}
	return string(str), nil
}

// IndexParamsStringToMap used by buildShowCreateTable and restoreDDL
func IndexParamsStringToMap(indexParams string) (map[string]string, error) {
	var result map[string]string
	err := json.Unmarshal([]byte(indexParams), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func IndexParamsToMap(def *tree.Index) (map[string]string, error) {
	res := make(map[string]string)

	switch def.KeyType {
	case tree.INDEX_TYPE_BTREE, tree.INDEX_TYPE_INVALID:
		// do nothing
	case tree.INDEX_TYPE_IVFFLAT:
		if def.IndexOption.AlgoParamList != 0 {
			res[IndexAlgoParamLists] = strconv.FormatInt(def.IndexOption.AlgoParamList, 10)
		} else {
			res[IndexAlgoParamLists] = "1" // set lists = 1 as default
		}

		if len(def.IndexOption.AlgoParamVectorOpType) > 0 {
			opType := ToLower(def.IndexOption.AlgoParamVectorOpType)
			if opType == IndexAlgoParamOpType_ip ||
				opType == IndexAlgoParamOpType_l2 ||
				opType == IndexAlgoParamOpType_cos {
				res[IndexAlgoParamOpType] = def.IndexOption.AlgoParamVectorOpType
			} else {
				return nil, moerr.NewInternalErrorNoCtx("invalid op_type. not of type '%s', '%s', '%s'",
					IndexAlgoParamOpType_ip, IndexAlgoParamOpType_l2, IndexAlgoParamOpType_cos)
			}
		} else {
			res[IndexAlgoParamOpType] = IndexAlgoParamOpType_l2 // set l2 as default
		}
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
