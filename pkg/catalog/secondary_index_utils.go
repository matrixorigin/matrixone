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

// IsNullIndexAlgo is used to skip printing the default "" index algo in the restoreDDL and buildShowCreateTable
func IsNullIndexAlgo(algo string) bool {
	_algo := strings.ToLower(strings.TrimSpace(algo))
	return _algo == MoIndexDefaultAlgo.ToString()
}

// IsRegularIndexAlgo are indexes which will be handled by regular index flow, ie the one where
// we have one hidden table.
func IsRegularIndexAlgo(algo string) bool {
	_algo := strings.ToLower(strings.TrimSpace(algo))
	return _algo == MoIndexDefaultAlgo.ToString() || _algo == MoIndexBTreeAlgo.ToString()
}

func IsIvfIndexAlgo(algo string) bool {
	_algo := strings.ToLower(strings.TrimSpace(algo))
	return _algo == MoIndexIvfFlatAlgo.ToString()
}

// ------------------------[START] IndexAlgoParams------------------------
const (
	IndexAlgoParamLists            = "lists"
	IndexAlgoParamSimilarityFn     = "similarity_function"
	IndexAlgoParamSimilarityFn_ip  = "ip"
	IndexAlgoParamSimilarityFn_l2  = "l2"
	IndexAlgoParamSimilarityFn_cos = "cos"
)

// IndexParamsToStringList used by buildShowCreateTable and restoreDDL
// Eg:- "LIST 10 similarity_function 'ip'"
func IndexParamsToStringList(indexParams string) (string, error) {
	result, err := IndexParamsStringToMap(indexParams)
	if err != nil {
		return "", err
	}

	res := ""
	if val, ok := result[IndexAlgoParamLists]; ok {
		res += fmt.Sprintf(" %s %s", IndexAlgoParamLists, val)
	}

	if similarityFn, ok := result[IndexAlgoParamSimilarityFn]; ok {
		if similarityFn == IndexAlgoParamSimilarityFn_ip ||
			similarityFn == IndexAlgoParamSimilarityFn_l2 ||
			similarityFn == IndexAlgoParamSimilarityFn_cos {
			res += fmt.Sprintf(" %s '%s'", IndexAlgoParamSimilarityFn, similarityFn)
		} else {
			return "", moerr.NewInternalErrorNoCtx("invalid similarity function. not of type '%s', '%s', '%s'",
				IndexAlgoParamSimilarityFn_ip, IndexAlgoParamSimilarityFn_l2, IndexAlgoParamSimilarityFn_cos)
		}
	}

	return res, nil
}

// IndexParamsToJsonString used by buildSecondaryIndexDef
// Eg:- {"lists":"10","similarity_function":"ip"}
func IndexParamsToJsonString(def *tree.Index) (string, error) {

	res, err := IndexParamsToMap(def)
	if err != nil {
		return "", err
	}

	if len(res) == 0 {
		return "", nil // don't return empty json "{}" string
	}

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

		if len(def.IndexOption.AlgoParamVectorSimilarityFn) > 0 {
			similarityFn := strings.ToLower(strings.TrimSpace(def.IndexOption.AlgoParamVectorSimilarityFn))
			if similarityFn == IndexAlgoParamSimilarityFn_ip ||
				similarityFn == IndexAlgoParamSimilarityFn_l2 ||
				similarityFn == IndexAlgoParamSimilarityFn_cos {
				res[IndexAlgoParamSimilarityFn] = def.IndexOption.AlgoParamVectorSimilarityFn
			} else {
				return nil, moerr.NewInternalErrorNoCtx("invalid similarity function. not of type '%s', '%s', '%s'",
					IndexAlgoParamSimilarityFn_ip, IndexAlgoParamSimilarityFn_l2, IndexAlgoParamSimilarityFn_cos)
			}
		} else {
			res[IndexAlgoParamSimilarityFn] = IndexAlgoParamSimilarityFn_cos // set cos as default
		}
	}
	return res, nil
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
