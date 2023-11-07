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

package plan

import (
	"encoding/json"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strconv"
	"strings"
)

const (
	IndexAlgoParamLists            = "lists"
	IndexAlgoParamSimilarityFn     = "similarity_function"
	IndexAlgoParamSimilarityFn_ip  = "ip"
	IndexAlgoParamSimilarityFn_l2  = "l2"
	IndexAlgoParamSimilarityFn_cos = "cos"
)

// indexParamsToStringList used by buildShowCreateTable and restoreDDL
func indexParamsToStringList(indexParams string) (string, error) {
	var result map[string]string
	err := json.Unmarshal([]byte(indexParams), &result)
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

// indexParamsToJsonString used by buildSecondaryIndexDef
func indexParamsToJsonString(def *tree.Index) (string, error) {

	res := make(map[string]string)

	switch def.KeyType {
	case tree.INDEX_TYPE_IVFFLAT:
		if def.IndexOption.AlgoParamList != 0 {
			res[IndexAlgoParamLists] = strconv.FormatInt(def.IndexOption.AlgoParamList, 10)
		}

		if len(def.IndexOption.AlgoParamVectorSimilarityFn) > 0 {
			similarityFn := strings.ToLower(strings.TrimSpace(def.IndexOption.AlgoParamVectorSimilarityFn))
			if similarityFn == IndexAlgoParamSimilarityFn_ip ||
				similarityFn == IndexAlgoParamSimilarityFn_l2 ||
				similarityFn == IndexAlgoParamSimilarityFn_cos {
				res[IndexAlgoParamSimilarityFn] = def.IndexOption.AlgoParamVectorSimilarityFn
			} else {
				return "", moerr.NewInternalErrorNoCtx("invalid similarity function. not of type '%s', '%s', '%s'",
					IndexAlgoParamSimilarityFn_ip, IndexAlgoParamSimilarityFn_l2, IndexAlgoParamSimilarityFn_cos)
			}
		}
	default:
		if len(res) == 0 {
			// don't return empty json string
			return "", nil
		}
	}

	str, err := json.Marshal(res)
	if err != nil {
		return "", err
	}
	return string(str), nil
}
