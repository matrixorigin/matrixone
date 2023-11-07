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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strconv"
)

func indexParamsToStringList(indexParams string) (string, error) {
	var result map[string]string
	err := json.Unmarshal([]byte(indexParams), &result)
	if err != nil {
		return "", err
	}

	res := ""
	if val, ok := result["lists"]; ok {
		res += " lists " + val
	}

	return res, nil
}

func indexParamsToJsonString(def *tree.Index) (string, error) {
	res := make(map[string]string)

	if def.IndexOption.AlgoParamList != 0 {
		res["lists"] = strconv.FormatInt(def.IndexOption.AlgoParamList, 10)
	}

	str, err := json.Marshal(res)
	if err != nil {
		return "", err
	}
	return string(str), nil
}
