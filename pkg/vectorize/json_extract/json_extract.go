// Copyright 2022 Matrix Origin
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

package json_extract

import (
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	JsonExtract func([][]byte, [][]byte, []*nulls.Nulls, []*bytejson.ByteJson, *nulls.Nulls, func([]byte, *bytejson.Path) (*bytejson.ByteJson, error)) ([]*bytejson.ByteJson, error)
)

func init() {
	JsonExtract = jsonExtract
}

func jsonExtract(json, path [][]byte, nsps []*nulls.Nulls, result []*bytejson.ByteJson, rnsp *nulls.Nulls, compute func([]byte, *bytejson.Path) (*bytejson.ByteJson, error)) ([]*bytejson.ByteJson, error) {
	if len(path) == 1 {
		pStar, err := types.ParseStringToPath(string(path[0]))
		if err != nil {
			return nil, err
		}
		for i := range json {
			if nsps[0].Contains(uint64(i)) {
				rnsp.Set(uint64(i))
				continue
			}
			ret, err := compute(json[i], &pStar)
			if err != nil {
				return nil, err
			}
			if ret.IsNull() {
				rnsp.Set(uint64(i))
				continue
			}
			result[i] = ret
		}
		return result, nil
	}
	if len(json) == 1 {
		for i := range path {
			if nsps[1].Contains(uint64(i)) {
				rnsp.Set(uint64(i))
				continue
			}
			pStar, err := types.ParseStringToPath(string(path[i]))
			if err != nil {
				return nil, err
			}
			ret, err := compute(json[0], &pStar)
			if err != nil {
				return nil, err
			}
			if ret.IsNull() {
				rnsp.Set(uint64(i))
				continue
			}
			result[i] = ret
		}
		return result, nil
	}
	for i := range path {
		if nsps[0].Contains(uint64(i)) || nsps[1].Contains(uint64(i)) {
			rnsp.Set(uint64(i))
			continue
		}
		pStar, err := types.ParseStringToPath(string(path[i]))
		if err != nil {
			return nil, err
		}
		ret, err := compute(json[i], &pStar)
		if err != nil {
			return nil, err
		}
		if ret.IsNull() {
			rnsp.Set(uint64(i))
			continue
		}
		result[i] = ret
	}
	return result, nil
}
