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
	JsonExtract func([][]byte, [][][]byte, []*nulls.Nulls, []*bytejson.ByteJson, *nulls.Nulls, int, func([]byte, []*bytejson.Path) (*bytejson.ByteJson, error)) ([]*bytejson.ByteJson, error)
)

func init() {
	JsonExtract = jsonExtract
}

func jsonExtract(json [][]byte, path [][][]byte, nsps []*nulls.Nulls, result []*bytejson.ByteJson, rnsp *nulls.Nulls, maxLen int, compute func([]byte, []*bytejson.Path) (*bytejson.ByteJson, error)) ([]*bytejson.ByteJson, error) {

	prePaths := make([]*bytejson.Path, len(path))
	paths := make([]*bytejson.Path, len(path))
	var preJson, curJson []byte

	for i := 0; i < maxLen; i++ {
		if nsps[0].Contains(uint64(i)) {
			rnsp.Set(uint64(i))
			continue
		}
		if i >= len(json) {
			curJson = preJson
		} else {
			curJson = json[i]
			preJson = json[i]
		}
		skip := false
		for j := 0; j < len(path); j++ {
			if nsps[j+1].Contains(uint64(i)) {
				rnsp.Set(uint64(i))
				skip = true
				break
			}
			if i >= len(path[j]) {
				paths[j] = prePaths[j]
				continue
			}
			p, err := types.ParseStringToPath(string(path[j][i]))
			if err != nil {
				return nil, err
			}
			paths[j] = &p
			prePaths[j] = &p
		}
		if skip {
			continue
		}
		r, err := compute(curJson, paths)
		if err != nil {
			return nil, err
		}
		result[i] = r
	}
	return result, nil
}
