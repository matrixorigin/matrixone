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
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	QueryByString func([][]byte, [][]byte, []*bytejson.ByteJson) ([]*bytejson.ByteJson, error)
	QueryByJson   func([][]byte, [][]byte, []*bytejson.ByteJson) ([]*bytejson.ByteJson, error)
)

func init() {
	QueryByString = byString
	QueryByJson = byJson
}

func byJson(json, path [][]byte, result []*bytejson.ByteJson) ([]*bytejson.ByteJson, error) {
	if len(path) == 1 {
		pStar, err := types.ParseStringToPath(string(path[0]))
		if err != nil {
			return nil, err
		}
		for i := range json {
			ret, err := byJsonOne(json[i], &pStar)
			if err != nil {
				return nil, err
			}
			result = append(result, ret)
		}
		return result, nil
	}
	if len(json) == 1 {
		for i := range path {
			pStar, err := types.ParseStringToPath(string(path[i]))
			if err != nil {
				return nil, err
			}
			ret, err := byJsonOne(json[0], &pStar)
			if err != nil {
				return nil, err
			}
			result = append(result, ret)
		}
		return result, nil
	}
	for i := range path {
		pStar, err := types.ParseStringToPath(string(path[i]))
		if err != nil {
			return nil, err
		}
		ret, err := byJsonOne(json[i], &pStar)
		if err != nil {
			return nil, err
		}
		result = append(result, ret)
	}
	return result, nil
}

func byJsonOne(json []byte, path *bytejson.Path) (*bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.Query(path), nil
}

func byString(json, path [][]byte, result []*bytejson.ByteJson) ([]*bytejson.ByteJson, error) {
	if len(path) == 1 {
		pStar, err := types.ParseStringToPath(string(path[0]))
		if err != nil {
			return nil, err
		}
		for i := range json {
			ret, err := byStringOne(json[i], &pStar)
			if err != nil {
				return nil, err
			}
			result = append(result, ret)
		}
		return result, nil
	}
	if len(json) == 1 {
		for i := range path {
			pStar, err := types.ParseStringToPath(string(path[i]))
			if err != nil {
				return nil, err
			}
			ret, err := byStringOne(json[0], &pStar)
			if err != nil {
				return nil, err
			}
			result = append(result, ret)
		}
		return result, nil
	}
	for i := range path {
		pStar, err := types.ParseStringToPath(string(path[i]))
		if err != nil {
			return nil, err
		}
		ret, err := byStringOne(json[i], &pStar)
		if err != nil {
			return nil, err
		}
		result = append(result, ret)
	}
	return result, nil
}
func byStringOne(json []byte, path *bytejson.Path) (*bytejson.ByteJson, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		return nil, err
	}
	return bj.Query(path), nil
}
