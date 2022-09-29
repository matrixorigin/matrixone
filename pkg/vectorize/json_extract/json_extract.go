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
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	QueryByString func([][]byte, [][]byte, [][]byte) ([][]byte, error)
	QueryByJson   func([][]byte, [][]byte, [][]byte) ([][]byte, error)
)

func init() {
	QueryByString = byString
	QueryByJson = byJson
}

func byJson(json, path, result [][]byte) ([][]byte, error) {
	// XXX The functoin only handles path is constant.
	if len(path) != 1 {
		panic("Json extract can only handle constant path for now.")
	}
	pStar, err := types.ParseStringToPath(string(path[0]))
	if err != nil {
		logutil.Infof("json extract: error:%v", err)
		return nil, err
	}
	for i := range json {
		ret, err := byJsonOne(json[i], &pStar)
		if err != nil {
			logutil.Infof("json extract: error:%v", err)
			return nil, err
		}
		result = append(result, ret)
	}
	return result, nil
}

func byJsonOne(json []byte, path *bytejson.Path) ([]byte, error) {
	//TODO check here
	bj := types.DecodeJson(json)
	return []byte(bj.Query(path).String()), nil
}

func byString(json, path, result [][]byte) ([][]byte, error) {
	// XXX The functoin only handles path is constant.
	if len(path) != 1 {
		panic("Json extract can only handle constant path for now.")
	}
	pStar, err := types.ParseStringToPath(string(path[0]))
	if err != nil {
		logutil.Infof("json qv: error:%v", err)
		return nil, err
	}
	for i := range json {
		ret, err := byStringOne(json[i], &pStar)
		if err != nil {
			logutil.Infof("json qv: error:%v", err)
			return nil, err
		}
		result = append(result, ret)
	}
	return result, nil
}
func byStringOne(json []byte, path *bytejson.Path) ([]byte, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		logutil.Debugf("json qvOne : error:%v", err)
		return nil, err
	}
	return []byte(bj.Query(path).String()), nil
}
