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
	QueryByString func(*types.Bytes, *types.Bytes, *types.Bytes) (*types.Bytes, error)
	QueryByJson   func(*types.Bytes, *types.Bytes, *types.Bytes) (*types.Bytes, error)
)

func init() {
	QueryByString = byString
	QueryByJson = byJson
}

func byJson(json *types.Bytes, path *types.Bytes, result *types.Bytes) (*types.Bytes, error) {
	pData := path.Data
	pStar, err := types.ParseStringToPath(string(pData))
	if err != nil {
		logutil.Infof("json qj: error:%v", err)
		return nil, err
	}
	for i := range json.Offsets {
		jOff, jLen := json.Offsets[i], json.Lengths[i]
		ret, err := byJsonOne(json.Data[jOff:jOff+jLen], &pStar)
		if err != nil {
			logutil.Infof("json qj: error:%v", err)
			return nil, err
		}
		result.AppendOnce(ret)
	}
	return result, nil
}

func byJsonOne(json []byte, path *bytejson.Path) ([]byte, error) {
	//TODO check here
	bj := types.DecodeJson(json)
	return []byte(bj.Query(*path).String()), nil
}

func byString(json *types.Bytes, path *types.Bytes, result *types.Bytes) (*types.Bytes, error) {
	pData := path.Data
	pStar, err := types.ParseStringToPath(string(pData))
	if err != nil {
		logutil.Infof("json qv: error:%v", err)
		return nil, err
	}
	for i := range json.Offsets {
		jOff, jLen := json.Offsets[i], json.Lengths[i]
		ret, err := byStringOne(json.Data[jOff:jOff+jLen], &pStar)
		if err != nil {
			logutil.Infof("json qv: error:%v", err)
			return nil, err
		}
		result.AppendOnce(ret)
	}
	return result, nil
}
func byStringOne(json []byte, path *bytejson.Path) ([]byte, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		logutil.Debugf("json qvOne : error:%v", err)
		return nil, err
	}
	return []byte(bj.Query(*path).String()), nil
}
