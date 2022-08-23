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

package jq

import (
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	ByVarchar func(*types.Bytes, *types.Bytes, *types.Bytes) (*types.Bytes, error)
	ByJson    func(*types.Bytes, *types.Bytes, *types.Bytes) (*types.Bytes, error)
)

func init() {
	ByVarchar = qVarchar
	ByJson = qJson
}

func qJson(json *types.Bytes, path *types.Bytes, result *types.Bytes) (*types.Bytes, error) {
	logutil.Debugf("json qj: json=%s, path=%s, result=%s", json, path, result)
	pData := path.Data
	pStar, err := types.ParseStringToPath(string(pData))
	if err != nil {
		logutil.Debugf("json qj: error:%v", err)
		return nil, err
	}
	for i := range json.Offsets {
		jOff, jLen := json.Offsets[i], json.Lengths[i]
		ret, err := qJsonOne(json.Data[jOff:jOff+jLen], &pStar)
		if err != nil {
			logutil.Debugf("json qj: error:%v", err)
			return nil, err
		}
		result.AppendOnce(ret)
	}
	return result, nil
}

func qJsonOne(json []byte, path *bytejson.Path) ([]byte, error) {
	//TODO check here
	bj := types.DecodeJson(json)
	return []byte(bj.Query(*path).String()), nil
}

func qVarchar(json *types.Bytes, path *types.Bytes, result *types.Bytes) (*types.Bytes, error) {
	logutil.Debugf("json qv: json=%s, path=%s, result=%s", json, path, result)
	pData := path.Data
	pStar, err := types.ParseStringToPath(string(pData))
	if err != nil {
		logutil.Debugf("json qv: error:%v", err)
		return nil, err
	}
	for i := range json.Offsets {
		jOff, jLen := json.Offsets[i], json.Lengths[i]
		ret, err := qVarcharOne(json.Data[jOff:jOff+jLen], &pStar)
		if err != nil {
			logutil.Debugf("json qv: error:%v", err)
			return nil, err
		}
		result.AppendOnce(ret)
	}
	return result, nil
}
func qVarcharOne(json []byte, path *bytejson.Path) ([]byte, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		logutil.Debugf("json qvOne : error:%v", err)
		return nil, err
	}
	return []byte(bj.Query(*path).String()), nil
}
