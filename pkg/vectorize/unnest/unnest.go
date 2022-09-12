// Copyright 2021 - 2022 Matrix Origin
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

package unnest

import (
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	Unnest func(jBytes, pBytes *types.Bytes, outer []bool, result *batch.Batch) (*batch.Batch, error)
)

func init() {
	Unnest = unnest
}

func addValueByIndex(bat *batch.Batch, index int32, val string) error {
	vec := bat.GetVector(index)
	return vector.Append(vec, val)
}

func addValue(bat *batch.Batch, val bytejson.UnnestResult) error {
	err := addValueByIndex(bat, 1, val.Key)
	if err != nil {
		return err
	}
	err = addValueByIndex(bat, 2, val.Path)
	if err != nil {
		return err
	}
	err = addValueByIndex(bat, 3, val.Index)
	if err != nil {
		return err
	}
	err = addValueByIndex(bat, 4, val.Value)
	if err != nil {
		return err
	}
	err = addValueByIndex(bat, 5, val.This)
	return err
}

func byOnePath(jBytes *types.Bytes, path *bytejson.Path, outer []bool, result *batch.Batch) (*batch.Batch, error) {

	for i := range jBytes.Lengths {
		off := jBytes.Offsets[i]
		l := jBytes.Lengths[i]
		json, err := types.ParseSliceToByteJson(jBytes.Data[off : off+l])
		if err != nil {
			return nil, err
		}
		out, err := json.Unnest(*path, outer[i], false, "both")
		if err != nil {
			return nil, err
		}

		for _, v := range out {
			vec := result.GetVector(0)
			newSeq := 0
			if vec.Length == 0 {
				newSeq = 1
			} else {
				seq := vector.GetFixedVectorValues[int](vec, vec.Length)
				newSeq = seq[vec.Length-1] + 1
			}
			err := vector.Append(vec, newSeq)
			if err != nil {
				return nil, err
			}
			err = addValue(result, *v)
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func unnest(jBytes, pBytes *types.Bytes, outer []bool, result *batch.Batch) (*batch.Batch, error) {
	if len(pBytes.Lengths) == 1 {
		path, err := types.ParseStringToPath(string(pBytes.Data))
		if err != nil {
			return nil, err
		}
		return byOnePath(jBytes, &path, outer, result)
	}
	if len(pBytes.Lengths) != len(jBytes.Lengths) {
		return nil, errors.New("the length of json and path is not equal")
	}
	logutil.Infof("unnest json and path,length:%d", len(pBytes.Lengths))
	return nil, nil
}
