// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

func MarshalAggFuncExec(exec AggFuncExec) ([]byte, error) {
	if exec.IsDistinct() {
		return nil, moerr.NewInternalErrorNoCtx("marshal distinct agg exec is not supported")
	}
	return exec.marshal()
}

func UnmarshalAggFuncExec(
	mg AggMemoryManager,
	data []byte) (AggFuncExec, error) {
	encoded := &EncodedAgg{}
	if err := encoded.Unmarshal(data); err != nil {
		return nil, err
	}

	info := encoded.GetInfo()

	exec := MakeAgg(mg, info.Id, info.IsDistinct, info.Args...)

	if encoded.Info.Id == aggIdOfGroupConcat {
		if len(encoded.Groups) > 0 && len(encoded.Groups[0]) > 0 {
			exec.(*groupConcatExec).separator = encoded.Groups[0]
		}
	}

	var mp *mpool.MPool = nil
	if mg != nil {
		mp = mg.Mp()
	}

	if err := exec.unmarshal(mp, encoded.Result, encoded.Groups); err != nil {
		exec.Free()
		return nil, err
	}
	return exec, nil
}

var _ = CopyAggFuncExec

func CopyAggFuncExec(mg AggMemoryManager, exec AggFuncExec) (AggFuncExec, error) {
	bs, err := MarshalAggFuncExec(exec)
	if err != nil {
		return nil, err
	}
	return UnmarshalAggFuncExec(mg, bs)
}
