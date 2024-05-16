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

	if encoded.GetExecType() == EncodedAggExecType_special_group_concat {
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

func (exec *multiAggFuncExec1[to]) marshal() ([]byte, error) {
	d := exec.multiAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_multi_return_fixed,
		Info:     d,
		Result:   r,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i] = exec.groups[i].Marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *multiAggFuncExec1[T]) unmarshal(mp *mpool.MPool, result []byte, groups [][]byte) error {
	exec.groups = make([]MultiAggRetFixed[T], len(groups))
	for i := range exec.groups {
		exec.groups[i] = exec.gGroup()
		exec.groups[i].Unmarshal(groups[i])
	}
	return exec.ret.unmarshal(result)
}

func (exec *multiAggFuncExec2) marshal() ([]byte, error) {
	d := exec.multiAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_multi_return_fixed,
		Info:     d,
		Result:   r,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i] = exec.groups[i].Marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *multiAggFuncExec2) unmarshal(mp *mpool.MPool, result []byte, groups [][]byte) error {
	exec.groups = make([]MultiAggRetVar, len(groups))
	for i := range exec.groups {
		exec.groups[i] = exec.gGroup()
		exec.groups[i].Unmarshal(groups[i])
	}
	return exec.ret.unmarshal(result)
}
