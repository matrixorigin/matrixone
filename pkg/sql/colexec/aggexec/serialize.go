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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (exec *singleAggFuncExec1[from, to]) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_single_fixed_fixed,
		Info:     d,
		Result:   r,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i] = exec.groups[i].marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *singleAggFuncExec1[from, to]) unmarshal(data []byte) error {
	exec.ret.free()
	return exec.ret.unmarshal(data)
}

func (exec *singleAggFuncExec2[from]) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_single_fixed_fixed,
		Info:     d,
		Result:   r,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i] = exec.groups[i].marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *singleAggFuncExec2[from]) unmarshal(data []byte) error {
	exec.ret.free()
	return exec.ret.unmarshal(data)
}

func (exec *singleAggFuncExec3[to]) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_single_fixed_fixed,
		Info:     d,
		Result:   r,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i] = exec.groups[i].marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *singleAggFuncExec3[to]) unmarshal(data []byte) error {
	exec.ret.free()
	return exec.ret.unmarshal(data)
}

func (exec *singleAggFuncExec4) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_single_fixed_fixed,
		Info:     d,
		Result:   r,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i] = exec.groups[i].marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *singleAggFuncExec4) unmarshal(data []byte) error {
	exec.ret.free()
	return exec.ret.unmarshal(data)
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
			encoded.Groups[i] = exec.groups[i].marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *multiAggFuncExec1[T]) unmarshal(data []byte) error {
	exec.ret.free()
	return exec.ret.unmarshal(data)
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
			encoded.Groups[i] = exec.groups[i].marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *multiAggFuncExec2) unmarshal(data []byte) error {
	exec.ret.free()
	return exec.ret.unmarshal(data)
}

func (exec *groupConcatExec) marshal() ([]byte, error) {
	d := exec.multiAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_special_group_concat,
		Info:     d,
		Result:   r,
		Groups:   [][]byte{exec.separator},
	}
	return encoded.Marshal()
}

func (exec *groupConcatExec) unmarshal(data []byte) error {
	exec.ret.free()
	return exec.ret.unmarshal(data)
}

func MarshalAggFuncExec(exec AggFuncExec) ([]byte, error) {
	return exec.marshal()
}

func UnmarshalAggFuncExec(
	proc *process.Process, data []byte) (AggFuncExec, error) {
	encoded := &EncodedAgg{}
	if err := encoded.Unmarshal(data); err != nil {
		return nil, err
	}

	info := encoded.GetInfo()

	var exec AggFuncExec
	switch encoded.GetExecType() {
	case EncodedAggExecType_special_group_concat:
		exec = MakeGroupConcat(
			proc, info.Id, info.IsDistinct, info.Args, info.Ret, string(encoded.Groups[0]))

	case EncodedAggExecType_single_fixed_fixed, EncodedAggExecType_single_fixed_var,
		EncodedAggExecType_single_var_fixed, EncodedAggExecType_single_var_var:
		exec = MakeAgg(
			proc, info.Id, info.IsDistinct, info.NullEmpty, info.Args[0], info.Ret)

	case EncodedAggExecType_multi_return_fixed, EncodedAggExecType_multi_return_var:
		exec = MakeMultiAgg(
			proc, info.Id, info.IsDistinct, info.NullEmpty, info.Args, info.Ret)

	default:
		return nil, moerr.NewInternalError(proc.Ctx, "unmarshal agg exec failed, unknown exec type %d", encoded.GetExecType())
	}

	if err := exec.unmarshal(encoded.Result); err != nil {
		exec.Free()
		return nil, err
	}
	return exec, nil
}
