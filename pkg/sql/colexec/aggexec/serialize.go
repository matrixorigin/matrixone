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

var _ = MarshalAggFuncExec
var _ = UnmarshalAggFuncExec

func MarshalAggFuncExec(exec AggFuncExec) ([]byte, error) {
	return exec.marshal()
}

// there must be 1 bug that, the result of unmarshal was read only. it cannot do any write operation.
func UnmarshalAggFuncExec(
	data []byte) (AggFuncExec, error) {
	encoded := &EncodedAgg{}
	if err := encoded.Unmarshal(data); err != nil {
		return nil, err
	}

	info := encoded.GetInfo()

	var exec = MakeAgg(nil, info.Id, info.IsDistinct, info.Args...)
	//switch encoded.GetExecType() {
	//case EncodedAggExecType_special_group_concat:
	//	exec = makeGroupConcat(
	//		nil, info.Id, info.IsDistinct, info.Args, info.Ret, string(encoded.Groups[0]))
	//
	//case EncodedAggExecType_special_count_column:
	//	exec = makeCount(
	//		nil, info.Id, info.IsDistinct, info.Args[0])
	//
	//case EncodedAggExecType_special_count_star:
	//	panic("count(*) not implement now.")
	//
	//case EncodedAggExecType_single_fixed_fixed, EncodedAggExecType_single_fixed_var,
	//	EncodedAggExecType_single_var_fixed, EncodedAggExecType_single_var_var:
	//	exec = makeSingleAgg(
	//		nil, info.Id, info.IsDistinct, info.Args[0])
	//
	//case EncodedAggExecType_multi_return_fixed, EncodedAggExecType_multi_return_var:
	//	exec = makeMultiAgg(
	//		nil, info.Id, info.IsDistinct, info.Args)
	//
	//default:
	//	return nil, moerr.NewInternalErrorNoCtx("Unmarshal agg exec failed, unknown exec type %d", encoded.GetExecType())
	//}

	if err := exec.unmarshal(encoded.Result, encoded.Groups); err != nil {
		exec.Free()
		return nil, err
	}
	return exec, nil
}

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
			encoded.Groups[i] = exec.groups[i].Marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *singleAggFuncExec1[from, to]) unmarshal(result []byte, groups [][]byte) error {
	exec.groups = make([]SingleAggFromFixedRetFixed[from, to], len(groups))
	for i := range exec.groups {
		exec.groups[i] = exec.gGroup()
		exec.groups[i].Unmarshal(groups[i])
	}
	return exec.ret.unmarshal(result)
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
			encoded.Groups[i] = exec.groups[i].Marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *singleAggFuncExec2[from]) unmarshal(result []byte, groups [][]byte) error {
	exec.groups = make([]SingleAggFromFixedRetVar[from], len(groups))
	for i := range exec.groups {
		exec.groups[i] = exec.gGroup()
		exec.groups[i].Unmarshal(groups[i])
	}
	return exec.ret.unmarshal(result)
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
			encoded.Groups[i] = exec.groups[i].Marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *singleAggFuncExec3[to]) unmarshal(result []byte, groups [][]byte) error {
	exec.groups = make([]SingleAggFromVarRetFixed[to], len(groups))
	for i := range exec.groups {
		exec.groups[i] = exec.gGroup()
		exec.groups[i].Unmarshal(groups[i])
	}
	return exec.ret.unmarshal(result)
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
			encoded.Groups[i] = exec.groups[i].Marshal()
		}
	}
	return encoded.Marshal()
}

func (exec *singleAggFuncExec4) unmarshal(result []byte, groups [][]byte) error {
	exec.groups = make([]SingleAggFromVarRetVar, len(groups))
	for i := range exec.groups {
		exec.groups[i] = exec.gGroup()
		exec.groups[i].Unmarshal(groups[i])
	}
	return exec.ret.unmarshal(result)
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

func (exec *multiAggFuncExec1[T]) unmarshal(result []byte, groups [][]byte) error {
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

func (exec *multiAggFuncExec2) unmarshal(result []byte, groups [][]byte) error {
	exec.groups = make([]MultiAggRetVar, len(groups))
	for i := range exec.groups {
		exec.groups[i] = exec.gGroup()
		exec.groups[i].Unmarshal(groups[i])
	}
	return exec.ret.unmarshal(result)
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

func (exec *groupConcatExec) unmarshal(result []byte, groups [][]byte) error {
	return exec.ret.unmarshal(result)
}

func (exec *countColumnExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_special_count_column,
		Info:     d,
		Result:   r,
		Groups:   nil,
	}
	return encoded.Marshal()
}

func (exec *countColumnExec) unmarshal(result []byte, groups [][]byte) error {
	return exec.ret.unmarshal(result)
}
