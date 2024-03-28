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
	hll "github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec/algos/kmeans"
)

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

	exec := MakeAgg(nil, info.Id, info.IsDistinct, info.Args...)

	if encoded.GetExecType() == EncodedAggExecType_special_group_concat {
		if len(encoded.Groups) > 0 && len(encoded.Groups[0]) > 0 {
			exec.(*groupConcatExec).separator = encoded.Groups[0]
		}
	}

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

func (exec *countStarExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}
	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_special_count_star,
		Info:     d,
		Result:   r,
		Groups:   nil,
	}
	return encoded.Marshal()
}

func (exec *countStarExec) unmarshal(result []byte, groups [][]byte) error {
	return exec.ret.unmarshal(result)
}

func (exec *approxCountFixedExec[T]) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}

	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_special_approx_count,
		Info:     d,
		Result:   r,
		Groups:   nil,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i], err = exec.groups[i].MarshalBinary()
			if err != nil {
				return nil, err
			}
		}
	}
	return encoded.Marshal()
}

func (exec *approxCountFixedExec[T]) unmarshal(result []byte, groups [][]byte) error {
	err := exec.ret.unmarshal(result)
	if err != nil {
		return err
	}
	if len(groups) > 0 {
		exec.groups = make([]*hll.Sketch, len(groups))
		for i := range exec.groups {
			exec.groups[i] = hll.New()
			if err = exec.groups[i].UnmarshalBinary(groups[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *approxCountVarExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}

	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_special_approx_count,
		Info:     d,
		Result:   r,
		Groups:   nil,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i], err = exec.groups[i].MarshalBinary()
			if err != nil {
				return nil, err
			}
		}
	}
	return encoded.Marshal()
}

func (exec *approxCountVarExec) unmarshal(result []byte, groups [][]byte) error {
	err := exec.ret.unmarshal(result)
	if err != nil {
		return err
	}
	if len(groups) > 0 {
		exec.groups = make([]*hll.Sketch, len(groups))
		for i := range exec.groups {
			exec.groups[i] = hll.New()
			if err = exec.groups[i].UnmarshalBinary(groups[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *medianColumnExecSelf[T, R]) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}

	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_special_median,
		Info:     d,
		Result:   r,
		Groups:   nil,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			if encoded.Groups[i], err = exec.groups[i].MarshalBinary(); err != nil {
				return nil, err
			}
		}
	}
	return encoded.Marshal()
}

func (exec *medianColumnExecSelf[T, R]) unmarshal(result []byte, groups [][]byte) error {
	if len(exec.groups) > 0 {
		exec.groups = make([]*vector.Vector, len(groups))
		for i := range exec.groups {
			exec.groups[i] = vector.NewVec(exec.singleAggInfo.argType)
			if err := exec.groups[i].UnmarshalBinary(groups[i]); err != nil {
				return err
			}
		}
	}
	return exec.ret.unmarshal(result)
}

func (exec *clusterCentersExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, err := exec.ret.marshal()
	if err != nil {
		return nil, err
	}

	encoded := &EncodedAgg{
		ExecType: EncodedAggExecType_secial_cluster_center,
		Info:     d,
		Result:   r,
		Groups:   nil,
	}

	encoded.Groups = make([][]byte, len(exec.groupData)+1)
	if len(exec.groupData) > 0 {
		for i := range exec.groupData {
			if encoded.Groups[i], err = exec.groupData[i].MarshalBinary(); err != nil {
				return nil, err
			}
		}
	}

	{
		t1 := uint16(exec.distType)
		t2 := uint16(exec.initType)

		bs := types.EncodeUint64(&exec.clusterCnt)
		bs = append(bs, types.EncodeUint16(&t1)...)
		bs = append(bs, types.EncodeUint16(&t2)...)
		bs = append(bs, types.EncodeBool(&exec.normalize)...)
		encoded.Groups[len(encoded.Groups)-1] = bs
	}
	return encoded.Marshal()
}

func (exec *clusterCentersExec) unmarshal(result []byte, groups [][]byte) error {
	if err := exec.ret.unmarshal(result); err != nil {
		return err
	}
	if len(groups) > 0 {
		exec.groupData = make([]*vector.Vector, len(groups)-1)
		for i := range exec.groupData {
			exec.groupData[i] = vector.NewVec(exec.singleAggInfo.argType)
			if err := exec.groupData[i].UnmarshalBinary(groups[i]); err != nil {
				return err
			}
		}
		bs := groups[len(groups)-1]
		if len(bs) != 13 { // 8+2+2+1
			return moerr.NewInternalErrorNoCtx("invalid cluster center exec data")
		}
		exec.clusterCnt = types.DecodeUint64(bs[:8])
		exec.distType = kmeans.DistanceType(types.DecodeUint16(bs[8:10]))
		exec.initType = kmeans.InitType(types.DecodeUint16(bs[10:12]))
		exec.normalize = types.DecodeBool(bs[12:])
	}
	return nil
}
