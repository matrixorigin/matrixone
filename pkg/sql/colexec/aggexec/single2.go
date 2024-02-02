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

import "github.com/matrixorigin/matrixone/pkg/container/vector"

// todo: move the codes following to single.go after finishing all.

func (exec *singleAggFuncExec2[from]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.groups[groupIndex].fillNull(setter)
		}
		return nil
	}

	exec.groups[groupIndex].fill(vector.MustFixedCol[from](vec)[row], setter)
	return nil
}

func (exec *singleAggFuncExec2[from]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()
	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.groups[groupIndex].fills(nil, true, length, setter)
			}
		} else {
			exec.groups[groupIndex].fills(vector.MustFixedCol[from](vec)[0], false, length, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if null {
					exec.groups[groupIndex].fillNull(setter)
				} else {
					exec.groups[groupIndex].fill(v, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					exec.groups[groupIndex].fill(v, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetValue(i)
		exec.groups[groupIndex].fill(v, setter)
	}
	return nil
}

func (exec *singleAggFuncExec2[from]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.groups[groupIdx].fillNull(setter)
					}
				}
			}
			return nil
		}

		value := vector.MustFixedCol[from](vec)[0]
		for i := 0; i < len(groups); i++ {
			if groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.groups[groupIdx].fill(value, setter)
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
				if groups[idx] != GroupNotMatched {
					v, null := exec.arg.w.GetValue(i)
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					if null {
						exec.groups[groupIdx].fillNull(setter)
					} else {
						exec.groups[groupIdx].fill(v, setter)
					}
				}
				idx++
			}
			return nil
		}

		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.groups[groupIdx].fill(v, setter)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, _ := exec.arg.w.GetValue(i)
			groupIdx := int(groups[idx] - 1)
			exec.ret.groupToSet = groupIdx
			exec.groups[groupIdx].fill(v, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec2[from]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].fill(exec.partialResult.(from), exec.ret.aggSet)
	}

	var err error
	for i, group := range exec.groups {
		if err = exec.ret.set(i, group.flush()); err != nil {
			return nil, err
		}
	}

	// copy from singleAggFuncExec1's Flush()
	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec2[from]) Free() {
	exec.ret.free()
}

func (exec *singleAggFuncExec3[to]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.groups[groupIndex].fillNull(setter)
		}
		return nil
	}

	exec.groups[groupIndex].fillBytes(vec.GetBytesAt(row), setter)
	return nil
}

func (exec *singleAggFuncExec3[to]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.groups[groupIndex].fills(nil, true, length, setter)
			}
		} else {
			exec.groups[groupIndex].fills(vec.GetBytesAt(0), false, length, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if null {
					exec.groups[groupIndex].fillNull(setter)
				} else {
					exec.groups[groupIndex].fillBytes(v, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					exec.groups[groupIndex].fillBytes(v, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetStrValue(i)
		exec.groups[groupIndex].fillBytes(v, setter)
	}
	return nil
}

func (exec *singleAggFuncExec3[to]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.groups[groupIdx].fillNull(setter)
					}
				}
			}
			return nil
		}

		value := vec.GetBytesAt(0)
		for i := 0; i < len(groups); i++ {
			if groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.groups[groupIdx].fillBytes(value, setter)
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
				if groups[idx] != GroupNotMatched {
					v, null := exec.arg.w.GetStrValue(i)
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					if null {
						exec.groups[groupIdx].fillNull(setter)
					} else {
						exec.groups[groupIdx].fillBytes(v, setter)
					}
				}
				idx++
			}
			return nil
		}

		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.groups[groupIdx].fillBytes(v, setter)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, _ := exec.arg.w.GetStrValue(i)
			groupIdx := int(groups[idx] - 1)
			exec.ret.groupToSet = groupIdx
			exec.groups[groupIdx].fillBytes(v, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec3[to]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].fillBytes(exec.partialResult.([]byte), exec.ret.aggSet)
	}

	for i, group := range exec.groups {
		exec.ret.set(i, group.flush())
	}

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec3[to]) Free() {
	exec.ret.free()
}

func (exec *singleAggFuncExec4) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.groups[groupIndex].fillNull(setter)
		}
		return nil
	}

	exec.groups[groupIndex].fillBytes(vec.GetBytesAt(row), setter)
	return nil
}

func (exec *singleAggFuncExec4) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()

	exec.ret.groupToSet = groupIndex
	setter := exec.ret.aggSet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.groups[groupIndex].fills(nil, true, length, setter)
			}
		} else {
			exec.groups[groupIndex].fills(vec.GetBytesAt(0), false, length, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if null {
					exec.groups[groupIndex].fillNull(setter)
				} else {
					exec.groups[groupIndex].fillBytes(v, setter)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					exec.groups[groupIndex].fillBytes(v, setter)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetStrValue(i)
		exec.groups[groupIndex].fillBytes(v, setter)
	}
	return nil
}

func (exec *singleAggFuncExec4) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.groups[groupIdx].fillNull(setter)
					}
				}
			}
			return nil
		}

		value := vec.GetBytesAt(0)
		for i := 0; i < len(groups); i++ {
			if groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.groups[groupIdx].fillBytes(value, setter)
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
				if groups[idx] != GroupNotMatched {
					v, null := exec.arg.w.GetStrValue(i)
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					if null {
						exec.groups[groupIdx].fillNull(setter)
					} else {
						exec.groups[groupIdx].fillBytes(v, setter)
					}
				}
				idx++
			}
			return nil
		}

		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.groups[groupIdx].fillBytes(v, setter)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, _ := exec.arg.w.GetStrValue(i)
			groupIdx := int(groups[idx] - 1)
			exec.ret.groupToSet = groupIdx
			exec.groups[groupIdx].fillBytes(v, setter)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec4) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.ret.groupToSet = exec.partialGroup
		exec.groups[exec.partialGroup].fillBytes(exec.partialResult.([]byte), exec.ret.aggSet)
	}

	var err error
	for i, group := range exec.groups {
		if err = exec.ret.set(i, group.flush()); err != nil {
			return nil, err
		}
	}

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec4) Free() {
	exec.ret.free()
}
