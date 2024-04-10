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

func (exec *singleAggFuncExec1[from, to]) distinctBulkFill(
	groupIndex int, vectors []*vector.Vector, length int) error {
	exec.ret.groupToSet = groupIndex
	getter := exec.ret.aggGet
	setter := exec.ret.aggSet

	vec := vectors[0]
	if vec.IsConst() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, 0); err != nil || !need {
			return err
		}

		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.ret.setGroupNotEmpty(groupIndex)
				return exec.fillNull(exec.groups[groupIndex], getter, setter)
			}
		} else {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fill(exec.groups[groupIndex], vector.MustFixedCol[from](vec)[0], getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	needs, err := exec.distinctHash.bulkFill(groupIndex, vectors)
	if err != nil {
		return err
	}

	mustNotEmpty := false
	if exec.receiveNull {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			if needs[i] {
				v, null := exec.arg.w.GetValue(i)
				mustNotEmpty = true
				if null {
					if err = exec.fillNull(exec.groups[groupIndex], getter, setter); err != nil {
						return err
					}
				} else {
					if err = exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		}

	} else {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			if needs[i] {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					mustNotEmpty = true
					if err = exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		}
	}
	if mustNotEmpty {
		exec.ret.setGroupNotEmpty(groupIndex)
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) distinctBatchFill(
	offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	needs, err := exec.distinctHash.batchFill(vectors, offset, groups)
	if err != nil {
		return err
	}

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if needs[0] && groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.ret.setGroupNotEmpty(groupIdx)
						if err = exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}

		value := vector.MustFixedCol[from](vec)[0]
		for i := 0; i < len(groups); i++ {
			if needs[i] && groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.ret.setGroupNotEmpty(groupIdx)
				if err = exec.fill(exec.groups[groupIdx], value, getter, setter); err != nil {
					return err
				}
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.receiveNull {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if needs[idx] && groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i)
				groupIdx := int(groups[idx] - 1)
				exec.ret.groupToSet = groupIdx
				exec.ret.setGroupNotEmpty(groupIdx)
				if null {
					if err = exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
						return err
					}
				} else {
					if err = exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
				}
			}
			idx++
		}

	} else {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if needs[idx] && groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.ret.setGroupNotEmpty(groupIdx)
					if err = exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
				}
			}
			idx++
		}
	}

	return nil
}

func (exec *singleAggFuncExec2[from]) distinctBulkFill(
	groupIndex int, vectors []*vector.Vector, length int) error {
	exec.ret.groupToSet = groupIndex
	getter := exec.ret.aggGet
	setter := exec.ret.aggSet

	vec := vectors[0]
	if vec.IsConst() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, 0); err != nil || !need {
			return err
		}

		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.ret.setGroupNotEmpty(groupIndex)
				return exec.fillNull(exec.groups[groupIndex], getter, setter)
			}
		} else {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fill(exec.groups[groupIndex], vector.MustFixedCol[from](vec)[0], getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	needs, err := exec.distinctHash.bulkFill(groupIndex, vectors)
	if err != nil {
		return err
	}

	mustNotEmpty := false
	if exec.receiveNull {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			if needs[i] {
				v, null := exec.arg.w.GetValue(i)
				mustNotEmpty = true
				if null {
					if err = exec.fillNull(exec.groups[groupIndex], getter, setter); err != nil {
						return err
					}
				} else {
					if err = exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		}

	} else {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			if needs[i] {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					mustNotEmpty = true
					if err = exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		}
	}
	if mustNotEmpty {
		exec.ret.setGroupNotEmpty(groupIndex)
	}
	return nil
}

func (exec *singleAggFuncExec2[from]) distinctBatchFill(
	offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	needs, err := exec.distinctHash.batchFill(vectors, offset, groups)
	if err != nil {
		return err
	}

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if needs[0] && groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.ret.setGroupNotEmpty(groupIdx)
						if err = exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}

		value := vector.MustFixedCol[from](vec)[0]
		for i := 0; i < len(groups); i++ {
			if needs[i] && groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.ret.setGroupNotEmpty(groupIdx)
				if err = exec.fill(exec.groups[groupIdx], value, getter, setter); err != nil {
					return err
				}
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.receiveNull {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if needs[idx] && groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i)
				groupIdx := int(groups[idx] - 1)
				exec.ret.groupToSet = groupIdx
				exec.ret.setGroupNotEmpty(groupIdx)
				if null {
					if err = exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
						return err
					}
				} else {
					if err = exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
				}
			}
			idx++
		}

	} else {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if needs[idx] && groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.ret.setGroupNotEmpty(groupIdx)
					if err = exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
				}
			}
			idx++
		}
	}

	return nil
}

func (exec *singleAggFuncExec3[to]) distinctBulkFill(
	groupIndex int, vectors []*vector.Vector, length int) error {
	exec.ret.groupToSet = groupIndex
	getter := exec.ret.aggGet
	setter := exec.ret.aggSet

	vec := vectors[0]
	if vec.IsConst() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, 0); err != nil || !need {
			return err
		}

		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.ret.setGroupNotEmpty(groupIndex)
				if err := exec.fillNull(exec.groups[groupIndex], getter, setter); err != nil {
					return err
				}
			}
		} else {
			exec.ret.setGroupNotEmpty(groupIndex)
			if err := exec.fill(exec.groups[groupIndex], vector.MustBytesCol(vec)[0], getter, setter); err != nil {
				return err
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	needs, err := exec.distinctHash.bulkFill(groupIndex, vectors)
	if err != nil {
		return err
	}

	mustNotEmpty := false
	if exec.receiveNull {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			if needs[i] {
				v, null := exec.arg.w.GetStrValue(i)
				mustNotEmpty = true
				if null {
					if err = exec.fillNull(exec.groups[groupIndex], getter, setter); err != nil {
						return err
					}
				} else {
					if err = exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		}

	} else {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			if needs[i] {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					mustNotEmpty = true
					if err = exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		}
	}
	if mustNotEmpty {
		exec.ret.setGroupNotEmpty(groupIndex)
	}
	return nil
}

func (exec *singleAggFuncExec3[to]) distinctBatchFill(
	offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	needs, err := exec.distinctHash.batchFill(vectors, offset, groups)
	if err != nil {
		return err
	}

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if needs[0] && groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.ret.setGroupNotEmpty(groupIdx)
						if err = exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}

		value := vector.MustBytesCol(vec)[0]
		for i := 0; i < len(groups); i++ {
			if needs[i] && groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.ret.setGroupNotEmpty(groupIdx)
				if err = exec.fill(exec.groups[groupIdx], value, getter, setter); err != nil {
					return err
				}
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.receiveNull {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if needs[idx] && groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i)
				groupIdx := int(groups[idx] - 1)
				exec.ret.groupToSet = groupIdx
				exec.ret.setGroupNotEmpty(groupIdx)
				if null {
					if err = exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
						return err
					}
				} else {
					if err = exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
				}
			}
			idx++
		}

	} else {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if needs[idx] && groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.ret.setGroupNotEmpty(groupIdx)
					if err = exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
				}
			}
			idx++
		}
	}

	return nil
}

func (exec *singleAggFuncExec4) distinctBulkFill(
	groupIndex int, vectors []*vector.Vector, length int) error {
	exec.ret.groupToSet = groupIndex
	getter := exec.ret.aggGet
	setter := exec.ret.aggSet

	vec := vectors[0]
	if vec.IsConst() {
		if need, err := exec.distinctHash.fill(groupIndex, vectors, 0); err != nil || !need {
			return err
		}

		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.ret.setGroupNotEmpty(groupIndex)
				return exec.fillNull(exec.groups[groupIndex], getter, setter)
			}
		} else {
			exec.ret.setGroupNotEmpty(groupIndex)
			return exec.fill(exec.groups[groupIndex], vector.MustBytesCol(vec)[0], getter, setter)
		}
		return nil
	}

	exec.arg.prepare(vec)
	needs, err := exec.distinctHash.bulkFill(groupIndex, vectors)
	if err != nil {
		return err
	}

	mustNotEmpty := false
	if exec.receiveNull {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			if needs[i] {
				v, null := exec.arg.w.GetStrValue(i)
				mustNotEmpty = true
				if null {
					if err = exec.fillNull(exec.groups[groupIndex], getter, setter); err != nil {
						return err
					}
				} else {
					if err = exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		}

	} else {
		for i, j := uint64(0), uint64(length); i < j; i++ {
			if needs[i] {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					mustNotEmpty = true
					if err = exec.fill(exec.groups[groupIndex], v, getter, setter); err != nil {
						return err
					}
				}
			}
		}
	}
	if mustNotEmpty {
		exec.ret.setGroupNotEmpty(groupIndex)
	}
	return nil
}

func (exec *singleAggFuncExec4) distinctBatchFill(
	offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	setter := exec.ret.aggSet
	getter := exec.ret.aggGet

	needs, err := exec.distinctHash.batchFill(vectors, offset, groups)
	if err != nil {
		return err
	}

	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if needs[0] && groups[i] != GroupNotMatched {
						groupIdx := int(groups[i] - 1)
						exec.ret.groupToSet = groupIdx
						exec.ret.setGroupNotEmpty(groupIdx)
						if err = exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}

		value := vector.MustBytesCol(vec)[0]
		for i := 0; i < len(groups); i++ {
			if needs[i] && groups[i] != GroupNotMatched {
				groupIdx := int(groups[i] - 1)
				exec.ret.groupToSet = groupIdx
				exec.ret.setGroupNotEmpty(groupIdx)
				if err = exec.fill(exec.groups[groupIdx], value, getter, setter); err != nil {
					return err
				}
			}
		}
		return nil
	}

	exec.arg.prepare(vec)
	if exec.receiveNull {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if needs[idx] && groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i)
				groupIdx := int(groups[idx] - 1)
				exec.ret.groupToSet = groupIdx
				exec.ret.setGroupNotEmpty(groupIdx)
				if null {
					if err = exec.fillNull(exec.groups[groupIdx], getter, setter); err != nil {
						return err
					}
				} else {
					if err = exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
				}
			}
			idx++
		}

	} else {
		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if needs[idx] && groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetStrValue(i)
				if !null {
					groupIdx := int(groups[idx] - 1)
					exec.ret.groupToSet = groupIdx
					exec.ret.setGroupNotEmpty(groupIdx)
					if err = exec.fill(exec.groups[groupIdx], v, getter, setter); err != nil {
						return err
					}
				}
			}
			idx++
		}
	}

	return nil
}
