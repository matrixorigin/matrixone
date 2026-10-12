// Copyright 2026 Matrix Origin
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

package function

import "github.com/matrixorigin/matrixone/pkg/container/types"

// conditionalVectorType 为条件分支选择共同向量类型。维度必须一致，
// 只允许 F32/F64 的无损提升；字符串和无类型 NULL 随向量分支转换。
// 动态宽度不能充当某个已知固定宽度，以免结果元数据承诺不存在的约束。
func conditionalVectorType(inputs []types.Type) (target types.Type, hasVector, ok bool) {
	for _, input := range inputs {
		if !input.Oid.IsArrayRelate() {
			continue
		}
		if !hasVector {
			target, hasVector = input, true
			continue
		}
		if input.Width != target.Width {
			return target, true, false
		}
		if input.Oid != target.Oid {
			if (input.Oid == types.T_array_float32 && target.Oid == types.T_array_float64) ||
				(input.Oid == types.T_array_float64 && target.Oid == types.T_array_float32) {
				target = types.New(types.T_array_float64, target.Width, 0)
			} else {
				return target, true, false
			}
		}
	}
	if hasVector {
		for _, input := range inputs {
			if !input.Oid.IsArrayRelate() && input.Oid != types.T_any && !input.Oid.IsMySQLString() {
				return target, true, false
			}
		}
	}
	return target, hasVector, true
}

func coalesceVectorReturnType(parameters []types.Type) types.Type {
	target, _, _ := conditionalVectorType(parameters)
	return target
}
