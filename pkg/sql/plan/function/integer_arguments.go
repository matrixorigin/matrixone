// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import "github.com/matrixorigin/matrixone/pkg/container/types"

// HasIntegerArgument exposes the parameter contract to prepared-plan rebinding.
// It does not change the global implicit conversion cost table.
func HasIntegerArgument(name string, position int) bool {
	id, ok := getFunctionIdByNameWithoutErr(name)
	if !ok || id < 0 || int(id) >= len(allSupportedFunctions) {
		return false
	}
	for _, p := range allSupportedFunctions[id].integerArguments {
		if p == position {
			return true
		}
	}
	return false
}

func (fn FuncNew) checkArgumentTypes(inputs []types.Type, modes []StringDomainCheckMode) checkResult {
	args := inputs
	changed := false
	for _, pos := range fn.integerArguments {
		if pos < 0 || pos >= len(inputs) {
			continue
		}
		target := inputs[pos]
		switch {
		case target.Oid.IsDecimal() || target.Oid.IsSignedInt():
			target = types.T_int64.ToType()
		case target.Oid == types.T_bit || target.Oid.IsUnsignedInt():
			target = types.T_uint64.ToType()
		default:
			continue
		}
		if target.Eq(inputs[pos]) {
			continue
		}
		if !changed {
			args = append([]types.Type(nil), inputs...)
			changed = true
		}
		args[pos] = target
	}
	check := fn.checkFn(fn.Overloads, args)
	if fn.stringDomainCheckFn != nil && len(modes) > 0 {
		check = fn.stringDomainCheckFn(fn.Overloads, args, modes)
	}
	if changed && check.status == succeedMatched {
		return newCheckResultWithCast(check.idx, args)
	}
	return check
}
