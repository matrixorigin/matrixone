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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// fixedAggregateInput keeps the old flat/constant dispatch. Selected values use
// the indirect arm; flat loops keep their existing direct slice indexing.
func fixedAggregateInput[T any](v *vector.Vector) ([]T, []int64, bool) {
	if !v.IsDictionary() {
		return vector.MustFixedColNoTypeCheck[T](v), nil, v.IsConst()
	}
	view := vector.ResolveFixedBytesReadView(v)
	selection := view.Selection
	if view.Constant {
		selection = nil
	}
	return types.DecodeSlice[T](view.Values), selection, true
}

// SupportsDictionaryInput is deliberately opt-in and excludes DISTINCT.
func SupportsDictionaryInput(exec AggFuncExec) bool {
	if exec == nil || exec.IsDistinct() {
		return false
	}
	capability, ok := exec.(interface{ dictionaryInputSupported() bool })
	return ok && capability.dictionaryInputSupported()
}
func (*countStarExec) dictionaryInputSupported() bool         { return true }
func (*countColumnExec) dictionaryInputSupported() bool       { return true }
func (*sumAvgExec[T, A]) dictionaryInputSupported() bool      { return true }
func (*sumAvgDecExec[A, S]) dictionaryInputSupported() bool   { return true }
func (*sumDecimal64FastExec) dictionaryInputSupported() bool  { return true }
func (*sumDecimal128FastExec) dictionaryInputSupported() bool { return true }
func (*minMaxExecFixed[T]) dictionaryInputSupported() bool    { return true }
func (*minMaxExecBytes) dictionaryInputSupported() bool       { return true }

// Preflight callers use logical metadata indices and translate only payload.
func fixedAggregateValueAt[T any](values []T, selection []int64, indirect bool, row int) T {
	if indirect {
		if selection == nil {
			row = 0
		} else {
			row = int(selection[row])
		}
	}
	return values[row]
}
