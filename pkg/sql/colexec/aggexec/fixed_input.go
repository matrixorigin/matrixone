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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// fixedAggregateInput returns a fixed column's values. A constant input is
// read through row zero (indirect with a nil selection).
func fixedAggregateInput[T any](v *vector.Vector) ([]T, []int64, bool) {
	return vector.MustFixedColNoTypeCheck[T](v), nil, v.IsConst()
}

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
