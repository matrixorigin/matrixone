// Copyright 2021 - 2022 Matrix Origin
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

package compare

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Compare interface {
	Vector() *vector.Vector
	Set(int, *vector.Vector)
	Compare(int, int, int64, int64) int
	Copy(int, int, int64, int64, *process.Process) error
}

type compare[T any] struct {
	xs        [][]T
	cmp       func(T, T) int
	ns        []*nulls.Nulls
	vs        []*vector.Vector
	cpy       func([]T, []T, int64, int64)
	nullsLast bool
}

type strCompare struct {
	desc      bool
	nullsLast bool
	vs        []*vector.Vector
}

func nullsCompare(n1, n2 *nulls.Nulls, i1, i2 int64, nullsLast bool) int {
	if nulls.Contains(n1, uint64(i1)) {
		if nulls.Contains(n2, uint64(i2)) {
			return 0
		} else {
			if nullsLast {
				return 1
			} else {
				return -1
			}
		}
	} else {
		if nulls.Contains(n2, uint64(i2)) {
			if nullsLast {
				return -1
			} else {
				return 1
			}
		} else {
			return 0
		}
	}
}
