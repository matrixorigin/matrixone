// Copyright 2021 Matrix Origin
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

package compute

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func ForeachApply[T types.FixedSizeT](vs any, offset, length uint32, sels []uint32, op func(any, uint32) error) (err error) {
	vals := vs.([]T)
	vals = vals[offset : offset+length]
	if len(sels) == 0 {
		for i, v := range vals {
			if err = op(v, uint32(i)); err != nil {
				return
			}
		}
	} else {
		for _, idx := range sels {
			v := vals[idx]
			if err = op(v, idx); err != nil {
				return
			}
		}
	}
	return
}
