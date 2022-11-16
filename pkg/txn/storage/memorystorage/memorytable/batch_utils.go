// Copyright 2022 Matrix Origin
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

package memorytable

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

type BatchIter func() (tuple []Nullable)

func NewBatchIter(b *batch.Batch) BatchIter {
	i := 0
	iter := func() (tuple []Nullable) {
		for {
			if i >= b.Vecs[0].Length() {
				return
			}
			if i < len(b.Zs) && b.Zs[i] == 0 {
				i++
				continue
			}
			break
		}
		for _, vec := range b.Vecs {
			value := VectorAt(vec, i)
			tuple = append(tuple, value)
		}
		i++
		return
	}
	return iter
}
