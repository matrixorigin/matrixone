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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// BatchIter iterates over a batch.Batch
type BatchIter func() (tuple []Nullable)

// NewBatchIter create an iter over b
func NewBatchIter(b *batch.Batch) BatchIter {
	i := 0
	length := b.RowCount()
	iter := func() (tuple []Nullable) {
		for attrIdx, vec := range b.Vecs {
			for {
				if i >= length {
					return
				}
				break
			}
			if vec.Length() < length {
				panic(fmt.Sprintf(
					"bad vector length, expecting %d, got %d. vector name: %s",
					length,
					vec.Length(),
					b.Attrs[attrIdx],
				))
			}
			value := VectorAt(vec, i)
			tuple = append(tuple, value)
		}
		i++
		return
	}
	return iter
}
