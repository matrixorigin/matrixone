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
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Nullable represents a nullable value
type Nullable struct {
	IsNull bool
	Value  any
}

// Equal reports whether tow Nullable values are equal
func (n Nullable) Equal(n2 Nullable) bool {
	if n.IsNull || n2.IsNull {
		return false
	}
	bsA, ok := n.Value.([]byte)
	if ok {
		bsB, ok := n2.Value.([]byte)
		if ok {
			return bytes.Equal(bsA, bsB)
		}
		panic(fmt.Sprintf("type not the same: %T %T", n.Value, n2.Value))
	}
	return n.Value == n2.Value
}

// AppendVector append the value to a vector
func (n Nullable) AppendVector(
	vec *vector.Vector, mp *mpool.MPool) error {
	value := n.Value
	str, ok := value.(string)
	if ok {
		value = []byte(str)
	}
	return vector.AppendAny(vec, value, n.IsNull, mp)
}
