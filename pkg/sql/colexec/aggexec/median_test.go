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

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

func TestMedianMarshal(t *testing.T) {
	m := hackAggMemoryManager()

	vs := NewVectors[int64](types.T_int64.ToType())
	defer vs.Free(m.mp)
	AppendMultiFixed(vs, 1, false, 262145, m.mp)
	assert.Equal(t, vs.Length(), 262145)
	assert.Equal(t, len(vs.vecs), 2)

	vs2 := NewVectors[int64](types.T_int64.ToType())
	defer vs2.Free(m.mp)
	AppendMultiFixed(vs2, 1, false, 262145, m.mp)
	assert.Equal(t, vs2.Length(), 262145)
	assert.Equal(t, len(vs2.vecs), 2)

	vs.Union(vs2, m.mp)
	assert.Equal(t, vs.Length(), 262145*2)
	assert.Equal(t, len(vs.vecs), 3)

	b, err := vs.MarshalBinary()
	assert.NoError(t, err)
	vs3 := NewEmptyVectors[int64]()
	defer vs3.Free(m.mp)
	err = vs3.Unmarshal(b, types.T_int64.ToType(), m.mp)
	assert.NoError(t, err)
	assert.Equal(t, vs.Length(), vs3.Length())
	assert.Equal(t, vs.vecs[0].Length(), vs3.vecs[0].Length())
}
