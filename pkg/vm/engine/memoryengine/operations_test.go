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

package memoryengine

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestVectorGobEncoding(t *testing.T) {
	//vec := vector.New(types.Type{
	//	Oid: types.T_char,
	//})
	//vec.Col = &types.Bytes{}

	vec := vector.New(types.Type{
		Oid: types.T_int16,
	})

	data, err := msgpack.Marshal(vec)
	assert.Nil(t, err)

	var v vector.Vector
	err = msgpack.Unmarshal(data, &v)
	assert.Nil(t, err)

	_, ok := v.Col.([]int16)
	assert.True(t, ok)
}
