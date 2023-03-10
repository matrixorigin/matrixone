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
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
)

func TestVectorGobEncoding(t *testing.T) {
	//vec := vector.New(types.Type{
	//	Oid: types.T_char,
	//})
	//vec.Col = &types.Bytes{}

	vec := vector.NewVec(types.T_int16.ToType())

	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(vec)
	assert.Nil(t, err)

	var v vector.Vector
	err = gob.NewDecoder(buf).Decode(&v)
	assert.Nil(t, err)

	ok := (len(vector.MustFixedCol[int16](&v)) == 0)
	assert.True(t, ok)
}
