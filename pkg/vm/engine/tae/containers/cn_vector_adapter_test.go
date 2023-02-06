// Copyright 2022 Matrix Origin
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

package containers

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestVectorResetWithData(t *testing.T) {
	defer testutils.AfterTest(t)()
	opts := withAllocator(Options{})
	vec := MakeVector(types.T_int8.ToType(), true, opts)
	vec.Append(int8(0))
	vec.Append(int8(1))
	vec.Append(int8(2))
	vec.Append(types.Null{})
	vec.Append(int8(4))

	buffer := new(bytes.Buffer)
	cloned := CloneWithBuffer(vec, buffer)

	t.Log(vec.String())
	t.Log(cloned.String())
	assert.True(t, vec.Equals(cloned))
	assert.Zero(t, cloned.Allocated())

	bs := vec.Bytes()
	buf := buffer.Bytes()
	res := bytes.Compare(bs.Storage, buf[:len(bs.Storage)])
	assert.Zero(t, res)
	res = bytes.Compare(bs.HeaderBuf(), buf[len(bs.Storage):len(bs.HeaderBuf())+len(bs.Storage)])
	assert.Zero(t, res)
}
