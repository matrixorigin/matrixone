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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/containers"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

func TestBatch1(t *testing.T) {
	vecTypes := types.MockColTypes(4)[2:]
	attrs := []string{"attr1", "attr2"}
	nullable := []bool{false, true}
	opts := new(containers.Options)
	opts.Capacity = 0
	bat := BuildBatch(attrs, vecTypes, nullable, opts)
	bat.Vecs[0].Append(int32(1))
	bat.Vecs[0].Append(int32(2))
	bat.Vecs[0].Append(int32(3))
	bat.Vecs[1].Append(int64(11))
	bat.Vecs[1].Append(int64(12))
	bat.Vecs[1].Append(int64(13))

	assert.Equal(t, 3, bat.Length())
	assert.False(t, bat.HasDelete())
	bat.Delete(1)
	assert.Equal(t, 3, bat.Length())
	assert.True(t, bat.HasDelete())
	assert.True(t, bat.IsDeleted(1))

	w := new(bytes.Buffer)
	_, err := bat.WriteTo(w)
	assert.NoError(t, err)

	r := bytes.NewBuffer(w.Bytes())
	bat2 := NewEmptyBatch()
	_, err = bat2.ReadFrom(r)
	assert.NoError(t, err)
	assert.True(t, bat.Equals(bat2))

	bat.Close()
}

func TestBatch2(t *testing.T) {
	vecTypes := types.MockColTypes(17)
	bat := MockBatch(vecTypes, 10, 3, nil)
	assert.Equal(t, 10, bat.Length())

	cloned := bat.CloneWindow(0, 5)
	assert.Equal(t, 5, cloned.Length())
	t.Log(cloned.Allocated())
	cloned.Close()
	cloned = bat.CloneWindow(0, bat.Length())
	assert.True(t, bat.Equals(cloned))
	cloned.Close()
	bat.Close()
}

func TestBatch3(t *testing.T) {
	vecTypes := types.MockColTypes(17)
	bat := MockBatch(vecTypes, 101, 3, nil)
	defer bat.Close()
	bats := bat.Split(5)
	assert.Equal(t, 5, len(bats))
	row := 0
	for _, b := range bats {
		row += b.Length()
	}
	assert.Equal(t, bat.Length(), row)

	bat2 := MockBatch(vecTypes, 20, 3, nil)
	bats = bat2.Split(2)
	t.Log(bats[0].Vecs[3].Length())
	t.Log(bats[1].Vecs[3].Length())
}
