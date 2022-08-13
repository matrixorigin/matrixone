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

package indexwrapper

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/assert"
)

func TestRevert(t *testing.T) {
	vec := containers.MockVector2(types.T_int64.ToType(), 20, 0)
	vec1 := vec.CloneWindow(0, 10)
	vec2 := vec.CloneWindow(8, 10)
	defer vec.Close()
	defer vec1.Close()
	defer vec2.Close()

	idx := NewMutableIndex(vec.GetType())

	ctx := new(index.KeysCtx)
	ctx.Keys = vec1
	ctx.SelectAll()

	//ts1 := uint64(99)
	ts1 := types.NextGlobalTsForTest().Next()
	resp, err := idx.BatchUpsert(ctx, 0, ts1)
	assert.NoError(t, err)
	assert.Nil(t, resp)
	_, err = idx.BatchDedup(vec1, nil)
	assert.Error(t, err)

	//ts2 := uint64(109)
	ts2 := ts1.Next()
	ctx.Keys = vec2
	ctx.SelectAll()
	resp, err = idx.BatchUpsert(ctx, vec1.Length(), ts2)
	assert.NoError(t, err)

	assert.Equal(t, 18, idx.art.Size())
	assert.Equal(t, ts2, idx.deletes.GetMaxTS())

	assert.False(t, idx.HasDeleteFrom(vec1.Get(7), ts2))
	assert.True(t, idx.HasDeleteFrom(vec1.Get(8), ts2))
	assert.True(t, idx.HasDeleteFrom(vec1.Get(9), ts2))
	deleted, existed := idx.IsKeyDeleted(vec1.Get(8), ts2)
	assert.True(t, deleted)
	assert.True(t, existed)
	deleted, existed = idx.IsKeyDeleted(vec1.Get(7), ts2)
	assert.False(t, deleted)
	assert.False(t, existed)

	err = idx.RevertUpsert(vec2, resp.UpdatedKeys, resp.UpdatedRows, ts2)
	assert.NoError(t, err)

	assert.Equal(t, 10, idx.art.Size())
	var zeroV types.TS
	assert.Equal(t, zeroV, idx.deletes.GetMaxTS())
	assert.False(t, idx.HasDeleteFrom(vec1.Get(7), ts2))
	assert.False(t, idx.HasDeleteFrom(vec1.Get(8), ts2))
	assert.False(t, idx.HasDeleteFrom(vec1.Get(9), ts2))
	deleted, existed = idx.IsKeyDeleted(vec1.Get(8), ts2)
	assert.False(t, deleted)
	assert.False(t, existed)
	t.Log(idx.String())
}
