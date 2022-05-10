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
// limitations under the License.

package model

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mock"
	"github.com/stretchr/testify/assert"
)

func TestEval(t *testing.T) {
	view := NewBlockView(123455)
	colTypes := mock.MockColTypes(14)
	rows := uint64(64)
	attrs1 := make([]int, 0)
	vecs1 := make([]vector.IVector, 0)
	for i, colType := range colTypes {
		attrs1 = append(attrs1, i)
		vec := vector.MockVector(colType, rows)
		vec.ResetReadonly()
		vecs1 = append(vecs1, vec)
	}
	bat, err := batch.NewBatch(attrs1, vecs1)
	assert.Nil(t, err)
	view.Raw = bat

	view.UpdateMasks[1] = &roaring.Bitmap{}
	view.UpdateMasks[1].Add(3)
	view.UpdateVals[1] = make(map[uint32]interface{})
	view.UpdateVals[1][3] = int16(7)

	view.UpdateMasks[13] = &roaring.Bitmap{}
	view.UpdateMasks[13].Add(4)
	view.UpdateVals[13] = make(map[uint32]interface{})
	view.UpdateVals[13][4] = []byte("testEval")

	view.Eval()

	vec1, err := view.AppliedIBatch.GetVectorByAttr(1)
	assert.Nil(t, err)
	val, err := vec1.GetValue(3)
	assert.Nil(t, err)
	assert.Equal(t, int16(7), val)
	t.Logf("%v", vec1)

	vec2, err := view.AppliedIBatch.GetVectorByAttr(13)
	assert.Nil(t, err)
	val, err = vec2.GetValue(4)
	assert.Nil(t, err)
	assert.Equal(t, []byte("testEval"), val)
	val, err = vec2.GetValue(5)
	assert.Nil(t, err)
	assert.Equal(t, []byte("str5"), val)
	t.Logf("%v", vec2)
}

func TestMarshal(t *testing.T) {
	view := NewBlockView(123455)

	colTypes := mock.MockColTypes(14)
	rows := uint64(64)
	attrs1 := make([]int, 0)
	vecs1 := make([]vector.IVector, 0)
	for i, colType := range colTypes {
		attrs1 = append(attrs1, i)
		vec := vector.MockVector(colType, rows)
		vec.ResetReadonly()
		vecs1 = append(vecs1, vec)
	}
	bat, err := batch.NewBatch(attrs1, vecs1)
	assert.Nil(t, err)
	view.AppliedIBatch = bat

	view.DeleteMask = &roaring.Bitmap{}
	view.DeleteMask.Add(0)
	view.DeleteMask.Add(3)
	view.DeleteMask.Add(88)

	// _, err = view.Marshal()
	// assert.Nil(t, err)
	buf, err := view.Marshal()
	assert.Nil(t, err)
	view2 := NewBlockView(0)
	view2.Unmarshal(buf)

	assert.Equal(t, uint64(123455), view2.Ts)

	assert.Equal(t, 3, int(view2.DeleteMask.GetCardinality()))
	assert.True(t, view2.DeleteMask.Contains(0))
	assert.True(t, view2.DeleteMask.Contains(3))
	assert.True(t, view2.DeleteMask.Contains(88))

	assert.Equal(t, len(view.AppliedIBatch.GetAttrs()), len(view2.AppliedIBatch.GetAttrs()))
}

func TestMarshal2(t *testing.T) {
	view := NewBlockView(123455)

	view.AppliedIBatch = nil

	view.DeleteMask = &roaring.Bitmap{}
	view.DeleteMask.Add(0)
	view.DeleteMask.Add(3)
	view.DeleteMask.Add(88)

	// _, err = view.Marshal()
	// assert.Nil(t, err)
	buf, err := view.Marshal()
	assert.Nil(t, err)
	view2 := NewBlockView(0)
	view2.Unmarshal(buf)

	assert.Equal(t, uint64(123455), view2.Ts)

	assert.Equal(t, 3, int(view2.DeleteMask.GetCardinality()))
	assert.True(t, view2.DeleteMask.Contains(0))
	assert.True(t, view2.DeleteMask.Contains(3))
	assert.True(t, view2.DeleteMask.Contains(88))

	assert.Nil(t, view2.AppliedIBatch)
}

func TestMarshal3(t *testing.T) {
	view := NewBlockView(123455)

	colTypes := mock.MockColTypes(14)
	rows := uint64(64)
	attrs1 := make([]int, 0)
	vecs1 := make([]vector.IVector, 0)
	for i, colType := range colTypes {
		attrs1 = append(attrs1, i)
		vec := vector.MockVector(colType, rows)
		vec.ResetReadonly()
		vecs1 = append(vecs1, vec)
	}
	bat, err := batch.NewBatch(attrs1, vecs1)
	assert.Nil(t, err)
	view.AppliedIBatch = bat

	// _, err = view.Marshal()
	// assert.Nil(t, err)
	buf, err := view.Marshal()
	assert.Nil(t, err)
	view2 := NewBlockView(0)
	view2.Unmarshal(buf)

	assert.Equal(t, uint64(123455), view2.Ts)

	assert.Equal(t, 0, int(view2.DeleteMask.GetCardinality()))

	assert.Equal(t, len(view.AppliedIBatch.GetAttrs()), len(view2.AppliedIBatch.GetAttrs()))
}
