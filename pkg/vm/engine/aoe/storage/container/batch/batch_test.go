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

package batch

import (
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	schema1 := metadata.MockSchemaAll(14)
	rows := uint64(64)
	attrs1 := make([]int, 0)
	vecs1 := make([]vector.IVector, 0)
	for _, colDef := range schema1.ColDefs {
		attrs1 = append(attrs1, colDef.Idx)
		vecs1 = append(vecs1, vector.MockVector(colDef.Type, rows))
	}
	_, err := NewBatch(make([]int, 0), make([]vector.IVector, 0))
	assert.NotNil(t, err)
	_, err = NewBatch([]int{1, 1}, make([]vector.IVector, 2))
	assert.NotNil(t, err)
	bat1, err := NewBatch(attrs1, vecs1)
	assert.Nil(t, err)
	assert.Equal(t, len(schema1.ColDefs), len(bat1.GetAttrs()))
	assert.Equal(t, vecs1[len(schema1.ColDefs)-1].Length(), bat1.Length())
	assert.True(t, bat1.IsReadonly())
	_, err = bat1.GetReaderByAttr(-1)
	assert.NotNil(t, err)
	reader1, err := bat1.GetReaderByAttr(0)
	assert.Nil(t, err)
	assert.Equal(t, dbi.StdVec, reader1.GetType())
	_, err = bat1.GetVectorByAttr(-1)
	assert.NotNil(t, err)
	vec1, err := bat1.GetVectorByAttr(13)
	assert.Nil(t, err)
	assert.Equal(t, dbi.StrVec, vec1.GetType())
	closed, err := bat1.IsVectorClosed(0)
	assert.Nil(t, err)
	assert.False(t, closed)
	closed, err = bat1.IsVectorClosed(-1)
	assert.NotNil(t, err)
	err = bat1.CloseVector(-1)
	assert.NotNil(t, err)
	err = bat1.CloseVector(0)
	assert.Nil(t, err)
	_, err = bat1.GetVectorByAttr(0)
	assert.NotNil(t, err)
	err = bat1.CloseVector(0)
	assert.NotNil(t, err)
	err = bat1.Close()
	assert.Nil(t, err)
	err = bat1.Close()
	assert.Nil(t, err)
}
