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

package vector

import (
	"bytes"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mock"

	"github.com/pierrec/lz4"
	"github.com/stretchr/testify/assert"
)

func TestWrapper(t *testing.T) {
	colTypes := mock.MockColTypes(14)
	capacity := uint64(10000)
	// wrappers := make([]*VectorWrapper, 0)
	for i, colType := range colTypes {
		vec := MockVector(colType, capacity)
		rov, err := vec.CopyToVector()
		assert.Nil(t, err)
		wrapper := NewVectorWrapper(rov)
		// wrappers = append(wrappers, wrapper)

		// not supported ops
		assert.Panics(t, func() {
			wrapper.PlacementNew(colType)
		})
		assert.Panics(t, func() {
			_, _ = wrapper.SliceReference(0, 1)
		})
		assert.Panics(t, func() {
			wrapper.Free(nil)
		})
		assert.Panics(t, func() {
			wrapper.Clean(nil)
		})
		assert.Panics(t, func() {
			wrapper.SetCol(nil)
		})
		assert.Panics(t, func() {
			_, _ = wrapper.IsNull(0)
		})
		assert.Panics(t, func() {
			wrapper.HasNull()
		})
		assert.Panics(t, func() {
			wrapper.IsReadonly()
		})
		assert.Panics(t, func() {
			wrapper.NullCnt()
		})
		assert.Panics(t, func() {
			wrapper.GetLatestView()
		})
		assert.Panics(t, func() {
			_, _ = wrapper.CopyToVectorWithBuffer(nil, nil)
		})
		assert.Panics(t, func() {
			_, _ = wrapper.AppendVector(nil, 0)
		})
		assert.NotNil(t, wrapper.SetValue(0, nil))
		assert.NotNil(t, wrapper.Append(0, nil))

		_, err = wrapper.GetValue(10000)
		assert.NotNil(t, err)
		_, err = wrapper.GetValue(0)
		assert.Nil(t, err)
		if i < 12 {
			assert.Equal(t, uint64(10000*colType.Size), wrapper.GetMemorySize())
		} else {
			assert.Equal(t, uint64(len(rov.Data)), wrapper.GetMemorySize())
		}

		assert.Nil(t, wrapper.Close())
		cpv, err := wrapper.CopyToVector()
		assert.Nil(t, err)
		assert.Equal(t, 10000, vector.Length(cpv))

		buf, err := wrapper.Marshal()
		assert.Nil(t, err)
		tmpv := NewEmptyWrapper(colType)
		assert.Nil(t, tmpv.Unmarshal(buf))
		assert.Equal(t, 10000, tmpv.Length())
	}

	v0, err := MockVector(colTypes[2], capacity).CopyToVector()
	assert.Nil(t, err)
	v1, err := MockVector(colTypes[12], capacity).CopyToVector()
	assert.Nil(t, err)
	w0 := NewVectorWrapper(v0)
	sv0, _ := v0.Show()
	osz := int64(len(sv0))
	f0 := common.MockCompressedFile(int64(lz4.CompressBlockBound(int(osz))), osz)
	w0.File = f0
	w1 := NewVectorWrapper(v1)
	f1 := common.NewMemFile(int64(len(w1.Data)))
	w1.File = f1
	f, err := os.Create("/tmp/testwrapper")
	assert.Nil(t, err)
	n, err := w1.WriteTo(f)
	assert.Nil(t, f.Close())

	f, err = os.Open("/tmp/testwrapper")
	assert.Nil(t, err)
	tmpw := NewEmptyWrapper(colTypes[12])
	tmpw.File = common.NewMemFile(n)
	na, err := tmpw.ReadFrom(f)
	assert.Equal(t, n, na)
	assert.Nil(t, err)
	assert.Equal(t, tmpw.Length(), w1.Length())
	assert.Nil(t, f.Close())

	f_, err := os.Create("/tmp/testwrapper1")
	assert.Nil(t, err)
	nw, err := w0.WriteTo(f_)
	assert.Nil(t, err)
	assert.Nil(t, f_.Close())

	v0s, err := v0.Show()
	assert.Nil(t, err)
	osz = int64(len(v0s))
	ff0 := common.MockCompressedFile(nw, osz)
	wn0 := VectorWrapperConstructor(ff0, false, func(node base.IMemoryNode) {
		// do nothing
	})

	f_, err = os.Open("/tmp/testwrapper1")
	assert.Nil(t, err)
	nr, err := wn0.ReadFrom(f_)
	assert.Nil(t, err)
	assert.Equal(t, nw, nr)
	assert.Nil(t, f_.Close())

	ff1 := common.MockCompressedFile(nw, osz)
	wn1 := VectorWrapperConstructor(ff1, false, func(node base.IMemoryNode) {
		// do nothing
	})
	f_, err = os.Open("/tmp/testwrapper1")
	assert.Nil(t, err)
	nr, err = wn1.(*VectorWrapper).ReadWithBuffer(f_, bytes.NewBuffer(make([]byte, 0)), bytes.NewBuffer(make([]byte, 0)))
	assert.Nil(t, err)
	assert.Equal(t, nw, nr)
	assert.Nil(t, f_.Close())
}
