package vector

import (
	"bytes"
	"github.com/pierrec/lz4"
	"github.com/stretchr/testify/assert"
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"testing"
)

func TestWrapper(t *testing.T) {
	schema := metadata.MockSchemaAll(14)
	capacity := uint64(10000)
	wrappers := make([]*VectorWrapper, 0)
	for i, colDef := range schema.ColDefs {
		vec := MockVector(colDef.Type, capacity)
		rov, err := vec.CopyToVector()
		assert.Nil(t, err)
		wrapper := NewVectorWrapper(rov)
		wrappers = append(wrappers, wrapper)

		// not supported ops
		assert.Panics(t, func() {
			wrapper.PlacementNew(colDef.Type)
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
			assert.Equal(t, uint64(10000*colDef.Type.Size), wrapper.GetMemorySize())
		} else {
			assert.Equal(t, uint64(len(rov.Data)), wrapper.GetMemorySize())
		}

		assert.Nil(t, wrapper.Close())
		cpv, err := wrapper.CopyToVector()
		assert.Nil(t, err)
		assert.Equal(t, 10000, cpv.Length())

		buf, err := wrapper.Marshal()
		assert.Nil(t, err)
		tmpv := NewEmptyWrapper(colDef.Type)
		assert.Nil(t, tmpv.Unmarshal(buf))
		assert.Equal(t, 10000, tmpv.Length())
	}

	v0, err := MockVector(schema.ColDefs[2].Type, capacity).CopyToVector()
	assert.Nil(t, err)
	v1, err := MockVector(schema.ColDefs[12].Type, capacity).CopyToVector()
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
	tmpw := NewEmptyWrapper(schema.ColDefs[12].Type)
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
	wn0 := VectorWrapperConstructor(ff0, false, func(node buf.IMemoryNode) {
		// do nothing
	})

	f_, err = os.Open("/tmp/testwrapper1")
	assert.Nil(t, err)
	nr, err := wn0.ReadFrom(f_)
	assert.Nil(t, err)
	assert.Equal(t, nw, nr)
	assert.Nil(t, f_.Close())

	ff1 := common.MockCompressedFile(nw, osz)
	wn1 := VectorWrapperConstructor(ff1, false, func(node buf.IMemoryNode) {
		// do nothing
	})
	f_, err = os.Open("/tmp/testwrapper1")
	assert.Nil(t, err)
	nr, err = wn1.(*VectorWrapper).ReadWithBuffer(f_, bytes.NewBuffer(make([]byte, 0)), bytes.NewBuffer(make([]byte, 0)))
	assert.Nil(t, err)
	assert.Equal(t, nw, nr)
	assert.Nil(t, f_.Close())
}
