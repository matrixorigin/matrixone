package containers

import (
	"io"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type vector[T any] struct {
	stlvec stl.Vector[T]
	impl   Vector
	typ    types.Type
	nulls  *roaring64.Bitmap
}

func NewVector[T any](typ types.Type, nullable bool, opts ...*Options) *vector[T] {
	vec := &vector[T]{
		stlvec: containers.NewVector[T](opts...),
		typ:    typ,
	}
	if nullable {
		vec.impl = newNullableVecImpl[T](vec)
	} else {
		vec.impl = newVecImpl[T](vec)
	}
	return vec
}

// func NewEmptyVector[T any](typ types.Type, opts ...*Options) *vector[T] {
// 	vec := new(vector[T])
// 	vec.typ = typ
// 	vec.stlvec = container.NewVector[T](opts...)
// 	return vec
// }

func (vec *vector[T]) Equals(o Vector) bool {
	if vec.Length() != o.Length() {
		return false
	}
	if vec.GetType() != o.GetType() {
		return false
	}
	if vec.Nullable() != o.Nullable() {
		return false
	}
	if vec.HasNull() != o.HasNull() {
		return false
	}
	if vec.HasNull() {
		if !vec.NullMask().Equals(o.NullMask()) {
			return false
		}
	}
	for i := 0; i < vec.Length(); i++ {
		if vec.Get(i) != o.Get(i) {
			return false
		}
	}
	return true
}
func (vec *vector[T]) IsView() bool                { return vec.impl.IsView() }
func (vec *vector[T]) Nullable() bool              { return vec.impl.Nullable() }
func (vec *vector[T]) IsNull(i int) bool           { return vec.impl.IsNull(i) }
func (vec *vector[T]) HasNull() bool               { return vec.impl.HasNull() }
func (vec *vector[T]) NullMask() *roaring64.Bitmap { return vec.impl.NullMask() }
func (vec *vector[T]) Bytes() *Bytes               { return vec.impl.Bytes() }
func (vec *vector[T]) Data() []byte                { return vec.impl.Data() }
func (vec *vector[T]) DataWindow(offset, length int) []byte {
	return vec.impl.DataWindow(offset, length)
}
func (vec *vector[T]) Get(i int) (v any)    { return vec.impl.Get(i) }
func (vec *vector[T]) Update(i int, v any)  { vec.impl.Update(i, v) }
func (vec *vector[T]) Delete(i int)         { vec.impl.Delete(i) }
func (vec *vector[T]) Append(v any)         { vec.impl.Append(v) }
func (vec *vector[T]) AppendMany(vs ...any) { vec.impl.AppendMany(vs...) }
func (vec *vector[T]) Extend(o Vector)      { vec.impl.Extend(o) }
func (vec *vector[T]) Length() int          { return vec.impl.Length() }
func (vec *vector[T]) Capacity() int        { return vec.impl.Capacity() }
func (vec *vector[T]) Allocated() int       { return vec.impl.Allocated() }

func (vec *vector[T]) GetAllocator() MemAllocator { return vec.stlvec.GetAllocator() }
func (vec *vector[T]) GetType() types.Type        { return vec.typ }
func (vec *vector[T]) String() string             { return vec.impl.String() }
func (vec *vector[T]) Close()                     { vec.impl.Close() }

func (vec *vector[T]) Window() Vector { return nil }

func (vec *vector[T]) Compact(deletes *roaring.Bitmap) {
	if deletes == nil || deletes.IsEmpty() {
		return
	}
	arr := deletes.ToArray()
	for i := len(arr) - 1; i >= 0; i-- {
		vec.Delete(int(arr[i]))
	}
}

func (vec *vector[T]) WriteTo(w io.Writer) (n int64, err error) {
	var nr int
	var tmpn int64
	// 1. Vector type
	if nr, err = w.Write(types.EncodeType(vec.GetType())); err != nil {
		return
	}
	n += int64(nr)
	// 2. Nullable
	if nr, err = w.Write(types.EncodeFixed(vec.Nullable())); err != nil {
		return
	}
	n += int64(nr)
	// 3. Vector data
	if tmpn, err = vec.stlvec.WriteTo(w); err != nil {
		return
	}
	n += tmpn
	if !vec.Nullable() {
		return
	}
	// 4. Nulls
	var nullBuf []byte
	if vec.nulls != nil {
		if nullBuf, err = vec.nulls.ToBytes(); err != nil {
			return
		}
	}
	if nr, err = w.Write(types.EncodeFixed(uint32(len(nullBuf)))); err != nil {
		return
	}
	n += int64(nr)
	if len(nullBuf) == 0 {
		return
	}
	if nr, err = w.Write(nullBuf); err != nil {
		return
	}
	n += int64(nr)

	return
}

func (vec *vector[T]) ReadFrom(r io.Reader) (n int64, err error) {
	var tmpn int64
	// 1. Vector type
	typeBuf := make([]byte, types.TypeSize)
	if _, err = r.Read(typeBuf); err != nil {
		return
	}
	vec.typ = types.DecodeType(typeBuf)
	n += int64(len(typeBuf))

	// 2. Nullable
	oneBuf := make([]byte, 1)
	if _, err = r.Read(oneBuf); err != nil {
		return
	}
	nullable := types.DecodeFixed[bool](oneBuf)
	n += 1

	if nullable {
		vec.impl = newNullableVecImpl(vec)
	} else {
		vec.impl = newVecImpl(vec)
	}

	// 3. Data
	if tmpn, err = vec.stlvec.ReadFrom(r); err != nil {
		return
	}
	n += tmpn

	// 4. Null
	if !nullable {
		return
	}
	fourBuf := make([]byte, int(unsafe.Sizeof(uint32(0))))
	if _, err = r.Read(fourBuf); err != nil {
		return
	}
	n += int64(len(fourBuf))
	nullSize := types.DecodeFixed[uint32](fourBuf)
	if nullSize == 0 {
		return
	}
	vec.nulls = roaring64.New()
	if tmpn, err = vec.nulls.ReadFrom(r); err != nil {
		return
	}
	n += tmpn
	return
}

func (vec *vector[T]) ReadVectorFromReader(r io.Reader) (created Vector, n int64, err error) {
	return
}

func (vec *vector[T]) Foreach(op ItOp, sels *roaring.Bitmap) (err error) {
	return vec.impl.ForeachWindow(0, vec.Length(), op, sels)
}

func (vec *vector[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) (err error) {
	return vec.impl.ForeachWindow(offset, length, op, sels)
}

func (vec *vector[T]) GetView() (view VectorView) {
	return newVecView(vec)
}
