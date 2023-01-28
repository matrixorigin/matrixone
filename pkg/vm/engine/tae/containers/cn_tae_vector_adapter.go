package containers

import (
	"bytes"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	cnNulls "github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cnVector "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"io"
	"unsafe"
)

type CnTaeVector[T any] struct {
	downstreamVector *cnVector.Vector
	mpool            *mpool.MPool
	nullable         bool
}

func NewTaeVector[T any](typ types.Type, nullable bool, opts ...Options) *CnTaeVector[T] {
	vec := CnTaeVector[T]{
		downstreamVector: cnVector.New(typ),
		nullable:         nullable,
	}

	// mpool
	var alloc *mpool.MPool
	if len(opts) > 0 {
		opt := opts[0]
		alloc = opt.Allocator
	}
	if alloc == nil {
		alloc = common.DefaultAllocator
	}
	vec.mpool = alloc

	return &vec
}

func (vec CnTaeVector[T]) Get(i int) any {
	return cnVector.GetColumn[T](vec.downstreamVector)[i]
}

func (vec CnTaeVector[T]) Length() int {
	return vec.downstreamVector.Length()
}

func (vec CnTaeVector[T]) Close() {
	vec.downstreamVector.Free(vec.mpool)
}

func (vec CnTaeVector[T]) HasNull() bool {
	return vec.downstreamVector.GetNulls().Any()
}

func (vec CnTaeVector[T]) Append(v any) {
	//TODO: is it required --- vec.tryCOW()

	_, isNull := v.(types.Null)
	if isNull {
		_ = vec.downstreamVector.Append(types.DefaultVal[T](), true, vec.mpool)

	} else {
		_ = vec.downstreamVector.Append(v, false, vec.mpool)
	}

}

func (vec CnTaeVector[T]) AppendMany(vs ...any) {

	//TODO: Understand what Copy on Write is doing to roStorage and how is it helping? vec.tryCOW()
	for _, v := range vs {
		vec.Append(v)
	}
}

func (vec CnTaeVector[T]) Nullable() bool {
	return vec.nullable
}

func (vec CnTaeVector[T]) IsView() bool {
	// TODO: What is the use of vector view? Can't find any usage.
	return false
}

func (vec CnTaeVector[T]) GetAllocator() *mpool.MPool {
	return vec.mpool
}

func (vec CnTaeVector[T]) GetView() VectorView {
	// TODO: As of now, can't find any usage. Do we need it?
	panic("implement me")
}

func (vec CnTaeVector[T]) IsNull(i int) bool {
	return vec.downstreamVector.GetNulls() != nil && vec.downstreamVector.GetNulls().Contains(uint64(i))
}

func (vec CnTaeVector[T]) NullMask() *roaring64.Bitmap {
	// TODO: Need to modify this

	Nsp := vec.downstreamVector.GetNulls().Np
	return BitmapToRoaring64Bitmap(Nsp)
}

func BitmapToRoaring64Bitmap(input *bitmap.Bitmap) *roaring64.Bitmap {
	result := roaring64.New()
	itr := input.Iterator()
	for itr.HasNext() {
		result.Add(itr.Next())
	}
	return result
}

func Roaring64ToBitmap(input *roaring64.Bitmap) *bitmap.Bitmap {
	result := bitmap.New(int(input.GetCardinality()))
	itr := input.Iterator()
	for itr.HasNext() {
		result.Add(itr.Next())
	}
	return result
}

func (vec CnTaeVector[T]) GetType() types.Type {
	return vec.downstreamVector.GetType()
}

func (vec CnTaeVector[T]) Allocated() int {
	return vec.downstreamVector.Size()
}

func (vec CnTaeVector[T]) String() string {
	s := ""
	end := 100
	if vec.Length() < end {
		end = vec.Length()
	}

	data := "Vals=["
	for i := 0; i < end; i++ {
		data = fmt.Sprintf("%s %v", data, vec.Get(i))
	}
	if vec.Length() > end {
		s = fmt.Sprintf("%s %s...]", s, data)
	} else {
		s = fmt.Sprintf("%s %s]", s, data)
	}

	return s
}

func (vec CnTaeVector[T]) Extend(src Vector) {
	vec.ExtendWithOffset(src, 0, src.Length())
}

// Issues

func (vec CnTaeVector[T]) ExtendWithOffset(src Vector, srcOff, srcLen int) {

	if srcLen <= 0 {
		return
	}

	if vec.downstreamVector.GetType().IsVarlen() {
		bs := src.Bytes()
		for i := srcOff; i < srcOff+srcLen; i++ {
			vec.Append(any(bs.GetVarValueAt(i)).(T))
		}
		return
	}

	slice := unsafe.Slice((*T)(src.SlicePtr()), srcOff+srcLen)
	for i := srcOff; i < srcOff+srcLen; i++ {
		vec.Append(any(slice[i]).(T))
	}
}

func (vec CnTaeVector[T]) Bytes() *Bytes {
	bs := stl.NewBytesWithTypeSize(-types.VarlenaSize)
	bs.Header = cnVector.GetColumn[types.Varlena](vec.downstreamVector)
	bs.Storage = vec.downstreamVector.GetArea()

	return bs
}

func (vec CnTaeVector[T]) Data() []byte {
	panic("unimpl")
	//TODO: wrong impl
	length := vec.downstreamVector.Length()
	var buffer []byte
	for i := 0; i < length; i++ {
		buffer = append(buffer, vec.downstreamVector.GetBytes(int64(i))...)
	}

	return buffer
}

func (vec CnTaeVector[T]) ResetWithData(bs *Bytes, nulls *roaring64.Bitmap) {
	panic("Unimpl")

	if vec.Nullable() {
		vec.downstreamVector.Nsp = &cnNulls.Nulls{Np: Roaring64ToBitmap(nulls)}
	}
	vec.downstreamVector.Col = bs.Header
	vec.downstreamVector.Read(bs.Storage)
}

// --------------- Yet to Implement

func (vec CnTaeVector[T]) Delete(i int) {
	panic("implement me")

	//vec.downstreamVector.Col = append(vec.slice[:i], vec.slice[i+1:]...)
	//size := len(vec.buf) - stl.Sizeof[T]()
	//vec.buf = vec.buf[:size]
	//
	//nulls := impl.derived.nulls
	//max := nulls.Maximum()
	//if max < uint64(i) {
	//	impl.vecBase.Delete(i)
	//	return
	//} else if max == uint64(i) {
	//	nulls.Remove(uint64(i))
	//	impl.vecBase.Delete(i)
	//	return
	//}
	//
	//return
}

func (vec CnTaeVector[T]) Update(i int, v any) {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Slice() any {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) SlicePtr() unsafe.Pointer {
	//TODO implement me
	return cnVector.GetPtrAt(vec.downstreamVector, 0)
}

func (vec CnTaeVector[T]) DataWindow(offset, length int) []byte {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Capacity() int {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) PPString(num int) string {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Foreach(op ItOp, sels *roaring.Bitmap) error {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) error {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Reset() {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Compact(bitmap *roaring.Bitmap) {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) AppendNoNulls(s any) {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Equals(o Vector) bool {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Window(offset, length int) Vector {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) WriteTo(w io.Writer) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) ReadFrom(r io.Reader) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) ReadFromFile(file common.IVFile, buffer *bytes.Buffer) error {
	//TODO implement me
	panic("implement me")
}
