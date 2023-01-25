package containers

import (
	"bytes"
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cnVector "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	vec.tryCOW()

	_, isNull := v.(types.Null)
	if isNull {
		_ = vec.downstreamVector.Append(types.DefaultVal[T](), true, vec.mpool)

	} else {
		_ = vec.downstreamVector.Append(v, false, vec.mpool)
	}

}

func (vec CnTaeVector[T]) AppendMany(vs ...any) {
	//TODO: Understand what Copy on Write is doing to roStorage and how is it helping?
	vec.tryCOW()
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

	result := roaring64.New()

	itr := vec.downstreamVector.GetNulls().Np.Iterator()
	for itr.HasNext() {
		result.Add(itr.Next())
	}

	return result

}

func (vec CnTaeVector[T]) GetType() types.Type {
	return vec.downstreamVector.GetType()
}

// Issue

func (vec CnTaeVector[T]) String() string {
	return ""
	//TODO: some issue
	//return vec.downstreamVector.String()
}

func (vec CnTaeVector[T]) Extend(src Vector) {
	srcVec, _ := src.(CnTaeVector[T])
	//TODO: Issue
	err := cnVector.Copy(vec.downstreamVector, srcVec.downstreamVector, 0, 0, vec.mpool)
	if err != nil {
		return
	}
}

// --------------- Yet to Implement

func (vec CnTaeVector[T]) Update(i int, v any) {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Data() []byte {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Bytes() *Bytes {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Slice() any {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) SlicePtr() unsafe.Pointer {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) DataWindow(offset, length int) []byte {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Capacity() int {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Allocated() int {
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

func (vec CnTaeVector[T]) ResetWithData(bs *Bytes, nulls *roaring64.Bitmap) {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Delete(i int) {
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

func (vec CnTaeVector[T]) ExtendWithOffset(src Vector, srcOff, srcLen int) {
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

func (vec CnTaeVector[T]) tryCOW() {

}
