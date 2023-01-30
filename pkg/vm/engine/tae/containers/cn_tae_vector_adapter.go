package containers

import (
	"bytes"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	cnNulls "github.com/matrixorigin/matrixone/pkg/container/nulls"
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
	vec.downstreamVector.Nsp = &cnNulls.Nulls{Np: bitmap.New(0)}

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
	return GetValue(vec.downstreamVector, uint32(i))
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
	//TODO: Do we need vec.tryCOW()?

	_, isNull := v.(types.Null)
	if isNull {
		_ = vec.downstreamVector.Append(types.DefaultVal[T](), true, vec.mpool)

	} else {
		_ = vec.downstreamVector.Append(v, false, vec.mpool)
	}

}

func (vec CnTaeVector[T]) AppendMany(vs ...any) {
	for _, v := range vs {
		vec.Append(v)
	}
}

func (vec CnTaeVector[T]) Nullable() bool {
	return vec.nullable
}

func (vec CnTaeVector[T]) GetAllocator() *mpool.MPool {
	return vec.mpool
}

func (vec CnTaeVector[T]) IsNull(i int) bool {
	return vec.downstreamVector.GetNulls() != nil && vec.downstreamVector.GetNulls().Contains(uint64(i))
}

func (vec CnTaeVector[T]) NullMask() *roaring64.Bitmap {
	return BitmapToRoaring64Bitmap(vec.downstreamVector.GetNulls().Np)
}

func BitmapToRoaring64Bitmap(input *bitmap.Bitmap) *roaring64.Bitmap {
	var np *roaring64.Bitmap
	if input != nil {
		np = roaring64.New()
		np.AddMany(input.ToArray())
		return np
	}

	return nil
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

// TODO: will be replaced by CN version alone
func (vec CnTaeVector[T]) String() string {
	s := fmt.Sprintf("StrVector:Len=%d[Rows];Cap=%d[Rows];Allocted:%d[Bytes]", vec.Length(), vec.Capacity(), vec.Allocated())

	end := 100
	if vec.Length() < end {
		end = vec.Length()
	}
	if end == 0 {
		return s
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

func (vec CnTaeVector[T]) Update(i int, v any) {
	UpdateValue(vec.downstreamVector, uint32(i), v)
}

func (vec CnTaeVector[T]) Reset() {

	if vec.Length() == 0 {
		return
	}

	cnVector.Reset(vec.downstreamVector)
	vec.downstreamVector.Nsp = nil
}

func (vec CnTaeVector[T]) Slice() any {
	return vec.downstreamVector.Col
}

// TO remove

func (vec CnTaeVector[T]) IsView() bool {
	// TODO: What is the use of vector view? Can't find any usage.
	panic("Soon Deprecated")
}

func (vec CnTaeVector[T]) GetView() VectorView {
	// TODO: As of now, can't find any usage. Do we need it?
	panic("Soon Deprecated")
}

func (vec CnTaeVector[T]) DataWindow(offset, length int) []byte {
	panic("Soon Deprecated")
	//if vec.GetType().IsVarlen() {
	//	panic("not support")
	//}
	//
	//start := offset * stl.Sizeof[T]()
	//end := start + length*stl.Sizeof[T]()
	//return vec.downstreamVector.GetData()[start:end]
}
func (vec CnTaeVector[T]) AppendNoNulls(s any) {
	slice := s.([]T)
	for _, v := range slice {
		vec.Append(any(v).(T))
	}
}

// Ok idea

func (vec CnTaeVector[T]) PPString(num int) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[T=%s][Len=%d][Data=(", vec.GetType().String(), vec.Length()))
	limit := vec.Length()
	if num > 0 && num < limit {
		limit = num
	}
	size := vec.Length()
	long := false
	if size > limit {
		long = true
		size = limit
	}
	for i := 0; i < size; i++ {
		if vec.IsNull(i) {
			_, _ = w.WriteString("null")
			continue
		}
		if vec.GetType().IsVarlen() {
			_, _ = w.WriteString(fmt.Sprintf("%s, ", vec.Get(i).([]byte)))
		} else {
			_, _ = w.WriteString(fmt.Sprintf("%v, ", vec.Get(i)))
		}
	}
	if long {
		_, _ = w.WriteString("...")
	}
	_, _ = w.WriteString(")]")
	return w.String()
}

func (vec CnTaeVector[T]) Equals(o Vector) bool {

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
	mask := vec.NullMask()
	for i := 0; i < vec.Length(); i++ {
		if mask != nil && mask.ContainsInt(i) {
			continue
		}
		var v T
		if _, ok := any(v).([]byte); ok {
			if !bytes.Equal(vec.Get(i).([]byte), o.Get(i).([]byte)) {
				return false
			}
		} else if _, ok := any(v).(types.Decimal64); ok {
			d := vec.Get(i).(types.Decimal64)
			od := vec.Get(i).(types.Decimal64)
			if d.Ne(od) {
				return false
			}
		} else if _, ok := any(v).(types.Decimal128); ok {
			d := vec.Get(i).(types.Decimal128)
			od := vec.Get(i).(types.Decimal128)
			if d.Ne(od) {
				return false
			}
		} else if _, ok := any(v).(types.TS); ok {
			d := vec.Get(i).(types.TS)
			od := vec.Get(i).(types.TS)
			if types.CompareTSTSAligned(d, od) != 0 {
				return false
			}
		} else if _, ok := any(v).(types.Rowid); ok {
			d := vec.Get(i).(types.Rowid)
			od := vec.Get(i).(types.Rowid)
			if types.CompareRowidRowidAligned(d, od) != 0 {
				return false
			}
		} else {
			if vec.Get(i) != o.Get(i) {
				return false
			}
		}
	}
	return true
}

func (vec CnTaeVector[T]) Allocated() int {
	if vec.GetType().IsVarlen() {
		// Only VarLen is allocated using mpool
		return vec.downstreamVector.Size()
	}
	return 0
}

func (vec CnTaeVector[T]) Bytes() *Bytes {
	return MoVecToBytes(vec.downstreamVector)
}

func (vec CnTaeVector[T]) CloneWindow(offset, length int, allocator ...*mpool.MPool) Vector {
	opts := Options{}
	if len(allocator) == 0 {
		opts.Allocator = vec.GetAllocator()
	} else {
		opts.Allocator = allocator[0]
	}
	cloned := NewVector[T](vec.GetType(), vec.Nullable(), opts)

	// Create a window
	window := cnVector.New(vec.GetType())
	cnVector.Window(vec.downstreamVector, offset, offset+length, window)
	cloned.downstreamVector = window

	return cloned
}

func (vec CnTaeVector[T]) Window(offset, length int) Vector {

	window := cnVector.New(vec.GetType())
	cnVector.Window(vec.downstreamVector, offset, offset+length, window)

	return CnTaeVector[T]{
		downstreamVector: window,
		mpool:            vec.GetAllocator(),
		nullable:         vec.nullable,
	}
}

func (vec CnTaeVector[T]) Foreach(op ItOp, sels *roaring.Bitmap) error {
	return vec.ForeachWindow(0, vec.Length(), op, sels)
}

// No Idea

func (vec CnTaeVector[T]) SlicePtr() unsafe.Pointer {
	//panic("Soon Deprecated")
	if vec.GetType().IsVarlen() {
		panic("not support")
	}
	return cnVector.GetPtrAt(vec.downstreamVector, 0)
}

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

func (vec CnTaeVector[T]) Delete(i int) {
	//TODO: Not concurrent
	vec.downstreamVector.Col = append(vec.downstreamVector.Col.([]T)[:i], vec.downstreamVector.Col.([]T)[i+1:]...)

	if !vec.HasNull() {
		vec.downstreamVector.Nsp.Np.Remove(uint64(i))
	}
	// TODO: resize the buffer
	//size := len(vec.downstreamVector.data) - stl.Sizeof[T]()
	//vec.buf = vec.buf[:size]
}

func (vec CnTaeVector[T]) Capacity() int {
	if vec.downstreamVector.Col != nil {
		return cap(vec.downstreamVector.Col.([]T))
	}
	return 0
}

func (vec CnTaeVector[T]) WriteTo(w io.Writer) (n int64, err error) {
	var nr int

	output, _ := vec.downstreamVector.MarshalBinary()
	if nr, err = w.Write(output); err != nil {
		return
	}
	n += int64(nr)

	return
}

func (vec CnTaeVector[T]) ReadFrom(r io.Reader) (n int64, err error) {
	all, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	err = vec.downstreamVector.UnmarshalBinary(all)
	if err != nil {
		return 0, err
	}

	return 0, err
}

// Issues

func (vec CnTaeVector[T]) Data() []byte {
	//TODO: wrong impl
	length := vec.downstreamVector.Length()
	var buffer []byte
	for i := 0; i < length; i++ {
		buffer = append(buffer, vec.downstreamVector.GetBytes(int64(i))...)
	}

	return buffer
}

func (vec CnTaeVector[T]) ResetWithData(bs *Bytes, nulls *roaring64.Bitmap) {

	if vec.Nullable() {
		vec.downstreamVector.Nsp = &cnNulls.Nulls{Np: Roaring64ToBitmap(nulls)}
	}
	vec.downstreamVector.Col = bs.Header
	vec.downstreamVector.Read(bs.Storage)
}

func (vec CnTaeVector[T]) ReadFromFile(f common.IVFile, buffer *bytes.Buffer) (err error) {

	stat := f.Stat()
	var n []byte
	var buf []byte
	var tmpNode []byte
	if stat.CompressAlgo() != compress.None {
		osize := int(stat.OriginSize())
		size := stat.Size()
		tmpNode, err = vec.GetAllocator().Alloc(int(size))
		if err != nil {
			return
		}
		defer vec.GetAllocator().Free(tmpNode)
		srcBuf := tmpNode[:size]
		if _, err = f.Read(srcBuf); err != nil {
			return
		}
		if buffer == nil {
			n, err = vec.GetAllocator().Alloc(osize)
			if err != nil {
				return
			}
			buf = n[:osize]
		} else {
			buffer.Reset()
			if osize > buffer.Cap() {
				buffer.Grow(osize)
			}
			buf = buffer.Bytes()[:osize]
		}
		if _, err = compress.Decompress(srcBuf, buf, compress.Lz4); err != nil {
			if n != nil {
				vec.GetAllocator().Free(n)
			}
			return nil
		}
	}

	err = vec.downstreamVector.UnmarshalBinary(buf)
	return err
}

// --------------- Yet to Implement

func (vec CnTaeVector[T]) ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) error {
	//TODO implement me
	panic("implement me")
}

func (vec CnTaeVector[T]) Compact(bitmap *roaring.Bitmap) {
	//TODO implement me
	panic("implement me")
}
