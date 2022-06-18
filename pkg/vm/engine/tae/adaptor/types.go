package adaptor

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type MemAllocator = stl.MemAllocator
type Options = container.Options

type Vector interface {
	Nullable() bool
	IsNull(i int) bool
	HasNull() bool
	NullMask() *roaring64.Bitmap

	Data() []byte
	Get(i int) any
	Update(i int, v any)
	Delete(i int)
	Append(v any)
	AppendMany(vs ...any)
	Extend(o Vector)

	Length() int
	Capacity() int
	Allocated() int
	GetAllocator() stl.MemAllocator
	GetType() types.Type
	String() string
}

type AttrRef struct {
	Name string
	Id   int
}

type Batch interface {
	AttrsRef() []*AttrRef
	GetVectorByName(name string) Vector
}
