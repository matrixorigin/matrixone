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
	IsView() bool
	Nullable() bool
	IsNull(i int) bool
	HasNull() bool
	NullMask() *roaring64.Bitmap

	Data() []byte
	DataWindow(offset, length int) []byte
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
	Window() Vector

	Close()
}

type Batch struct {
	AttrName []string
	AttrRef  []int
	Vecs     []Vector
	nameidx  map[string]int
	refidx   map[int]int
}
