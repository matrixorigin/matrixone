package adaptor

import (
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type MemAllocator = stl.MemAllocator
type Options = container.Options
type Bytes = stl.Bytes

type Vector interface {
	IsView() bool
	Nullable() bool
	IsNull(i int) bool
	HasNull() bool
	NullMask() *roaring64.Bitmap

	Data() []byte
	Bytes() *Bytes
	DataWindow(offset, length int) []byte
	Get(i int) any
	Update(i int, v any)
	Delete(i int)
	Append(v any)
	AppendMany(vs ...any)
	Extend(o Vector)
	Compact(deletes *roaring.Bitmap)

	Length() int
	Capacity() int
	Allocated() int
	GetAllocator() stl.MemAllocator
	GetType() types.Type
	String() string
	Window() Vector

	// Marshal() ([]byte, error)
	// Unmarshal(buf []byte) error
	WriteTo(w io.Writer) (int64, error)
	ReadFrom(r io.Reader) (int64, error)

	// ReadVectorFromReader(r io.Reader) (Vector, int64, error)

	Close()
}

type Batch struct {
	Attrs   []string
	Vecs    []Vector
	Deletes *roaring.Bitmap
	nameidx map[string]int
	// refidx  map[int]int
}
