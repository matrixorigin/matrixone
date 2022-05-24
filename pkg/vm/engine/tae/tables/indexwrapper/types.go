package indexwrapper

import (
	"io"

	"github.com/RoaringBitmap/roaring"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

type Index interface {
	io.Closer
	Destroy() error

	Dedup(any) error
	BatchDedup(*movec.Vector) (*roaring.Bitmap, error)
	BatchInsert(*movec.Vector, uint32, uint32, uint32, bool) error
	Delete(any) error
	Find(any) (uint32, error)
	ReadFrom(data.Block) error
	WriteTo(data.Block) error
}
