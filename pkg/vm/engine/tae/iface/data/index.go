package data

import (
	"io"

	"github.com/RoaringBitmap/roaring"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
)

type Index interface {
	io.Closer
	Destroy() error

	// Dedup(any) error
	// Insert(any) error
	BatchDedup(*movec.Vector) (*roaring.Bitmap, error)
	BatchInsert(*movec.Vector, uint32, uint32, uint32, bool) error
	Delete(any) error
	Find(any) (uint32, error)
}
