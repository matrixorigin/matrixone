package index

import (
	"errors"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var (
	ErrNotFound  = errors.New("tae index: key not found")
	ErrDuplicate = errors.New("tae index: key duplicate")
	ErrWrongType = errors.New("tae index: wrong type")
)

type SecondaryIndex interface {
	Insert(key any, row uint32) error
	BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify, upsert bool) error
	Update(key any, row uint32) error
	BatchUpdate(keys *vector.Vector, offsets []uint32, start uint32) error
	Delete(key any) error
	Search(key any) (uint32, error)
	Contains(key any) bool
	ContainsAny(keys *vector.Vector, visibility, mask *roaring.Bitmap) bool
	String() string
}
