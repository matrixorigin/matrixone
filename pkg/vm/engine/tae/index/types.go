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

type KeysCtx struct {
	Keys         *vector.Vector
	Selects      *roaring.Bitmap
	Start, Count int
	NeedVerify   bool
}

type BatchResp struct {
	UpdatedKeys *roaring.Bitmap
	UpdatedRows *roaring.Bitmap
}

type SecondaryIndex interface {
	Insert(key any, row uint32) error
	// BatchInsert(keys *vector.Vector, start int, count int, offset uint32, verify, upsert bool) (updatedpos, updatedrow *roaring.Bitmap, err error)
	BatchInsert(keys *KeysCtx, startRow uint32, upsert bool) (resp *BatchResp, err error)
	Update(key any, row uint32) error
	BatchUpdate(keys *vector.Vector, offsets []uint32, start uint32) error
	Delete(key any) (old uint32, err error)
	Search(key any) (uint32, error)
	Contains(key any) bool
	ContainsAny(keys *vector.Vector, keyselects, rowmask *roaring.Bitmap) bool
	String() string
}

type MutipleRowsIndex interface {
	Insert(key any, row uint32) error
	DeleteOne(key any, row uint32) (int, error)
	DeleteAll(key any) error
	GetRowsNode(key any) (*RowsNode, bool)
	Contains(key any) bool
	ContainsRow(key any, row uint32) bool
	Size() int
	RowCount(key any) int
}
