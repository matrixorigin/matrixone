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
	Keys *vector.Vector

	// Select the key where this bitmap indicates.
	// Nil to select all
	Selects *roaring.Bitmap
	// Select a continous interval [Start, Start+Count) from keys
	Start, Count uint32

	// Whether need to verify Keys
	NeedVerify bool
}

func (ctx *KeysCtx) SelectAll() {
	ctx.Count = uint32(vector.Length(ctx.Keys))
}

type BatchResp struct {
	UpdatedKeys *roaring.Bitmap
	UpdatedRows *roaring.Bitmap
}

type SecondaryIndex interface {
	Insert(key any, row uint32) error
	BatchInsert(keys *KeysCtx, startRow uint32, upsert bool) (resp *BatchResp, err error)
	Update(key any, row uint32) error
	BatchUpdate(keys *vector.Vector, offsets []uint32, start uint32) error
	Delete(key any) (old uint32, err error)
	Search(key any) (uint32, error)
	Contains(key any) bool
	ContainsAny(keysCtx *KeysCtx, rowmask *roaring.Bitmap) bool
	String() string
	Size() int
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
