package indexwrapper

import (
	"io"

	"github.com/RoaringBitmap/roaring"
	movec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func TranslateError(err error) error {
	if err == nil {
		return err
	}
	if err == index.ErrDuplicate {
		return data.ErrDuplicate
	}
	if err == index.ErrNotFound {
		return data.ErrNotFound
	}
	if err == index.ErrWrongType {
		return data.ErrWrongType
	}
	return err
}

type Index interface {
	io.Closer
	Destroy() error

	// Dedup returns wether the specified key is existed
	// If key is existed, return ErrDuplicate
	// If any other unknown error happens, return error
	// If key is not found, return nil
	Dedup(key any) error

	BatchDedup(keys *movec.Vector, rowmask *roaring.Bitmap) (keyselects *roaring.Bitmap, err error)

	// BatchUpsert batch insert the specific keys
	// If any deduplication, it will fetch the old value first, fill the active map with new value, insert the old value into delete map
	// If any other unknown error hanppens, return error
	BatchUpsert(keysCtx *index.KeysCtx, offset uint32, ts uint64) error

	// Delete delete the specific key
	// If the specified key not found in active map, return ErrNotFound
	// If any other error happens, return error
	// Delete the specific key from active map and then insert it into delete map
	Delete(key any, ts uint64) error
	GetActiveRow(key any) (row uint32, err error)
	IsKeyDeleted(key any, ts uint64) (deleted, existed bool)
	HasDeleteFrom(key any, fromTs uint64) bool
	GetMaxDeleteTS() uint64

	ReadFrom(data.Block) error
	WriteTo(data.Block) error
}
