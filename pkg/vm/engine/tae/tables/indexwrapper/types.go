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

	Dedup(any) error
	BatchDedup(keys *movec.Vector, invisibility *roaring.Bitmap) (visibility *roaring.Bitmap, err error)
	BatchUpsert(*movec.Vector, uint32, uint32, uint32, uint64) error
	Delete(any, uint64) error
	GetActiveRow(any) (uint32, error)

	IsKeyDeleted(any, uint64) (deleted, existed bool)

	ReadFrom(data.Block) error
	WriteTo(data.Block) error
}
