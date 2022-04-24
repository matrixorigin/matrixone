package acif

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type IAppendableBlockIndexHolder interface {
	IBlockIndexHolder
	BatchInsert(keys *vector.Vector, start uint32, count int, offset uint32, verify bool) error
	Delete(key interface{}) error
	Search(key interface{}) (uint32, error)
	Upgrade() (INonAppendableBlockIndexHolder, error)
	BatchDedup(keys *vector.Vector) error
}

type INonAppendableBlockIndexHolder interface {
	IBlockIndexHolder
	MayContainsKey(key interface{}) bool
	MayContainsAnyKeys(keys *vector.Vector) (error, *roaring.Bitmap)
}

type IBlockIndexHolder interface {
	GetHostBlockId() uint64
}
