package acif

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

type IAppendableBlockIndexHolder interface {
	IBlockIndexHolder
	BatchInsert(keys *vector.Vector, start uint32, count int, offset uint32, verify bool) error
	Delete(key interface{}) error
	Search(key interface{}) (uint32, error)
	//Upgrade() (INonAppendableBlockIndexHolder, error)
	BatchDedup(keys *vector.Vector) error
}

type INonAppendableBlockIndexHolder interface {
	IBlockIndexHolder
	MayContainsKey(key interface{}) bool
	MayContainsAnyKeys(keys *vector.Vector) (error, *roaring.Bitmap)
	InitFromHost(host data.Block, schema *catalog.Schema, bufManager base.INodeManager) error
}

type IBlockIndexHolder interface {
	GetHostBlockId() uint64
	Destroy() error
}
