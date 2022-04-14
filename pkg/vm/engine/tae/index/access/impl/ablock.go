package impl

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/basic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
)

type appendableBlockIndexHolder struct {
	// TODO: add host, and deal with upgrade logic
	treeIndex basic.ARTMap
	zoneMapIndex *basic.ZoneMap
}

func NewAppendableBlockIndexHolder(pkType types.Type) *appendableBlockIndexHolder {
	holder := new(appendableBlockIndexHolder)
	holder.treeIndex = basic.NewSimpleARTMap(pkType, nil)
	holder.zoneMapIndex = basic.NewZoneMap(pkType, nil)
	return holder
}

func (holder *appendableBlockIndexHolder) BatchInsert(keys *vector.Vector, start uint32, count int, offset uint32, verify bool) error {
	// TODO: consume `count` when needed
	if err := holder.zoneMapIndex.BatchUpdate(keys, start, -1); err != nil {
		return err
	}
	if err := holder.treeIndex.BatchInsert(keys, int(start), count, offset, verify); err != nil {
		return err
	}
	return nil
}

func (holder *appendableBlockIndexHolder) Delete(key interface{}) error {
	return holder.treeIndex.Delete(key)
}

func (holder *appendableBlockIndexHolder) Search(key interface{}) (rowOffset uint32, err error) {
	var exist bool
	if exist, err = holder.zoneMapIndex.MayContainsKey(key); err != nil {
		return 0, err
	}
	if !exist {
		return 0, errors.ErrKeyNotFound
	}
	if rowOffset, err = holder.treeIndex.Search(key); err != nil {
		return 0, err
	}
	return rowOffset, nil
}

func (holder *appendableBlockIndexHolder) BatchDedup(keys *vector.Vector) error {
	var filter *roaring.Bitmap
	var exist bool
	var err error
	exist, filter, err = holder.zoneMapIndex.MayContainsAnyKeys(keys)
	if err != nil {
		return err
	}
	if !exist {
		return nil
	}
	exist, err = holder.treeIndex.ContainsAnyKeys(keys, filter)
	if err != nil {
		return err
	}
	if exist {
		return errors.ErrKeyDuplicate
	}
	return nil
}

