package impl

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/io"
)

type nonAppendableBlockIndexHolder struct {
	host              data.Block
	zoneMapIndex      *io.BlockZoneMapIndexReader
	staticFilterIndex *io.StaticFilterIndexReader
}

func (holder *nonAppendableBlockIndexHolder) MayContainsKey(key interface{}) bool {
	var err error
	var exist bool
	exist, err = holder.zoneMapIndex.MayContainsKey(key)
	if err != nil {
		return false
	}
	if !exist {
		return false
	}
	exist, err = holder.staticFilterIndex.MayContainsKey(key)
	if err != nil {
		return false
	}
	if !exist {
		return false
	}
	return true
}

// MayContainsAnyKeys returns nil, nil if no keys is duplicated, otherwise return ErrDuplicate and the indexes of
// duplicated keys in the input vector.
func (holder *nonAppendableBlockIndexHolder) MayContainsAnyKeys(keys *vector.Vector) (error, *roaring.Bitmap) {
	var err error
	var pos *roaring.Bitmap
	var exist bool
	exist, pos, err = holder.zoneMapIndex.MayContainsAnyKeys(keys)
	if err != nil {
		return err, nil
	}
	if !exist {
		return nil, nil
	}
	exist, pos, err = holder.staticFilterIndex.MayContainsAnyKeys(keys, pos)
	if err != nil {
		return err, nil
	}
	if !exist {
		return nil, nil
	}
	return errors.ErrKeyDuplicate, pos
}

func NewNonAppendableBlockIndexHolder() *nonAppendableBlockIndexHolder {
	return nil
}

func (holder *nonAppendableBlockIndexHolder) GetHostBlockId() uint64 {
	return holder.host.GetID()
}
