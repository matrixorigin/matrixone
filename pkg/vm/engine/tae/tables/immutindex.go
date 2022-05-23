package tables

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/io"
)

type immutableIndex struct {
	zonemap *io.BlockZoneMapIndexReader
	filter  *io.StaticFilterIndexReader
}

func (index *immutableIndex) Find(any) (uint32, error) { panic("not supported") }
func (index *immutableIndex) Delete(any) error         { panic("not supported") }
func (index *immutableIndex) BatchInsert(*vector.Vector, uint32, uint32, uint32, bool) error {
	panic("not supported")
}

func (index *immutableIndex) Dedup(key interface{}) (err error) {
	var exist bool
	exist, err = index.zonemap.MayContainsKey(key)
	// 1. check zm has some error. return err
	if err != nil {
		return
	}
	// 2. if not in [min, max], key is definitely not found
	if !exist {
		return
	}
	exist, err = index.filter.MayContainsKey(key)
	// 3. check bloomfilter has some error. return err
	if err != nil {
		return
	}
	if exist {
		err = data.ErrPossibleDuplicate
	}
	return
}

func (index *immutableIndex) BatchDedup(keys *vector.Vector) (visibility *roaring.Bitmap, err error) {
	var exist bool
	exist, visibility, err = index.zonemap.MayContainsAnyKeys(keys)
	// 1. check zonemap has some unknown error. return err
	if err != nil {
		return
	}
	// 2. all keys are not in [min, max]. definitely not
	if !exist {
		return
	}
	exist, visibility, err = index.filter.MayContainsAnyKeys(keys, visibility)
	// 3. check bloomfilter has some unknown error. return err
	if err != nil {
		return
	}
	// 4. all keys were checked. definitely not
	if !exist {
		return
	}
	err = data.ErrPossibleDuplicate
	return
}

func (index *immutableIndex) Close() (err error) {
	// TODO
	return
}

func (index *immutableIndex) Destroy() (err error) {
	if err = index.zonemap.Destroy(); err != nil {
		return
	}
	err = index.filter.Destroy()
	return
}
