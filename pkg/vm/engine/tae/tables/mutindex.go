package tables

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/basic"
)

type mutableIndex struct {
	art     basic.ARTMap
	zonemap *basic.ZoneMap
}

func newMutableIndex(keyT types.Type) *mutableIndex {
	return &mutableIndex{
		art:     basic.NewSimpleARTMap(keyT, nil),
		zonemap: basic.NewZoneMap(keyT),
	}
}

func (index *mutableIndex) BatchInsert(keys *vector.Vector, start uint32, count uint32, offset uint32, verify bool) (err error) {
	// TODO: consume `count` when needed
	if err = index.zonemap.BatchUpdate(keys, start, -1); err != nil {
		return
	}
	err = index.art.BatchInsert(keys, int(start), int(count), offset, verify)
	return
}

func (index *mutableIndex) Delete(key interface{}) error {
	return index.art.Delete(key)
}

func (index *mutableIndex) Find(key interface{}) (row uint32, err error) {
	exist := index.zonemap.Contains(key)
	// 1. key is definitely not existed
	if !exist {
		err = data.ErrNotFound
		return
	}
	// 2. search art tree for key
	row, err = index.art.Search(key)
	return
}

func (index *mutableIndex) Dedup(any) error { panic("implement me") }
func (index *mutableIndex) BatchDedup(keys *vector.Vector) (visibility *roaring.Bitmap, err error) {
	visibility, exist := index.zonemap.ContainsAny(keys)
	// 1. all keys are definitely not existed
	if !exist {
		return
	}
	exist, err = index.art.ContainsAnyKeys(keys, visibility)
	if err != nil {
		return
	}
	if exist {
		err = data.ErrDuplicate
	}
	return
}

func (index *mutableIndex) Destroy() error {
	return index.Close()
}

func (index *mutableIndex) Close() error {
	index.art = nil
	index.zonemap = nil
	return nil
}
