package indexwrapper

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type mutableIndex struct {
	art     index.SecondaryIndex
	zonemap *index.ZoneMap
}

func NewMutableIndex(keyT types.Type) *mutableIndex {
	return &mutableIndex{
		art:     index.NewSimpleARTMap(keyT),
		zonemap: index.NewZoneMap(keyT),
	}
}

func (index *mutableIndex) BatchInsert(keys *vector.Vector, start uint32, count uint32, offset uint32, verify bool) (err error) {
	// TODO: consume `count` when needed
	if err = index.zonemap.BatchUpdate(keys, start, -1); err != nil {
		return
	}
	// logutil.Infof("Pre: %s", index.art.String())
	err = index.art.BatchInsert(keys, int(start), int(count), offset, verify, true)
	// logutil.Infof("Post: %s", index.art.String())
	return
}

func (index *mutableIndex) Delete(key any) error {
	return index.art.Delete(key)
}

func (index *mutableIndex) Find(key any) (row uint32, err error) {
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
func (index *mutableIndex) BatchDedup(keys *vector.Vector, invisibility *roaring.Bitmap) (visibility *roaring.Bitmap, err error) {
	visibility, exist := index.zonemap.ContainsAny(keys)
	// 1. all keys are definitely not existed
	if !exist {
		return
	}
	exist = index.art.ContainsAny(keys, visibility, invisibility)
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

func (index *mutableIndex) ReadFrom(data.Block) error { panic("not supported") }
func (index *mutableIndex) WriteTo(data.Block) error  { panic("not supported") }
