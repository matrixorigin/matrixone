package indexwrapper

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type mutableIndex struct {
	art     index.SecondaryIndex
	zonemap *index.ZoneMap
	deletes *DeletesMap
}

func NewMutableIndex(keyT types.Type) *mutableIndex {
	return &mutableIndex{
		art:     index.NewSimpleARTMap(keyT),
		zonemap: index.NewZoneMap(keyT),
		deletes: NewDeletesMap(keyT),
	}
}

func (index *mutableIndex) BatchUpsert(keys *vector.Vector, start uint32, count uint32, offset uint32, ts uint64) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if err = index.zonemap.BatchUpdate(keys, start, -1); err != nil {
		return
	}
	// logutil.Infof("Pre: %s", index.art.String())
	// logutil.Infof("Post: %s", index.art.String())
	keyspos, rows, err := index.art.BatchInsert(keys, int(start), int(count), offset, false, true)
	if keyspos != nil {
		posArr := keyspos.ToArray()
		rowArr := rows.ToArray()
		for i := 0; i < len(posArr); i++ {
			key := compute.GetValue(keys, posArr[i])
			if err = index.deletes.LogDeletedKey(key, rowArr[i], ts); err != nil {
				return
			}
		}
	}
	return
}

func (index *mutableIndex) IsKeyDeleted(key any, ts uint64) (deleted, existed bool) {
	return index.deletes.IsKeyDeleted(key, ts)
}

func (index *mutableIndex) Delete(key any, ts uint64) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	var old uint32
	if old, err = index.art.Delete(key); err != nil {
		return
	}
	err = index.deletes.LogDeletedKey(key, old, ts)
	return
}

func (index *mutableIndex) GetActiveRow(key any) (row uint32, err error) {
	defer func() {
		err = TranslateError(err)
	}()
	exist := index.zonemap.Contains(key)
	// 1. key is definitely not existed
	if !exist {
		err = data.ErrNotFound
		return
	}
	// 2. search art tree for key
	row, err = index.art.Search(key)
	err = TranslateError(err)
	return
}

func (index *mutableIndex) Dedup(any) error { panic("implement me") }
func (index *mutableIndex) BatchDedup(keys *vector.Vector, rowmask *roaring.Bitmap) (keyselects *roaring.Bitmap, err error) {
	keyselects, exist := index.zonemap.ContainsAny(keys)
	// 1. all keys are definitely not existed
	if !exist {
		return
	}
	exist = index.art.ContainsAny(keys, keyselects, rowmask)
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
