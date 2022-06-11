package indexwrapper

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
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

func (idx *mutableIndex) BatchUpsert(keysCtx *index.KeysCtx, offset uint32, ts uint64) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if err = idx.zonemap.BatchUpdate(keysCtx); err != nil {
		return
	}
	// logutil.Infof("Pre: %s", idx.art.String())
	// logutil.Infof("Post: %s", idx.art.String())
	resp, err := idx.art.BatchInsert(keysCtx, offset, true)
	if resp != nil {
		posArr := resp.UpdatedKeys.ToArray()
		rowArr := resp.UpdatedRows.ToArray()
		for i := 0; i < len(posArr); i++ {
			key := compute.GetValue(keysCtx.Keys, posArr[i])
			if err = idx.deletes.LogDeletedKey(key, rowArr[i], ts); err != nil {
				return
			}
		}
	}
	return
}

func (idx *mutableIndex) HasDeleteFrom(key any, fromTs uint64) bool {
	return idx.deletes.HasDeleteFrom(key, fromTs)
}

func (idx *mutableIndex) IsKeyDeleted(key any, ts uint64) (deleted, existed bool) {
	return idx.deletes.IsKeyDeleted(key, ts)
}

func (idx *mutableIndex) GetMaxDeleteTS() uint64 { return idx.deletes.GetMaxTS() }
func (idx *mutableIndex) Delete(key any, ts uint64) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	var old uint32
	if old, err = idx.art.Delete(key); err != nil {
		return
	}
	err = idx.deletes.LogDeletedKey(key, old, ts)
	return
}

func (idx *mutableIndex) GetActiveRow(key any) (row uint32, err error) {
	defer func() {
		err = TranslateError(err)
		// logutil.Infof("[Trace][GetActiveRow] key=%v: err=%v", key, err)
	}()
	exist := idx.zonemap.Contains(key)
	// 1. key is definitely not existed
	if !exist {
		err = data.ErrNotFound
		return
	}
	// 2. search art tree for key
	row, err = idx.art.Search(key)
	err = TranslateError(err)
	return
}

func (idx *mutableIndex) Dedup(any) error { panic("implement me") }
func (idx *mutableIndex) BatchDedup(keys *vector.Vector, rowmask *roaring.Bitmap) (keyselects *roaring.Bitmap, err error) {
	keyselects, exist := idx.zonemap.ContainsAny(keys)
	// 1. all keys are definitely not existed
	if !exist {
		return
	}
	ctx := new(index.KeysCtx)
	ctx.Keys = keys
	ctx.Selects = keyselects
	ctx.SelectAll()
	exist = idx.art.ContainsAny(ctx, rowmask)
	if exist {
		err = data.ErrDuplicate
	}
	return
}

func (idx *mutableIndex) Destroy() error {
	return idx.Close()
}

func (idx *mutableIndex) Close() error {
	idx.art = nil
	idx.zonemap = nil
	return nil
}

func (idx *mutableIndex) ReadFrom(data.Block) error { panic("not supported") }
func (idx *mutableIndex) WriteTo(data.Block) error  { panic("not supported") }
