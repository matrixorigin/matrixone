// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexwrapper

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
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

func (idx *mutableIndex) BatchUpsert(keysCtx *index.KeysCtx, offset int, ts uint64) (resp *index.BatchResp, err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if err = idx.zonemap.BatchUpdate(keysCtx); err != nil {
		return
	}
	// logutil.Infof("Pre: %s", idx.art.String())
	// logutil.Infof("Post: %s", idx.art.String())
	resp, err = idx.art.BatchInsert(keysCtx, uint32(offset), true)
	if resp != nil {
		posArr := resp.UpdatedKeys.ToArray()
		rowArr := resp.UpdatedRows.ToArray()
		for i := 0; i < len(posArr); i++ {
			key := keysCtx.Keys.Get(int(posArr[i]))
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

func (idx *mutableIndex) IsKeyDeleted(key any, ts uint64) (deleted bool, existed bool) {
	return idx.deletes.IsKeyDeleted(key, ts)
}

func (idx *mutableIndex) GetMaxDeleteTS() uint64 { return idx.deletes.GetMaxTS() }

func (idx *mutableIndex) RevertUpsert(keys containers.Vector, updatePositions, updateRows *roaring.Bitmap, ts uint64) (err error) {
	defer func() {
		err = TranslateError(err)
	}()

	delOp := func(key any, _ int) error {
		_, err := idx.art.Delete(key)
		return err
	}
	if err = keys.Foreach(delOp, nil); err != nil {
		return
	}
	if updatePositions != nil {
		posArr := updatePositions.ToArray()
		rowArr := updateRows.ToArray()
		for i := 0; i < len(posArr); i++ {
			key := keys.Get(int(posArr[i]))
			idx.deletes.RemoveOne(key, rowArr[i])
			if err = idx.art.Insert(key, rowArr[i]); err != nil {
				return
			}
		}
		idx.deletes.RemoveTs(ts)
	}

	return
}

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

func (idx *mutableIndex) String() string {
	return idx.art.String()
}
func (idx *mutableIndex) Dedup(any) error { panic("implement me") }
func (idx *mutableIndex) BatchDedup(keys containers.Vector, rowmask *roaring.Bitmap) (keyselects *roaring.Bitmap, err error) {
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
