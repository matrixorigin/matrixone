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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

var _ Index = (*immutableIndex)(nil)

type immutableIndex struct {
	defaultIndexImpl
	zmReader *ZMReader
	bfReader *BFReader
}

func NewImmutableIndex() *immutableIndex {
	return new(immutableIndex)
}

func (index *immutableIndex) Dedup(key any) (err error) {
	exist := index.zmReader.Contains(key)
	// 1. if not in [min, max], key is definitely not found
	if !exist {
		return
	}
	if index.bfReader != nil {
		exist, err = index.bfReader.MayContainsKey(key)
		// 2. check bloomfilter has some error. return err
		if err != nil {
			err = TranslateError(err)
			return
		}
		// 3. all keys were checked. definitely not
		if !exist {
			return
		}
	}

	err = moerr.NewTAEPossibleDuplicate()
	return
}

func (index *immutableIndex) BatchDedup(keys containers.Vector, rowmask *roaring.Bitmap) (keyselects *roaring.Bitmap, err error) {
	keyselects, exist := index.zmReader.ContainsAny(keys)
	// 1. all keys are not in [min, max]. definitely not
	if !exist {
		return
	}
	if index.bfReader != nil {
		exist, keyselects, err = index.bfReader.MayContainsAnyKeys(keys, keyselects)
		// 2. check bloomfilter has some unknown error. return err
		if err != nil {
			err = TranslateError(err)
			return
		}
		// 3. all keys were checked. definitely not
		if !exist {
			return
		}
	}
	err = moerr.NewTAEPossibleDuplicate()
	return
}

func (index *immutableIndex) Close() (err error) {
	// TODO
	return
}

func (index *immutableIndex) Destroy() (err error) {
	if index.zmReader != nil {
		if err = index.zmReader.Destroy(); err != nil {
			return
		}
	}
	if index.bfReader != nil {
		err = index.bfReader.Destroy()
	}
	return
}

func (index *immutableIndex) ReadFrom(blk data.Block, colDef *catalog.ColDef, metas ...IndexMeta) (err error) {
	entry := blk.GetMeta().(*catalog.BlockEntry)
	file := blk.GetBlockFile()
	colFile, err := file.OpenColumn(colDef.Idx)
	if err != nil {
		return
	}
	defer colFile.Close()
	for _, meta := range metas {
		idxFile, err := colFile.OpenIndexFile(int(meta.InternalIdx))
		if err != nil {
			return err
		}
		id := entry.AsCommonID()
		id.PartID = uint32(meta.InternalIdx) + 1000
		id.Idx = meta.ColIdx
		switch meta.IdxType {
		case BlockZoneMapIndex:
			size := idxFile.Stat().Size()
			buf := make([]byte, size)
			if _, err = idxFile.Read(buf); err != nil {
				idxFile.Unref()
				return err
			}
			index.zmReader = NewZMReader(blk.GetBufMgr(), idxFile, id, colDef.Type)
		case StaticFilterIndex:
			size := idxFile.Stat().Size()
			buf := make([]byte, size)
			if _, err = idxFile.Read(buf); err != nil {
				idxFile.Unref()
				return err
			}
			index.bfReader = NewBFReader(blk.GetBufMgr(), idxFile, id)
		default:
			panic("unsupported index type")
		}
	}
	return
}
