// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package impl

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	gCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/io"
)

type nonAppendableBlockIndexHolder struct {
	host              data.Block
	zoneMapIndex      *io.BlockZoneMapIndexReader
	staticFilterIndex *io.StaticFilterIndexReader
	schema            *catalog.Schema
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

func NewEmptyNonAppendableBlockIndexHolder() *nonAppendableBlockIndexHolder {
	return &nonAppendableBlockIndexHolder{}
}

func (holder *nonAppendableBlockIndexHolder) InitFromHost(host data.Block, schema *catalog.Schema, bufManager base.INodeManager) error {
	holder.host = host
	holder.schema = schema
	pkIdx := schema.PrimaryKey
	blkFile := host.GetBlockFile()
	idxMetas, err := blkFile.LoadIndexMeta()
	if err != nil {
		return err
	}

	colFile, err := blkFile.OpenColumn(int(pkIdx))
	if err != nil {
		return err
	}
	for _, meta := range idxMetas.Metas {
		internal := meta.InternalIdx
		colFile.GetDataFileStat()
		idxFile, err := colFile.OpenIndexFile(int(internal))
		if err != nil {
			return err
		}
		switch meta.IdxType {
		case common.BlockZoneMapIndex:
			size := idxFile.Stat().Size()
			buf := make([]byte, size)
			_, err = idxFile.Read(buf)
			if err != nil {
				return err
			}
			reader := io.NewBlockZoneMapIndexReader()
			// TODO: refactor id generation
			id := gCommon.ID{
				BlockID:   host.GetID().BlockID,
				SegmentID: uint64(meta.InternalIdx),
				Idx:       meta.ColIdx,
			}
			err = reader.Init(bufManager, idxFile, &id)
			if err != nil {
				return err
			}
			holder.zoneMapIndex = reader
		case common.StaticFilterIndex:
			size := idxFile.Stat().Size()
			buf := make([]byte, size)
			_, err = idxFile.Read(buf)
			if err != nil {
				return err
			}
			reader := io.NewStaticFilterIndexReader()
			// TODO: refactor id generation
			id := gCommon.ID{
				BlockID:   host.GetID().BlockID,
				SegmentID: uint64(meta.InternalIdx),
				Idx:       meta.ColIdx,
			}
			err = reader.Init(bufManager, idxFile, &id)
			if err != nil {
				return err
			}
			holder.staticFilterIndex = reader
		default:
			panic("unsupported index type for block")
		}
	}
	return nil
}

func (holder *nonAppendableBlockIndexHolder) Destroy() (err error) {
	if err = holder.zoneMapIndex.Destroy(); err != nil {
		return err
	}
	if err = holder.staticFilterIndex.Destroy(); err != nil {
		return err
	}
	return nil
}

func (holder *nonAppendableBlockIndexHolder) GetHostBlockId() uint64 {
	return holder.host.GetID().BlockID
}
