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

package blockio

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type Reader struct {
	reader objectio.Reader
	block  *blockFile
	fs     *objectio.ObjectFS
}

func NewReader(fs *objectio.ObjectFS, block *blockFile, name string) *Reader {
	reader, err := objectio.NewObjectReader(name, fs.Service)
	if err != nil {
		panic(any(err))
	}
	return &Reader{
		fs:     fs,
		block:  block,
		reader: reader,
	}
}

func (r *Reader) LoadDeletes(id *common.ID) (mask *roaring.Bitmap, err error) {
	/*name := EncodeDeleteName(id, r.fs)
	f, err := r.fs.OpenFile(name, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	size := f.Stat().Size()
	if size == 0 {
		return
	}
	node := mpool.DefaultAllocator.Alloc(size)
	defer mpool.DefaultAllocator.Free(node)
	if _, err = f.Read(node.Buf[:size]); err != nil {
		return
	}
	mask = roaring.New()
	err = mask.UnmarshalBinary(node.Buf[:size])*/
	return
}

func (r *Reader) LoadBlkColumns(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	opts *containers.Options) (bat *containers.Batch, err error) {
	bat = containers.NewBatch()

	metaKey := r.block.getMetaKey()
	for i := range r.block.columns {
		vec := containers.MakeVector(colTypes[i], nullables[i], opts)
		bat.AddVector(colNames[i], vec)
		if metaKey.End() == 0 {
			continue
		}
		col, err := r.block.GetMeta().GetColumn(uint16(i))
		if err != nil {
			return bat, err
		}
		data, err := col.GetData()
		if err != nil {
			return bat, err
		}
		r := bytes.NewBuffer(data.Entries[0].Data)
		if _, err = vec.ReadFrom(r); err != nil {
			return bat, err
		}
		bat.Vecs[i] = vec
	}
	return bat, err
}

func (r *Reader) ReadMeta(extent objectio.Extent) (objectio.BlockObject, error) {
	extents := make([]objectio.Extent, 1)
	extents[0] = extent
	block, err := r.reader.ReadMeta(extents)
	if err != nil {
		return nil, err
	}
	return block[0], err
}
