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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type Reader struct {
	reader objectio.Reader
	fs     *objectio.ObjectFS
	key    string
	meta   *Meta
}

func NewReader(fs *objectio.ObjectFS, key string) (*Reader, error) {
	meta, err := DecodeMetaLocToMeta(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(meta.GetKey(), fs.Service)
	if err != nil {
		return nil, err
	}
	return &Reader{
		fs:     fs,
		reader: reader,
		key:    key,
		meta:   meta,
	}, nil
}

func (r *Reader) LoadBlkColumns(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	opts *containers.Options) (*containers.Batch, error) {
	bat := containers.NewBatch()

	block, err := r.ReadMeta(nil)
	if err != nil {
		return nil, err
	}
	for i := range colNames {
		vec := containers.MakeVector(colTypes[i], nullables[i], opts)
		bat.AddVector(colNames[i], vec)
		if r.meta.GetLoc().End() == 0 {
			continue
		}
		col, err := block.GetColumn(uint16(i))
		if err != nil {
			return bat, err
		}
		data, err := col.GetData(nil)
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

func (r *Reader) ReadMeta(m *mpool.MPool) (objectio.BlockObject, error) {
	extents := make([]objectio.Extent, 1)
	extents[0] = r.meta.GetLoc()
	block, err := r.reader.ReadMeta(extents, m)
	if err != nil {
		return nil, err
	}
	return block[0], err
}

func (r *Reader) GetDataObject(idx uint16, m *mpool.MPool) objectio.ColumnObject {
	block, err := r.ReadMeta(m)
	if err != nil {
		panic(any(err))
	}
	if block == nil {
		return nil
	}
	object, err := block.GetColumn(idx)
	if err != nil {
		panic(any(err))
	}
	return object
}
