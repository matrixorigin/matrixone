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

package objectio

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type ObjectReader struct {
	object *Object
	name   string
}

func NewObjectReader(name string, fs fileservice.FileService) (Reader, error) {
	reader := &ObjectReader{
		name:   name,
		object: NewObject(name, fs),
	}
	return reader, nil
}

func (r *ObjectReader) ReadMeta(extent Extent) (*Block, error) {
	var err error
	meta := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 1),
	}
	meta.Entries[0] = fileservice.IOEntry{
		Offset: int(extent.offset),
		Size:   int(extent.Length()),
	}
	err = r.object.fs.Read(context.Background(), meta)
	if err != nil {
		return nil, err
	}
	block := &Block{}
	err = block.UnMarshalMeta(meta.Entries[0].Data)
	if err != nil {
		return nil, err
	}
	return block, err
}

func (r *ObjectReader) Read(extent Extent, idxs []uint16) (*fileservice.IOVector, error) {
	var err error
	block, err := r.ReadMeta(extent)
	if err != nil {
		return nil, err
	}
	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, idx := range idxs {
		col := block.columns[idx]
		entry := fileservice.IOEntry{
			Offset: int(col.GetMeta().location.Offset()),
			Size:   int(col.GetMeta().location.Length()),
		}
		data.Entries = append(data.Entries, entry)
	}
	err = r.object.fs.Read(nil, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *ObjectReader) ReadIndex(extent Extent, idxs []uint16) (*fileservice.IOVector, error) {
	var err error
	block, err := r.ReadMeta(extent)
	if err != nil {
		return nil, err
	}
	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, idx := range idxs {
		col := block.columns[idx]
		entry := fileservice.IOEntry{
			Offset: int(col.GetMeta().bloomFilter.Offset()),
			Size:   int(col.GetMeta().bloomFilter.Length()),
		}
		data.Entries = append(data.Entries, entry)
	}
	err = r.object.fs.Read(nil, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
