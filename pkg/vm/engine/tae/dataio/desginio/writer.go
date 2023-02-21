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

package desginio

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type DataWriter struct {
	writer  objectio.Writer
	isSetPK bool
	pk      uint16
}

func NewDataWriter(fs fileservice.FileService, name string) (dataio.Writer, error) {
	writer, err := objectio.NewObjectWriter(name, fs)
	if err != nil {
		return nil, err
	}
	return &DataWriter{
		writer:  writer,
		isSetPK: false,
	}, nil
}

func (w *DataWriter) SetPrimaryKey(idx uint16) {
	w.isSetPK = true
	w.pk = idx
}

func (w *DataWriter) WriteBatch(batch *batch.Batch) (objectio.BlockObject, error) {
	block, err := w.writer.Write(batch)
	if err != nil {
		return nil, err
	}
	for i, vec := range batch.Vecs {
		columnData := containers.NewVectorWithSharedMemory(vec, true)
		zm := index.NewZoneMap(vec.Typ)
		ctx := new(index.KeysCtx)
		ctx.Keys = columnData
		ctx.Count = vec.Length()
		defer ctx.Keys.Close()
		err := zm.BatchUpdate(ctx)
		if err != nil {
			return nil, err
		}
		buf, err := zm.Marshal()
		if err != nil {
			return nil, err
		}
		zoneMap, err := objectio.NewZoneMap(uint16(i), buf)
		if err != nil {
			return nil, err
		}
		w.writer.WriteIndex(block, zoneMap)
		if !w.isSetPK || uint16(i) != w.pk {
			continue
		}
		// create bloomfilter
		sf, err := index.NewBinaryFuseFilter(columnData)
		if err != nil {
			return nil, err
		}
		bf, err := sf.Marshal()
		if err != nil {
			return nil, err
		}
		bloomFilter := objectio.NewBloomFilter(uint16(i), compress.Lz4, bf)
		if err != nil {
			return nil, err
		}
		w.writer.WriteIndex(block, bloomFilter)
	}
	return block, nil
}

func (w *DataWriter) WriteBatchWithOutIndex(batch *batch.Batch) (objectio.BlockObject, error) {
	return w.writer.Write(batch)
}

func (w *DataWriter) WriteBlockAndZoneMap(batch *batch.Batch, idxs []uint16) (objectio.BlockObject, error) {
	block, err := w.writer.Write(batch)
	if err != nil {
		return nil, err
	}
	for _, idx := range idxs {
		var zoneMap objectio.IndexData
		vec := containers.NewVectorWithSharedMemory(batch.Vecs[idx], true)
		zm := index.NewZoneMap(batch.Vecs[idx].Typ)
		ctx := new(index.KeysCtx)
		ctx.Keys = vec
		ctx.Count = batch.Vecs[idx].Length()
		defer ctx.Keys.Close()
		err = zm.BatchUpdate(ctx)
		if err != nil {
			return nil, err
		}
		buf, err := zm.Marshal()
		if err != nil {
			return nil, err
		}
		zoneMap, err = objectio.NewZoneMap(idx, buf)
		if err != nil {
			return nil, err
		}
		w.writer.WriteIndex(block, zoneMap)
	}
	return block, nil
}

func (w *DataWriter) Sync(ctx context.Context) ([]objectio.BlockObject, error) {
	blocks, err := w.writer.WriteEnd(ctx)
	return blocks, err
}
