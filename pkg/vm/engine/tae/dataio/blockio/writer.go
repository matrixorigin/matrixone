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
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	indexwrapper2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type BlockWriter struct {
	writer  objectio.Writer
	isSetPK bool
	pk      uint16
}

func NewBlockWriter(fs fileservice.FileService, name string) (dataio.Writer, error) {
	writer, err := objectio.NewObjectWriter(name, fs)
	if err != nil {
		return nil, err
	}
	return &BlockWriter{
		writer:  writer,
		isSetPK: false,
	}, nil
}

func (w *BlockWriter) SetPrimaryKey(idx uint16) {
	w.isSetPK = true
	w.pk = idx
}

func (w *BlockWriter) WriteBlock(columns *containers.Batch) (block objectio.BlockObject, err error) {
	bat := batch.New(true, columns.Attrs)
	bat.Vecs = containers.UnmarshalToMoVecs(columns.Vecs)
	block, err = w.writer.Write(bat)
	return
}

func (w *BlockWriter) WriteBatch(batch *batch.Batch) (objectio.BlockObject, error) {
	block, err := w.writer.Write(batch)
	if err != nil {
		return nil, err
	}
	for i, vec := range batch.Vecs {
		columnData := containers.NewVectorWithSharedMemory(vec, true)
		zmPos := 0
		zoneMapWriter := indexwrapper2.NewZMWriter()
		if err = zoneMapWriter.Init(w.writer, block, common.Plain, uint16(i), uint16(zmPos)); err != nil {
			return nil, err
		}
		err = zoneMapWriter.AddValues(columnData)
		if err != nil {
			return nil, err
		}
		_, err = zoneMapWriter.Finalize()
		if err != nil {
			return nil, err
		}
		if !w.isSetPK {
			continue
		}
		bfPos := 1
		bfWriter := indexwrapper2.NewBFWriter()
		if err = bfWriter.Init(w.writer, block, common.Plain, uint16(i), uint16(bfPos)); err != nil {
			return nil, err
		}
		if err = bfWriter.AddValues(columnData); err != nil {
			return nil, err
		}
		_, err = bfWriter.Finalize()
		if err != nil {
			return nil, err
		}
	}
	return block, nil
}

func (w *BlockWriter) WriteBatchWithOutIndex(batch *batch.Batch) (objectio.BlockObject, error) {
	return w.writer.Write(batch)
}

func (w *BlockWriter) Sync(ctx context.Context) ([]objectio.BlockObject, objectio.Extent, error) {
	blocks, err := w.writer.WriteEnd(ctx)
	return blocks, blocks[0].GetExtent(), err
}

func (w *BlockWriter) WriteBlockAndZoneMap(batch *batch.Batch, idxs []uint16) (objectio.BlockObject, error) {
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
