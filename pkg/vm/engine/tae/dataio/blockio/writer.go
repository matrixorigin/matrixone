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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type BlockWriter struct {
	writer         objectio.Writer
	objMetaBuilder *ObjectColumnMetasBuilder
	isSetPK        bool
	pk             uint16
	name           string
}

func NewBlockWriter(fs fileservice.FileService, name string) (*BlockWriter, error) {
	writer, err := objectio.NewObjectWriter(name, fs)
	if err != nil {
		return nil, err
	}
	return &BlockWriter{
		writer:  writer,
		isSetPK: false,
		name:    name,
	}, nil
}

func (w *BlockWriter) SetPrimaryKey(idx uint16) {
	w.isSetPK = true
	w.pk = idx
}

func (w *BlockWriter) WriteBlock(columns *containers.Batch) (block objectio.BlockObject, err error) {
	bat := batch.New(true, columns.Attrs)
	bat.Vecs = containers.UnmarshalToMoVecs(columns.Vecs)
	block, err = w.WriteBatch(bat)
	return
}

func (w *BlockWriter) WriteBatch(batch *batch.Batch) (objectio.BlockObject, error) {
	block, err := w.writer.Write(batch)
	if err != nil {
		return nil, err
	}
	if w.objMetaBuilder == nil {
		w.objMetaBuilder = NewObjectColumnMetasBuilder(len(batch.Vecs))
	}
	for i, vec := range batch.Vecs {
		if i == 0 {
			w.objMetaBuilder.AddRowCnt(vec.Length())
		}
		if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
			continue
		}
		columnData := containers.NewVectorWithSharedMemory(vec)
		// update null count and distinct value
		w.objMetaBuilder.InspectVector(i, columnData)
		zmPos := 0
		zoneMapWriter := NewZMWriter(vec.GetType().Oid)
		if err = zoneMapWriter.Init(w.writer, block, common.Plain, uint16(i), uint16(zmPos)); err != nil {
			return nil, err
		}
		err = zoneMapWriter.AddValues(columnData)
		if err != nil {
			return nil, err
		}
		err = zoneMapWriter.Finalize()
		// update zonemap
		w.objMetaBuilder.UpdateZm(i, &zoneMapWriter.zonemap)
		if err != nil {
			return nil, err
		}
		if !w.isSetPK || w.pk != uint16(i) {
			continue
		}
		bfPos := 1
		bfWriter := NewBFWriter()
		if err = bfWriter.Init(w.writer, block, common.Plain, uint16(i), uint16(bfPos)); err != nil {
			return nil, err
		}
		if err = bfWriter.AddValues(columnData); err != nil {
			return nil, err
		}
		err = bfWriter.Finalize()
		if err != nil {
			return nil, err
		}
	}
	return block, nil
}

func (w *BlockWriter) WriteBlockWithOutIndex(columns *containers.Batch) (objectio.BlockObject, error) {
	bat := batch.New(true, columns.Attrs)
	bat.Vecs = containers.UnmarshalToMoVecs(columns.Vecs)
	return w.writer.Write(bat)
}

func (w *BlockWriter) WriteBatchWithOutIndex(batch *batch.Batch) (objectio.BlockObject, error) {
	return w.writer.Write(batch)
}

func (w *BlockWriter) Sync(ctx context.Context) ([]objectio.BlockObject, objectio.Extent, error) {
	if w.objMetaBuilder != nil {
		cnt, meta := w.objMetaBuilder.Build()
		w.writer.WriteObjectMeta(ctx, cnt, meta)
	}
	blocks, err := w.writer.WriteEnd(ctx)
	if len(blocks) == 0 {
		logutil.Info("[WriteEnd]", common.OperationField(w.name),
			common.OperandField("[Size=0]"))
		return blocks, objectio.Extent{}, err
	}
	logutil.Info("[WriteEnd]", common.OperationField(w.String(blocks)))
	return blocks, blocks[0].GetExtent(), err
}

func (w *BlockWriter) String(
	blocks []objectio.BlockObject) string {
	size, err := GetObjectSizeWithBlocks(blocks)
	if err != nil {
		return fmt.Sprintf("name: %s, err: %s", w.name, err.Error())
	}
	return fmt.Sprintf("name: %s, block count: %d, size: %d",
		w.name,
		len(blocks),
		size,
	)
}
