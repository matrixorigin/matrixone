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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type BlockWriter struct {
	writer         *objectio.ObjectWriter
	objMetaBuilder *ObjectColumnMetasBuilder
	isSetPK        bool
	pk             uint16
	nameStr        string
	name           objectio.ObjectName
}

func NewBlockWriter(fs fileservice.FileService, name string) (*BlockWriter, error) {
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterETL, name, fs)
	if err != nil {
		return nil, err
	}
	return &BlockWriter{
		writer:  writer,
		isSetPK: false,
		nameStr: name,
	}, nil
}

// seqnums is the column's seqnums of the batch written by `WriteBatch`. `WriteBatchWithoutIndex` will ignore the seqnums
func NewBlockWriterNew(fs fileservice.FileService, name objectio.ObjectName, schemaVer uint32, seqnums []uint16) (*BlockWriter, error) {
	writer, err := objectio.NewObjectWriter(name, fs, schemaVer, seqnums)
	if err != nil {
		return nil, err
	}
	return &BlockWriter{
		writer:  writer,
		isSetPK: false,
		nameStr: name.String(),
		name:    name,
	}, nil
}

func (w *BlockWriter) SetPrimaryKey(idx uint16) {
	w.isSetPK = true
	w.pk = idx
}

func (w *BlockWriter) SetAppendable() {
	w.writer.SetAppendable()
}

// WriteBatch write a batch whose schema is decribed by seqnum in NewBlockWriterNew
func (w *BlockWriter) WriteBatch(batch *batch.Batch) (objectio.BlockObject, error) {
	block, err := w.writer.Write(batch)
	if err != nil {
		return nil, err
	}
	if w.objMetaBuilder == nil {
		w.objMetaBuilder = NewObjectColumnMetasBuilder(len(batch.Vecs))
	}
	seqnums := w.writer.GetSeqnums()
	for i, vec := range batch.Vecs {
		isPK := false
		if i == 0 {
			w.objMetaBuilder.AddRowCnt(vec.Length())
		}
		if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
			continue
		}
		if w.isSetPK && w.pk == uint16(i) {
			isPK = true
		}
		columnData := containers.ToTNVector(vec, common.DefaultAllocator)
		// update null count and distinct value
		w.objMetaBuilder.InspectVector(i, columnData, isPK)

		// Build ZM
		zm := index.NewZM(vec.GetType().Oid, vec.GetType().Scale)
		if err = index.BatchUpdateZM(zm, columnData.GetDownstreamVector()); err != nil {
			return nil, err
		}
		index.SetZMSum(zm, columnData.GetDownstreamVector())
		// Update column meta zonemap
		w.writer.UpdateBlockZM(objectio.SchemaData, int(block.GetID()), seqnums[i], zm)
		// update object zonemap
		w.objMetaBuilder.UpdateZm(i, zm)

		if !w.isSetPK || w.pk != uint16(i) {
			continue
		}
		w.objMetaBuilder.AddPKData(columnData)
		bf, err := index.NewBinaryFuseFilter(columnData)
		if err != nil {
			return nil, err
		}
		buf, err := bf.Marshal()
		if err != nil {
			return nil, err
		}

		if err = w.writer.WriteBF(int(block.GetID()), seqnums[i], buf); err != nil {
			return nil, err
		}
	}
	return block, nil
}

func (w *BlockWriter) WriteTombstoneBatch(batch *batch.Batch) (objectio.BlockObject, error) {
	block, err := w.writer.WriteTombstone(batch)
	if err != nil {
		return nil, err
	}
	for i, vec := range batch.Vecs {
		columnData := containers.ToTNVector(vec, common.DefaultAllocator)
		// Build ZM
		zm := index.NewZM(vec.GetType().Oid, vec.GetType().Scale)
		if err = index.BatchUpdateZM(zm, columnData.GetDownstreamVector()); err != nil {
			return nil, err
		}
		index.SetZMSum(zm, columnData.GetDownstreamVector())
		// Update column meta zonemap
		w.writer.UpdateBlockZM(objectio.SchemaTombstone, 0, uint16(i), zm)
	}
	return block, nil
}

func (w *BlockWriter) WriteSubBatch(batch *batch.Batch, dataType objectio.DataMetaType) (objectio.BlockObject, int, error) {
	return w.writer.WriteSubBlock(batch, dataType)
}

func (w *BlockWriter) Sync(ctx context.Context) ([]objectio.BlockObject, objectio.Extent, error) {
	if w.objMetaBuilder != nil {
		if w.isSetPK {
			w.objMetaBuilder.SetPKNdv(w.pk, w.objMetaBuilder.GetTotalRow())
		}
		cnt, meta := w.objMetaBuilder.Build()
		w.writer.WriteObjectMeta(ctx, cnt, meta)
		columnsData := w.objMetaBuilder.GetPKData()
		bf, err := index.NewBinaryFuseFilterByVectors(columnsData)
		if err != nil {
			return nil, nil, err
		}
		buf, err := bf.Marshal()
		if err != nil {
			return nil, nil, err
		}
		w.writer.WriteObjectMetaBF(buf)
	}
	blocks, err := w.writer.WriteEnd(ctx)
	if len(blocks) == 0 {
		logutil.Debug("[WriteEnd]", common.OperationField(w.nameStr),
			common.OperandField("[Size=0]"), common.OperandField(w.writer.GetSeqnums()))
		return blocks, objectio.Extent{}, err
	}

	logutil.Debug("[WriteEnd]",
		common.OperationField(w.String(blocks)),
		common.OperandField(w.writer.GetSeqnums()),
		common.OperandField(w.writer.GetMaxSeqnum()))
	return blocks, blocks[0].BlockHeader().MetaLocation(), err
}

func (w *BlockWriter) GetName() objectio.ObjectName {
	return w.name
}

func (w *BlockWriter) String(
	blocks []objectio.BlockObject) string {
	size, err := GetObjectSizeWithBlocks(blocks)
	if err != nil {
		return fmt.Sprintf("name: %s, err: %s", w.nameStr, err.Error())
	}
	return fmt.Sprintf("name: %s, block count: %d, size: %d",
		w.nameStr,
		len(blocks),
		size,
	)
}
