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
		if i == 0 {
			w.objMetaBuilder.AddRowCnt(vec.Length())
		}
		if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
			continue
		}
		columnData := containers.ToDNVector(vec)
		// update null count and distinct value
		ndv := w.objMetaBuilder.InspectVector(i, columnData)

		// set col distinct value
		block.MustGetColumn(uint16(i)).SetNdv(ndv)
		// Build ZM
		zm := index.NewZM(vec.GetType().Oid, vec.GetType().Scale)
		if err = index.BatchUpdateZM(zm, columnData); err != nil {
			return nil, err
		}
		// Update column meta zonemap
		w.writer.UpdateBlockZM(int(block.GetID()), seqnums[i], zm)
		// update object zonemap
		w.objMetaBuilder.UpdateZm(i, zm)

		if !w.isSetPK || w.pk != uint16(i) {
			continue
		}
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

// WriteBatch write a fixed schema batch, usually not a user table
func (w *BlockWriter) WriteBatchWithOutIndex(batch *batch.Batch) (objectio.BlockObject, error) {
	return w.writer.WriteWithoutSeqnum(batch)
}

func (w *BlockWriter) Sync(ctx context.Context) ([]objectio.BlockObject, objectio.Extent, error) {
	if w.objMetaBuilder != nil {
		cnt, meta := w.objMetaBuilder.Build()
		w.writer.WriteObjectMeta(ctx, cnt, meta)
	}
	blocks, err := w.writer.WriteEnd(ctx)
	if len(blocks) == 0 {
		logutil.Info("[WriteEnd]", common.OperationField(w.nameStr),
			common.OperandField("[Size=0]"), common.OperandField(w.writer.GetSeqnums()))
		return blocks, objectio.Extent{}, err
	}
	logutil.Info("[WriteEnd]",
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
