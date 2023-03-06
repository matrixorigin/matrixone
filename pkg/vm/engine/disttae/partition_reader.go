// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
)

type PartitionReader struct {
	typsMap              map[string]types.Type
	inserts              []*batch.Batch
	deletes              map[types.Rowid]uint8
	skipBlocks           map[uint64]uint8
	iter                 partitionStateIter
	sourceBatchNameIndex map[string]int

	// the following attributes are used to support cn2s3
	procMPool       *mpool.MPool
	s3FileService   fileservice.FileService
	s3BlockReader   dataio.Reader
	extendId2s3File map[string]int

	// used to get idx of sepcified col
	colIdxMp        map[string]int
	blockBatch      *BlockBatch
	currentFileName string
}

type BlockBatch struct {
	metas  []string
	idx    int
	length int
}

func (blockBatch *BlockBatch) read() (res string) {
	if blockBatch.idx == blockBatch.length {
		return
	}
	res = blockBatch.metas[blockBatch.idx]
	blockBatch.idx++
	return
}

func (blockBatch *BlockBatch) hasRows() bool {
	return blockBatch.idx < blockBatch.length
}

func (blockBatch *BlockBatch) setBat(bat *batch.Batch) {
	blockBatch.metas = vector.MustStrCols(bat.Vecs[0])
	blockBatch.idx = 0
	blockBatch.length = len(blockBatch.metas)
}

var _ engine.Reader = new(PartitionReader)

func (p *PartitionReader) Close() error {
	p.iter.Close()
	return nil
}

func (p *PartitionReader) getIdxs(colNames []string) (res []uint16) {
	for _, str := range colNames {
		v, ok := p.colIdxMp[str]
		if !ok {
			panic("not existed col in partitionReader")
		}
		res = append(res, uint16(v))
	}
	return
}

func (p *PartitionReader) Read(ctx context.Context, colNames []string, expr *plan.Expr, mp *mpool.MPool) (*batch.Batch, error) {
	if p == nil {
		return nil, nil
	}
	if p.blockBatch == nil {
		p.blockBatch = &BlockBatch{}
	}

	if len(p.inserts) > 0 || p.blockBatch.hasRows() {
		var bat *batch.Batch
		if p.blockBatch.hasRows() || p.inserts[0].Attrs[0] == catalog.BlockMeta_MetaLoc {
			var err error
			//var ivec *fileservice.IOVector
			var bats []*batch.Batch
			// read block
			// These blocks may have been written to s3 before the transaction was committed if the transaction is huge, but note that these blocks are only invisible to other transactions
			if !p.blockBatch.hasRows() {
				p.blockBatch.setBat(p.inserts[0])
				p.inserts = p.inserts[1:]
			}
			metaLoc := p.blockBatch.read()
			name := strings.Split(metaLoc, ":")[0]
			if name != p.currentFileName {
				p.s3BlockReader, err = blockio.NewObjectReader(p.s3FileService, metaLoc)
				p.extendId2s3File[name] = 0
				p.currentFileName = name
				if err != nil {
					return nil, err
				}
			}
			_, _, extent, _, _ := blockio.DecodeLocation(metaLoc)
			for _, name := range colNames {
				if name == catalog.Row_ID {
					return nil, moerr.NewInternalError(ctx, "The current version does not support modifying the data read from s3 within a transaction")
				}
			}
			bats, err = p.s3BlockReader.LoadColumns(context.Background(), p.getIdxs(colNames), []uint32{extent.Id()}, p.procMPool)
			if err != nil {
				return nil, err
			}
			rbat := bats[0]
			rbat.SetAttributes(colNames)
			rbat.Cnt = 1
			rbat.SetZs(rbat.Vecs[0].Length(), p.procMPool)
			return rbat, nil
		} else {
			bat = p.inserts[0].GetSubBatch(colNames)
			p.inserts = p.inserts[1:]
			b := batch.NewWithSize(len(colNames))
			b.SetAttributes(colNames)
			for i, name := range colNames {
				b.Vecs[i] = vector.New(p.typsMap[name])
			}
			if _, err := b.Append(ctx, mp, bat); err != nil {
				return nil, err
			}
			return b, nil
		}
	}

	const maxRows = 8192

	b := batch.NewWithSize(len(colNames))
	b.SetAttributes(colNames)
	for i, name := range colNames {
		b.Vecs[i] = vector.New(p.typsMap[name])
	}
	rows := 0

	appendFuncs := make([]func(*vector.Vector, *vector.Vector, int64) error, len(b.Attrs))
	for i, name := range b.Attrs {
		if name == catalog.Row_ID {
			appendFuncs[i] = vector.GetUnionOneFunction(types.T_Rowid.ToType(), mp)
		} else {
			appendFuncs[i] = vector.GetUnionOneFunction(p.typsMap[name], mp)
		}
	}

	for p.iter.Next() {
		entry := p.iter.Entry()

		if _, ok := p.deletes[entry.RowID]; ok {
			continue
		}

		if p.skipBlocks != nil {
			if _, ok := p.skipBlocks[entry.BlockID]; ok {
				continue
			}
		}

		if p.sourceBatchNameIndex == nil {
			p.sourceBatchNameIndex = make(map[string]int)
			for i, name := range entry.Batch.Attrs {
				p.sourceBatchNameIndex[name] = i
			}
		}

		for i, name := range b.Attrs {
			if name == catalog.Row_ID {
				if err := b.Vecs[i].Append(entry.RowID, false, mp); err != nil {
					return nil, err
				}
			} else {
				appendFuncs[i](
					b.Vecs[i],
					entry.Batch.Vecs[p.sourceBatchNameIndex[name]],
					entry.Offset,
				)
			}
		}

		rows++
		if rows == maxRows {
			break
		}
	}

	if rows > 0 {
		b.SetZs(rows, mp)
	}
	if rows == 0 {
		return nil, nil
	}

	return b, nil
}
