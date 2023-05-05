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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

type PartitionReader struct {
	typsMap              map[string]types.Type
	inserts              []*batch.Batch
	deletes              map[types.Rowid]uint8
	skipBlocks           map[types.Blockid]uint8
	iter                 logtailreplay.PartitionStateIter
	sourceBatchNameIndex map[string]int

	// the following attributes are used to support cn2s3
	procMPool       *mpool.MPool
	s3FileService   fileservice.FileService
	s3BlockReader   *blockio.BlockReader
	extendId2s3File map[string]int

	// used to get idx of sepcified col
	colIdxMp        map[string]int
	blockBatch      *BlockBatch
	currentFileName string
	deletedBlocks   *deletedBlocks
}

// BlockBatch is used to record the metaLoc info
// for the s3 block written by current txn, it's
// stored in the txn writes
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
	blockBatch.metas = vector.MustStrCol(bat.Vecs[0])
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
		if str == catalog.Row_ID {
			continue
		}
		v, ok := p.colIdxMp[str]
		if !ok {
			panic("not existed col in partitionReader")
		}
		res = append(res, uint16(v))
	}
	return
}

func (p *PartitionReader) Read(ctx context.Context, colNames []string, expr *plan.Expr, mp *mpool.MPool, vp engine.VectorPool) (*batch.Batch, error) {
	if p == nil {
		return nil, nil
	}
	if p.blockBatch == nil {
		p.blockBatch = &BlockBatch{}
	}
	// dumpBatch or compaction will set some batches as nil
	if len(p.inserts) > 0 && (p.inserts[0] == nil || p.inserts[0].Length() == 0) {
		p.inserts = p.inserts[1:]
		return &batch.Batch{}, nil
	}
	if len(p.inserts) > 0 || p.blockBatch.hasRows() {
		var bat *batch.Batch
		if p.blockBatch.hasRows() || p.inserts[0].Attrs[0] == catalog.BlockMeta_MetaLoc { // boyu should handle delete for s3 block
			var err error
			var location objectio.Location
			//var ivec *fileservice.IOVector
			// read block
			// These blocks may have been written to s3 before the transaction was committed if the transaction is huge,
			//  but note that these blocks are only invisible to other transactions
			if !p.blockBatch.hasRows() {
				p.blockBatch.setBat(p.inserts[0])
				p.inserts = p.inserts[1:]
			}
			metaLoc := p.blockBatch.read()
			location, err = blockio.EncodeLocationFromString(metaLoc)
			if err != nil {
				return nil, err
			}
			name := location.Name().String()
			if name != p.currentFileName {
				p.s3BlockReader, err = blockio.NewObjectReader(p.s3FileService, location)
				p.extendId2s3File[name] = 0
				p.currentFileName = name
				if err != nil {
					return nil, err
				}
			}
			bat, err = p.s3BlockReader.LoadColumns(context.Background(), p.getIdxs(colNames), location.ID(), p.procMPool)
			if err != nil {
				return nil, err
			}
			rbat := bat
			for i, vec := range rbat.Vecs {
				rbat.Vecs[i], err = vec.Dup(p.procMPool)
				if err != nil {
					return nil, err
				}
			}
			rbat.SetAttributes(colNames)
			rbat.Cnt = 1
			rbat.SetZs(rbat.Vecs[0].Length(), p.procMPool)

			var hasRowId bool
			// note: if there is rowId colName, it must be the last one
			if colNames[len(colNames)-1] == catalog.Row_ID {
				hasRowId = true
			}
			sid := location.Name().SegmentId()
			blkid := objectio.NewBlockid(&sid, location.Name().Num(), uint16(location.ID()))
			if hasRowId {
				// add rowId col for rbat
				lens := rbat.Length()
				vec := vector.NewVec(types.T_Rowid.ToType())
				for i := 0; i < lens; i++ {
					if err := vector.AppendFixed(vec, *objectio.NewRowid(blkid, uint32(i)), false,
						p.procMPool); err != nil {
						return rbat, err
					}
				}
				rbat.Vecs = append(rbat.Vecs, vec)
			}

			deletes := p.deletedBlocks.getDeletedOffsetsByBlock(string(blkid[:]))
			if len(deletes) != 0 {
				rbat.AntiShrink(deletes)
			}
			logutil.Debug(testutil.OperatorCatchBatch("partition reader[s3]", rbat))
			return rbat, nil
		} else {
			bat = p.inserts[0].GetSubBatch(colNames)
			rowIds := vector.MustFixedCol[types.Rowid](p.inserts[0].Vecs[0])
			p.inserts = p.inserts[1:]
			b := batch.NewWithSize(len(colNames))
			b.SetAttributes(colNames)
			for i, name := range colNames {
				if vp == nil {
					b.Vecs[i] = vector.NewVec(p.typsMap[name])
				} else {
					b.Vecs[i] = vp.GetVector(p.typsMap[name])
				}
			}
			for i, vec := range b.Vecs {
				srcVec := bat.Vecs[i]
				uf := vector.GetUnionOneFunction(*vec.GetType(), mp)
				for j := 0; j < bat.Length(); j++ {
					if _, ok := p.deletes[rowIds[j]]; ok {
						continue
					}
					if err := uf(vec, srcVec, int64(j)); err != nil {
						return nil, err
					}
				}
			}
			b.SetZs(bat.Length(), p.procMPool)
			logutil.Debug(testutil.OperatorCatchBatch("partition reader[workspace]", b))
			return b, nil
		}
	}
	const maxRows = 8192
	b := batch.NewWithSize(len(colNames))
	b.SetAttributes(colNames)
	for i, name := range colNames {
		if vp == nil {
			b.Vecs[i] = vector.NewVec(p.typsMap[name])
		} else {
			b.Vecs[i] = vp.GetVector(p.typsMap[name])
		}
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
				if err := vector.AppendFixed(b.Vecs[i], entry.RowID, false, mp); err != nil {
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
	// XXX I'm not sure `normal` is a good description
	logutil.Debug(testutil.OperatorCatchBatch("partition reader[normal]", b))
	return b, nil
}
