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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type PartitionReader struct {
	end         bool
	typsMap     map[string]types.Type
	firstCalled bool
	readTime    memtable.Time
	tx          *memtable.Transaction
	inserts     []*batch.Batch
	deletes     map[types.Rowid]uint8
	skipBlocks  map[uint64]uint8
	iter        partitionIter
	proc        *process.Process

	// the following attributes are used to support cn2s3
	s3FileService   fileservice.FileService
	s3BlockReader   objectio.Reader
	extendId2s3File map[string]int
	// used to get idx of sepcified col
	colIdxMp        map[string]int
	blockBatch      *BlockBatch
	currentFileName string
}

type partitionIter interface {
	First() bool
	Next() bool
	Close() error
	Read() (RowID, DataValue, error)
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
	if p.end {
		return nil, nil
	}
	if p.blockBatch == nil {
		p.blockBatch = &BlockBatch{}
	}

	if len(p.inserts) > 0 || p.blockBatch.hasRows() {
		var bat *batch.Batch
		if p.blockBatch.hasRows() || p.inserts[0].Attrs[0] == catalog.BlockMeta_MetaLoc {
			var err error
			var ivec *fileservice.IOVector
			// read block
			// These blocks may have been written to s3 before the transaction was committed if the transaction is huge, but note that these blocks are only invisible to other transactions
			if !p.blockBatch.hasRows() {
				p.blockBatch.setBat(p.inserts[0])
				p.inserts = p.inserts[1:]
			}
			metaLoc := p.blockBatch.read()
			name := strings.Split(metaLoc, ":")[0]
			if name != p.currentFileName {
				p.s3BlockReader, err = objectio.NewObjectReader(name, p.s3FileService)
				p.extendId2s3File[name] = 0
				p.currentFileName = name
				if err != nil {
					return nil, err
				}
			}
			_, extent, _ := blockio.DecodeMetaLoc(metaLoc)
			for _, name := range colNames {
				if name == catalog.Row_ID {
					return nil, moerr.NewInternalError(ctx, "The current version does not support modifying the data read from s3 within a transaction")
				}
			}
			ivec, err = p.s3BlockReader.Read(ctx, extent, p.getIdxs(colNames), p.proc.GetMPool())
			if err != nil {
				return nil, err
			}
			rbat := batch.NewWithSize(len(colNames))
			rbat.SetAttributes(colNames)
			rbat.Cnt = 1
			for i, e := range ivec.Entries {
				rbat.Vecs[i] = vector.New(p.typsMap[colNames[i]])
				if err = rbat.Vecs[i].Read(e.Object.([]byte)); err != nil {
					return nil, err
				}
			}
			rbat.SetZs(rbat.Vecs[0].Length(), p.proc.GetMPool())
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

	b := batch.NewWithSize(len(colNames))
	b.SetAttributes(colNames)
	for i, name := range colNames {
		b.Vecs[i] = vector.New(p.typsMap[name])
	}
	rows := 0

	fn := p.iter.Next
	if !p.firstCalled {
		fn = p.iter.First
		p.firstCalled = true
	}

	maxRows := 8192 // i think 8192 is better than 4096
	for ok := fn(); ok; ok = p.iter.Next() {
		dataKey, dataValue, err := p.iter.Read()
		if err != nil {
			return nil, err
		}

		if _, ok := p.deletes[types.Rowid(dataKey)]; ok {
			continue
		}

		if dataValue.op == opDelete {
			continue
		}

		if p.skipBlocks != nil {
			if _, ok := p.skipBlocks[rowIDToBlockID(dataKey)]; ok {
				continue
			}
		}

		for i, name := range b.Attrs {
			if name == catalog.Row_ID {
				if err := b.Vecs[i].Append(types.Rowid(dataKey), false, mp); err != nil {
					return nil, err
				}
				continue
			}
			value, ok := dataValue.value[name]
			if !ok {
				panic(fmt.Sprintf("invalid column name: %v", name))
			}
			if err := value.AppendVector(b.Vecs[i], mp); err != nil {
				return nil, err
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
