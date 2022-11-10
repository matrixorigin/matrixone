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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memtable"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type PartitionReader struct {
	end         bool
	typsMap     map[string]types.Type
	firstCalled bool
	readTime    memtable.Time
	tx          *memtable.Transaction
	index       memtable.Tuple
	inserts     []*batch.Batch
	deletes     map[types.Rowid]uint8
	skipBlocks  map[uint64]uint8
	iter        *memtable.TableIter[RowID, DataValue]
	data        *memtable.Table[RowID, DataValue, *DataRow]
}

var _ engine.Reader = new(PartitionReader)

func (p *PartitionReader) Close() error {
	p.iter.Close()
	return nil
}

func (p *PartitionReader) Read(colNames []string, expr *plan.Expr, mp *mpool.MPool) (*batch.Batch, error) {
	if p == nil {
		return nil, nil
	}
	if p.end {
		return nil, nil
	}
	if len(p.inserts) > 0 {
		bat := p.inserts[0].GetSubBatch(colNames)
		p.inserts = p.inserts[1:]
		b := batch.New(false, colNames)
		for i, name := range colNames {
			b.Vecs[i] = vector.New(p.typsMap[name])
		}
		if _, err := b.Append(mp, bat); err != nil {
			return nil, err
		}
		return b, nil
	}
	b := batch.New(false, colNames)
	for i, name := range colNames {
		b.Vecs[i] = vector.New(p.typsMap[name])
	}
	rows := 0
	if len(p.index) > 0 {
		p.iter.Close()
		itr := p.data.NewIndexIter(p.tx, p.index, p.index)
		for ok := itr.First(); ok; ok = itr.Next() {
			entry := itr.Item()
			if _, ok := p.deletes[types.Rowid(entry.Key)]; ok {
				continue
			}
			if p.skipBlocks != nil {
				if _, ok := p.skipBlocks[rowIDToBlockID(entry.Key)]; ok {
					continue
				}
			}
			dataValue, err := p.data.Get(p.tx, entry.Key)
			if err != nil {
				itr.Close()
				p.end = true
				return nil, err
			}
			if dataValue.op == opDelete {
				continue
			}
			for i, name := range b.Attrs {
				if name == catalog.Row_ID {
					b.Vecs[i].Append(types.Rowid(entry.Key), false, mp)
					continue
				}
				value, ok := dataValue.value[name]
				if !ok {
					panic(fmt.Sprintf("invalid column name: %v", name))
				}
				value.AppendVector(b.Vecs[i], mp)
			}
			rows++
		}
		if rows > 0 {
			b.InitZsOne(rows)
			for _, vec := range b.Vecs {
				nulls.TryExpand(vec.GetNulls(), rows)
			}
		}
		itr.Close()
		p.end = true
		if rows == 0 {
			return nil, nil
		}
		return b, nil
	}

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
				b.Vecs[i].Append(types.Rowid(dataKey), false, mp)
				continue
			}
			value, ok := dataValue.value[name]
			if !ok {
				panic(fmt.Sprintf("invalid column name: %v", name))
			}
			value.AppendVector(b.Vecs[i], mp)
		}

		rows++
		if rows == maxRows {
			break
		}
	}

	if rows > 0 {
		b.InitZsOne(rows)
		for _, vec := range b.Vecs {
			nulls.TryExpand(vec.GetNulls(), rows)
		}
	}
	if rows == 0 {
		return nil, nil
	}

	return b, nil
}
