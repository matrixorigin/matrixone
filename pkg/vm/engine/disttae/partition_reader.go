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

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type PartitionReader struct {
	table     *txnTable
	txnOffset int // Transaction writes offset used to specify the starting position for reading data.
	prepared  bool
	// inserted rows comes from txn.writes.
	inserts []*batch.Batch
	//deleted rows comes from txn.writes or partitionState.rows.
	deletes  map[types.Rowid]uint8
	iter     logtailreplay.RowsIter
	seqnumMp map[string]int
	typsMap  map[string]types.Type
}

var _ engine.Reader = new(PartitionReader)

func (p *PartitionReader) SetFilterZM(objectio.ZoneMap) {
}

func (p *PartitionReader) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (p *PartitionReader) SetOrderBy([]*plan.OrderBySpec) {
}

func (p *PartitionReader) Close() error {
	//p.withFilterMixin.reset()
	p.inserts = nil
	p.deletes = nil
	return p.iter.Close()
}

func (p *PartitionReader) prepare() error {
	txn := p.table.getTxn()
	var inserts []*batch.Batch
	var deletes map[types.Rowid]uint8
	//prepare inserts and deletes for partition reader.
	if !txn.readOnly.Load() && !p.prepared {
		inserts = make([]*batch.Batch, 0)
		deletes = make(map[types.Rowid]uint8)
		//load inserts and deletes from txn.writes.

		txnOffset := p.txnOffset
		if p.table.db.op.IsSnapOp() {
			txnOffset = p.table.getTxn().GetSnapshotWriteOffset()
		}

		p.table.getTxn().forEachTableWrites(p.table.db.databaseId, p.table.tableId,
			txnOffset, func(entry Entry) {
				if entry.typ == INSERT || entry.typ == INSERT_TXN {
					if entry.bat == nil || entry.bat.IsEmpty() {
						return
					}
					if entry.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
						return
					}
					inserts = append(inserts, entry.bat)
					return
				}
				//entry.typ == DELETE
				if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
					/*
						CASE:
						create table t1(a int);
						begin;
						truncate t1; //txnDatabase.Truncate will DELETE mo_tables
						show tables; // t1 must be shown
					*/
					if entry.isGeneratedByTruncate() {
						return
					}
					//deletes in txn.Write maybe comes from PartitionState.Rows ,
					// PartitionReader need to skip them.
					vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
					for _, v := range vs {
						deletes[v] = 0
					}
				}
			})
		//deletes maybe comes from PartitionState.rows, PartitionReader need to skip them;
		// so, here only load deletes which don't belong to PartitionState.blks.
		if err := p.table.LoadDeletesForMemBlocksIn(p.table._partState.Load(), deletes); err != nil {
			return err
		}
		p.inserts = inserts
		p.deletes = deletes
		p.prepared = true
	}
	return nil
}

func (p *PartitionReader) Read(
	_ context.Context,
	colNames []string,
	_ *plan.Expr,
	mp *mpool.MPool,
	pool engine.VectorPool) (result *batch.Batch, err error) {
	if p == nil {
		return
	}
	// prepare the data for read.
	if err = p.prepare(); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil && result != nil {
			if pool == nil {
				result.Clean(mp)
			} else {
				pool.PutBatch(result)
			}
		}
	}()
	result = batch.NewWithSize(len(colNames))
	result.SetAttributes(colNames)
	for i, name := range colNames {
		if pool == nil {
			result.Vecs[i] = vector.NewVec(p.typsMap[name])
		} else {
			result.Vecs[i] = pool.GetVector(p.typsMap[name])
		}
	}

	// read batch resided in the memory from txn.writes.
	if len(p.inserts) > 0 {
		bat := p.inserts[0].GetSubBatch(colNames)

		rowIDs := vector.MustFixedCol[types.Rowid](p.inserts[0].Vecs[0])
		p.inserts = p.inserts[1:]

		for i, vec := range result.Vecs {
			uf := vector.GetUnionOneFunction(*vec.GetType(), mp)

			for j, k := int64(0), int64(bat.RowCount()); j < k; j++ {
				if _, ok := p.deletes[rowIDs[j]]; ok {
					continue
				}
				if err = uf(vec, bat.Vecs[i], j); err != nil {
					return
				}
			}
		}

		logutil.Debugf("read %v with %v", colNames, p.seqnumMp)
		//		CORNER CASE:
		//		if some rowIds[j] is in p.deletes above, then some rows has been filtered.
		//		the bat.RowCount() is not always the right value for the result batch b.
		result.SetRowCount(result.Vecs[0].Length())
		if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
			logutil.Debug(testutil.OperatorCatchBatch(
				"partition reader[workspace:memory]",
				result))
		}
		return result, err
	}

	{
		const maxRows = 8192
		rows := 0
		appendFuncs := make([]func(*vector.Vector, *vector.Vector, int64) error, len(result.Attrs))
		for i, name := range result.Attrs {
			if name == catalog.Row_ID {
				appendFuncs[i] = vector.GetUnionOneFunction(types.T_Rowid.ToType(), mp)
			} else {
				appendFuncs[i] = vector.GetUnionOneFunction(p.typsMap[name], mp)
			}
		}
		// read rows from partitionState.rows.
		for p.iter.Next() {
			entry := p.iter.Entry()
			if _, ok := p.deletes[entry.RowID]; ok {
				continue
			}

			for i, name := range result.Attrs {
				if name == catalog.Row_ID {
					if err = vector.AppendFixed(
						result.Vecs[i],
						entry.RowID,
						false,
						mp); err != nil {
						return nil, err
					}
				} else {
					idx := 2 /*rowid and commits*/ + p.seqnumMp[name]
					if idx >= len(entry.Batch.Vecs) /*add column*/ ||
						entry.Batch.Attrs[idx] == "" /*drop column*/ {
						err = vector.AppendAny(
							result.Vecs[i],
							nil,
							true,
							mp)
					} else {
						err = appendFuncs[i](
							result.Vecs[i],
							entry.Batch.Vecs[2 /*rowid and commits*/ +p.seqnumMp[name]],
							entry.Offset,
						)
					}
					if err != nil {
						return nil, err
					}
				}
			}
			rows++
			if rows == maxRows {
				break
			}
		}

		if rows == 0 {
			if pool == nil {
				result.Clean(mp)
			} else {
				pool.PutBatch(result)
			}
			return nil, nil
		}

		result.SetRowCount(rows)
		if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
			logutil.Debug(testutil.OperatorCatchBatch(
				"partition reader[snapshot: partitionState.rows]",
				result))
		}
		return result, nil
	}
}
