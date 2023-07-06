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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"go.uber.org/zap"

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
	withFilterMixin
	table    *txnTable
	prepared bool
	// inserted rows comes from txn.writes.
	inserts []*batch.Batch
	//deleted rows comes from txn.writes or partitionState.rows.
	deletes map[types.Rowid]uint8
	iter    logtailreplay.RowsIter
}

var _ engine.Reader = new(PartitionReader)

func (p *PartitionReader) Close() error {
	p.iter.Close()
	return nil
}

func (p *PartitionReader) prepare() error {
	txn := p.table.db.txn
	var inserts []*batch.Batch
	var deletes map[types.Rowid]uint8
	//prepare inserts and deletes for partition reader.
	if !txn.readOnly.Load() && !p.prepared {
		inserts = make([]*batch.Batch, 0, len(p.table.writes))
		deletes = make(map[types.Rowid]uint8)
		for _, entry := range p.table.writes {
			if entry.typ == INSERT {
				if entry.bat == nil || entry.bat.Length() == 0 {
					continue
				}
				if entry.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
					continue
				}
				inserts = append(inserts, entry.bat)
				continue
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
					continue
				}
				//deletes in txn.Write maybe comes from PartitionState.Rows ,
				// PartitionReader need to skip them.
				vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
				for _, v := range vs {
					deletes[v] = 0
				}
			}
		}
		//deletes maybe comes from PartitionState.rows, PartitionReader need to skip them;
		// so, here only load deletes which don't belong to PartitionState.blks.
		p.table.LoadDeletesForVolatileBlocksIn(p.table._partState, false, deletes)
		p.inserts = inserts
		p.deletes = deletes
		p.prepared = true
	}
	return nil
}

func (p *PartitionReader) Read(
	ctx context.Context,
	colNames []string,
	expr *plan.Expr,
	mp *mpool.MPool,
	vp engine.VectorPool) (*batch.Batch, error) {
	if p == nil {
		return nil, nil
	}
	if err := p.prepare(); err != nil {
		return nil, err
	}
	p.tryUpdateColumns(colNames)
	//read batch resides in memory from txn.writes.
	if len(p.inserts) > 0 {
		bat := p.inserts[0].GetSubBatch(colNames)
		rowIds := vector.MustFixedCol[types.Rowid](p.inserts[0].Vecs[0])
		p.inserts = p.inserts[1:]
		b := batch.NewWithSize(len(colNames))
		b.SetAttributes(colNames)
		for i := range colNames {
			if vp == nil {
				b.Vecs[i] = vector.NewVec(p.columns.colTypes[i])
			} else {
				b.Vecs[i] = vp.GetVector(p.columns.colTypes[i])
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
		logutil.Debugf("read %v with %v", colNames, p.columns.seqnums)
		//		CORNER CASE:
		//		if some rowIds[j] is in p.deletes above, then some rows has been filtered.
		//		the bat.Length() is not always the right value for the result batch b.
		b.SetZs(b.Vecs[0].Length(), mp)
		if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
			logutil.Debug(testutil.OperatorCatchBatch(
				"partition reader[workspace:memory]",
				b))
		}
		return b, nil
	}

	//read batch from partitionState.rows.
	{
		const maxRows = 8192
		b := batch.NewWithSize(len(colNames))
		b.SetAttributes(colNames)
		for i := range colNames {
			if vp == nil {
				b.Vecs[i] = vector.NewVec(p.columns.colTypes[i])
			} else {
				b.Vecs[i] = vp.GetVector(p.columns.colTypes[i])
			}
		}
		rows := 0
		appendFuncs := make([]func(*vector.Vector, *vector.Vector, int64) error, len(b.Attrs))
		for i, name := range b.Attrs {
			if name == catalog.Row_ID {
				appendFuncs[i] = vector.GetUnionOneFunction(types.T_Rowid.ToType(), mp)
			} else {
				appendFuncs[i] = vector.GetUnionOneFunction(p.columns.colTypes[i], mp)
			}
		}
		//read rows from partitionState.rows.
		for p.iter.Next() {
			entry := p.iter.Entry()
			if _, ok := p.deletes[entry.RowID]; ok {
				continue
			}
			for i, name := range b.Attrs {
				if name == catalog.Row_ID {
					if err := vector.AppendFixed(
						b.Vecs[i],
						entry.RowID,
						false,
						mp); err != nil {
						return nil, err
					}
				} else {
					idx := 2 /*rowid and commits*/ + p.columns.seqnums[i]
					if int(idx) >= len(entry.Batch.Vecs) /*add column*/ ||
						entry.Batch.Attrs[idx] == "" /*drop column*/ {
						if err := vector.AppendAny(
							b.Vecs[i],
							nil,
							true,
							mp); err != nil {
							return nil, err
						}
					} else {
						appendFuncs[i](
							b.Vecs[i],
							entry.Batch.Vecs[2 /*rowid and commits*/ +p.columns.seqnums[i]],
							entry.Offset,
						)
					}

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
		if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
			logutil.Debug(testutil.OperatorCatchBatch(
				"partition reader[snapshot: partitionState.rows]",
				b))
		}
		return b, nil
	}
}
