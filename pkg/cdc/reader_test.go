// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestNewTableReader(t *testing.T) {
	type args struct {
		cnTxnClient  client.TxnClient
		cnEngine     engine.Engine
		mp           *mpool.MPool
		packerPool   *fileservice.Pool[*types.Packer]
		info         *DbTableInfo
		sinker       Sinker
		wMarkUpdater *WatermarkUpdater
		tableDef     *plan.TableDef
		restartFunc  func(*DbTableInfo) error
	}

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{},
			{},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{
				"a",
				"b",
			},
		},
	}

	tests := []struct {
		name string
		args args
		want Reader
	}{
		{
			name: "t1",
			args: args{
				tableDef: tableDef,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNilf(t, NewTableReader(tt.args.cnTxnClient, tt.args.cnEngine, tt.args.mp, tt.args.packerPool, tt.args.info, tt.args.sinker, tt.args.wMarkUpdater, tt.args.tableDef, tt.args.restartFunc), "NewTableReader(%v, %v, %v, %v, %v, %v, %v, %v, %v)", tt.args.cnTxnClient, tt.args.cnEngine, tt.args.mp, tt.args.packerPool, tt.args.info, tt.args.sinker, tt.args.wMarkUpdater, tt.args.tableDef, tt.args.restartFunc)
		})
	}
}

func Test_tableReader_Run(t *testing.T) {
	type fields struct {
		cnTxnClient           client.TxnClient
		cnEngine              engine.Engine
		mp                    *mpool.MPool
		packerPool            *fileservice.Pool[*types.Packer]
		info                  *DbTableInfo
		sinker                Sinker
		wMarkUpdater          *WatermarkUpdater
		tick                  *time.Ticker
		restartFunc           func(*DbTableInfo) error
		insTsColIdx           int
		insCompositedPkColIdx int
		delTsColIdx           int
		delCompositedPkColIdx int
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pool := fileservice.NewPool(
		128,
		func() *types.Packer {
			return types.NewPacker()
		},
		func(packer *types.Packer) {
			packer.Reset()
		},
		func(packer *types.Packer) {
			packer.Close()
		},
	)

	type args struct {
		ctx context.Context
		ar  *ActiveRoutine
	}

	stub1 := gostub.Stub(&GetTxnOp,
		func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		})
	defer stub1.Reset()

	stub2 := gostub.Stub(&FinishTxnOp,
		func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {

		})
	defer stub2.Reset()

	stub3 := gostub.Stub(&GetTxn,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator) error {
			return nil
		})
	defer stub3.Reset()

	stub4 := gostub.Stub(&GetRelationById,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator, tableId uint64) (dbName string, tblName string, rel engine.Relation, err error) {
			return "", "", nil, nil
		})
	defer stub4.Reset()

	stub5 := gostub.Stub(&GetSnapshotTS,
		func(txnOp client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{
				PhysicalTime: 100,
				LogicalTime:  0,
			}
		})
	defer stub5.Reset()

	var packer *types.Packer
	put := pool.Get(&packer)
	defer put.Put()

	mp := mpool.MustNewZero()

	//
	stub6 := gostub.Stub(&CollectChanges,
		func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
			return newTestChangesHandle("test", "t1", 20, 23, types.TS{}, mp, packer),
				nil
		})
	defer stub6.Reset()

	u := &WatermarkUpdater{
		accountId:    1,
		taskId:       uuid.New(),
		ie:           newWmMockSQLExecutor(),
		watermarkMap: &sync.Map{},
	}

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "t1",
			fields: fields{
				info: &DbTableInfo{
					SourceTblName: "t1",
				},
				tick:                  time.NewTicker(time.Millisecond * 300),
				packerPool:            pool,
				wMarkUpdater:          u,
				mp:                    mp,
				insTsColIdx:           0,
				insCompositedPkColIdx: 3,
				sinker:                NewConsoleSinker(nil, nil),
			},
			args: args{
				ctx: ctx,
				ar:  NewCdcActiveRoutine(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &tableReader{
				cnTxnClient:           tt.fields.cnTxnClient,
				cnEngine:              tt.fields.cnEngine,
				mp:                    tt.fields.mp,
				packerPool:            tt.fields.packerPool,
				info:                  tt.fields.info,
				sinker:                tt.fields.sinker,
				wMarkUpdater:          tt.fields.wMarkUpdater,
				tick:                  tt.fields.tick,
				restartFunc:           tt.fields.restartFunc,
				insTsColIdx:           tt.fields.insTsColIdx,
				insCompositedPkColIdx: tt.fields.insCompositedPkColIdx,
				delTsColIdx:           tt.fields.delTsColIdx,
				delCompositedPkColIdx: tt.fields.delCompositedPkColIdx,
			}
			reader.Run(tt.args.ctx, tt.args.ar)
		})
	}
}

//func Test_tableReader_readTable(t *testing.T) {
//	type fields struct {
//		cnTxnClient           client.TxnClient
//		cnEngine              engine.Engine
//		mp                    *mpool.MPool
//		packerPool            *fileservice.Pool[*types.Packer]
//		info                  *DbTableInfo
//		sinker                Sinker
//		wMarkUpdater          *WatermarkUpdater
//		tick                  *time.Ticker
//		restartFunc           func(*DbTableInfo) error
//		insTsColIdx           int
//		insCompositedPkColIdx int
//		delTsColIdx           int
//		delCompositedPkColIdx int
//	}
//
//	type args struct {
//		ctx context.Context
//		ar  *ActiveRoutine
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		wantErr assert.ErrorAssertionFunc
//	}{
//		{
//			name: "t1",
//			fields: fields{
//				packerPool: fileservice.NewPool(
//					128,
//					func() *types.Packer {
//						return types.NewPacker()
//					},
//					func(packer *types.Packer) {
//						packer.Reset()
//					},
//					func(packer *types.Packer) {
//						packer.Close()
//					},
//				),
//			},
//			args: args{
//				ctx: context.Background(),
//				ar:  NewCdcActiveRoutine(),
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			reader := &tableReader{
//				cnTxnClient:           tt.fields.cnTxnClient,
//				cnEngine:              tt.fields.cnEngine,
//				mp:                    tt.fields.mp,
//				packerPool:            tt.fields.packerPool,
//				info:                  tt.fields.info,
//				sinker:                tt.fields.sinker,
//				wMarkUpdater:          tt.fields.wMarkUpdater,
//				tick:                  tt.fields.tick,
//				restartFunc:           tt.fields.restartFunc,
//				insTsColIdx:           tt.fields.insTsColIdx,
//				insCompositedPkColIdx: tt.fields.insCompositedPkColIdx,
//				delTsColIdx:           tt.fields.delTsColIdx,
//				delCompositedPkColIdx: tt.fields.delCompositedPkColIdx,
//			}
//			reader.readTable(tt.args.ctx, tt.args.ar)
//		})
//	}
//}

var _ engine.ChangesHandle = new(testChangesHandle)

const (
	batchCnt = 6
	rowCnt   = 3
)

type testChangesHandle struct {
	dbName, tblName string
	dbId, tblId     uint64
	data            []*batch.Batch
	mp              *mpool.MPool
	packer          *types.Packer
	called          int
	toTs            types.TS
}

func newTestChangesHandle(
	dbName, tblName string,
	dbId, tblId uint64,
	toTs types.TS,
	mp *mpool.MPool,
	packer *types.Packer,
) *testChangesHandle {
	ret := &testChangesHandle{
		dbName:  dbName,
		tblName: tblName,
		dbId:    dbId,
		tblId:   tblId,
		mp:      mp,
		packer:  packer,
		toTs:    toTs,
	}
	/*
		assume tables looks like:
			test.t*
		same schema :
			create table t1(a int,b int, primary key(a,b))
	*/

	if dbName == "test" && strings.HasPrefix(tblName, "t") {
		ret.makeData()
	}
	return ret
}

func (changes *testChangesHandle) makeData() {
	changes.packer.Reset()
	defer func() {
		changes.packer.Reset()
	}()
	//no checkpoint
	//insert:
	//ts,a,b,cpk
	//delete:
	//ts cpk
	for i := 0; i < batchCnt+1; i++ {
		bat := allocTestBatch(
			[]string{
				"ts",
				"a",
				"b",
				"cpk",
			},
			[]types.Type{
				types.T_TS.ToType(),
				types.T_int32.ToType(),
				types.T_int32.ToType(),
				types.T_varchar.ToType(),
			},
			0,
			changes.mp,
		)
		bat.SetRowCount(rowCnt)
		for j := 0; j < rowCnt; j++ {
			//ts
			_ = vector.AppendFixed(bat.Vecs[0], changes.toTs, false, changes.mp)
			//a
			_ = vector.AppendFixed(bat.Vecs[1], int32(j), false, changes.mp)
			//b
			_ = vector.AppendFixed(bat.Vecs[2], int32(j), false, changes.mp)
			//cpk
			changes.packer.Reset()
			changes.packer.EncodeInt32(int32(j))
			changes.packer.EncodeInt32(int32(j))
			_ = vector.AppendBytes(bat.Vecs[3], changes.packer.Bytes(), false, changes.mp)
		}

		changes.data = append(changes.data, bat)
	}
}

func (changes *testChangesHandle) Next(ctx context.Context, mp *mpool.MPool) (data *batch.Batch, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	if changes.called < 1 {
		data = changes.data[changes.called]
		changes.called++
		return data, tombstone, engine.ChangesHandle_Snapshot, nil
	} else if changes.called >= 1 && changes.called < batchCnt-2 {
		data = changes.data[changes.called]
		changes.called++
		return data, tombstone, engine.ChangesHandle_Tail_wip, nil
	} else if changes.called == batchCnt-2 {
		data = changes.data[changes.called]
		changes.called++
		return data, tombstone, engine.ChangesHandle_Tail_done, nil
	} else if changes.called == batchCnt-1 {
		data = changes.data[changes.called]
		changes.called++
		return data, tombstone, engine.ChangesHandle_Tail_wip, nil
	}
	return nil, nil, engine.ChangesHandle_Tail_wip, err
}

func (changes *testChangesHandle) Close() error {
	return nil
}

func allocTestBatch(
	attrName []string,
	tt []types.Type,
	batchSize int,
	mp *mpool.MPool,
) *batch.Batch {
	batchData := batch.New(true, attrName)

	//alloc space for vector
	for i := 0; i < len(attrName); i++ {
		vec := vector.NewVec(tt[i])
		if err := vec.PreExtend(batchSize, mp); err != nil {
			panic(err)
		}
		vec.SetLength(batchSize)
		batchData.Vecs[i] = vec
	}

	batchData.SetRowCount(batchSize)
	return batchData
}

func Test_changesHandle(t *testing.T) {
	newTestChangesHandle("db", "t1", 20, 23, types.TS{}, nil, nil)
}
