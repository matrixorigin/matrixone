package postdml

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFullText(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().ResetRetry(gomock.Any()).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	database := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(database, nil).AnyTimes()

	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	relation.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

	proc := testutil.NewProc()
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx
	proc.Base.TxnOperator = txnOperator

	arg := PostDml{
		PostDmlCtx: &PostDmlCtx{
			Ref: &plan.ObjectRef{
				Obj:        0,
				SchemaName: "testDb",
				ObjName:    "testTable",
			},
			FullText: &PostDmlFullTextCtx{
				IsDelete:        true,
				IsInsert:        true,
				SourceTableName: "src",
				IndexTableName:  "index_tblan",
				PrimaryKeyIdx:   1,
				PrimaryKeyName:  "pk",
				Parts:           []string{"body", "title"},
				AlgoParams:      "",
			},
		},
		ctr: container{},
	}

	resetChildren(&arg)
	err := arg.Prepare(proc)
	require.NoError(t, err)
	_, err = arg.Call(proc)
	require.NoError(t, err)

	arg.Reset(proc, false, nil)

	err = arg.Prepare(proc)
	require.NoError(t, err)
	_, err = arg.Call(proc)
	require.NoError(t, err)

	logutil.Infof("%v", proc.Base.PostDmlSqlList)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func resetChildren(arg *PostDml) {
	op := colexec.NewMockOperator()
	bat := colexec.MakeMockBatchsWithRowID()
	op.WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func newBatch(proc *process.Process, rows int64) *batch.Batch {
	// not random
	ts := []types.Type{types.New(types.T_Rowid, 0, 0), types.New(types.T_int32, 0, 0), types.New(types.T_int32, 0, 0)}
	bat := testutil.NewBatch(ts, false, int(rows), proc.Mp())
	pkAttr := make([]string, 3)
	pkAttr[0] = "rowid"
	pkAttr[1] = "pk"
	pkAttr[2] = "partition_id"
	bat.SetAttributes(pkAttr)
	return bat
}

/*
func TestSplitBatch(t *testing.T) {
	type fields struct {
		ctr           container
		PostDmlCtx    *PostDmlCtx
		SegmentMap    map[string]int32
		RemotePostDml bool
		IBucket       uint32
		Nbucket       uint32
	}

	type args struct {
		proc   *process.Process
		srcBat *batch.Batch
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test_partition_table_1",
			fields: fields{
				ctr: container{
					resBat:           batch.New(false, []string{"rowid", "pk", "partition_id"}),
					partitionSources: []engine.Relation{nil, nil},
				},
				PostDmlCtx: &PostDmlCtx{
					RowIdIdx:              0,
					PrimaryKeyIdx:         1,
					PartitionIndexInBatch: 2,
					PartitionTableIDs:     []uint64{1, 2},
				},
				SegmentMap:    map[string]int32{},
				RemotePostDml: false,
				IBucket:       0,
				Nbucket:       1,
			},
			args: args{
				proc: testutil.NewProc(),
			},
			wantErr: true,
		},
		// {
		// 	name: "test_non_partition_table",
		// 	fields: fields{
		// 		ctr: container{
		// 			resBat: batch.New(false, []string{"rowid", "pk"}),
		// 		},
		// 		PostDmlCtx: &PostDmlCtx{
		// 			PrimaryKeyIdx: 1,
		// 			RowIdIdx: 0,
		// 		},
		// 	},
		// 	args: args{
		// 		proc: testutil.NewProc(),
		// 		srcBat: batch.NewWithSize(2),
		// 	},
		// 	wantErr: false,
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			postdml := &Deletion{
				ctr:           tt.fields.ctr,
				PostDmlCtx:    tt.fields.PostDmlCtx,
				RemotePostDml: tt.fields.RemotePostDml,
				IBucket:       tt.fields.IBucket,
				Nbucket:       tt.fields.Nbucket,
			}
			tt.args.srcBat = newBatch(tt.args.proc, 3)
			if tt.name == "test_partition_table_1" {
				vector.SetFixedAtWithTypeCheck(tt.args.srcBat.GetVector(2), 0, int32(-1))
			}
			if err := postdml.SplitBatch(tt.args.proc, tt.args.srcBat); (err != nil) != tt.wantErr {
				t.Errorf("Deletion.SplitBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
*/
