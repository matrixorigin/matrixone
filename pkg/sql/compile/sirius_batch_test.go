// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

type siriusBatchRecorder struct {
	capacity                           uint64
	rows                               uint32
	vectors                            []SiriusInputVector
	acquired, published, released      int
	acquireErr, publishErr, releaseErr error
	onAcquire                          func(context.Context) error
	onPublish                          func(uint32, []SiriusInputVector) error
}

func (r *siriusBatchRecorder) Acquire(ctx context.Context, bytes uint64) (SiriusInputLease, error) {
	r.acquired++
	if r.onAcquire != nil {
		if err := r.onAcquire(ctx); err != nil {
			return nil, err
		}
	}
	if r.acquireErr != nil {
		return nil, r.acquireErr
	}
	r.capacity = bytes
	return r, nil
}
func (r *siriusBatchRecorder) Capacity() uint64 { return r.capacity }
func (r *siriusBatchRecorder) Release() error   { r.released++; return r.releaseErr }
func (r *siriusBatchRecorder) Publish(_ context.Context, rows uint32, vs []SiriusInputVector) error {
	r.published++
	r.rows, r.vectors = rows, make([]SiriusInputVector, len(vs))
	for i, v := range vs {
		r.vectors[i] = SiriusInputVector{Class: v.Class, Data: append([]byte(nil), v.Data...), Area: append([]byte(nil), v.Area...), Nulls: append([]byte(nil), v.Nulls...)}
	}
	if r.onPublish != nil {
		return r.onPublish(rows, vs)
	}
	return r.publishErr
}

func TestSiriusBatchPublicationLayoutAndOwnership(t *testing.T) {
	proc := testutil.NewProcess(t)
	bat := batch.NewWithSize(3)
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[2] = vector.NewConstNull(types.T_int64.ToType(), 3, proc.Mp())
	long := strings.Repeat("x", 25)
	for i, value := range []string{"a", long, "ignored"} {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i+1), i == 2, proc.Mp()))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(value), i == 2, proc.Mp()))
	}
	bat.SetRowCount(3)
	columns := []SiriusReadColumn{{Type: planpb.Type{Id: int32(types.T_int64)}}, {Type: planpb.Type{Id: int32(types.T_varchar)}}, {Type: planpb.Type{Id: int32(types.T_int64)}}}
	recorder := &siriusBatchRecorder{}
	require.NoError(t, publishSiriusBatch(t.Context(), recorder, bat, columns))
	require.Equal(t, 1, recorder.acquired)
	require.Equal(t, 1, recorder.published)
	require.Equal(t, 1, recorder.released)
	require.Equal(t, uint32(3), recorder.rows)
	require.Equal(t, uint64(1), binary.LittleEndian.Uint64(recorder.vectors[0].Data))
	require.Equal(t, byte(4), recorder.vectors[0].Nulls[0])
	require.Equal(t, byte(1), recorder.vectors[1].Data[0])
	require.Equal(t, []byte(long), recorder.vectors[1].Area)
	d := recorder.vectors[1].Data[types.VarlenaSize:]
	require.Equal(t, uint32(types.VarlenaBigHdr), binary.LittleEndian.Uint32(d))
	require.Equal(t, uint32(0), binary.LittleEndian.Uint32(d[4:]))
	require.Equal(t, uint32(25), binary.LittleEndian.Uint32(d[8:]))
	require.Equal(t, uint32(2), recorder.vectors[2].Class)

	publication, release := errors.New("publication"), errors.New("release")
	recorder.publishErr, recorder.releaseErr = publication, release
	err := publishSiriusBatch(t.Context(), recorder, bat, columns)
	require.ErrorIs(t, err, publication)
	require.ErrorIs(t, err, release)
	require.Equal(t, 2, recorder.released)
	recorder.acquireErr = errors.New("no credit")
	require.ErrorIs(t, publishSiriusBatch(t.Context(), recorder, bat, columns), recorder.acquireErr)
	require.Equal(t, 2, recorder.released, "failed acquisition owns no lease")
	columns[0].Type.NotNullable = true
	require.ErrorContains(t, publishSiriusBatch(t.Context(), recorder, bat, columns), "nonnullable")
	require.Equal(t, 3, recorder.acquired, "schema/null rejection happens before credit")
}

func TestSiriusSlicesBoundLogicalExpansion(t *testing.T) {
	proc := testutil.NewProcess(t)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 9, proc.Mp())
	bat.SetRowCount(9)
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	columns := []SiriusReadColumn{{Type: planpb.Type{Id: int32(types.T_int64)}}}
	end, err := siriusSliceEnd(bat, columns, 0, 35)
	require.NoError(t, err)
	require.Equal(t, 3, end, "constant NULL expansion is not hidden by a zero-byte payload")
	require.Zero(t, siriusSliceBytes(bat, 0, end))
	end, err = siriusSliceEnd(bat, columns, 3, 35)
	require.NoError(t, err)
	require.Equal(t, 6, end)
}

func TestSiriusSplitAndConstantVarlenaPublication(t *testing.T) {
	proc := testutil.NewProcess(t)
	columns := []SiriusReadColumn{{Type: planpb.Type{Id: int32(types.T_varchar)}}}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	for _, value := range []string{strings.Repeat("a", 25), strings.Repeat("b", 26), "c"} {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), false, proc.Mp()))
	}
	bat.SetRowCount(3)
	recorder := &siriusBatchRecorder{}
	require.NoError(t, publishSiriusSlice(t.Context(), recorder, bat, columns, 1, 2))
	require.Equal(t, []byte(strings.Repeat("b", 26)), recorder.vectors[0].Area)
	require.Zero(t, binary.LittleEndian.Uint32(recorder.vectors[0].Data[4:]), "split descriptors must rebase to the new area")
	end, err := siriusSliceEnd(bat, columns, 0, 58)
	require.NoError(t, err)
	require.Equal(t, 1, end)
	constant, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(strings.Repeat("x", 25)), 3, proc.Mp())
	require.NoError(t, err)
	bat.Vecs[0].Free(proc.Mp())
	bat.Vecs[0] = constant
	require.NoError(t, publishSiriusBatch(t.Context(), recorder, bat, columns))
	require.Equal(t, uint32(1), recorder.vectors[0].Class)
	require.Len(t, recorder.vectors[0].Data, types.VarlenaSize)
	require.Len(t, recorder.vectors[0].Area, 25)
	require.Equal(t, uint32(3), recorder.rows)
}

func TestSiriusNativeElementSizesAndBatchRejections(t *testing.T) {
	for _, group := range []struct {
		width int
		oids  []types.T
	}{
		{1, []types.T{types.T_bool, types.T_int8, types.T_uint8}},
		{2, []types.T{types.T_int16, types.T_uint16}},
		{4, []types.T{types.T_int32, types.T_uint32, types.T_float32, types.T_date}},
		{8, []types.T{types.T_int64, types.T_uint64, types.T_float64, types.T_decimal64, types.T_timestamp}},
		{16, []types.T{types.T_decimal128}},
		{24, []types.T{types.T_char, types.T_varchar, types.T_binary, types.T_varbinary}},
	} {
		for _, oid := range group.oids {
			width, err := siriusElementSize(oid)
			require.NoError(t, err)
			require.Equal(t, group.width, width)
		}
	}
	_, err := siriusElementSize(types.T_decimal256)
	require.Error(t, err, "wide types stay declined until numeric support is delivered")
	proc := testutil.NewProcess(t)
	bat := batch.NewWithSize(1)
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	bat.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())
	bat.SetRowCount(1)
	recorder := &siriusBatchRecorder{}
	require.Error(t, publishSiriusBatch(t.Context(), recorder, bat, nil))
	require.Error(t, publishSiriusBatch(t.Context(), recorder, bat, []SiriusReadColumn{{Type: planpb.Type{Id: int32(types.T_varchar)}}}))
	bat.Vecs[0].SetLength(2)
	require.Error(t, publishSiriusBatch(t.Context(), recorder, bat, []SiriusReadColumn{{Type: planpb.Type{Id: int32(types.T_int64)}}}))
	require.Zero(t, recorder.acquired)
	require.NoError(t, publishSiriusBatch(t.Context(), recorder, nil, nil))
}

type siriusCountingReader struct {
	readutil.EmptyReader
	reads, closes atomic.Int32
}

func (r *siriusCountingReader) Read(_ context.Context, _ []string, _ *planpb.Expr, mp *mpool.MPool, bat *batch.Batch) (bool, error) {
	if r.reads.Add(1) > 2 {
		return true, nil
	}
	for _, n := range []int64{0, 1, 2} {
		if err := vector.AppendFixed(bat.Vecs[0], n, false, mp); err != nil {
			return false, err
		}
	}
	bat.SetRowCount(3)
	return false, nil
}
func (r *siriusCountingReader) Close() error { r.closes.Add(1); return nil }

func TestSiriusPublicationBackPressureStopsTableScan(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()

	proc.Base.TxnOperator = txnOp
	c := allocateNewCompile(proc)
	t.Cleanup(c.Release)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	proc.ReplaceTopCtx(ctx)
	proc.ResetQueryContext()
	proc.BuildPipelineContext(ctx)
	intType := planpb.Type{Id: int32(types.T_int64)}
	node := &planpb.Node{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, TableDef: &planpb.TableDef{Name: "t", Cols: []*planpb.ColDef{{Name: "n", Typ: intType}}}}
	query := &planpb.Query{Steps: []int32{0}, Nodes: []*planpb.Node{node}}
	c.initAnalyzeModule(query)
	reader := &siriusCountingReader{}
	scope := newScope(Normal)
	scope.Proc = proc.NewContextChildProc(0)
	scope.DataSource = &Source{R: reader, Attributes: []string{"n"}, TableDef: node.TableDef}
	scope.setRootOperator(constructTableScan(node))
	c.scopes = []*Scope{scope}
	started := make(chan struct{})
	recorder := &siriusBatchRecorder{onAcquire: func(ctx context.Context) error { close(started); <-ctx.Done(); return context.Cause(ctx) }}
	scope.setRootOperator(output.NewArgument().WithBlock(false).WithFunc(func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
		return publishSiriusBatch(ctx, recorder, bat, []SiriusReadColumn{{Type: intType}})
	}))
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); done <- scope.Run(c) }()
	t.Cleanup(func() { cancel(); <-finished })
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("scan did not reach acquisition")
	}
	require.Equal(t, int32(1), reader.reads.Load(), "a blocked publisher must not pull the next reader batch")
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not stop the publisher")
	}
	require.Equal(t, int32(1), reader.closes.Load())
	require.Zero(t, recorder.published)
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSiriusReaderSchemaRequiresPhysicalIdentity(t *testing.T) {
	expected := &planpb.TableDef{TblId: 7, Version: 3, Cols: []*planpb.ColDef{{Name: "n", Seqnum: 2, ColId: 9, Typ: planpb.Type{Id: int32(types.T_int64)}}}}
	node := &planpb.Node{TableDef: expected}
	require.NoError(t, validateSiriusReaderSchema(node, expected))
	actual := plan2.DeepCopyTableDef(expected, true)
	actual.Cols[0].Seqnum++
	require.ErrorContains(t, validateSiriusReaderSchema(node, actual), "column definition")
	actual = plan2.DeepCopyTableDef(expected, true)
	actual.Version++
	require.ErrorContains(t, validateSiriusReaderSchema(node, actual), "table definition")
}

type siriusSpecRelation struct {
	*readerPathCaptureRelation
	definition *planpb.TableDef
	readers    []engine.Reader
}

func (r *siriusSpecRelation) GetTableDef(context.Context) *planpb.TableDef { return r.definition }
func (r *siriusSpecRelation) BuildReaders(_ context.Context, _ any, _ *planpb.Expr, _ engine.RelData, count int, readView client.WorkspaceReadView, _ bool, _ engine.TombstoneApplyPolicy, _ engine.FilterHint) ([]engine.Reader, error) {
	if count != len(r.readers) || readView != client.NewWorkspaceReadView(1, 2, 3) {
		return nil, errors.New("reader DOP/statement read view changed")
	}
	r.buildReadersCalls++
	return r.readers, nil
}

func TestEmbeddedSiriusReaderReusesScanFilterProjectionAndFetch(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	tx := mock_frontend.NewMockTxnOperator(ctrl)
	tx.EXPECT().IsSnapOp().Return(false).AnyTimes()
	tx.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	tx.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	tx.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()

	proc.Base.TxnOperator = tx
	intType := planpb.Type{Id: int32(types.T_int64)}
	filter, err := plan2.BindFuncExprImplByPlanExpr(t.Context(), ">", []*planpb.Expr{plan2.GetColExpr(intType, 0, 0), plan2.MakePlan2Int64ConstExprWithType(0)})
	require.NoError(t, err)
	definition := &planpb.TableDef{TblId: 7, Version: 3, Name: "t", Cols: []*planpb.ColDef{{Name: "n", Typ: intType}}}
	node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{SchemaName: "db", ObjName: "t"}, TableDef: definition,
		Stats: &planpb.Stats{Outcnt: 3, BlockNum: 1}, FilterList: []*planpb.Expr{filter}, ProjectList: []*planpb.Expr{plan2.GetColExpr(intType, 0, 0)},
		Offset: plan2.MakePlan2Int64ConstExprWithType(1), Limit: plan2.MakePlan2Int64ConstExprWithType(1),
		RuntimeFilterProbeList: []*planpb.RuntimeFilterSpec{{Tag: 7}},
	}
	reader := &siriusCountingReader{}
	relation := &siriusSpecRelation{readerPathCaptureRelation: &readerPathCaptureRelation{rangesData: readutil.BuildEmptyRelData()}, definition: definition, readers: []engine.Reader{reader}}
	db := &readerPathCaptureDatabase{relation: relation}
	eng := &readerPathCaptureEngine{database: db}
	spec := siriusReaderSpec{parent: proc, e: eng, addr: "cn:6001", ncpu: 1, txnReadView: client.NewWorkspaceReadView(1, 2, 3), node: plan2.DeepCopyNode(node), columns: []SiriusReadColumn{{Type: intType}}}
	recorder := &siriusBatchRecorder{}
	require.NoError(t, spec.run(t.Context(), recorder))
	require.Equal(t, uint32(1), recorder.rows)
	require.Equal(t, uint64(2), binary.LittleEndian.Uint64(recorder.vectors[0].Data))
	require.Equal(t, int32(1), reader.closes.Load())
	require.Equal(t, 0, eng.buildBlockReadersCalls, "local MO input must use the complete relation reader")
	require.Len(t, node.RuntimeFilterProbeList, 1, "only the private scan copy may lose GPU-owned runtime filters")
	require.Zero(t, proc.Mp().CurrNB())
}

func TestEmbeddedSiriusReaderPreservesParallelScanAndCleanup(t *testing.T) {
	for _, dop := range []int{1, 2} {
		t.Run(fmt.Sprintf("DOP%d_without_fetch", dop), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			ctrl := gomock.NewController(t)
			tx := mock_frontend.NewMockTxnOperator(ctrl)
			tx.EXPECT().IsSnapOp().Return(false).AnyTimes()
			tx.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
			tx.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
			tx.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()

			proc.Base.TxnOperator = tx
			intType := planpb.Type{Id: int32(types.T_int64)}
			definition := &planpb.TableDef{TblId: 7, Version: 3, Name: "t", Cols: []*planpb.ColDef{{Name: "n", Typ: intType}}}
			node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{SchemaName: "db", ObjName: "t"}, TableDef: definition,
				Stats: &planpb.Stats{Outcnt: 6, BlockNum: 16}, ProjectList: []*planpb.Expr{plan2.GetColExpr(intType, 0, 0)},
			}
			readers := make([]engine.Reader, dop)
			for i := range readers {
				readers[i] = &siriusCountingReader{}
			}
			relation := &siriusSpecRelation{readerPathCaptureRelation: &readerPathCaptureRelation{rangesData: readutil.BuildEmptyRelData()}, definition: definition, readers: readers}
			eng := &readerPathCaptureEngine{database: &readerPathCaptureDatabase{relation: relation}}
			spec := siriusReaderSpec{parent: proc, e: eng, addr: "cn:6001", ncpu: dop, txnReadView: client.NewWorkspaceReadView(1, 2, 3), node: node, columns: []SiriusReadColumn{{Type: intType}}}
			var values []int64
			recorder := &siriusBatchRecorder{onPublish: func(rows uint32, vectors []SiriusInputVector) error {
				for i := range rows {
					values = append(values, int64(binary.LittleEndian.Uint64(vectors[0].Data[int(i)*8:])))
				}
				return nil
			}}
			require.NoError(t, spec.run(t.Context(), recorder))
			require.Equal(t, int32(dop), node.Stats.Dop, "the merge boundary must preserve scan parallelism")
			require.Equal(t, 1, relation.buildReadersCalls)
			var expected []int64
			for _, reader := range readers {
				r := reader.(*siriusCountingReader)
				require.Equal(t, int32(3), r.reads.Load(), "each reader reaches EOF")
				require.Equal(t, int32(1), r.closes.Load(), "every opened reader closes exactly once")
				expected = append(expected, 0, 1, 2, 0, 1, 2)
			}
			require.ElementsMatch(t, expected, values)
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}
