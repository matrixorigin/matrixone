// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// cutoffBarrierSource delays the real remote data source at its first block.
// The test observes its actual block choices without replacing pruning logic.
type cutoffBarrierSource struct {
	engine.DataSource
	ready, published, pruned chan struct{}
	calls                    int
	filtered, sawWorse       bool
	closed                   bool
}

func (s *cutoffBarrierSource) Next(ctx context.Context, cols []string, typs []types.Type,
	seqnums []uint16, pk int32, filter any, mp *mpool.MPool, bat *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {
	if s.calls == 0 {
		close(s.ready)
		select {
		case <-s.published:
		case <-ctx.Done():
			return nil, engine.End, ctx.Err()
		}
	}
	blk, state, err := s.DataSource.Next(ctx, cols, typs, seqnums, pk, filter, mp, bat)
	s.calls++
	if err == nil && s.calls == 2 {
		s.sawWorse = state != engine.End
		close(s.pruned)
	}
	return blk, state, err
}

func (s *cutoffBarrierSource) SetFilterZM(zm objectio.ZoneMap) {
	s.filtered = true
	s.DataSource.SetFilterZM(zm)
}

func (s *cutoffBarrierSource) Close() {
	s.closed = true
	s.DataSource.Close()
}

type cutoffFailingWitness struct {
	*colexec.MockOperator
	ready, published, pruned chan struct{}
	board                    *message.MessageBoard
	emitted                  bool
	err                      error
}

func (p *cutoffFailingWitness) Call(proc *process.Process) (vm.CallResult, error) {
	if !p.emitted {
		select {
		case <-p.ready:
		case <-proc.Ctx.Done():
			return vm.CancelResult, proc.Ctx.Err()
		}
		p.emitted = true
		return p.MockOperator.Call(proc)
	}
	// Order invokes the child again only after accepting the first batch and
	// publishing its cutoff. Fail only after the other reader has consumed it.
	close(p.published)
	select {
	case <-p.pruned:
	case <-proc.Ctx.Done():
		return vm.CancelResult, proc.Ctx.Err()
	}
	p.board.Close()
	return vm.CancelResult, p.err
}

func TestScanCutoffRemoteReaderPrunesThenWitnessFails(t *testing.T) {
	oldRuntime := runtime.ServiceRuntime("")
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	defer runtime.SetupServiceBasedRuntime("", oldRuntime)
	catalog.SetupDefines("")
	c := NewMockCompile(t)
	defer func() {
		c.proc.Free()
		require.Zero(t, c.proc.Mp().CurrNB())
	}()
	c.execType = plan2.ExecTypeAP_ONECN
	c.anal = &AnalyzeModule{qry: &plan.Query{}}
	c.hasMergeOp = true
	ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), catalog.System_Account), 20*time.Second)
	defer cancel()
	queryCtx := c.proc.Base.GetContextBase().BuildQueryCtx(ctx)
	c.proc.BuildPipelineContext(queryCtx)
	board := message.NewMessageBoard()
	defer board.CloseAndDrain()
	c.proc.SetMessageBoard(board)
	rootProc := c.proc.NewNoContextChildProc(2)
	witnessProc := rootProc.NewNoContextChildProc(0)
	readerProc := rootProc.NewNoContextChildProc(0)
	defer func() {
		rootProc.Cancel(process.ErrPipelineStopped)
		for _, proc := range []*process.Process{witnessProc, readerProc, rootProc} {
			proc.Free()
		}
	}()
	rootProc.Reg.MergeReceivers[0] = process.NewPipelineEdge(1, 1)
	rootProc.Reg.MergeReceivers[1] = process.NewPipelineEdge(1, 1)

	fs := testutil.NewSharedFS()
	defer fs.Close(ctx)
	writer := ioutil.ConstructWriter(0, []uint16{0}, -1, false, false, fs)
	for _, values := range [][]int64{{0, 1}, {9, 10}} {
		b := partialInputBatch(c.proc, values)
		defer b.Clean(c.proc.Mp())
		_, err := writer.WriteBatch(b)
		require.NoError(t, err)
	}
	_, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	require.Equal(t, uint32(2), stats.BlkCnt())
	data := readutil.NewBlockListRelationData(0)
	for i := uint16(0); i < 2; i++ {
		blk := stats.ConstructBlockInfo(i)
		data.AppendBlockInfo(&blk)
	}
	typ := plan.Type{Id: int32(types.T_int64), NotNullable: true}
	key := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k", ColPos: 0}}}
	by := []*plan.OrderBySpec{{Expr: key}}
	tableDef := &plan.TableDef{Name: "t", Cols: []*plan.ColDef{{Name: "k", Typ: typ}},
		Name2ColIndex: map[string]int32{"k": 0}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "k"}}
	ready, published, pruned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	source := &cutoffBarrierSource{DataSource: readutil.NewRemoteDataSource(ctx, fs, timestamp.Timestamp{}, data),
		ready: ready, published: published, pruned: pruned}
	rd, err := readutil.NewReader(ctx, c.proc.Mp(), nil, fs, tableDef, timestamp.Timestamp{}, nil, source, 0, engine.FilterHint{})
	require.NoError(t, err)
	defer func() {
		if !source.closed {
			require.NoError(t, rd.Close())
		}
	}()
	rd.SetOrderBy(by)
	require.True(t, readutil.SupportsScanCutoff(rd))

	const k = 16385 // exceeds the resident Top threshold and survives remote validation
	witnessBatch := batch.NewWithSize(1)
	defer witnessBatch.Clean(c.proc.Mp())
	witnessBatch.Vecs[0], err = vector.NewConstFixed(types.T_int64.ToType(), int64(2), k, c.proc.Mp())
	require.NoError(t, err)
	witnessBatch.SetRowCount(k)
	var operators []vm.Operator
	defer func() {
		for _, op := range operators {
			op.Free(c.proc, true, nil)
			op.Release()
		}
	}()
	original := order.NewArgument()
	operators = append(operators, original)
	original.OrderBySpec, original.ScanCutoffLimit, original.ScanCutoffTag = by, k, 42
	codec := &scopeContext{id: 1, root: &scopeContext{}, parent: &scopeContext{}}
	_, instruction, err := convertToPipelineInstruction(original, c.proc, codec, 1)
	require.NoError(t, err)
	wire, err := instruction.Marshal()
	require.NoError(t, err)
	decoded := new(pipeline.Instruction)
	require.NoError(t, decoded.Unmarshal(wire))
	restored, err := convertToVmOperator(decoded, codec, nil)
	require.NoError(t, err)
	witnessOrder := restored.(*order.Order)
	operators = append(operators, witnessOrder)
	require.Equal(t, uint64(k), witnessOrder.ScanCutoffLimit)
	require.Equal(t, int32(42), witnessOrder.ScanCutoffTag)
	witnessOrder.ScanCutoff = order.NewScanCutoff(k, types.T_int64.ToType(), false)
	readerOrder := dupOperator(witnessOrder, 1, 2).(*order.Order)
	operators = append(operators, readerOrder)
	require.Same(t, witnessOrder.ScanCutoff, readerOrder.ScanCutoff)

	injected := errors.New("witness failed after cutoff publication")
	leaf := &cutoffFailingWitness{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{witnessBatch}),
		ready: ready, published: published, pruned: pruned, board: board, err: injected}
	operators = append(operators, leaf)
	witnessOrder.AppendChild(leaf)
	witnessConnector := connector.NewArgument().WithReg(rootProc.Reg.MergeReceivers[0])
	operators = append(operators, witnessConnector)
	witnessConnector.AppendChild(witnessOrder)
	scan := table_scan.NewArgument()
	operators = append(operators, scan)
	scan.TopValueMsgTag, scan.Reader, scan.Attrs, scan.Types = 42, rd, []string{"k"}, []plan.Type{typ}
	readerOrder.AppendChild(scan)
	readerConnector := connector.NewArgument().WithReg(rootProc.Reg.MergeReceivers[1])
	operators = append(operators, readerConnector)
	readerConnector.AppendChild(readerOrder)
	mergeOp := merge.NewArgument()
	mergeOrder := mergeorder.NewArgument()
	operators = append(operators, mergeOp, mergeOrder)
	mergeOrder.OrderBySpecs = by
	mergeOrder.AppendChild(mergeOp)
	var returned int
	out := output.NewArgument().WithFunc(func(b *batch.Batch, _ *perfcounter.CounterSet) error {
		if b != nil {
			returned += b.RowCount()
		}
		return nil
	})
	operators = append(operators, out)
	out.AppendChild(mergeOrder)
	root := &Scope{Magic: Merge, Proc: rootProc, RootOp: out, PreScopes: []*Scope{
		{Magic: Normal, Proc: witnessProc, RootOp: witnessConnector},
		{Magic: Normal, Proc: readerProc, RootOp: readerConnector},
	}}
	root.buildContextFromParentCtx(c.proc.Ctx)
	err = root.MergeRun(c)
	require.ErrorContains(t, err, injected.Error())
	require.Zero(t, returned, "a page cannot use witnesses from a failed producer")
	require.True(t, source.filtered, "the other table scan must receive the published cutoff")
	require.Equal(t, 2, source.calls)
	require.False(t, source.sawWorse, "the real remote source must skip the worse persisted block")
	require.True(t, source.closed)
	require.Nil(t, witnessOrder.ScanCutoff)
	require.Nil(t, readerOrder.ScanCutoff)
	require.True(t, board.IsClosed())
}
