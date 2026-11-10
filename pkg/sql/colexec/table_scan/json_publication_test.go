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

package table_scan

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTableScanConsumesPublishedJSONVersion(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		t.Run(fmt.Sprint(filtered), func(t *testing.T) { testTableScanPublishedJSONVersion(t, filtered) })
	}
	t.Run("accounted", func(t *testing.T) { testTableScanPublishedJSONVersion(t, true, true) })
}
func TestTableScanDeliversJSONVersionToStandaloneProjection(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		t.Run(fmt.Sprint(filtered), func(t *testing.T) { testTableScanPublishedJSONVersion(t, filtered, false, true) })
	}
}
func testTableScanPublishedJSONVersion(t *testing.T, filtered bool, accounted ...bool) {
	external := len(accounted) > 1 && accounted[1]
	proc := testutil.NewProc(t)
	defer func() { proc.Free(); require.Zero(t, proc.GetMPool().CurrNB()) }()
	var selection *vector.AllocationAccountSelection
	if len(accounted) != 0 && accounted[0] {
		registry, err := mpool.NewAllocationAccountRegistry(1, 256)
		require.NoError(t, err)
		account, err := registry.Open(1 << 20)
		require.NoError(t, err)
		defer func() {
			require.Zero(t, account.Seal().Used)
			require.Zero(t, registry.LiveAllocationMetadata())
			_, err := registry.Finalize(account)
			require.NoError(t, err)
		}()
		selection, err = vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
		require.NoError(t, err)
	}
	ctrl := gomock.NewController(t)
	reader := mock_frontend.NewMockReader(ctrl)
	reader.EXPECT().Close().Return(nil).AnyTimes()
	reader.EXPECT().GetOrderBy().Return(nil).AnyTimes()
	reader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ []string, _ *pb.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
		publication := objectio.JSONReadSetFromContext(ctx)
		require.NotNil(t, publication)
		admit := publication.CachePinAdmission()
		require.NotNil(t, admit)
		budget, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		beforePin := budget.Snapshot().Used
		releasePin, err := admit(128)
		require.NoError(t, err)
		require.Equal(t, beforePin+128, budget.Snapshot().Used)
		releasePin()
		require.Equal(t, beforePin, budget.Snapshot().Used)
		publication.BindColumns([]uint16{0, 1})
		builder, err := vector.NewJSONReaderBuilder(mp, out.Vecs[0].AllocationAccountSelection(), publication.CachePinAdmission())
		require.NoError(t, err)
		defer builder.Close()
		for _, text := range []string{`{"keep":1}`, `{"keep":2}`} {
			value, err := types.ParseStringToByteJson(text)
			require.NoError(t, err)
			raw, err := value.Marshal()
			require.NoError(t, err)
			require.NoError(t, vector.AppendBytes(out.Vecs[0], raw, false, mp))
			require.NoError(t, builder.Append(raw, false))
		}
		require.NoError(t, vector.AppendFixedList(out.Vecs[1], []bool{false, true}, nil, mp))
		out.SetRowCount(2)
		version, err := builder.Finish()
		require.NoError(t, err)
		publication.Publish(0, version)
		if external {
			changed, err := types.ParseStringToByteJson(`{"keep":9}`)
			require.NoError(t, err)
			raw, err := changed.Marshal()
			require.NoError(t, err)
			for row := 0; row < out.RowCount(); row++ {
				require.NoError(t, vector.SetBytesAt(out.Vecs[0], row, raw, mp))
			}
		}
		return false, nil
	}).Times(1)
	typ := types.T_json.ToType()
	resolved, err := function.GetFunctionByName(proc.Ctx, "json_value", []types.Type{typ, types.T_varchar.ToType()})
	require.NoError(t, err)
	column := &pb.Expr{Typ: typ.PlanType(), Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 0}}}
	path := &pb.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Sval{Sval: "$.keep"}}}}
	expression := &pb.Expr{Typ: resolved.GetReturnType().PlanType(), Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{Obj: resolved.GetEncodedOverloadID(), ObjName: "json_value"}, Args: []*pb.Expr{column, path}}}}
	scan := &TableScan{Reader: reader, Attrs: []string{"doc", "mask"}, Types: []pb.Type{typ.PlanType(), types.T_bool.ToType().PlanType()}}
	if filtered {
		scan.FilterExprs = []*pb.Expr{lateTestCol(1, types.T_bool.ToType())}
	}
	scan.OutputAllocation = selection
	if !external {
		scan.ProjectList = []*pb.Expr{expression}
	} else if !filtered {
		scan.ProjectList = []*pb.Expr{}
	} else {
		// A reordered output must map output ordinals back to storage.
		scan.ProjectList = []*pb.Expr{lateTestCol(1, types.T_bool.ToType()), column}
		parentColumn := &pb.Expr{Typ: typ.PlanType(), Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 1}}}
		expression = &pb.Expr{Typ: resolved.GetReturnType().PlanType(), Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{Obj: resolved.GetEncodedOverloadID(), ObjName: "json_value"}, Args: []*pb.Expr{parentColumn, path}}}}
	}
	defer scan.Free(proc, false, nil)
	require.NoError(t, scan.Prepare(proc))
	var output *batch.Batch
	if external {
		consumer := projection.NewArgument()
		defer consumer.Release()
		consumer.ProjectList = []*pb.Expr{expression}
		consumer.AppendChild(scan)
		require.NoError(t, consumer.Prepare(proc))
		defer consumer.Free(proc, false, nil)
		result, err := consumer.Call(proc)
		require.NoError(t, err)
		output = result.Batch
	} else {
		result, err := scan.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, scan.jsonPublication())
		changed, err := types.ParseStringToByteJson(`{"keep":9}`)
		require.NoError(t, err)
		changedRaw, err := changed.Marshal()
		require.NoError(t, err)
		require.NoError(t, vector.SetBytesAt(result.Batch.Vecs[0], 0, changedRaw, proc.Mp()))
		output, err = scan.ExecProjection(proc, result.Batch)
		require.NoError(t, err)
	}
	expectedRows := 2
	if filtered {
		expectedRows = 1
	}
	require.Equal(t, expectedRows, output.RowCount())
	want := "1"
	if filtered {
		want = "2"
	}
	require.Equal(t, want, string(output.Vecs[0].GetBytesAt(0)))
	if selection != nil {
		require.Same(t, selection, output.Vecs[0].AllocationAccountSelection())
	}
	require.Nil(t, scan.jsonPublication())
}

func TestTableScanDeliveryReleasesPartialAndCanceledVersions(t *testing.T) {
	for _, mode := range []string{"partial", "cancel", "row-domain"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			beforeBudget := budget.Snapshot().Used
			input := batch.NewWithSize(2)
			defer input.Clean(proc.Mp())
			doc, err := types.ParseStringToByteJson(`{"keep":1}`)
			require.NoError(t, err)
			raw, err := doc.Marshal()
			require.NoError(t, err)
			for i := range input.Vecs {
				input.Vecs[i] = vector.NewVec(types.T_json.ToType())
				require.NoError(t, vector.AppendBytes(input.Vecs[i], raw, false, proc.Mp()))
			}
			input.SetRowCount(1)
			admission, err := function.JSONReaderMemoryAdmission(proc)
			require.NoError(t, err)
			builder, err := vector.NewJSONReaderBuilder(proc.Mp(), nil, admission)
			require.NoError(t, err)
			defer builder.Close()
			require.NoError(t, builder.Append(raw, false))
			if mode == "row-domain" {
				require.NoError(t, builder.Append(raw, false))
			}
			version, err := builder.Finish()
			require.NoError(t, err)
			set, err := objectio.NewJSONReadSetWithAdmission([]int{0, 1}, nil, admission)
			require.NoError(t, err)
			set.BindColumns([]uint16{0, 1})
			set.Publish(0, version)
			scan := &TableScan{}
			require.NoError(t, scan.RequestJSONInputs(proc, []int{0, 1}))
			scan.ctr.jsonState.publication = set
			input.Vecs[1].GetBytesAt(0)[0] = 0xfd
			if mode == "cancel" {
				ctx, cancel := context.WithCancel(proc.Ctx)
				proc.Ctx = ctx
				cancel()
			}
			versions, err := scan.PublishJSONInputs(proc, input, []int{0, 1})
			if mode == "partial" {
				require.NoError(t, err)
				require.NotNil(t, versions.Readers[0])
				declined, present := versions.Readers[1]
				require.True(t, present)
				require.Nil(t, declined, "malformed input must be left to evaluation")
				colexec.CloseJSONInputVersions(versions)
			} else {
				require.Error(t, err)
				require.Nil(t, versions)
			}
			require.Nil(t, scan.jsonPublication())
			_, err = version.Window(0, 1)
			require.Error(t, err, "the Read scope must release its reference on every failure")
			scan.Reset(proc, false, nil)
			require.Equal(t, beforeBudget, budget.Snapshot().Used)
			input.Clean(proc.Mp())
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestTableScanJSONRequestMetadataReleasedOnReset(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	before := budget.Snapshot().Used
	scan := &TableScan{}
	require.NoError(t, scan.RequestJSONInputs(proc, []int{0, 1}))
	charged := budget.Snapshot().Used
	require.Greater(t, charged, before)
	require.NoError(t, scan.RequestJSONInputs(proc, []int{0, 1}))
	require.Equal(t, charged, budget.Snapshot().Used, "the same demand reuses one request scope")
	scan.Reset(proc, false, nil)
	require.Equal(t, before, budget.Snapshot().Used)
	require.Nil(t, scan.ctr.jsonState)
	require.NoError(t, scan.RequestJSONInputs(proc, []int{0}))
	scan.Free(proc, false, nil)
	require.Equal(t, before, budget.Snapshot().Used)
}

func TestTableScanWithoutJSONDemandDoesNotAllocateState(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	before := budget.Snapshot().Used
	scan := &TableScan{}
	allocations := testing.AllocsPerRun(10, func() {
		if err := scan.RequestJSONInputs(proc, nil); err != nil {
			panic("unexpected JSON request error")
		}
	})
	require.Zero(t, allocations)
	require.Nil(t, scan.ctr.jsonState)
	require.Equal(t, before, budget.Snapshot().Used)
}
