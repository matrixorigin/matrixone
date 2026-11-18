// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestReaderMembershipTotalityUsesExecutableFilters(t *testing.T) {
	for _, location := range []string{"safe", "static", "runtime"} {
		t.Run(location, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			scan := NewArgument()
			t.Cleanup(scan.Release)
			scan.Types = []plan.Type{{Id: int32(types.T_int32)}}
			scan.ctr.readerFilter = &engine.ReaderFilterRequest{Select: func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
				t.Fatal("admission must not execute a predicate")
				return engine.ReaderFilterResult{}, nil
			}}
			scan.ctr.allFilterExecutors = []colexec.ExpressionExecutor{nil}
			scan.ctr.domainProbes = []localDomainProbeState{{column: 0}}
			scan.FilterExprs = []*plan.Expr{lateTestCompare(t, ">", 0, 0)}
			if location != "safe" {
				unknown := lateTestCompare(t, ">", 0, 0)
				unknown.GetF().Func.Obj = -1
				if location == "runtime" {
					scan.RuntimeFilterExprs = []*plan.Expr{unknown}
				} else {
					scan.FilterExprs = []*plan.Expr{unknown}
				}
			}
			scan.prepareReaderMembership(proc)
			require.NotNil(t, scan.ctr.readerFilter.Membership)
			require.Equal(t, location == "safe", scan.ctr.readerFilter.Membership.PredicatesTotal)
		})
	}
}

type localDomainTestReader struct {
	engine.Reader
	values []int64
	nullAt int
	reads  int
}

func (r *localDomainTestReader) Read(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	r.reads++
	if r.reads > 1 || len(r.values) == 0 {
		return true, nil
	}
	for i, value := range r.values {
		if err := vector.AppendFixed(out.Vecs[0], value, i == r.nullAt, mp); err != nil {
			return false, err
		}
	}
	out.SetRowCount(len(r.values))
	return false, nil
}

func (*localDomainTestReader) Close() error { return nil }

type membershipDecliningTestReader struct{ *localDomainTestReader }

func (r *membershipDecliningTestReader) ReadWithFilter(ctx context.Context, cols []string, early []int, request *engine.ReaderFilterRequest, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	if r.reads == 0 {
		if err := out.Vecs[0].PreExtend(len(r.values), mp); err != nil {
			return false, err
		}
	}
	end, err := r.Read(ctx, cols, nil, mp, out)
	if err == nil && !end {
		_, err = request.Select(out, early)
	}
	return end, err
}

func TestLocalDomainScanSourceAndProbe(t *testing.T) {
	for _, tc := range []struct {
		name    string
		empty   bool
		ndv     float64
		pass    bool
		decline bool
		tight   bool
		limit   uint64
	}{
		{name: "complete domain filters before output", ndv: 10000},
		{name: "ordinary reader keeps post-read membership", ndv: 10000, decline: true},
		{name: "tight ordinary control", ndv: 20, pass: true, decline: true, tight: true},
		{name: "tight optional publication", ndv: 10000, decline: true, tight: true},
		{name: "affordable optional publication", ndv: 10000, decline: true, tight: true, limit: 128},
		{name: "empty domain avoids probe read", empty: true, ndv: 10000},
		{name: "unprofitable probe keeps all rows", ndv: 20, pass: true},
		{name: "unknown probe NDV keeps all rows", pass: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetMessageBoard(message.NewMessageBoard())
			t.Cleanup(proc.Free)
			budget := process.MustNewExecutionResourceBudget(16<<20, 16<<20)
			g, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			t.Cleanup(g.Close)
			require.NoError(t, proc.SetExecutionResourceBudgetForTesting(g))
			domain, _ := runtimefilter.NewLocalDomain(types.T_int64, 1000, 1, 1, 1000000)
			t.Cleanup(domain.Close)
			typ := plan.Type{Id: int32(types.T_int64)}
			var account *mpool.AllocationAccount
			if tc.tight {
				registry, err := mpool.NewAllocationAccountRegistry(1, 128)
				require.NoError(t, err)
				limit := tc.limit
				if limit == 0 {
					limit = 64
				}
				account, err = registry.Open(limit)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.Zero(t, account.Snapshot().Used)
					_, _, err := registry.CompleteTerminal(account)
					require.NoError(t, err)
				})
			}
			makeScan := func(id int) *TableScan {
				s := NewArgument().WithTypes([]plan.Type{typ})
				s.Attrs = []string{"id"}
				s.SetAnalyzeControl(id, true)
				s.SetMaxParallel(1)
				t.Cleanup(func() {
					s.Free(proc, false, nil)
					s.closeReader()
					s.Release()
				})
				return s
			}
			source := makeScan(0)
			sourceReader := &localDomainTestReader{values: []int64{1, 2}, nullAt: -1}
			if tc.empty {
				sourceReader.values = nil
			}
			source.Reader = sourceReader
			source.LocalDomainBuilds = []runtimefilter.LocalDomainBinding{{Domain: domain, Column: "id"}}
			probe := makeScan(1)
			if tc.tight {
				probe.ProjectList = []*plan.Expr{lateTestCol(0, types.T_int64.ToType())}
				fn, err := function.GetFunctionByName(proc.Ctx, "isnotnull", []types.Type{types.T_int64.ToType()})
				require.NoError(t, err)
				probe.FilterExprs = []*plan.Expr{{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "isnotnull", Obj: fn.GetEncodedOverloadID()}, Args: []*plan.Expr{lateTestCol(0, types.T_int64.ToType())},
				}}}}
			}
			probeReader := &localDomainTestReader{values: []int64{1, 2, 99, 0}, nullAt: 3}
			if tc.tight {
				probeReader.values, probeReader.nullAt = []int64{1, 99, 99, 99, 99, 99, 99, 99}, -1
				allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
				require.NoError(t, err)
				probe.OutputAllocation = allocation
			}
			probe.Reader = probeReader
			if tc.decline {
				probe.Reader = &membershipDecliningTestReader{probeReader}
			}
			probe.LocalDomainProbes = []runtimefilter.LocalDomainBinding{{Domain: domain, Column: "id", ProbeNDV: tc.ndv}}
			require.NoError(t, source.Prepare(proc))
			require.NoError(t, probe.Prepare(proc))
			if tc.tight {
				consumer := &ownedColumnUseConsumer{MockOperator: colexec.NewMockOperator()}
				consumer.AppendChild(probe)
				vm.ConfigureDictionaryBoundaries(consumer)
				require.True(t, probe.ctr.dictionaryBoundary.AllowsDictionaryOutput())
			}
			predicateCalls := 0
			var predicateInput *batch.Batch
			if tc.decline {
				if old := probe.ctr.readerFilter; old != nil {
					if old.Exact != nil {
						old.Exact.Close()
					}
					old.FreeMembership(proc.Mp())
				}
				probe.ctr.readerFilter = &engine.ReaderFilterRequest{
					Select: func(input *batch.Batch, _ []int) (engine.ReaderFilterResult, error) {
						predicateCalls++
						predicateInput = input
						probe.ctr.filterReadMetrics = true
						return engine.ReaderFilterResult{All: true}, nil
					},
					Membership: &engine.ReaderMembershipFilter{Columns: []int{0}, Select: func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
						t.Fatal("the ordinary reader does not execute the optional membership callback")
						return engine.ReaderFilterResult{}, nil
					}},
				}
			}
			for {
				result, err := source.Call(proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
			}
			result, err := probe.Call(proc)
			require.NoError(t, err)
			if tc.decline {
				require.Equal(t, 1, predicateCalls)
				if !tc.pass {
					require.False(t, probe.ctr.readerFilter.Membership.Bypass)
				}
			}
			if tc.empty {
				require.Nil(t, result.Batch)
				require.Zero(t, probeReader.reads)
			} else if tc.tight && tc.limit > 64 {
				require.True(t, result.Batch.HasDictionary())
				require.Equal(t, 1, result.Batch.RowCount())
				require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
			} else if tc.tight {
				require.Same(t, predicateInput, result.Batch)
				require.Equal(t, probeReader.values, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))
				require.EqualValues(t, 64, account.Snapshot().Used)
				if !tc.pass {
					require.EqualValues(t, 8, probe.OpAnalyzer.GetOpStats().ExtraStats["RuntimeDomainPublicationCapacityFallbackRows"])
				}
			} else if tc.pass {
				require.Equal(t, len(probeReader.values), result.Batch.RowCount())
				require.True(t, result.Batch.Vecs[0].IsNull(3), "PASS must not perform even NULL rejection")
			} else {
				require.NotNil(t, result.Batch)
				values := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0])
				require.Contains(t, values, int64(1))
				require.Contains(t, values, int64(2))
				require.NotContains(t, values, int64(0), "ordinary equality cannot match a NULL probe")
				require.Equal(t, int64(4), probe.ctr.domainProbes[0].tested)
				require.Equal(t, int64(4-len(values)), probe.ctr.domainProbes[0].rejected)
			}
			source.Reset(proc, false, nil)
			probe.Reset(proc, false, nil)
			domain.Close()
			require.Zero(t, g.Snapshot().Used)
		})
	}
}

func TestLocalDomainProbeConstantAndShape(t *testing.T) {
	for _, name := range []string{"constant", "full constant", "constant NULL", "full constant NULL", "wrong type", "short flat vector"} {
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetMessageBoard(message.NewMessageBoard())
			t.Cleanup(proc.Free)
			domain, _ := runtimefilter.NewLocalDomain(types.T_int64, 1000, 1, 1, 1000000)
			t.Cleanup(domain.Close)
			source, err := domain.Begin(proc, 0, 1)
			require.NoError(t, err)
			keys := testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
			t.Cleanup(func() { keys.Free(proc.Mp()) })
			require.NoError(t, source.AddVector(proc.Ctx, keys, 1))
			source.Complete(false)
			bloom, empty, err := domain.Wait(proc.Ctx)
			require.NoError(t, err)
			require.False(t, empty)
			var probe *vector.Vector
			switch name {
			case "constant":
				probe, err = vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, proc.Mp())
			case "full constant":
				probe, err = vector.NewConstFixed(types.T_int64.ToType(), int64(7), 3, proc.Mp())
			case "full constant NULL":
				probe = vector.NewConstNull(types.T_int64.ToType(), 3, proc.Mp())
			case "constant NULL":
				probe = vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())
			case "wrong type":
				probe = testutil.MakeInt32Vector([]int32{7, 7, 7}, nil, proc.Mp())
			default:
				probe = testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
			}
			require.NoError(t, err)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = probe
			bat.SetRowCount(3)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			scan := &TableScan{ctr: container{buf: bat, domainRows: make([]int64, 0, 3),
				domainProbes: []localDomainProbeState{{domain: domain, bloom: bloom, mask: make([]uint8, 3)}}}}
			require.NoError(t, scan.applyLocalDomains(proc))
			if name == "constant NULL" || name == "full constant NULL" {
				require.Zero(t, bat.RowCount())
			} else {
				require.Equal(t, 3, bat.RowCount())
			}
			state := scan.ctr.domainProbes[0]
			if name == "wrong type" || name == "short flat vector" {
				require.True(t, state.pass)
				require.Equal(t, int64(3), state.bypassed)
			} else {
				require.Equal(t, int64(3), state.tested)
			}
		})
	}
}

func TestLocalDomainMaskCompositionAndCounters(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprintf("domains=%d", count), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetMessageBoard(message.NewMessageBoard())
			t.Cleanup(proc.Free)
			input := batch.NewWithSize(1)
			input.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2, 3, 0}, []uint64{3}, proc.Mp())
			input.SetRowCount(4)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			scan := &TableScan{ctr: container{domainRows: make([]int64, 0, 4)}}
			want := []bool{true, true, true, false}
			for i := range count {
				domain, _ := runtimefilter.NewLocalDomain(types.T_int64, 1000, 1, 1, 1000000)
				t.Cleanup(domain.Close)
				source, err := domain.Begin(proc, 0, 1)
				require.NoError(t, err)
				keys := testutil.MakeInt64Vector([]int64{int64(i + 1), int64(i + 2)}, nil, proc.Mp())
				t.Cleanup(func() { keys.Free(proc.Mp()) })
				require.NoError(t, source.AddVector(proc.Ctx, keys, keys.Length()))
				source.Complete(false)
				bloom, _, err := domain.Wait(proc.Ctx)
				require.NoError(t, err)
				probe := localDomainProbeState{domain: domain, bloom: bloom, mask: make([]uint8, 4), tested: runtimefilter.LocalDomainBatchRows - 4}
				rejected := int64(0)
				for row := range 4 {
					// Scalar membership is the independent mask-consumer oracle;
					// Bloom false positives remain permitted.
					keep := row != 3 && bloom.Test(input.Vecs[0].GetData()[row*8:row*8+8])
					want[row] = want[row] && keep
					if !keep {
						rejected++
					}
				}
				scan.ctr.domainProbes = append(scan.ctr.domainProbes, probe)
				t.Cleanup(func() { require.Equal(t, rejected, scan.ctr.domainProbes[i].rejected) })
			}
			rows, all, err := scan.selectLocalDomains(proc, input)
			require.NoError(t, err)
			require.False(t, all)
			var expected []int64
			for row, keep := range want {
				if keep {
					expected = append(expected, int64(row))
				}
			}
			require.Equal(t, expected, rows)
			for _, p := range scan.ctr.domainProbes {
				require.EqualValues(t, runtimefilter.LocalDomainBatchRows, p.tested)
				require.True(t, p.pass, "the low-rejection transition must preserve current-batch filtering")
			}
			_, all, err = scan.selectLocalDomains(proc, input)
			require.NoError(t, err)
			require.True(t, all, "the next batch must bypass even NULL rejection")
			for _, p := range scan.ctr.domainProbes {
				require.EqualValues(t, 4, p.bypassed)
			}
		})
	}
}

func BenchmarkLocalDomainMaskConsumer(b *testing.B) {
	for _, nullable := range []bool{false, true} {
		b.Run(fmt.Sprintf("nullable=%t", nullable), func(b *testing.B) {
			proc := testutil.NewProcess(b)
			proc.SetMessageBoard(message.NewMessageBoard())
			b.Cleanup(proc.Free)
			domain, _ := runtimefilter.NewLocalDomain(types.T_int32, 1000, 1, 1, 1000000)
			b.Cleanup(domain.Close)
			source, err := domain.Begin(proc, 0, 1)
			require.NoError(b, err)
			keys := vector.NewVec(types.T_int32.ToType())
			b.Cleanup(func() { keys.Free(proc.Mp()) })
			require.NoError(b, vector.AppendFixedList(keys, []int32{1, 2}, nil, proc.Mp()))
			require.NoError(b, source.AddVector(proc.Ctx, keys, 2))
			source.Complete(false)
			bloom, _, err := domain.Wait(proc.Ctx)
			require.NoError(b, err)
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			b.Cleanup(func() { input.Clean(proc.Mp()) })
			const rows = runtimefilter.LocalDomainBatchRows
			for row := range rows {
				require.NoError(b, vector.AppendFixed(input.Vecs[0], int32(row%4), nullable && row%7 == 0, proc.Mp()))
			}
			input.SetRowCount(rows)
			scan := &TableScan{ctr: container{domainRows: make([]int64, 0, rows), domainProbes: []localDomainProbeState{{domain: domain, bloom: bloom, mask: make([]uint8, rows)}}}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, _, err := scan.selectLocalDomains(proc, input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
