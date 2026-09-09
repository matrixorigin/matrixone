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

package ivfflat

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestRequiredMembershipStoragePaths(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	for _, tc := range []struct {
		name                          string
		integer, residual, full, desc bool
	}{
		{"integer", true, false, false, false}, {"integer_full", true, false, true, false},
		{"integer_residual", true, true, false, false}, {"string", false, false, false, false},
		{"string_full", false, false, true, false}, {"descending_fallback", true, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := types.T_varchar
			if tc.integer {
				typ = types.T_int64
			}
			keys := vector.NewVec(typ.ToType())
			defer keys.Free(proc.Mp())
			if tc.integer {
				require.NoError(t, vector.AppendFixed(keys, int64(7), false, proc.Mp()))
			} else {
				require.NoError(t, vector.AppendBytes(keys, []byte("member"), false, proc.Mp()))
			}
			data, err := keys.MarshalBinary()
			require.NoError(t, err)
			p := sqlexec.NewSqlProcess(proc)
			p.IvfHasMembershipFilter, p.IvfMembershipFilterRequired, p.IvfRuntimeFilterData = true, true, data
			if tc.integer {
				payload, err := docfilter.Build(keys)
				require.NoError(t, err)
				p.IvfMembershipFilterObject, err = docfilter.New(payload)
				require.NoError(t, err)
				defer p.IvfMembershipFilterObject.Free()
			}
			if tc.desc {
				p.IndexReaderParam = &plan.IndexReaderParam{OrderBy: []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC}}}
			}
			scanner := &scriptedRelationScanner{t: t}
			p.RelationScanner = scanner
			scanner.run = func(req sqlexec.RelationScanRequest) executor.Result {
				require.Equal(t, tc.desc, req.PostFilterTopOnly)
				require.Equal(t, !tc.desc && (!tc.integer || tc.residual), req.FilterBeforeTopK)
				require.Equal(t, tc.integer && !tc.desc, req.FilterHint.BF != nil)
				require.Empty(t, req.FilterHint.MembershipFilterBytes)
				require.Len(t, req.BlockFilters, 1)
				prefix := req.BlockFilters[0].GetF()
				if tc.full {
					require.Equal(t, function.PrefixEqualFunctionName, prefix.Func.ObjName)
					packer := types.NewPacker()
					defer packer.Close()
					packer.EncodeInt64(9)
					require.Equal(t, string(packer.Bytes()), prefix.Args[1].GetLit().GetSval())
				} else {
					require.Equal(t, function.PrefixInFunctionName, prefix.Func.ObjName)
				}
				require.Equal(t, !tc.integer || tc.desc, expressionContainsFunction(req.Filter, "in"), "the exact domain is either BF or an exact expression")
				return executor.Result{Mp: proc.Mp()}
			}
			var filters []*plan.Expr
			if tc.residual {
				filters = []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}}
			}
			centroids := []int64{3}
			if tc.full {
				centroids = nil
			}
			idx := &IvfflatSearchIndex[float32]{QuantMul: 1}
			res, err := idx.scanEntriesInDomain(p,
				vectorindex.IndexConfig{Ivfflat: vectorindex.IvfflatIndexConfig{Metric: uint16(metric.Metric_L2sqDistance)}},
				vectorindex.IndexTableConfig{PKeyType: int32(typ)}, []float32{0}, 9, centroids, nil, filters, 1, tc.full)
			require.NoError(t, err)
			res.Close()
			require.Len(t, scanner.requests, 1)
			require.False(t, canUseStorageTopK(p, nil, nil, 1, true), "nil centroids alone do not authorize a fast scan")
		})
	}
}

func expressionContainsFunction(expr *plan.Expr, name string) bool {
	f := expr.GetF()
	if f == nil {
		return false
	}
	if f.Func.ObjName == name {
		return true
	}
	for _, arg := range f.Args {
		if expressionContainsFunction(arg, name) {
			return true
		}
	}
	return false
}

type generationAdmission struct {
	denyAt, calls int
	held          int64
}

func (a *generationAdmission) Acquire(n int64) (int64, bool) {
	a.calls++
	if a.calls == a.denyAt {
		return a.held, false
	}
	a.held += n
	return a.held, true
}
func (a *generationAdmission) Release(n int64) int64 { a.held -= n; return a.held }

func TestPlanGenerationFailsClosedBeforeStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	proc.Base.TxnOperator = mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.SessionInfo.StorageEngine = mock_frontend.NewMockEngine(ctrl) // no calls allowed
	spec := &plan.VectorIndexScan{Index: &plan.IndexDef{IndexAlgoParams: `{"lists":"1","op_type":"vector_l2_ops"}`},
		SourceTable: &plan.ObjectRef{}, SourceTableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "pk", Typ: plan.Type{Id: int32(types.T_int64)}}},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk"}, Name2ColIndex: map[string]int32{"pk": 0}}, ScanWork: &plan.VectorIndexScanWork{}}
	keys := vector.NewVec(types.T_int64.ToType())
	defer keys.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(keys, int64(7), false, proc.Mp()))
	data, err := keys.MarshalBinary()
	require.NoError(t, err)
	req := searchplugin.Request{MembershipFilter: data, HasMembershipFilter: true, MembershipFilterRequired: true, CandidateBudget: 1}
	for _, count := range []int{0, -1} {
		readers, err := NewPlanReaders(proc, spec, req, count)
		require.Error(t, err)
		require.Nil(t, readers)
	}
	for _, change := range []func(*searchplugin.Request){
		func(r *searchplugin.Request) { r.HasMembershipFilter = false },
		func(r *searchplugin.Request) { r.Identity.PartitionCount = 2 },
		func(r *searchplugin.Request) { r.MembershipFilter = []byte{1, 2} },
		func(r *searchplugin.Request) { r.HasFirstRound = true },
	} {
		bad := req
		change(&bad)
		readers, err := NewPlanReaders(proc, spec, bad, 2)
		require.Error(t, err)
		require.Nil(t, readers)
	}
	empty := req
	empty.MembershipFilter = nil
	readers, err := NewPlanReaders(proc, spec, empty, 2)
	require.NoError(t, err)
	require.Len(t, readers, 2)
	for _, r := range readers {
		end, err := r.Read(proc.Ctx, nil, nil, proc.Mp(), nil)
		require.NoError(t, err)
		require.True(t, end)
		require.NoError(t, r.Close())
	}

	rt := moruntime.ServiceRuntime(proc.GetService())
	old, exists := rt.GetGlobalVariables(moruntime.CNMemoryThrottler)
	for _, denyAt := range []int{1, 2, 0} {
		t.Run(string(rune('0'+denyAt)), func(t *testing.T) {
			admission := &generationAdmission{denyAt: denyAt}
			rt.SetGlobalVariables(moruntime.CNMemoryThrottler, admission)
			defer func() {
				if exists {
					rt.SetGlobalVariables(moruntime.CNMemoryThrottler, old)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.CNMemoryThrottler, admission)
				}
			}()
			readers, err := NewPlanReaders(proc, spec, req, 2)
			require.Error(t, err) // deny build, deny reconstruction, or reject incomplete metadata after acquiring both
			require.Nil(t, readers)
			if denyAt > 0 {
				var admissionErr *docfilter.MemoryAdmissionError
				require.ErrorAs(t, err, &admissionErr)
			}
			require.Zero(t, admission.held, "partial construction releases every admitted allocation")
			require.NoError(t, proc.Ctx.Err())
		})
	}
	child := proc.NewContextChildProc(0)
	child.Cancel(context.Canceled)
	readers, err = NewPlanReaders(child, spec, req, 2)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, readers)
}
