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

package table_scan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type ownedSelectionTestReader struct {
	lateMaterializationTestReader
	t              *testing.T
	selectCalls    int
	expectedSelect bool
	uses           []engine.ReaderColumnUse
}

func (r *ownedSelectionTestReader) ReadWithFilter(_ context.Context, _ []string, early []int, filter *engine.ReaderFilterRequest, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	r.reads++
	if r.reads > 1 {
		return true, nil
	}
	require.Equal(r.t, []int{0, 1}, early)
	require.Equal(r.t, r.expectedSelect, filter.Select != nil)
	r.uses = append([]engine.ReaderColumnUse(nil), filter.ColumnUses...)
	source := batch.NewWithSize(2)
	defer source.Clean(mp)
	source.Vecs[0] = vector.NewVec(types.New(types.T_decimal64, 15, 2))
	source.Vecs[1] = vector.NewVec(types.New(types.T_char, 15, 0))
	require.NoError(r.t, vector.AppendFixedList(source.Vecs[0], []types.Decimal64{100, 0, 200, 300}, nil, mp))
	for _, phone := range []string{"13-567-890-1234", "13-567-890-1234", "31-567-890-1234"} {
		require.NoError(r.t, vector.AppendBytes(source.Vecs[1], []byte(phone), false, mp))
	}
	require.NoError(r.t, vector.AppendBytes(source.Vecs[1], nil, true, mp))
	source.SetRowCount(4)
	count := 0
	var selected engine.ReaderFilterResult
	if filter.Select != nil {
		r.selectCalls++
		var err error
		selected, err = filter.Select(source, early)
		if err != nil {
			return false, err
		}
		require.Equal(r.t, 4, source.RowCount(), "selection callback cannot shrink borrowed input")
		count = source.RowCount()
		if !selected.All {
			count = len(selected.Sels)
		}
	} else {
		if _, err := filter.Apply(source, early); err != nil {
			return false, err
		}
		count = source.RowCount()
		selected.All = true
	}
	for col, v := range source.Vecs {
		if filter.ColumnUse(col, len(out.Vecs)) == engine.ReaderColumnUnused {
			continue
		}
		var err error
		if selected.All {
			err = out.Vecs[col].UnionBatch(v, 0, count, nil, mp)
		} else {
			err = out.Vecs[col].Union(v, selected.Sels, mp)
		}
		if err != nil {
			return false, err
		}
	}
	out.SetRowCount(count)
	return false, nil
}

// Exposing only Reader's method set proves the capability fallback, even when
// the underlying fixture also implements LateMaterializationReader.
type eagerSelectionTestReader struct{ engine.Reader }

func TestOwnedOutputReaderSelectionEntry(t *testing.T) {
	for _, shape := range []string{"flat", "accepting", "missing-account", "unsupported-reader", "summary", "pad-char"} {
		t.Run(shape, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			t.Cleanup(proc.Free)
			var allocation *vector.AllocationAccountSelection
			if shape != "missing-account" {
				registry, err := mpool.NewAllocationAccountRegistry(1, 128)
				require.NoError(t, err)
				account, err := registry.Open(1 << 20)
				require.NoError(t, err)
				t.Cleanup(func() {
					require.Zero(t, account.Snapshot().Used)
					_, _, err := registry.CompleteTerminal(account)
					require.NoError(t, err)
				})
				allocation, err = vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
				require.NoError(t, err)
			}
			if shape == "summary" {
				proc.Ctx = context.WithValue(proc.Ctx, defines.ReaderSummaryKey{}, true)
			}
			if shape == "pad-char" {
				proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return "PAD_CHAR_TO_FULL_LENGTH", nil })
			}
			balance, phone := types.New(types.T_decimal64, 15, 2), types.New(types.T_char, 15, 0)
			bind := func(name string, args ...*pb.Expr) *pb.Expr {
				typs := make([]types.Type, len(args))
				for i, arg := range args {
					typs[i] = types.MustTypeFromPlan(arg.Typ)
				}
				resolved, err := function.GetFunctionByName(proc.Ctx, name, typs)
				require.NoError(t, err)
				return &pb.Expr{Typ: resolved.GetReturnType().PlanType(), Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: name, Obj: resolved.GetEncodedOverloadID()}, Args: args}}}
			}
			integer := func(n int64) *pb.Expr {
				return &pb.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I64Val{I64Val: n}}}}
			}
			zero := &pb.Expr{Typ: balance.PlanType(), Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Decimal64Val{Decimal64Val: &pb.Decimal64{}}}}}
			substring := bind("substring", lateTestCol(1, phone), integer(1), integer(2))
			listType := types.MustTypeFromPlan(substring.Typ)
			payload := vector.NewVec(listType)
			require.NoError(t, vector.AppendBytes(payload, []byte("13"), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(payload, []byte("31"), false, proc.Mp()))
			data, err := payload.MarshalBinary()
			payload.Free(proc.Mp())
			require.NoError(t, err)
			list := &pb.Expr{Typ: listType.PlanType(), Expr: &pb.Expr_Vec{Vec: &pb.LiteralVec{Len: 2, Data: data}}}
			scan := &TableScan{Attrs: []string{"c_acctbal", "c_phone"}, Types: []pb.Type{balance.PlanType(), phone.PlanType()}, OutputAllocation: allocation, FilterExprs: []*pb.Expr{bind(">", lateTestCol(0, balance), zero), bind("in", substring, list)}}
			scan.ProjectList = []*pb.Expr{lateTestCol(0, balance)}
			t.Cleanup(func() { scan.Free(proc, false, nil) })
			passes := 1
			if shape == "flat" {
				passes = 2
			}
			for pass := 0; pass < passes; pass++ {
				reader := &ownedSelectionTestReader{t: t, expectedSelect: shape != "pad-char"}
				scan.Reader = reader
				if shape == "unsupported-reader" {
					scan.Reader = &eagerSelectionTestReader{Reader: reader}
				}
				require.NoError(t, scan.Prepare(proc))
				if shape == "accepting" {
					parent := &ownedColumnUseConsumer{MockOperator: colexec.NewMockOperator()}
					parent.AppendChild(scan)
					vm.ConfigureDictionaryBoundaries(parent)
					require.True(t, scan.ctr.dictionaryBoundary.AllowsDictionaryOutput())
				} else {
					parent := colexec.NewMockOperator()
					parent.AppendChild(scan)
					vm.ConfigureDictionaryBoundaries(parent)
					require.False(t, scan.ctr.dictionaryBoundary.AllowsDictionaryOutput())
				}
				result, err := vm.Exec(scan, proc)
				require.NoError(t, err)
				if shape == "missing-account" || shape == "unsupported-reader" || shape == "summary" {
					require.Nil(t, result.Batch)
					require.True(t, reader.eagerRead)
					require.Zero(t, reader.reads)
				} else {
					require.NotNil(t, result.Batch)
					require.False(t, result.Batch.HasDictionary())
					require.Equal(t, []types.Decimal64{100, 200}, vector.MustFixedColNoTypeCheck[types.Decimal64](result.Batch.Vecs[0]))
					require.Equal(t, 2, result.Batch.RowCount())
					if shape == "accepting" {
						require.Equal(t, []engine.ReaderColumnUse{engine.ReaderColumnRetain, engine.ReaderColumnUnused}, reader.uses)
					} else {
						require.Equal(t, []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnUnused}, reader.uses)
						require.Nil(t, scan.ctr.readerWorkspace)
					}
					require.Equal(t, shape != "pad-char", reader.selectCalls > 0)
					require.Zero(t, scan.ctr.dictionaryInputs.Gathers, "the second predicate must borrow phone values")
					result, err = vm.Exec(scan, proc)
					require.NoError(t, err)
					require.Nil(t, result.Batch)
				}
				scan.Reset(proc, false, nil)
				require.True(t, reader.closed)
			}
		})
	}
}
