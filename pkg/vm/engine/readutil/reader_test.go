// Copyright 2021 Matrix Origin
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

package readutil

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type singlePersistedBlockSource struct {
	info       objectio.BlockInfo
	emitted    bool
	closeCount int32
}

func (s *singlePersistedBlockSource) Next(
	context.Context,
	[]string,
	[]types.Type,
	[]uint16,
	int32,
	any,
	*mpool.MPool,
	*batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {
	if s.emitted {
		return nil, engine.End, nil
	}
	s.emitted = true
	return &s.info, engine.Persisted, nil
}

func (*singlePersistedBlockSource) ApplyTombstones(
	context.Context,
	*objectio.Blockid,
	[]int64,
	engine.TombstoneApplyPolicy,
) ([]int64, error) {
	return nil, nil
}

func (*singlePersistedBlockSource) GetTombstones(
	context.Context,
	*objectio.Blockid,
) (objectio.Bitmap, error) {
	return objectio.Bitmap{}, nil
}

func (*singlePersistedBlockSource) SetOrderBy([]*plan.OrderBySpec)  {}
func (*singlePersistedBlockSource) GetOrderBy() []*plan.OrderBySpec { return nil }
func (*singlePersistedBlockSource) SetFilterZM(objectio.ZoneMap)    {}
func (s *singlePersistedBlockSource) Close() {
	atomic.AddInt32(&s.closeCount, 1)
}
func (*singlePersistedBlockSource) String() string { return "singlePersistedBlockSource" }

func TestReaderLateMaterializationSkipsPersistedPayload(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	writeMP := mpool.MustNewZero()
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[1] = vector.NewVec(types.T_text.ToType())
	for i := 0; i < 8; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, writeMP))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("persisted-payload"), false, writeMP))
	}
	input.SetRowCount(8)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	input.Clean(writeMP)
	require.Zero(t, writeMP.CurrNB())
	mpool.DeleteMPool(writeMP)

	tableDef := &plan.TableDef{
		Name:          "late_reader_test",
		Name2ColIndex: map[string]int32{"id": 0, "payload": 1},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Cols: []*plan.ColDef{{
			Name:    "id",
			Seqnum:  0,
			Primary: true,
			Typ:     plan.Type{Id: int32(types.T_int32)},
		}, {
			Name:   "payload",
			Seqnum: 1,
			Typ:    plan.Type{Id: int32(types.T_text)},
		}},
	}
	source := &singlePersistedBlockSource{info: stats.ConstructBlockInfo(0)}
	queryMP := mpool.MustNewZero()
	r, err := NewReader(
		ctx,
		queryMP,
		nil,
		fs,
		tableDef,
		timestamp.Timestamp{},
		nil,
		source,
		0,
		engine.FilterHint{},
	)
	require.NoError(t, err)
	mr := NewMergeReader([]engine.Reader{r})

	output := batch.NewWithSize(2)
	output.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int32.ToType())
	output.Vecs[1] = vector.NewOffHeapVecWithType(types.T_text.ToType())
	isEnd, err := mr.ReadWithFilter(
		ctx,
		[]string{"id", "payload"},
		[]int{0},
		func(bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
			require.Equal(t, []int{0}, loaded)
			require.Equal(t, 8, bat.Vecs[0].Length())
			require.Zero(t, bat.Vecs[1].Length())
			bat.Vecs[0].CleanOnlyData()
			bat.SetRowCount(0)
			return engine.ReaderFilterResult{}, nil
		},
		queryMP,
		output,
	)
	require.NoError(t, err)
	require.False(t, isEnd)
	require.Zero(t, output.RowCount())
	require.Zero(t, output.Vecs[0].Length())
	require.Zero(t, output.Vecs[1].Length())

	require.NoError(t, mr.Close())
	require.Equal(t, int32(1), atomic.LoadInt32(&source.closeCount))

	output.Clean(queryMP)
	require.Zero(t, queryMP.CurrNB())
	mpool.DeleteMPool(queryMP)
}

func TestMergeReaderRejectsNilReaderFilter(t *testing.T) {
	mr := NewMergeReader(nil)
	_, err := mr.ReadWithFilter(context.Background(), nil, nil, nil, nil, nil)
	require.ErrorContains(t, err, "nil reader filter")
}

type lateMaterializationReaderStub struct {
	end        bool
	err        error
	closeCount int32
}

func (r *lateMaterializationReaderStub) Read(
	context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch,
) (bool, error) {
	return r.end, r.err
}

func (r *lateMaterializationReaderStub) ReadWithFilter(
	context.Context, []string, []int, engine.ReaderFilter, *mpool.MPool, *batch.Batch,
) (bool, error) {
	return r.end, r.err
}

func (r *lateMaterializationReaderStub) Close() error {
	atomic.AddInt32(&r.closeCount, 1)
	return nil
}

func (*lateMaterializationReaderStub) SetOrderBy([]*plan.OrderBySpec)       {}
func (*lateMaterializationReaderStub) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (*lateMaterializationReaderStub) SetIndexParam(*plan.IndexReaderParam) {}
func (*lateMaterializationReaderStub) SetFilterZM(objectio.ZoneMap)         {}

func TestMergeReaderReadWithFilterClosesChildren(t *testing.T) {
	t.Run("on end", func(t *testing.T) {
		child := &lateMaterializationReaderStub{end: true}
		mr := NewMergeReader([]engine.Reader{child})
		isEnd, err := mr.ReadWithFilter(
			context.Background(), nil, nil,
			func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
				return engine.ReaderFilterResult{}, nil
			}, nil, nil,
		)
		require.NoError(t, err)
		require.True(t, isEnd)
		require.Equal(t, int32(1), atomic.LoadInt32(&child.closeCount))
		require.NoError(t, mr.Close())
		require.Equal(t, int32(1), atomic.LoadInt32(&child.closeCount))
	})

	t.Run("on filter error", func(t *testing.T) {
		filterErr := errors.New("filter failure")
		child := &lateMaterializationReaderStub{err: filterErr}
		mr := NewMergeReader([]engine.Reader{child})
		_, err := mr.ReadWithFilter(
			context.Background(), nil, nil,
			func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
				return engine.ReaderFilterResult{}, nil
			}, nil, nil,
		)
		require.ErrorIs(t, err, filterErr)
		require.Equal(t, int32(1), atomic.LoadInt32(&child.closeCount))
		require.NoError(t, mr.Close())
		require.Equal(t, int32(1), atomic.LoadInt32(&child.closeCount))
	})
}

func TestReaderSetIndexParamDoesNotPreallocateDistHeap(t *testing.T) {
	r := &reader{}
	limit := uint64(^uint(0) >> 1)

	vectorCol := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{ColPos: 3},
		},
	}
	vectorLit := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2, NotNullable: true},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_VecVal{
					VecVal: string(types.ArrayToBytes[float32]([]float32{0, 0})),
				},
			},
		},
	}
	orderExpr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_float64), NotNullable: true},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: metric.DistFn_L2Distance},
				Args: []*plan.Expr{
					vectorCol,
					vectorLit,
				},
			},
		},
	}
	param := &plan.IndexReaderParam{
		OrderBy:      []*plan.OrderBySpec{{Expr: orderExpr}},
		Limit:        plan2.MakePlan2Uint64ConstExprWithType(limit),
		OrigFuncName: metric.DistFn_L2Distance,
		DistRange: &plan.DistRange{
			LowerBoundType: plan.BoundType_INCLUSIVE,
			LowerBound: &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: -1}}},
			},
			UpperBoundType: plan.BoundType_INCLUSIVE,
			UpperBound: &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 2}}},
			},
		},
	}

	require.NotPanics(t, func() {
		r.SetIndexParam(param)
	})
	require.NotNil(t, r.orderByLimit)
	require.Equal(t, limit, r.orderByLimit.Limit)
	require.Equal(t, plan.BoundType_UNBOUNDED, r.orderByLimit.LowerBoundType)
	require.Equal(t, plan.BoundType_INCLUSIVE, r.orderByLimit.UpperBoundType)
	require.Equal(t, float64(4), r.orderByLimit.UpperBound)
	require.Zero(t, len(r.orderByLimit.DistHeap))
	require.Zero(t, cap(r.orderByLimit.DistHeap))
}

func TestReaderSetIndexParamSupportsOrderedLimit(t *testing.T) {
	r := &reader{}
	param := &plan.IndexReaderParam{
		OrderBy: []*plan.OrderBySpec{
			{
				Expr: &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{ColPos: 1},
					},
				},
				Flag: plan.OrderBySpec_DESC,
			},
		},
		Limit: plan2.MakePlan2Uint64ConstExprWithType(8),
	}

	r.SetIndexParam(param)

	require.NotNil(t, r.orderByLimit)
	require.True(t, r.orderByLimit.OrderedLimit)
	require.True(t, r.orderByLimit.Desc)
	require.Equal(t, int32(1), r.orderByLimit.ColPos)
	require.Equal(t, uint64(8), r.orderByLimit.Limit)
	require.Nil(t, r.orderByLimit.NumVec)
}

func TestReaderSetIndexParamIgnoresUnevaluatedLimit(t *testing.T) {
	validOrderBy := []*plan.OrderBySpec{{
		Expr: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
		},
	}}
	params := []*plan.IndexReaderParam{{
		OrderBy: validOrderBy,
		Limit: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_uint64)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		},
	}, {
		OrderBy: validOrderBy,
		Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Isnull: true,
			Value:  &plan.Literal_U64Val{U64Val: 8},
		}}},
	}, {
		OrderBy: validOrderBy,
		Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_I64Val{I64Val: 8},
		}}},
	}, {
		OrderBy: []*plan.OrderBySpec{{
			Expr: nil,
		}},
		Limit: plan2.MakePlan2Uint64ConstExprWithType(8),
	}, {
		OrderBy: []*plan.OrderBySpec{nil},
		Limit:   plan2.MakePlan2Uint64ConstExprWithType(8),
	}, {
		OrderBy: validOrderBy,
		Limit:   plan2.MakePlan2Uint64ConstExprWithType(0),
	}, {
		OrderBy: validOrderBy,
		Limit:   plan2.MakePlan2Uint64ConstExprWithType(uint64(^uint(0)>>1) + 1),
	}}

	for _, param := range params {
		r := &reader{}
		require.NotPanics(t, func() { r.SetIndexParam(param) })
		require.Nil(t, r.orderByLimit)
	}
}
