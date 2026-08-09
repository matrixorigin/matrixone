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

package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func frontendResultBatch(mp *mpool.MPool, first *vector.Vector, rows int) *batch.Batch {
	totals := make([]int64, rows)
	for i := range totals {
		totals[i] = int64((i + 1) * 10)
	}
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = first
	bat.Vecs[1] = testutil.MakeInt64Vector(totals, nil, mp)
	bat.SetRowCount(rows)
	return bat
}

func TestMysqlProtocolWriteUsesBatchLogicalRowCount(t *testing.T) {
	sv, err := getSystemVariables("test/system_vars_config.toml")
	require.NoError(t, err)
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	setPu("", pu)
	setSessionAlloc("", NewLeakCheckAllocator())

	ioSession, err := NewIOSession(&testConn{}, pu, "")
	require.NoError(t, err)
	proto := NewMysqlClientProtocol("", 0, ioSession, 1024, sv)
	t.Cleanup(proto.Close)

	ctx := context.Background()
	ses := NewSession(ctx, "", proto, nil)
	ses.SetCmd(COM_QUERY)
	ses.mrs = &MysqlResultSet{}
	for _, name := range []string{"projection_value", "total"} {
		column := &MysqlColumn{}
		column.SetName(name)
		column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		ses.mrs.AddColumn(column)
	}
	proto.ses = ses
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses}

	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	tests := []struct {
		name  string
		rows  int
		first func() *vector.Vector
	}{
		{
			name: "flat control",
			rows: 3,
			first: func() *vector.Vector {
				return testutil.MakeInt64Vector([]int64{7, 8, 9}, nil, mp)
			},
		},
		{
			name: "ordinary constant",
			rows: 3,
			first: func() *vector.Vector {
				vec, newErr := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 3, mp)
				require.NoError(t, newErr)
				return vec
			},
		},
		{
			name: "broadcast constant",
			rows: 3,
			first: func() *vector.Vector {
				vec, newErr := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
				require.NoError(t, newErr)
				return vec
			},
		},
		{
			name: "broadcast constant null",
			rows: 2,
			first: func() *vector.Vector {
				return vector.NewConstNull(types.T_int64.ToType(), 1, mp)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bat := frontendResultBatch(mp, test.first(), test.rows)
			defer bat.Clean(mp)

			before := proto.tcpConn.sequenceId
			require.NoError(t, proto.Write(execCtx, nil, bat))
			require.Equal(t, uint8(test.rows), proto.tcpConn.sequenceId-before)
		})
	}
}

func TestGetDataFromPipelineUsesBatchLogicalRowCount(t *testing.T) {
	ctx := statistic.ContextWithStatsInfo(context.Background(), &statistic.StatsInfo{})
	ses := &Session{feSessionImpl: feSessionImpl{respr: &NullResp{}}}
	execCtx := &ExecCtx{reqCtx: ctx, ses: ses}
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })

	first, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
	require.NoError(t, err)
	bat := frontendResultBatch(mp, first, 3)
	require.NoError(t, getDataFromPipeline(ses, execCtx, bat, nil))
	require.Equal(t, int64(3), ses.sentRows.Load())
	bat.Clean(mp)

	nullBatch := frontendResultBatch(mp, vector.NewConstNull(types.T_int64.ToType(), 1, mp), 2)
	require.NoError(t, getDataFromPipeline(ses, execCtx, nullBatch, nil))
	require.Equal(t, int64(5), ses.sentRows.Load())
	nullBatch.Clean(mp)
}

func TestSaveBatchUsesBatchLogicalRowCount(t *testing.T) {
	ses := &Session{limitResultSize: -1}
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })

	first, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
	require.NoError(t, err)
	bat := frontendResultBatch(mp, first, 3)
	require.NoError(t, saveBatch(context.Background(), ses, bat))
	require.Equal(t, uint64(3), ses.queryRowCount)
	bat.Clean(mp)

	nullBatch := frontendResultBatch(mp, vector.NewConstNull(types.T_int64.ToType(), 1, mp), 2)
	require.NoError(t, saveBatch(context.Background(), ses, nullBatch))
	require.Equal(t, uint64(5), ses.queryRowCount)
	nullBatch.Clean(mp)
}

func TestSaveBatchValidatesBeforeAccountingAndSizing(t *testing.T) {
	tests := []struct {
		name string
		bat  func() *batch.Batch
	}{
		{
			name: "nil batch",
			bat:  func() *batch.Batch { return nil },
		},
		{
			name: "negative row count",
			bat: func() *batch.Batch {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				bat.SetRowCount(-1)
				return bat
			},
		},
		{
			name: "zero vectors",
			bat: func() *batch.Batch {
				bat := batch.NewWithSize(0)
				bat.SetRowCount(1)
				return bat
			},
		},
		{
			name: "nil vector",
			bat: func() *batch.Batch {
				bat := batch.NewWithSize(1)
				bat.SetRowCount(1)
				return bat
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ses := &Session{limitResultSize: -1, queryRowCount: 41}
			bat := test.bat()
			err := saveBatch(context.Background(), ses, bat)
			require.Error(t, err)
			require.Equal(t, uint64(41), ses.queryRowCount)
		})
	}
}

func TestNormalizeQueryResultBatchForPersistence(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })

	t.Run("aligned vectors are a no-op", func(t *testing.T) {
		constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 3, mp)
		require.NoError(t, err)
		bat := batch.NewWithSize(2)
		bat.Attrs = []string{"constant", "flat"}
		bat.Vecs[0] = constant
		bat.Vecs[1] = testutil.MakeInt64Vector([]int64{10, 20, 30}, nil, mp)
		bat.SetRowCount(3)
		defer bat.Clean(mp)

		normalized, release, err := normalizeQueryResultBatchForPersistence(bat, mp)
		require.NoError(t, err)
		require.Same(t, bat, normalized)
		require.Nil(t, release)
		require.Same(t, bat.Vecs[0], normalized.Vecs[0])
		require.Same(t, bat.Vecs[1], normalized.Vecs[1])
	})

	t.Run("first and non-first constants are normalized", func(t *testing.T) {
		firstConstant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
		require.NoError(t, err)
		laterConstant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(11), 1, mp)
		require.NoError(t, err)
		constantNull := vector.NewConstNull(types.T_int64.ToType(), 1, mp)
		bat := batch.NewWithSize(4)
		bat.Attrs = []string{"first_constant", "flat", "later_constant", "constant_null"}
		bat.Vecs[0] = firstConstant
		bat.Vecs[1] = testutil.MakeInt64Vector([]int64{10, 20, 30}, nil, mp)
		bat.Vecs[2] = laterConstant
		bat.Vecs[3] = constantNull
		bat.SetRowCount(3)
		defer bat.Clean(mp)
		before := mp.CurrNB()

		normalized, release, err := normalizeQueryResultBatchForPersistence(bat, mp)
		require.NoError(t, err)
		require.NotSame(t, bat, normalized)
		require.NotNil(t, release)
		require.Equal(t, bat.Attrs, normalized.Attrs)
		require.NotSame(t, bat.Vecs[0], normalized.Vecs[0])
		require.Same(t, bat.Vecs[1], normalized.Vecs[1])
		require.NotSame(t, bat.Vecs[2], normalized.Vecs[2])
		require.NotSame(t, bat.Vecs[3], normalized.Vecs[3])
		for _, vec := range normalized.Vecs {
			require.Equal(t, 3, vec.Length())
		}
		require.Equal(t, []int{1, 3, 1, 1}, []int{
			bat.Vecs[0].Length(), bat.Vecs[1].Length(), bat.Vecs[2].Length(), bat.Vecs[3].Length(),
		})
		require.Equal(t, 3, bat.RowCount())
		require.Equal(t, int64(7), vector.GetFixedAtWithTypeCheck[int64](normalized.Vecs[0], 2))
		require.Equal(t, int64(11), vector.GetFixedAtWithTypeCheck[int64](normalized.Vecs[2], 2))
		require.True(t, normalized.Vecs[3].IsNull(2))

		release()
		require.Equal(t, before, mp.CurrNB())
		require.Equal(t, []int{1, 3, 1, 1}, []int{
			bat.Vecs[0].Length(), bat.Vecs[1].Length(), bat.Vecs[2].Length(), bat.Vecs[3].Length(),
		})
	})

	t.Run("zero rows shorten only the persistence view", func(t *testing.T) {
		constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
		require.NoError(t, err)
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = testutil.MakeInt64Vector(nil, nil, mp)
		bat.Vecs[1] = constant
		bat.SetRowCount(0)
		defer bat.Clean(mp)
		before := mp.CurrNB()

		normalized, release, err := normalizeQueryResultBatchForPersistence(bat, mp)
		require.NoError(t, err)
		require.NotNil(t, release)
		require.Same(t, bat.Vecs[0], normalized.Vecs[0])
		require.NotSame(t, bat.Vecs[1], normalized.Vecs[1])
		require.Zero(t, normalized.RowCount())
		require.Zero(t, normalized.Vecs[0].Length())
		require.Zero(t, normalized.Vecs[1].Length())
		require.Equal(t, 1, bat.Vecs[1].Length())

		release()
		require.Equal(t, before, mp.CurrNB())
		require.Equal(t, 1, bat.Vecs[1].Length())
	})

	t.Run("invalid row shapes are rejected without mutation", func(t *testing.T) {
		tests := []struct {
			name string
			vec  func() *vector.Vector
		}{
			{
				name: "short flat",
				vec: func() *vector.Vector {
					return testutil.MakeInt64Vector([]int64{7}, nil, mp)
				},
			},
			{
				name: "empty constant",
				vec: func() *vector.Vector {
					return vector.NewConstNull(types.T_int64.ToType(), 0, mp)
				},
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				vec := test.vec()
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vec
				bat.SetRowCount(3)
				defer bat.Clean(mp)
				before := mp.CurrNB()
				originalLength := vec.Length()

				normalized, release, err := normalizeQueryResultBatchForPersistence(bat, mp)
				require.Error(t, err)
				require.Nil(t, normalized)
				require.Nil(t, release)
				require.Equal(t, originalLength, vec.Length())
				require.Equal(t, 3, bat.RowCount())
				require.Equal(t, before, mp.CurrNB())
			})
		}
	})

	t.Run("duplicate failure releases prior replacements", func(t *testing.T) {
		limited, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
		require.NoError(t, err)
		defer mpool.DeleteMPool(limited)

		payload := make([]byte, 600<<10)
		bat := batch.NewWithSize(2)
		for i := range bat.Vecs {
			vec := vector.NewOffHeapVecWithType(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(vec, payload, false, mp))
			vec.SetClass(vector.CONSTANT)
			bat.Vecs[i] = vec
		}
		bat.SetRowCount(3)
		defer bat.Clean(mp)

		normalized, release, err := normalizeQueryResultBatchForPersistence(bat, limited)
		require.Error(t, err)
		require.Nil(t, normalized)
		require.Nil(t, release)
		require.Zero(t, limited.CurrNB())
		require.Equal(t, 3, bat.RowCount())
		require.Equal(t, 1, bat.Vecs[0].Length())
		require.Equal(t, 1, bat.Vecs[1].Length())
	})
}

type queryResultFailingWriteFS struct {
	fileservice.FileService
}

func (fs *queryResultFailingWriteFS) Write(context.Context, fileservice.IOVector) error {
	return errors.New("injected query result write failure")
}

func TestSaveBatchReleasesNormalizedVectorsOnWriteError(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.limitResultSize = 64
	ses.SetStmtId(uuid.New())

	pu := getPu(ses.GetService())
	originalFS := pu.FileService
	pu.FileService = &queryResultFailingWriteFS{FileService: originalFS}
	defer func() { pu.FileService = originalFS }()

	constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, ses.GetMemPool())
	require.NoError(t, err)
	bat := frontendResultBatch(ses.GetMemPool(), constant, 3)
	defer bat.Clean(ses.GetMemPool())
	before := ses.GetMemPool().CurrNB()

	err = saveBatch(context.Background(), ses, bat)
	require.ErrorContains(t, err, "injected query result write failure")
	require.Equal(t, before, ses.GetMemPool().CurrNB())
	require.Equal(t, 3, bat.RowCount())
	require.Equal(t, 1, bat.Vecs[0].Length())
	require.Equal(t, 3, bat.Vecs[1].Length())
}

func TestSaveBatchPersistsBatchLogicalCardinalityForResultScan(t *testing.T) {
	tests := []struct {
		name      string
		rows      int
		makeBatch func(*mpool.MPool) *batch.Batch
		check     func(*testing.T, *batch.Batch)
	}{
		{
			name: "broadcast constant first",
			rows: 3,
			makeBatch: func(mp *mpool.MPool) *batch.Batch {
				constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
				require.NoError(t, err)
				return frontendResultBatch(mp, constant, 3)
			},
			check: func(t *testing.T, bat *batch.Batch) {
				require.Equal(t, []int64{7, 7, 7}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0]))
				require.Equal(t, []int64{10, 20, 30}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[1]))
			},
		},
		{
			name: "broadcast constant null first",
			rows: 3,
			makeBatch: func(mp *mpool.MPool) *batch.Batch {
				return frontendResultBatch(mp, vector.NewConstNull(types.T_int64.ToType(), 1, mp), 3)
			},
			check: func(t *testing.T, bat *batch.Batch) {
				for row := 0; row < 3; row++ {
					require.True(t, bat.Vecs[0].IsNull(uint64(row)))
				}
				require.Equal(t, []int64{10, 20, 30}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[1]))
			},
		},
		{
			name: "broadcast constant after flat first",
			rows: 3,
			makeBatch: func(mp *mpool.MPool) *batch.Batch {
				constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
				require.NoError(t, err)
				bat := batch.NewWithSize(2)
				bat.Vecs[0] = testutil.MakeInt64Vector([]int64{10, 20, 30}, nil, mp)
				bat.Vecs[1] = constant
				bat.SetRowCount(3)
				return bat
			},
			check: func(t *testing.T, bat *batch.Batch) {
				require.Equal(t, []int64{10, 20, 30}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0]))
				require.Equal(t, []int64{7, 7, 7}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[1]))
			},
		},
		{
			name: "zero logical rows",
			rows: 0,
			makeBatch: func(mp *mpool.MPool) *batch.Batch {
				constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
				require.NoError(t, err)
				return frontendResultBatch(mp, constant, 0)
			},
			check: func(t *testing.T, bat *batch.Batch) {
				require.Zero(t, bat.Vecs[0].Length())
				require.Zero(t, bat.Vecs[1].Length())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			ses.limitResultSize = 64
			stmtID := uuid.New()
			ses.SetStmtId(stmtID)

			input := test.makeBatch(ses.GetMemPool())
			defer input.Clean(ses.GetMemPool())
			require.NoError(t, saveBatch(context.Background(), ses, input))

			path := catalog.BuildQueryResultPath(
				ses.GetTenantInfo().GetTenant(), stmtID.String(), 1)
			param := &external.ExternalParam{
				ExParamConst: external.ExParamConst{
					Attrs: []plan.ExternAttr{
						{ColName: "c0", ColIndex: 0},
						{ColName: "c1", ColIndex: 1},
					},
					Cols: []*plan.ColDef{
						{Typ: plan.Type{Id: int32(types.T_int64)}},
						{Typ: plan.Type{Id: int32(types.T_int64)}},
					},
					Extern: &tree.ExternParam{ExParam: tree.ExParam{
						FileService: getPu(ses.GetService()).FileService,
					}},
				},
				ExParam: external.ExParam{
					Fileparam: &external.ExFileparam{Filepath: path},
					Filter:    &external.FilterParam{},
				},
			}
			proc := testutil.NewProcess(t)
			defer proc.Free()
			reader := external.NewZonemapReader(param, proc)
			_, err := reader.Open(param, proc)
			require.NoError(t, err)

			output := batch.NewWithSize(2)
			output.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			output.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			defer output.Clean(proc.Mp())
			finished, err := reader.ReadBatch(
				context.Background(), output, proc,
				process.NewAnalyzer(0, false, false, "result scan"),
			)
			require.NoError(t, err)
			require.True(t, finished)
			require.NoError(t, reader.Close())
			require.Equal(t, test.rows, output.RowCount())
			require.Equal(t, test.rows, output.Vecs[0].Length())
			require.Equal(t, test.rows, output.Vecs[1].Length())
			test.check(t, output)
		})
	}
}
