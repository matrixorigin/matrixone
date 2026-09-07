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

package function

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestSequenceDatabase(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	database, null := sequenceDatabase("session_db", nil, 0)
	require.Equal(t, "session_db", database)
	require.False(t, null)

	storedDatabase, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("view_db"), 1, proc.Mp())
	require.NoError(t, err)
	defer storedDatabase.Free(proc.Mp())
	database, null = sequenceDatabase("session_db", vector.GenerateFunctionStrParameter(storedDatabase), 0)
	require.Equal(t, "view_db", database)
	require.False(t, null)

	nullDatabase := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
	defer nullDatabase.Free(proc.Mp())
	database, null = sequenceDatabase("session_db", vector.GenerateFunctionStrParameter(nullDatabase), 0)
	require.Empty(t, database)
	require.True(t, null)
}

func TestCurrvalResolvesDatabaseOncePerBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)

	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.InitSeq()
	proc.Base.TxnOperator = txn
	proc.Base.SessionInfo.Database = "session_db"
	proc.Base.SessionInfo.SeqCurValues[1] = "42"
	proc.Ctx = context.WithValue(proc.Ctx, defines.EngineKey{}, eng)

	eng.EXPECT().Database(gomock.Any(), "session_db", txn).Return(db, nil).Times(1)
	db.EXPECT().Relation(gomock.Any(), "seq", nil).Return(rel, nil).Times(4)
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).Times(4)

	input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("seq"), 4, proc.Mp())
	require.NoError(t, err)
	defer input.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(4))

	require.NoError(t, Currval([]*vector.Vector{input}, result, proc, 4, nil))
	for i := 0; i < 4; i++ {
		require.Equal(t, []byte("42"), result.GetResultVector().GetBytesAt(i))
	}
}

func TestSequenceHiddenOverloadExecutors(t *testing.T) {
	for _, test := range []struct {
		name       string
		functionID int
		overloadID int
		args       []types.T
	}{
		{name: "nextval", functionID: NEXTVAL, overloadID: 1, args: []types.T{types.T_varchar, types.T_varchar}},
		{name: "setval", functionID: SETVAL, overloadID: 2, args: []types.T{types.T_varchar, types.T_varchar, types.T_bool, types.T_varchar}},
		{name: "currval", functionID: CURRVAL, overloadID: 1, args: []types.T{types.T_varchar, types.T_varchar}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fn := allSupportedFunctions[test.functionID]
			require.Equal(t, test.functionID, fn.functionId)
			require.Greater(t, len(fn.Overloads), test.overloadID)
			overload := fn.Overloads[test.overloadID]
			require.Equal(t, test.args, overload.args)
			require.NotNil(t, overload.newOp())
		})
	}
}

type sequenceRelationStub struct {
	engine.Relation
	defs []engine.TableDef
	err  error
}

func (s *sequenceRelationStub) TableDefs(context.Context) ([]engine.TableDef, error) {
	return s.defs, s.err
}

func sequenceProperties(properties ...engine.Property) *engine.PropertiesDef {
	return &engine.PropertiesDef{Properties: properties}
}

func TestRequireSequence(t *testing.T) {
	var typedNil *engine.PropertiesDef

	tests := []struct {
		name string
		defs []engine.TableDef
		want bool
	}{
		{
			name: "kind is not positional",
			defs: []engine.TableDef{
				&engine.VersionDef{Version: 1},
				sequenceProperties(
					engine.Property{Key: "unrelated", Value: catalog.SystemSequenceRel},
					engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel},
				),
			},
			want: true,
		},
		{
			name: "ordinary relation",
			defs: []engine.TableDef{
				sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemOrdinaryRel}),
			},
		},
		{
			name: "missing kind",
			defs: []engine.TableDef{
				sequenceProperties(engine.Property{Key: "not-relkind", Value: catalog.SystemSequenceRel}),
			},
		},
		{
			name: "empty definitions",
		},
		{
			name: "empty properties",
			defs: []engine.TableDef{sequenceProperties()},
		},
		{
			name: "non-property definitions only",
			defs: []engine.TableDef{&engine.VersionDef{Version: 1}},
		},
		{
			name: "typed nil properties",
			defs: []engine.TableDef{typedNil},
		},
		{
			name: "nil definition",
			defs: []engine.TableDef{nil},
		},
		{
			name: "conflicting markers",
			defs: []engine.TableDef{
				sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel}),
				sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemOrdinaryRel}),
			},
		},
		{
			name: "reversed conflicting markers",
			defs: []engine.TableDef{
				sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemOrdinaryRel}),
				sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel}),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := requireSequence(context.Background(), &sequenceRelationStub{defs: test.defs})
			if test.want {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, "internal error: Table input is not a sequence")
			}
		})
	}
}

func TestRequireSequencePropagatesTableDefsError(t *testing.T) {
	want := errors.New("table definitions unavailable")
	err := requireSequence(context.Background(), &sequenceRelationStub{err: want})
	require.ErrorIs(t, err, want)
}

type countingSequenceSQLHelper struct {
	calls int
}

func (h *countingSequenceSQLHelper) GetCompilerContext() any {
	return nil
}

func (h *countingSequenceSQLHelper) ExecSql(string) ([][]interface{}, error) {
	h.calls++
	return nil, nil
}

func (h *countingSequenceSQLHelper) ExecSqlWithCtx(context.Context, string) ([][]interface{}, error) {
	h.calls++
	return nil, nil
}

func (h *countingSequenceSQLHelper) GetSubscriptionMeta(string) (*plan.SubscriptionMeta, error) {
	h.calls++
	return nil, nil
}

func TestNextvalRejectsNonSequenceBeforeSQL(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.InitSeq()
	proc.Base.TxnOperator = txn
	proc.Base.SessionInfo.SeqAddValues[7] = "sentinel"
	proc.Base.SessionInfo.SeqLastValue[0] = "last"
	sql := new(countingSequenceSQLHelper)
	proc.Base.SessionInfo.SqlHelper = sql

	eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "fake", nil).Return(rel, nil)
	rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
		sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemOrdinaryRel}),
	}, nil)

	_, err := nextval("fake", "db", proc, eng, txn)
	require.EqualError(t, err, "internal error: Table input is not a sequence")
	require.Zero(t, sql.calls)
	require.Equal(t, "sentinel", proc.Base.SessionInfo.SeqAddValues[7])
	require.Equal(t, "last", proc.Base.SessionInfo.SeqLastValue[0])
}

func TestSetvalRejectsNonSequenceBeforeSQL(t *testing.T) {
	ctrl := gomock.NewController(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.InitSeq()
	proc.Base.TxnOperator = txn
	proc.Base.SessionInfo.SeqAddValues[7] = "sentinel"
	proc.Base.SessionInfo.SeqLastValue[0] = "last"
	sql := new(countingSequenceSQLHelper)
	proc.Base.SessionInfo.SqlHelper = sql

	eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "fake", nil).Return(rel, nil)
	rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
		sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemOrdinaryRel}),
	}, nil)

	_, err := setval("fake", "20", true, "db", proc, txn, eng)
	require.EqualError(t, err, "internal error: Table input is not a sequence")
	require.Zero(t, sql.calls)
	require.Equal(t, "sentinel", proc.Base.SessionInfo.SeqAddValues[7])
	require.Equal(t, "last", proc.Base.SessionInfo.SeqLastValue[0])
}

func TestSequenceSQLSharedTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Base.TxnOperator = mock_frontend.NewMockTxnOperator(ctrl)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 42)
	proc.Base.IsFrontend = true
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "lower_case_table_names" {
			return int64(0), nil
		}
		return nil, nil
	})
	exec := mock_executor.NewMockSQLExecutor(ctrl)
	rt := runtime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.InternalSQLExecutor)
	rt.SetGlobalVariables(runtime.InternalSQLExecutor, exec)
	t.Cleanup(func() {
		rt.CompareAndDeleteGlobalVariables(runtime.InternalSQLExecutor, exec)
		if hadPrevious {
			rt.SetGlobalVariables(runtime.InternalSQLExecutor, previous)
		}
	})
	retryErr := moerr.NewTxnNeedRetryNoCtx()
	exec.EXPECT().Exec(gomock.Any(), "select metadata for update", gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ string, opts executor.Options) (executor.Result, error) {
			require.Equal(t, true, ctx.Value(defines.BgKey{}))
			require.Same(t, proc.GetTxnOperator(), opts.Txn())
			require.True(t, opts.DisableIncrStatement())
			require.True(t, opts.HasAccountID())
			require.Equal(t, uint32(42), opts.AccountID())
			require.Equal(t, "view_db", opts.Database())
			require.Equal(t, int64(0), opts.LowerCaseTableNames())
			require.Same(t, time.UTC, opts.GetTimeZone())
			require.True(t, opts.IsFrontend())
			return executor.Result{}, retryErr
		}).Times(1)
	_, err := sequenceSQL(proc, "view_db", "select metadata for update")
	require.ErrorIs(t, err, retryErr, "the owning outer statement must retry")
	require.Equal(t, "`d``b`.`s``q`", sequenceTableName("d`b", "s`q"))
}

func TestSequenceSQLStopsOnCanceledContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Base.TxnOperator = mock_frontend.NewMockTxnOperator(ctrl)
	ctx, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = ctx

	exec := mock_executor.NewMockSQLExecutor(ctrl)
	rt := runtime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.InternalSQLExecutor)
	rt.SetGlobalVariables(runtime.InternalSQLExecutor, exec)
	t.Cleanup(func() {
		rt.CompareAndDeleteGlobalVariables(runtime.InternalSQLExecutor, exec)
		if hadPrevious {
			rt.SetGlobalVariables(runtime.InternalSQLExecutor, previous)
		}
	})
	exec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	_, err := sequenceSQL(proc, "db", "select metadata for update")
	require.ErrorIs(t, err, context.Canceled)
}

func TestSequenceSQLMetadata(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		rows      int
		wrongType bool
		wantError bool
	}{
		{name: "one row", rows: 1},
		{name: "multiple rows", rows: 2, wantError: true},
		{name: "wrong flag type", rows: 1, wrongType: true, wantError: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.Base.TxnOperator = mock_frontend.NewMockTxnOperator(ctrl)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 42)
			columnTypes := []types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(), types.T_bool.ToType(), types.T_bool.ToType()}
			if scenario.wrongType {
				columnTypes[6] = types.T_int64.ToType()
			}
			before := proc.Mp().CurrNB()
			data := executor.NewMemResult(columnTypes, proc.Mp())
			data.NewBatchWithRowCount(scenario.rows)
			for i := range columnTypes {
				if columnTypes[i].Oid == types.T_bool {
					require.NoError(t, executor.AppendFixedRows(data, i, make([]bool, scenario.rows)))
				} else {
					require.NoError(t, executor.AppendFixedRows(data, i, make([]int64, scenario.rows)))
				}
			}
			exec := executor.NewMemExecutor(func(string) (executor.Result, error) { return data.GetResult(), nil })
			rt := runtime.ServiceRuntime(proc.GetService())
			previous, hadPrevious := rt.GetGlobalVariables(runtime.InternalSQLExecutor)
			rt.SetGlobalVariables(runtime.InternalSQLExecutor, exec)
			t.Cleanup(func() {
				rt.CompareAndDeleteGlobalVariables(runtime.InternalSQLExecutor, exec)
				if hadPrevious {
					rt.SetGlobalVariables(runtime.InternalSQLExecutor, previous)
				}
			})
			rows, err := sequenceSQL(proc, "db", "select metadata for update")
			if scenario.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0), int64(0), int64(0), int64(0), int64(0), false, false}}, rows)
			}
			require.Equal(t, before, proc.Mp().CurrNB(), "executor result must be released even on metadata errors")
		})
	}
}
