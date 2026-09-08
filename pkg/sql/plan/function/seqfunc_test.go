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
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// sequenceGateProbeContext makes admission observable without changing the
// production gate API. AcquireSequence evaluates Context.Done before it can
// block on the shared permit, so this gives concurrent-function tests a
// deterministic proof that the sibling actually reached the gate.
type sequenceGateProbeContext struct {
	context.Context
	doneCalled chan struct{}
	once       sync.Once
}

func (c *sequenceGateProbeContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.doneCalled) })
	return c.Context.Done()
}

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

func TestNextvalChildProcessesSerializeMetadataAndSessionPublication(t *testing.T) {
	ctrl := gomock.NewController(t)
	parent := testutil.NewProcess(t)
	defer parent.Free()
	parent.InitSeq()
	parent.Base.SessionInfo.Database = "db"
	parent.Base.TxnOperator = mock_frontend.NewMockTxnOperator(ctrl)
	parent.Ctx = defines.AttachAccountId(parent.Ctx, 42)

	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", parent.Base.TxnOperator).Return(db, nil).Times(2)
	db.EXPECT().Relation(gomock.Any(), "seq", nil).Return(rel, nil).Times(2)
	rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
		sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel}),
	}, nil).Times(2)
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(7)).Times(2)
	parent.Ctx = context.WithValue(parent.Ctx, defines.EngineKey{}, eng)

	type sequenceMetadataState struct {
		sync.Mutex
		last             int64
		called           bool
		selectCalls      int
		firstSelectReady chan struct{}
		releaseFirst     chan struct{}
	}
	state := &sequenceMetadataState{
		last:             1,
		firstSelectReady: make(chan struct{}),
		releaseFirst:     make(chan struct{}),
	}
	var releaseFirstOnce sync.Once
	releaseFirst := func() {
		releaseFirstOnce.Do(func() { close(state.releaseFirst) })
	}
	t.Cleanup(releaseFirst)
	makeMetadataResult := func(last int64, called bool) (executor.Result, error) {
		mem := executor.NewMemResult([]types.Type{
			types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
			types.T_int64.ToType(), types.T_int64.ToType(), types.T_bool.ToType(),
			types.T_bool.ToType(),
		}, parent.Mp())
		mem.NewBatchWithRowCount(1)
		if err := executor.AppendFixedRows(mem, 0, []int64{last}); err != nil {
			return executor.Result{}, err
		}
		if err := executor.AppendFixedRows(mem, 1, []int64{1}); err != nil {
			return executor.Result{}, err
		}
		if err := executor.AppendFixedRows(mem, 2, []int64{100}); err != nil {
			return executor.Result{}, err
		}
		if err := executor.AppendFixedRows(mem, 3, []int64{1}); err != nil {
			return executor.Result{}, err
		}
		if err := executor.AppendFixedRows(mem, 4, []int64{1}); err != nil {
			return executor.Result{}, err
		}
		if err := executor.AppendFixedRows(mem, 5, []bool{false}); err != nil {
			return executor.Result{}, err
		}
		if err := executor.AppendFixedRows(mem, 6, []bool{called}); err != nil {
			return executor.Result{}, err
		}
		return mem.GetResult(), nil
	}
	sqlExec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		normalized := strings.ToLower(strings.TrimSpace(sql))
		if strings.HasPrefix(normalized, "select last_seq_num") {
			state.Lock()
			state.selectCalls++
			selectCall := state.selectCalls
			last, called := state.last, state.called
			state.Unlock()
			switch selectCall {
			case 1:
				close(state.firstSelectReady)
				<-state.releaseFirst
			}
			return makeMetadataResult(last, called)
		}
		if strings.HasPrefix(normalized, "update") {
			state.Lock()
			if strings.Contains(normalized, "is_called = true") {
				state.called = true
			} else {
				pos := strings.LastIndex(normalized, "=")
				value, parseErr := strconv.ParseInt(strings.TrimSpace(normalized[pos+1:]), 10, 64)
				if parseErr != nil {
					state.Unlock()
					return executor.Result{}, parseErr
				}
				state.last = value
			}
			state.Unlock()
			return executor.Result{}, nil
		}
		return executor.Result{}, fmt.Errorf("unexpected sequence SQL: %s", sql)
	})
	runtimeService := runtime.ServiceRuntime(parent.GetService())
	previous, hadPrevious := runtimeService.GetGlobalVariables(runtime.InternalSQLExecutor)
	runtimeService.SetGlobalVariables(runtime.InternalSQLExecutor, sqlExec)
	t.Cleanup(func() {
		runtimeService.CompareAndDeleteGlobalVariables(runtime.InternalSQLExecutor, sqlExec)
		if hadPrevious {
			runtimeService.SetGlobalVariables(runtime.InternalSQLExecutor, previous)
		}
	})

	call := func(proc *process.Process) (string, error) {
		input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("seq"), 1, proc.Mp())
		if err != nil {
			return "", err
		}
		defer input.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
		defer result.Free()
		if err = result.PreExtendAndReset(1); err != nil {
			return "", err
		}
		if err = Nextval([]*vector.Vector{input}, result, proc, 1, nil); err != nil {
			return "", err
		}
		return string(result.GetResultVector().GetBytesAt(0)), nil
	}

	left := parent.NewContextChildProc(0)
	right := parent.NewContextChildProc(0)
	leftResult := make(chan struct {
		value string
		err   error
	}, 1)
	leftDone := make(chan struct{})
	go func() {
		defer close(leftDone)
		value, err := call(left)
		leftResult <- struct {
			value string
			err   error
		}{value: value, err: err}
	}()
	select {
	case <-state.firstSelectReady:
	case <-time.After(time.Second):
		t.Fatal("first NEXTVAL did not reach metadata read")
	}

	rightGateCalled := make(chan struct{})
	right.Ctx = &sequenceGateProbeContext{Context: right.Ctx, doneCalled: rightGateCalled}
	rightResult := make(chan struct {
		value string
		err   error
	}, 1)
	rightDone := make(chan struct{})
	t.Cleanup(func() {
		releaseFirst()
		select {
		case <-leftDone:
		case <-time.After(time.Second):
		}
		select {
		case <-rightDone:
		case <-time.After(time.Second):
		}
	})
	go func() {
		defer close(rightDone)
		value, err := call(right)
		rightResult <- struct {
			value string
			err   error
		}{value: value, err: err}
	}()
	// The first operation is stopped between SELECT and UPDATE. The second
	// operation must reach the gate while the first operation still owns the
	// shared permit. Observing Context.Done is a deterministic admission
	// barrier; a second metadata SQL call before the first UPDATE would
	// therefore be a gate violation, not merely a scheduling guess.
	select {
	case <-rightGateCalled:
	case <-time.After(time.Second):
		t.Fatal("sibling NEXTVAL did not reach the sequence gate")
	}
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	probeRelease, probeErr := parent.AcquireSequence(probeCtx)
	probeCancel()
	if probeErr == nil {
		probeRelease()
		t.Fatal("sequence gate was available while first operation was blocked")
	}
	require.ErrorIs(t, probeErr, context.DeadlineExceeded)
	select {
	case <-rightResult:
		t.Fatal("sibling NEXTVAL completed before first operation completed")
	default:
	}
	releaseFirst()

	var first, second struct {
		value string
		err   error
	}
	select {
	case first = <-leftResult:
	case <-time.After(time.Second):
		t.Fatal("first NEXTVAL did not finish")
	}
	select {
	case second = <-rightResult:
	case <-time.After(time.Second):
		t.Fatal("second NEXTVAL did not finish")
	}
	select {
	case <-leftDone:
	case <-time.After(time.Second):
		t.Fatal("first NEXTVAL worker did not terminate")
	}
	select {
	case <-rightDone:
	case <-time.After(time.Second):
		t.Fatal("second NEXTVAL worker did not terminate")
	}
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.ElementsMatch(t, []string{"1", "2"}, []string{first.value, second.value})
	state.Lock()
	require.Equal(t, int64(2), state.last)
	require.True(t, state.called)
	require.Equal(t, 2, state.selectCalls)
	state.Unlock()
	require.Equal(t, "2", parent.GetSessionInfo().SeqLastValue[0])
}

func TestMixedSequenceOperationsSerializeStatePublication(t *testing.T) {
	ctrl := gomock.NewController(t)
	parent := testutil.NewProcess(t)
	defer parent.Free()
	parent.InitSeq()
	parent.Base.SessionInfo.Database = "db"
	parent.Base.SessionInfo.SeqAddValues[7] = "1"
	parent.Base.SessionInfo.SeqLastValue[0] = "1"
	parent.Base.TxnOperator = mock_frontend.NewMockTxnOperator(ctrl)
	parent.Ctx = defines.AttachAccountId(parent.Ctx, 42)

	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", parent.Base.TxnOperator).Return(db, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "seq", nil).Return(rel, nil).AnyTimes()
	rel.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
		sequenceProperties(engine.Property{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel}),
	}, nil).AnyTimes()
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(7)).AnyTimes()
	parent.Ctx = context.WithValue(parent.Ctx, defines.EngineKey{}, eng)

	type metadataState struct {
		sync.Mutex
		last          int64
		called        bool
		updateEntered chan struct{}
		releaseUpdate chan struct{}
		updateOnce    sync.Once
	}
	state := &metadataState{
		last:          1,
		called:        true,
		updateEntered: make(chan struct{}),
		releaseUpdate: make(chan struct{}),
	}
	makeMetadataResult := func(last int64, called bool) (executor.Result, error) {
		mem := executor.NewMemResult([]types.Type{
			types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
			types.T_int64.ToType(), types.T_int64.ToType(), types.T_bool.ToType(),
			types.T_bool.ToType(),
		}, parent.Mp())
		mem.NewBatchWithRowCount(1)
		for i, values := range []any{
			[]int64{last}, []int64{0}, []int64{100}, []int64{1}, []int64{1},
			[]bool{false}, []bool{called},
		} {
			if i < 5 {
				if err := executor.AppendFixedRows(mem, i, values.([]int64)); err != nil {
					return executor.Result{}, err
				}
			} else if err := executor.AppendFixedRows(mem, i, values.([]bool)); err != nil {
				return executor.Result{}, err
			}
		}
		return mem.GetResult(), nil
	}
	sqlExec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		normalized := strings.ToLower(strings.TrimSpace(sql))
		if strings.HasPrefix(normalized, "select last_seq_num") {
			state.Lock()
			last, called := state.last, state.called
			state.Unlock()
			return makeMetadataResult(last, called)
		}
		if strings.HasPrefix(normalized, "update") {
			if strings.Contains(normalized, "last_seq_num") {
				state.updateOnce.Do(func() { close(state.updateEntered) })
				<-state.releaseUpdate
				pos := strings.LastIndex(normalized, "=")
				value, err := strconv.ParseInt(strings.TrimSpace(normalized[pos+1:]), 10, 64)
				if err != nil {
					return executor.Result{}, err
				}
				state.Lock()
				state.last = value
				state.Unlock()
			}
			return executor.Result{}, nil
		}
		return executor.Result{}, fmt.Errorf("unexpected sequence SQL: %s", sql)
	})
	runtimeService := runtime.ServiceRuntime(parent.GetService())
	previous, hadPrevious := runtimeService.GetGlobalVariables(runtime.InternalSQLExecutor)
	runtimeService.SetGlobalVariables(runtime.InternalSQLExecutor, sqlExec)
	t.Cleanup(func() {
		select {
		case <-state.releaseUpdate:
		default:
			close(state.releaseUpdate)
		}
		runtimeService.CompareAndDeleteGlobalVariables(runtime.InternalSQLExecutor, sqlExec)
		if hadPrevious {
			runtimeService.SetGlobalVariables(runtime.InternalSQLExecutor, previous)
		}
	})

	setvalCall := func(proc *process.Process) (string, error) {
		table, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("seq"), 1, proc.Mp())
		if err != nil {
			return "", err
		}
		defer table.Free(proc.Mp())
		value, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("10"), 1, proc.Mp())
		if err != nil {
			return "", err
		}
		defer value.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
		defer result.Free()
		if err = result.PreExtendAndReset(1); err != nil {
			return "", err
		}
		if err = Setval([]*vector.Vector{table, value}, result, proc, 1, nil); err != nil {
			return "", err
		}
		return string(result.GetResultVector().GetBytesAt(0)), nil
	}
	currvalCall := func(proc *process.Process) (string, error) {
		input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("seq"), 1, proc.Mp())
		if err != nil {
			return "", err
		}
		defer input.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
		defer result.Free()
		if err = result.PreExtendAndReset(1); err != nil {
			return "", err
		}
		if err = Currval([]*vector.Vector{input}, result, proc, 1, nil); err != nil {
			return "", err
		}
		return string(result.GetResultVector().GetBytesAt(0)), nil
	}
	lastvalCall := func(proc *process.Process) (string, error) {
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
		defer result.Free()
		if err := result.PreExtendAndReset(1); err != nil {
			return "", err
		}
		if err := Lastval(nil, result, proc, 1, nil); err != nil {
			return "", err
		}
		return string(result.GetResultVector().GetBytesAt(0)), nil
	}

	setProc := parent.NewContextChildProc(0)
	currProc := parent.NewContextChildProc(0)
	lastProc := parent.NewContextChildProc(0)
	currGateCalled := make(chan struct{})
	currProc.Ctx = &sequenceGateProbeContext{Context: currProc.Ctx, doneCalled: currGateCalled}
	lastGateCalled := make(chan struct{})
	lastProc.Ctx = &sequenceGateProbeContext{Context: lastProc.Ctx, doneCalled: lastGateCalled}
	setDone := make(chan struct{})
	currDone := make(chan struct{})
	lastDone := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-state.releaseUpdate:
		default:
			close(state.releaseUpdate)
		}
		for _, done := range []<-chan struct{}{setDone, currDone, lastDone} {
			select {
			case <-done:
			case <-time.After(time.Second):
			}
		}
	})
	setResult := make(chan struct {
		value string
		err   error
	}, 1)
	go func() {
		defer close(setDone)
		value, err := setvalCall(setProc)
		setResult <- struct {
			value string
			err   error
		}{value: value, err: err}
	}()
	select {
	case <-state.updateEntered:
	case <-time.After(time.Second):
		t.Fatal("SETVAL did not reach its metadata update")
	}

	currResult := make(chan struct {
		value string
		err   error
	}, 1)
	lastResult := make(chan struct {
		value string
		err   error
	}, 1)
	go func() {
		defer close(currDone)
		value, err := currvalCall(currProc)
		currResult <- struct {
			value string
			err   error
		}{value: value, err: err}
	}()
	go func() {
		defer close(lastDone)
		value, err := lastvalCall(lastProc)
		lastResult <- struct {
			value string
			err   error
		}{value: value, err: err}
	}()
	// SETVAL's metadata UPDATE is blocked before setVal publishes SeqAddValues
	// and LASTVAL. Both readers must reach the same Base-owned permit before it
	// is released; this is an admission barrier rather than a timed absence
	// check. If either gate were removed, its result would be observable below.
	select {
	case <-currGateCalled:
	case <-time.After(time.Second):
		t.Fatal("CURRVAL did not reach the sequence gate")
	}
	select {
	case <-lastGateCalled:
	case <-time.After(time.Second):
		t.Fatal("LASTVAL did not reach the sequence gate")
	}
	select {
	case result := <-currResult:
		t.Fatalf("CURRVAL bypassed SETVAL publication gate: %+v", result)
	default:
	}
	select {
	case result := <-lastResult:
		t.Fatalf("LASTVAL bypassed SETVAL publication gate: %+v", result)
	default:
	}
	close(state.releaseUpdate)

	set := <-setResult
	curr := <-currResult
	last := <-lastResult
	require.NoError(t, set.err)
	require.NoError(t, curr.err)
	require.NoError(t, last.err)
	require.Equal(t, "10", set.value)
	require.Equal(t, "10", curr.value)
	require.Equal(t, "10", last.value)
	state.Lock()
	require.Equal(t, int64(10), state.last)
	state.Unlock()
	require.Equal(t, "10", parent.GetSessionInfo().SeqLastValue[0])
}
