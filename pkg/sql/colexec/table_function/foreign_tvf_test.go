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

package table_function

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/foreigntvf"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// fakeTvfCacheSession implements process.Session + process.ForeignConnCache.
type fakeTvfCacheSession struct {
	conns map[string]process.ForeignConn
}

func (s *fakeTvfCacheSession) GetTempTable(dbName, alias string) (string, bool) { return "", false }
func (s *fakeTvfCacheSession) AddTempTable(dbName, alias, realName string)      {}
func (s *fakeTvfCacheSession) RemoveTempTable(dbName, alias string)             {}
func (s *fakeTvfCacheSession) RemoveTempTableByRealName(realName string)        {}
func (s *fakeTvfCacheSession) GetSqlModeNoAutoValueOnZero() (bool, bool)        { return false, false }
func (s *fakeTvfCacheSession) PutForeignConn(_ context.Context, handle string, c process.ForeignConn) (process.ForeignConn, error) {
	if s.conns == nil {
		s.conns = make(map[string]process.ForeignConn)
	}
	if existing, ok := s.conns[handle]; ok && existing != nil {
		return existing, nil
	}
	s.conns[handle] = c
	return c, nil
}
func (s *fakeTvfCacheSession) GetForeignConn(handle string) (process.ForeignConn, bool) {
	c, ok := s.conns[handle]
	return c, ok
}
func (s *fakeTvfCacheSession) RemoveForeignConn(handle string) (process.ForeignConn, bool) {
	c, ok := s.conns[handle]
	if ok {
		delete(s.conns, handle)
	}
	return c, ok
}

// fakeForeignConn is a foreigntvf.Conn that replays a fixed CSV stream.
type fakeForeignConn struct {
	kind foreigntvf.Kind
	csv  string
}

func (c *fakeForeignConn) Close() error          { return nil }
func (c *fakeForeignConn) Kind() foreigntvf.Kind { return c.kind }
func (c *fakeForeignConn) Query(ctx context.Context, q string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(c.csv)), nil
}

func constStrExpr(v string) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar), Width: 256},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: v}}},
	}
}

func mkForeignTvfArg(t *testing.T, kind string, noSchema bool, schemaCols []plan2.ParseJsonlOptionsCol, rets []*plan.ColDef, attrs []string, query, handle string) *TableFunction {
	params, err := json.Marshal(plan2.ForeignTVFParam{Kind: kind, NoSchema: noSchema, Cols: schemaCols})
	require.NoError(t, err)
	name := "sql_tvf"
	if kind == plan2.ForeignTVFKindESQL {
		name = "esql_tvf"
	}
	return &TableFunction{
		Attrs:    attrs,
		Rets:     rets,
		Args:     []*plan.Expr{constStrExpr(query), constStrExpr(handle)},
		Params:   params,
		FuncName: name,
		IsSingle: true,
	}
}

func driveForeignTvf(t *testing.T, proc *process.Process, arg *TableFunction) []*batch.Batch {
	// fake retSchema the framework normally builds in Prepare
	retSchema := make([]types.Type, len(arg.Rets))
	for i := range arg.Rets {
		typ := arg.Rets[i].Typ
		retSchema[i] = types.New(types.T(typ.Id), typ.Width, typ.Scale)
	}
	arg.ctr.retSchema = retSchema

	var st tvfState
	var err error
	if arg.FuncName == "esql_tvf" {
		st, err = esqlTvfPrepare(proc, arg)
	} else {
		st, err = sqlTvfPrepare(proc, arg)
	}
	require.NoError(t, err)

	inputBat := batch.EmptyForConstFoldBatch
	for i := range arg.ctr.executorsForArgs {
		arg.ctr.argVecs[i], err = arg.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{inputBat}, nil)
		require.NoError(t, err)
	}

	require.NoError(t, st.start(arg, proc, 0, nil))
	var out []*batch.Batch
	for {
		res, err := st.call(arg, proc)
		require.NoError(t, err)
		if res.Batch.IsDone() {
			break
		}
		// copy out the interesting values before the state reuses the batch
		out = append(out, res.Batch)
		break // single batch is enough for these fixtures
	}
	t.Cleanup(func() {
		st.reset(arg, proc)
		st.free(arg, proc, false, nil)
		arg.ctr.cleanExecutors()
	})
	return out
}

func foreignTvfRets() []*plan.ColDef {
	mk := func(name string, tt types.T) *plan.ColDef {
		typ := tt.ToType()
		return &plan.ColDef{Name: name, Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale}}
	}
	return []*plan.ColDef{mk("id", types.T_int64), mk("name", types.T_varchar)}
}

func TestForeignTvfOperatorSchemaMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	ses := &fakeTvfCacheSession{}
	proc.Session = ses
	ses.PutForeignConn(context.TODO(), "sql:fixture", &fakeForeignConn{
		kind: foreigntvf.KindSQL,
		csv:  "\"1\",\"alice\"\n\"2\",\\N\n",
	})

	schemaCols := []plan2.ParseJsonlOptionsCol{{Name: "id", Type: "int64"}, {Name: "name", Type: "string"}}
	arg := mkForeignTvfArg(t, plan2.ForeignTVFKindSQL, false, schemaCols,
		foreignTvfRets(), []string{"id", "name"}, "select whatever", "sql:fixture")

	out := driveForeignTvf(t, proc, arg)
	require.Len(t, out, 1)
	bat := out[0]
	require.Equal(t, 2, bat.RowCount())
	ids := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])
	require.Equal(t, []int64{1, 2}, ids[:2])
	require.Equal(t, "alice", bat.Vecs[1].GetStringAt(0))
	require.True(t, bat.Vecs[1].GetNulls().Contains(1))
}

func TestForeignTvfOperatorNoSchemaMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	ses := &fakeTvfCacheSession{}
	proc.Session = ses
	// esql dialect: header line is skipped in no-schema mode too.
	ses.PutForeignConn(context.TODO(), "esql:fixture", &fakeForeignConn{
		kind: foreigntvf.KindESQL,
		csv:  "a,b\r\n1,x\r\n2,\"y,z\"\r\n",
	})

	jsonCol := &plan.ColDef{Name: "result", Typ: plan.Type{Id: int32(types.T_json)}}
	arg := mkForeignTvfArg(t, plan2.ForeignTVFKindESQL, true, nil,
		[]*plan.ColDef{jsonCol}, []string{"result"}, "FROM idx", "esql:fixture")

	out := driveForeignTvf(t, proc, arg)
	require.Len(t, out, 1)
	bat := out[0]
	require.Equal(t, 2, bat.RowCount())
}

func TestForeignTvfOperatorErrors(t *testing.T) {
	proc := testutil.NewProcess(t)

	schemaCols := []plan2.ParseJsonlOptionsCol{{Name: "id", Type: "int64"}}
	rets := []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}}}

	// no interactive session
	proc.Session = nil
	arg := mkForeignTvfArg(t, plan2.ForeignTVFKindSQL, false, schemaCols, rets, []string{"id"}, "q", "h")
	retSchema := []types.Type{types.T_int64.ToType()}
	arg.ctr.retSchema = retSchema
	st, err := sqlTvfPrepare(proc, arg)
	require.NoError(t, err)
	inputBat := batch.EmptyForConstFoldBatch
	for i := range arg.ctr.executorsForArgs {
		arg.ctr.argVecs[i], err = arg.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{inputBat}, nil)
		require.NoError(t, err)
	}
	require.Error(t, st.start(arg, proc, 0, nil))
	arg.ctr.cleanExecutors()

	// unknown handle
	proc.Session = &fakeTvfCacheSession{}
	arg2 := mkForeignTvfArg(t, plan2.ForeignTVFKindSQL, false, schemaCols, rets, []string{"id"}, "q", "sql:nosuch")
	arg2.ctr.retSchema = retSchema
	st2, err := sqlTvfPrepare(proc, arg2)
	require.NoError(t, err)
	for i := range arg2.ctr.executorsForArgs {
		arg2.ctr.argVecs[i], err = arg2.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{inputBat}, nil)
		require.NoError(t, err)
	}
	require.Error(t, st2.start(arg2, proc, 0, nil))
	arg2.ctr.cleanExecutors()

	// bad params json fails Prepare
	arg3 := mkForeignTvfArg(t, plan2.ForeignTVFKindSQL, false, schemaCols, rets, []string{"id"}, "q", "h")
	arg3.Params = []byte("not json")
	_, err = sqlTvfPrepare(proc, arg3)
	require.Error(t, err)

	_ = vm.CallResult{} // keep vm imported for clarity of the driver above
}

// TestForeignTvfPrepareDispatch goes through TableFunction.Prepare's name
// switch (the operator-framework entry the direct esqlTvfPrepare calls above
// bypass).
func TestForeignTvfPrepareDispatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, name := range []string{"sql_tvf", "esql_tvf"} {
		kind := plan2.ForeignTVFKindSQL
		if name == "esql_tvf" {
			kind = plan2.ForeignTVFKindESQL
		}
		arg := mkForeignTvfArg(t, kind, true, nil,
			[]*plan.ColDef{{Name: "result", Typ: plan.Type{Id: int32(types.T_json)}}},
			[]string{"result"}, "q", "h")
		require.Equal(t, name, arg.FuncName)
		require.NoError(t, arg.Prepare(proc))
		arg.Free(proc, false, nil)
	}
}

// TestForeignTvfSessionVarFallback covers start()'s conn==NULL path: the
// config comes from @sql_tvf_config, and an invalid value errors cleanly.
func TestForeignTvfSessionVarFallback(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Session = &fakeTvfCacheSession{}
	proc.SetResolveVariableFunc(func(name string, sys, glob bool) (any, error) {
		return `{"driver":"nope","dsn":"x"}`, nil
	})

	schemaCols := []plan2.ParseJsonlOptionsCol{{Name: "id", Type: "int64"}}
	rets := []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), Width: 64}}}
	arg := mkForeignTvfArg(t, plan2.ForeignTVFKindSQL, false, schemaCols, rets, []string{"id"}, "q", "h")
	// only the query argument: conn absent -> session-var fallback
	arg.Args = arg.Args[:1]
	arg.ctr.retSchema = []types.Type{types.T_int64.ToType()}
	st, err := sqlTvfPrepare(proc, arg)
	require.NoError(t, err)
	inputBat := batch.EmptyForConstFoldBatch
	for i := range arg.ctr.executorsForArgs {
		arg.ctr.argVecs[i], err = arg.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{inputBat}, nil)
		require.NoError(t, err)
	}
	err = st.start(arg, proc, 0, nil)
	require.ErrorContains(t, err, "unsupported driver")
	arg.ctr.cleanExecutors()
}

// TestForeignTvfNoSchemaByteBudget proves the schema-less path is bounded by
// bytes as well as rows: a foreign result of large text values must split
// across multiple batches at the configured budget instead of retaining
// everything in one call (regression: only the 8192-row bound applied).
func TestForeignTvfNoSchemaByteBudget(t *testing.T) {
	proc := testutil.NewProcess(t)
	// budget = MaxMsgSize * 0.6 = 600KB
	proc.Base.Lim.MaxMsgSize = 1 << 20
	ses := &fakeTvfCacheSession{}
	proc.Session = ses

	// 20 rows of ~100KB each (~2MB total, >3x the budget)
	bigVal := strings.Repeat("x", 100*1024)
	var csv strings.Builder
	const totalRows = 20
	for i := 0; i < totalRows; i++ {
		csv.WriteString("\"" + bigVal + "\"\n")
	}
	ses.PutForeignConn(context.TODO(), "sql:big", &fakeForeignConn{
		kind: foreigntvf.KindSQL, csv: csv.String(),
	})

	jsonCol := &plan.ColDef{Name: "result", Typ: plan.Type{Id: int32(types.T_json)}}
	arg := mkForeignTvfArg(t, plan2.ForeignTVFKindSQL, true, nil,
		[]*plan.ColDef{jsonCol}, []string{"result"}, "q", "sql:big")

	retSchema := []types.Type{types.T_json.ToType()}
	arg.ctr.retSchema = retSchema
	st, err := sqlTvfPrepare(proc, arg)
	require.NoError(t, err)
	inputBat := batch.EmptyForConstFoldBatch
	for i := range arg.ctr.executorsForArgs {
		arg.ctr.argVecs[i], err = arg.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{inputBat}, nil)
		require.NoError(t, err)
	}
	require.NoError(t, st.start(arg, proc, 0, nil))

	total, calls, maxRows := 0, 0, 0
	for {
		res, err := st.call(arg, proc)
		require.NoError(t, err)
		if res.Batch.IsDone() {
			break
		}
		rows := res.Batch.RowCount()
		total += rows
		calls++
		if rows > maxRows {
			maxRows = rows
		}
	}
	require.Equal(t, totalRows, total)
	require.Greater(t, calls, 1, "one call retained the whole oversized result")
	// ~600KB budget / ~100KB rows: each batch stays in the budget's ballpark
	require.LessOrEqual(t, maxRows, 7)
	st.reset(arg, proc)
	st.free(arg, proc, false, nil)
	arg.ctr.cleanExecutors()
}
