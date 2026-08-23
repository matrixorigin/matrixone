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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/foreigntvf"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// fakeTvfSession implements both process.Session and process.ForeignConnCache.
type fakeTvfSession struct {
	conns map[string]process.ForeignConn
}

func (s *fakeTvfSession) GetTempTable(dbName, alias string) (string, bool) { return "", false }
func (s *fakeTvfSession) AddTempTable(dbName, alias, realName string)      {}
func (s *fakeTvfSession) RemoveTempTable(dbName, alias string)             {}
func (s *fakeTvfSession) RemoveTempTableByRealName(realName string)        {}
func (s *fakeTvfSession) GetSqlModeNoAutoValueOnZero() (bool, bool)        { return false, false }

func (s *fakeTvfSession) PutForeignConn(handle string, conn process.ForeignConn) process.ForeignConn {
	if s.conns == nil {
		s.conns = make(map[string]process.ForeignConn)
	}
	if existing, ok := s.conns[handle]; ok && existing != nil {
		return existing
	}
	s.conns[handle] = conn
	return conn
}
func (s *fakeTvfSession) GetForeignConn(handle string) (process.ForeignConn, bool) {
	c, ok := s.conns[handle]
	return c, ok
}
func (s *fakeTvfSession) RemoveForeignConn(handle string) (process.ForeignConn, bool) {
	c, ok := s.conns[handle]
	if ok {
		delete(s.conns, handle)
	}
	return c, ok
}

type fakeClosableConn struct{ closed bool }

func (c *fakeClosableConn) Close() error { c.closed = true; return nil }

func TestForeignTvfDisconnectBuiltin(t *testing.T) {
	proc := testutil.NewProcess(t)
	ses := &fakeTvfSession{}
	proc.Session = ses

	conn := &fakeClosableConn{}
	ses.PutForeignConn("sql:abc", conn)

	// existing handle -> true (and closed); unknown -> false; NULL -> NULL.
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(),
			[]string{"sql:abc", "sql:missing", ""},
			[]bool{false, false, true})},
		NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false},
			[]bool{false, false, true}),
		builtInSqlTvfDisconnect)
	ok, info := tc.Run()
	require.True(t, ok, info)
	require.True(t, conn.closed)
	_, found := ses.GetForeignConn("sql:abc")
	require.False(t, found)
}

func TestForeignTvfBuiltinsRequireInteractiveSession(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Session = nil // no interactive session

	for _, fn := range []fEvalFn{builtInSqlTvfDisconnect, builtInEsqlTvfDisconnect} {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"h"}, nil)},
			NewFunctionTestResult(types.T_bool.ToType(), true, nil, nil),
			fn)
		ok, info := tc.Run()
		require.True(t, ok, info)
	}
	for _, fn := range []fEvalFn{builtInSqlTvfConnect, builtInEsqlTvfConnect} {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"{}"}, nil)},
			NewFunctionTestResult(types.T_varchar.ToType(), true, nil, nil),
			fn)
		ok, info := tc.Run()
		require.True(t, ok, info)
	}
}

func TestForeignTvfConnectBadConfig(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Session = &fakeTvfSession{}

	// A bad driver fails before any dialing; proves the connect builtin routes
	// the config through foreigntvf and surfaces its error.
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(),
			[]string{`{"driver":"nope","dsn":"x"}`}, nil)},
		NewFunctionTestResult(types.T_varchar.ToType(), true, nil, nil),
		builtInSqlTvfConnect)
	ok, info := tc.Run()
	require.True(t, ok, info)

	// NULL config with no resolvable @sql_tvf_config errors.
	tc = NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(),
			[]string{""}, []bool{true})},
		NewFunctionTestResult(types.T_varchar.ToType(), true, nil, nil),
		builtInSqlTvfConnect)
	ok, info = tc.Run()
	require.True(t, ok, info)
}

func TestForeignTvfKindConstantsMatchPlan(t *testing.T) {
	// The operator resolves plan.ForeignTVFParam.Kind strings into
	// foreigntvf.Kind values; keep them aligned.
	require.Equal(t, "esql", string(foreigntvf.KindESQL))
	require.Equal(t, "sql", string(foreigntvf.KindSQL))
}
