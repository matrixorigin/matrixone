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

package frontend

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Reuse the catalog/result fixture and real Interpreter; route body CALLs back
// through doInterpretCall, as the production background executor does.
type procedureRecursionTestSession struct {
	FeSession
	database string
	bodies   map[string]string
	entered  *[]string
	bodyErr  error
}

func (s *procedureRecursionTestSession) GetDatabaseName() string { return s.database }

func (s *procedureRecursionTestSession) GetBackgroundExec(context.Context, ...*BackgroundExecOption) BackgroundExec {
	return s.newExecutor()
}

func (s *procedureRecursionTestSession) GetShareTxnBackgroundExec(context.Context, bool) BackgroundExec {
	return s.newExecutor()
}

func (s *procedureRecursionTestSession) newExecutor() BackgroundExec {
	b := &procedureRecursionTestExec{session: s, database: s.database}
	b.init()
	for identity, body := range s.bodies {
		database, routine, _ := strings.Cut(identity, ".")
		sql, _ := getSqlForSpBody(context.Background(), routine, database)
		b.sql2result[sql] = newMrsForStoredProcedure([][]interface{}{{"sql", body, "[]", ""}})
	}
	return b
}

type procedureRecursionTestExec struct {
	backgroundExecTest
	session  *procedureRecursionTestSession
	database string
}

func (b *procedureRecursionTestExec) Exec(ctx context.Context, sql string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.currentSql = sql
	if strings.HasPrefix(sql, "use ") {
		b.database = strings.TrimPrefix(sql, "use ")
	} else if sql == "select 1" {
		*b.session.entered = append(*b.session.entered, b.database)
		return b.session.bodyErr
	} else if strings.HasPrefix(sql, "call ") {
		stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
		if err != nil {
			return err
		}
		defer stmt.Free()
		child := *b.session
		child.database = b.database
		_, err = doInterpretCall(ctx, &child, stmt.(*tree.CallStmt), true, 0, new(int64))
		return err
	}
	return nil
}

func (b *procedureRecursionTestExec) GetExecResultSet() []interface{} {
	if result := b.sql2result[b.currentSql]; result != nil {
		return []interface{}{result}
	}
	return nil
}

func TestInterpretCallRecursion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limit   int64
		bodies  map[string]string
		entered int
		reject  bool
	}{
		{"direct default zero", 0, map[string]string{"db.a": "begin select 1; call a(); end"}, 1, true},
		{"direct one permits two instances", 1, map[string]string{"db.a": "begin select 1; call a(); end"}, 2, true},
		{"different routines at zero", 0, map[string]string{
			"db.a": "begin select 1; call b(); end", "db.b": "begin select 1; call c(); end", "db.c": "begin select 1; end",
		}, 3, false},
		{"mutual zero", 0, map[string]string{
			"db.a": "begin select 1; call b(); end", "db.b": "begin select 1; call a(); end",
		}, 2, true},
		{"mutual one permits four instances", 1, map[string]string{
			"db.a": "begin select 1; call b(); end", "db.b": "begin select 1; call a(); end",
		}, 4, true},
		{"same name in another schema", 0, map[string]string{
			"db.a": "begin select 1; call other.a(); end", "other.a": "begin select 1; end",
		}, 2, false},
		{"cross schema reentry", 0, map[string]string{
			"db.a": "begin select 1; call other.a(); end", "other.a": "begin select 1; call db.a(); end",
		}, 2, true},
		{"sibling calls reuse parent", 0, map[string]string{
			"db.a": "begin select 1; call b(); call b(); end", "db.b": "begin select 1; end",
		}, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			t.Cleanup(ses.Close)
			ctx := context.Background()
			require.NoError(t, ses.SetSessionSysVar(ctx, "max_sp_recursion_depth", tc.limit))
			var entered []string
			fixture := &procedureRecursionTestSession{FeSession: ses, database: "db", bodies: tc.bodies, entered: &entered}
			call := &tree.CallStmt{Name: tree.NewProcedureName("a", tree.ObjectNamePrefix{})}
			_, err := doInterpretCall(ctx, fixture, call, false, 0, new(int64))
			if tc.reject {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrSPRecursionLimit), "%v", err)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, entered, tc.entered)
			// Reusing the original context after any success/error must start at
			// level zero. The prior frames cannot survive in session state.
			fixture.bodies = map[string]string{"db.a": "begin select 1; end"}
			_, err = doInterpretCall(ctx, fixture, call, false, 0, new(int64))
			require.NoError(t, err)
			require.Len(t, entered, tc.entered+1)
		})
	}
}

func TestInterpretCallRecursionBodyErrorAndCancelReuse(t *testing.T) {
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	var entered []string
	wantErr := errors.New("body failed")
	fixture := &procedureRecursionTestSession{
		FeSession: ses, database: "db", bodies: map[string]string{"db.a": "begin select 1; end"}, entered: &entered, bodyErr: wantErr,
	}
	call := &tree.CallStmt{Name: tree.NewProcedureName("a", tree.ObjectNamePrefix{})}
	ctx := context.Background()
	_, err := doInterpretCall(ctx, fixture, call, false, 0, new(int64))
	require.ErrorIs(t, err, wantErr)
	fixture.bodyErr = nil
	_, err = doInterpretCall(ctx, fixture, call, false, 0, new(int64))
	require.NoError(t, err)
	require.Len(t, entered, 2)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = doInterpretCall(canceled, fixture, call, false, 0, new(int64))
	require.ErrorIs(t, err, context.Canceled)
	_, err = doInterpretCall(ctx, fixture, call, false, 0, new(int64))
	require.NoError(t, err)
	require.Len(t, entered, 3)
}

func TestBackSessionProcedureRecursionLimit(t *testing.T) {
	ctx := context.Background()
	ses := newFeatureLimitTestSession(t)
	parent := &backSession{feSessionImpl: feSessionImpl{upstream: ses}}
	child := &backSession{parentBackSession: parent}
	for _, limit := range []int64{0, 1, 255, 0} {
		require.NoError(t, ses.SetSessionSysVar(ctx, "max_sp_recursion_depth", limit))
		for _, back := range []*backSession{parent, child} {
			value, err := back.GetSessionSysVar("max_sp_recursion_depth")
			require.NoError(t, err)
			require.Equal(t, limit, value)
		}
	}
	value, err := (&backSession{}).GetSessionSysVar("max_sp_recursion_depth")
	require.NoError(t, err)
	require.Equal(t, int64(0), value)
	value, err = newFeatureLimitTestSession(t).GetSessionSysVar("max_sp_recursion_depth")
	require.NoError(t, err)
	require.Equal(t, int64(0), value)
}

func TestSQLProcedureContextAndDynamicLimit(t *testing.T) {
	ctx := context.Background()
	ses := newFeatureLimitTestSession(t)
	parent, err := enterSQLProcedure(ctx, ses, "db", "a")
	require.NoError(t, err)
	// The real back-exec StatsInfo context wrapper must preserve the frame.
	wrapped, _ := installBackExecStatsInfo(parent, time.Now(), 0)
	_, err = enterSQLProcedure(wrapped, ses, "db", "a")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrSPRecursionLimit))
	require.NoError(t, ses.SetSessionSysVar(ctx, "max_sp_recursion_depth", int64(1)))
	child, err := enterSQLProcedure(wrapped, ses, "db", "a")
	require.NoError(t, err)
	_, err = enterSQLProcedure(child, ses, "db", "a")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrSPRecursionLimit))
	_, err = enterSQLProcedure(wrapped, ses, "db", "a")
	require.NoError(t, err) // sibling reentry after child returns
	require.NoError(t, ses.SetSessionSysVar(ctx, "max_sp_recursion_depth", int64(0)))
	_, err = enterSQLProcedure(parent, ses, "db", "a")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrSPRecursionLimit))
	_, err = enterSQLProcedure(ctx, ses, "db", "a")
	require.NoError(t, err) // a later top-level CALL is independent
}

type procedureRecursionLimitTestSession struct {
	FeSession
	value any
	err   error
}

func (s *procedureRecursionLimitTestSession) GetSessionSysVar(string) (interface{}, error) {
	return s.value, s.err
}

func TestSQLProcedureRecursionLimitBoundaries(t *testing.T) {
	ctx := context.Background()
	ses := &procedureRecursionLimitTestSession{value: int64(255)}
	active := ctx
	for instance := 0; instance <= 255; instance++ {
		var err error
		active, err = enterSQLProcedure(active, ses, "db", "a")
		require.NoError(t, err, "instance %d", instance)
	}
	_, err := enterSQLProcedure(active, ses, "db", "a")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrSPRecursionLimit))
	_, err = enterSQLProcedure(ctx, ses, "db", "a")
	require.NoError(t, err)

	ses.value = nil
	_, err = enterSQLProcedure(ctx, ses, "db", "a")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	wantErr := errors.New("session variable lookup failed")
	ses.err = wantErr
	_, err = enterSQLProcedure(ctx, ses, "db", "a")
	require.ErrorIs(t, err, wantErr)
}
