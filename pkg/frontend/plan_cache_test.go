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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

type trackedStatement struct {
	freed int
}

func (s *trackedStatement) String() string           { return "" }
func (s *trackedStatement) Format(*tree.FmtCtx)      {}
func (s *trackedStatement) GetStatementType() string { return "" }
func (s *trackedStatement) GetQueryType() string     { return "" }
func (s *trackedStatement) StmtKind() tree.StmtKind  { return 0 }
func (s *trackedStatement) Free()                    { s.freed++ }

func Test_BasicGet(t *testing.T) {
	pc := newPlanCache(5)

	pc.cache("abc", nil, nil)
	require.True(t, pc.isCached("abc"))
	require.Equal(t, pc.get("abc").sql, "abc")

	pc.cache("abcd", nil, nil)
	require.True(t, pc.isCached("abcd"))
	require.Equal(t, pc.get("abcd").sql, "abcd")

	require.False(t, pc.isCached("abcde"))
}

func Test_LRU(t *testing.T) {
	pc := newPlanCache(3)

	pc.cache("1", nil, nil)
	pc.cache("2", nil, nil)
	pc.cache("3", nil, nil)
	require.True(t, pc.isCached("1"))
	require.True(t, pc.isCached("2"))
	require.True(t, pc.isCached("3"))

	pc.cache("4", nil, nil)
	require.True(t, pc.isCached("4"))
	require.False(t, pc.isCached("1"))

	require.Equal(t, pc.get("2").sql, "2")
	pc.cache("5", nil, nil)
	require.True(t, pc.isCached("5"))
	require.True(t, pc.isCached("4"))
	require.True(t, pc.isCached("2"))
	require.False(t, pc.isCached("3"))
}

func Test_CleanCache(t *testing.T) {
	pc := newPlanCache(3)

	pc.cache("1", nil, nil)
	pc.cache("2", nil, nil)
	pc.cache("3", nil, nil)
	pc.cache("4", nil, nil)
	require.False(t, pc.isCached("1"))
	require.True(t, pc.isCached("2"))
	require.True(t, pc.isCached("3"))
	require.True(t, pc.isCached("4"))

	pc.clean()

	require.Nil(t, pc.get("1"))
	require.Nil(t, pc.get("2"))
	require.Nil(t, pc.get("3"))
	require.Nil(t, pc.get("4"))
	require.False(t, pc.isCached("1"))
	require.False(t, pc.isCached("2"))
	require.False(t, pc.isCached("3"))
	require.False(t, pc.isCached("3"))

	pc.cache("1", nil, nil)
	pc.cache("2", nil, nil)
	pc.cache("3", nil, nil)
	pc.cache("4", nil, nil)
	require.False(t, pc.isCached("1"))
	require.True(t, pc.isCached("2"))
	require.True(t, pc.isCached("3"))
	require.True(t, pc.isCached("4"))
	require.Nil(t, pc.get("1"))
	require.NotNil(t, pc.get("2"))
	require.NotNil(t, pc.get("3"))
	require.NotNil(t, pc.get("4"))
}

func Test_CleanCacheRemovesEntriesAndFreesStatements(t *testing.T) {
	pc := newPlanCache(3)
	entries := []struct {
		sql  string
		stmt *trackedStatement
	}{
		{sql: "1", stmt: &trackedStatement{}},
		{sql: "2", stmt: &trackedStatement{}},
		{sql: "3", stmt: &trackedStatement{}},
	}

	for _, entry := range entries {
		pc.cache(entry.sql, []tree.Statement{entry.stmt}, []*plan.Plan{{}})
	}

	lruList := pc.lruList
	pc.clean()

	require.Zero(t, lruList.Len())
	require.Nil(t, pc.lruList)
	require.Nil(t, pc.cachePool)
	for _, entry := range entries {
		require.Equal(t, 1, entry.stmt.freed)
	}

	pc.clean()
}

func TestSessionReleasePlanCache(t *testing.T) {
	pc := newPlanCache(2)
	first := &trackedStatement{}
	second := &trackedStatement{}
	pc.cache("1", []tree.Statement{first, second}, []*plan.Plan{{}, {}})
	lruList := pc.lruList

	ses := &Session{planCache: pc}
	ses.releasePlanCache()

	require.Zero(t, lruList.Len())
	require.Nil(t, pc.lruList)
	require.Nil(t, pc.cachePool)
	require.Equal(t, 1, first.freed)
	require.Equal(t, 1, second.freed)
}

func TestFreeStmtsSkipsNil(t *testing.T) {
	good := &trackedStatement{}
	stmts := []tree.Statement{nil, good, nil}

	freeStmts(stmts)

	require.Equal(t, 1, good.freed)
	require.Nil(t, stmts[1])
}

func Test_CleanOnEmptyCache(t *testing.T) {
	pc := newPlanCache(3)
	require.NotPanics(t, func() { pc.clean() })
	require.Nil(t, pc.lruList)
	require.Nil(t, pc.cachePool)

	pc2 := newPlanCache(3)
	pc2.cache("1", nil, nil)
	pc2.clean()
	require.NotPanics(t, func() { pc2.clean() }) // idempotent
	require.Nil(t, pc2.lruList)
	require.Nil(t, pc2.cachePool)
}

func Test_SessionAccessorsWithNilPlanCache(t *testing.T) {
	ses := &Session{planCache: nil}

	require.NotPanics(t, func() {
		ses.cachePlan("x", []tree.Statement{&trackedStatement{}}, []*plan.Plan{{}})
	})
	require.Nil(t, ses.getCachedPlan("x"))
	require.False(t, ses.isCached("x"))
	require.NotPanics(t, func() { ses.cleanCache() })
	require.NotPanics(t, func() { ses.releasePlanCache() })
}
