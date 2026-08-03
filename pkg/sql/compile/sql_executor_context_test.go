// Copyright 2023 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

func TestSQLExecutorCompilerContextReservesLifecycleRestoreStagingForFrontend(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Base.IsFrontend = true
	ctx := &compilerContext{
		ctx:  context.Background(),
		proc: proc,
	}
	_, _, err := ctx.Resolve(
		"history",
		catalog.LifecycleRestoreTableNamePrefix+
			"0123456789abcdef0123456789abcdef",
		nil,
	)
	require.ErrorContains(t, err, "Lifecycle Restore staging")

	proc.Base.IsFrontend = false
	_, _, err = ctx.Resolve(
		"",
		catalog.LifecycleRestoreTableNamePrefix+
			"0123456789abcdef0123456789abcdef",
		nil,
	)
	require.NoError(t, err)
}

func TestNewInternalStatementContextPreservesRootAndClaimsStatsOnce(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	parentStats := statistic.NewStatsInfo()
	parent := resource.ContextWithRoot(
		statistic.ContextWithStatsInfo(context.Background(), parentStats),
		root)

	child := newInternalStatementContext(parent)
	childStats := statistic.StatsInfoFromContext(child)
	childAgain := newInternalStatementContext(parent)
	childAgainStats := statistic.StatsInfoFromContext(childAgain)

	require.Same(t, root, resource.RootFromContext(child))
	require.NotNil(t, childStats)
	require.NotSame(t, parentStats, childStats)
	require.NotSame(t, childStats, childAgainStats)

	for _, stats := range []*statistic.StatsInfo{parentStats, childStats, childAgainStats} {
		_, ok := stats.ClaimRootPhaseResource()
		require.True(t, ok)
		_, ok = stats.ClaimRootPhaseResource()
		require.False(t, ok)
	}
}

func Test_panic(t *testing.T) {
	r := func() {
		err := recover()
		require.Equal(t, "not supported in internal sql executor", err)
	}

	c := &compilerContext{}

	func() {
		defer r()
		_ = c.CheckSubscriptionValid("", "", "")
	}()

	func() {
		defer r()
		_, _ = c.IsPublishing("")
	}()

	func() {
		defer r()
		c.SetQueryingSubscription(nil)
	}()

	func() {
		defer r()
		_, _ = c.ResolveUdf("", nil)
	}()

	func() {
		defer r()
		_, _ = c.ResolveAccountIds(nil)
	}()

	func() {
		defer r()
		_, _, _ = c.GetQueryResultMeta("")
	}()
}

func TestCompilerContext_Database(t *testing.T) {
	ctrl := gomock.NewController(t)
	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().GetDatabaseId(gomock.Any()).Return("1")
	engine := mock_frontend.NewMockEngine(ctrl)
	engine.EXPECT().Database(gomock.Any(), "", nil).Return(database, nil).Times(2)

	c := &compilerContext{
		proc:   testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
		engine: engine,
	}

	exists := c.DatabaseExists("", &plan.Snapshot{})
	require.Equal(t, exists, true)

	_, err := c.GetDatabaseId("", &plan.Snapshot{})
	require.Nil(t, err)

	sql := c.GetRootSql()
	require.Equal(t, sql, "")
}

func TestCompilerContextBuildTableDefByMoColumns(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	tableDef := &plan.TableDef{
		Name: "src",
		Cols: []*plan.ColDef{
			{Name: "a"},
			{Name: "b"},
		},
	}
	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().GetTableDef(gomock.Any()).Return(tableDef)
	relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(42))
	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().Relation(gomock.Any(), "src", nil).Return(relation, nil)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)

	c := &compilerContext{
		defaultDB: "db",
		engine:    eng,
		proc:      proc,
	}
	actual, err := c.BuildTableDefByMoColumns("db", "src")
	require.NoError(t, err)
	require.Equal(t, tableDef.Cols, actual.Cols)
	require.NotSame(t, tableDef, actual)
}

func TestCompilerContextBuildTableDefByMoColumnsPropagatesRelationError(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	relationErr := moerr.NewInternalErrorNoCtx("relation failed")

	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().Relation(gomock.Any(), "src", nil).Return(nil, relationErr)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)

	c := &compilerContext{
		defaultDB: "db",
		engine:    eng,
		proc:      proc,
	}
	actual, err := c.BuildTableDefByMoColumns("db", "src")
	require.Nil(t, actual)
	require.ErrorIs(t, err, relationErr)
}

func TestCompilerContextBuildTableDefByMoColumnsNoSuchTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	database := mock_frontend.NewMockDatabase(ctrl)
	database.EXPECT().Relation(gomock.Any(), "src", nil).Return(nil, nil)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)

	c := &compilerContext{
		defaultDB: "db",
		engine:    eng,
		proc:      proc,
	}
	actual, err := c.BuildTableDefByMoColumns("db", "src")
	require.Nil(t, actual)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
}
