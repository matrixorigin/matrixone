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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

type testSubscriptionResolver struct {
	meta *plan.SubscriptionMeta
}

func (r testSubscriptionResolver) GetSubscriptionMeta(string, *plan.Snapshot) (*plan.SubscriptionMeta, error) {
	return r.meta, nil
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

func TestCompilerContextUnsupportedResolversReturnErrors(t *testing.T) {
	c := &compilerContext{
		ctx:  context.Background(),
		proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
	}

	_, err := c.ResolveUdf("f", nil)
	require.Error(t, err)
	_, err = c.ResolveAccountIds([]string{"acc"})
	require.Error(t, err)
	_, err = c.ResolveSnapshotWithSnapshotName("sn")
	require.Error(t, err)
	require.Error(t, c.CheckSubscriptionValid("sub", "acc", "pub"))
	_, _, err = c.ResolveSubscriptionTableById(1, nil)
	require.Error(t, err)
	_, err = c.IsPublishing("db")
	require.Error(t, err)
	_, err = c.CheckTimeStampValid(1)
	require.Error(t, err)
	_, _, err = c.GetQueryResultMeta("uuid")
	require.Error(t, err)
}

func TestCompilerContextDelegatesViewMetadataResolvers(t *testing.T) {
	oldCtx := context.WithValue(context.Background(), struct{ name string }{"old"}, "old")
	delegate := plan.NewMockCompilerContext(false)
	delegate.SetContext(oldCtx)
	delegate.ResolveAccountIdsFunc = func(names []string) ([]uint32, error) {
		require.Equal(t, []string{"acc"}, names)
		return []uint32{7}, nil
	}
	refreshCtx := context.WithValue(context.Background(), struct{ name string }{"new"}, "new")
	refreshCtx = context.WithValue(refreshCtx, viewMetadataCompilerContextKey{}, delegate)
	c := &compilerContext{
		ctx:       refreshCtx,
		defaultDB: "db",
		proc:      testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
	}

	obj, def, err := c.ResolveSubscriptionTableById(1, &plan.SubscriptionMeta{})
	require.NoError(t, err)
	require.Nil(t, obj)
	require.Nil(t, def)
	publishing, err := c.IsPublishing("db")
	require.NoError(t, err)
	require.False(t, publishing)
	snapshot, err := c.ResolveSnapshotWithSnapshotName("sn")
	require.NoError(t, err)
	require.Nil(t, snapshot)
	valid, err := c.CheckTimeStampValid(1)
	require.NoError(t, err)
	require.False(t, valid)
	udf, err := c.ResolveUdf("f", nil)
	require.NoError(t, err)
	require.Nil(t, udf)
	accountIDs, err := c.ResolveAccountIds([]string{"acc"})
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, accountIDs)
	require.Same(t, oldCtx, delegate.GetContext(), "delegate context must be restored after every call")
}

func TestCompilerContextQueryingSubscription(t *testing.T) {
	c := &compilerContext{}
	meta := &plan.SubscriptionMeta{}

	c.SetQueryingSubscription(meta)

	require.Same(t, meta, c.GetQueryingSubscription())
}

func TestCompilerContextUsesRefreshSubscriptionResolver(t *testing.T) {
	meta := &plan.SubscriptionMeta{SubName: "subdb"}
	procCtx := context.WithValue(
		context.Background(),
		viewMetadataSubscriptionResolverKey{},
		viewMetadataSubscriptionResolver(testSubscriptionResolver{meta: meta}),
	)
	c := &compilerContext{
		ctx:  context.Background(),
		proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
	}
	c.proc.ReplaceTopCtx(procCtx)

	actual, err := c.GetSubscriptionMeta("subdb", nil)

	require.NoError(t, err)
	require.Same(t, meta, actual)

	snapshot := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1}}
	actual, err = c.GetSubscriptionMeta("subdb", snapshot)
	require.NoError(t, err)
	require.Same(t, meta, actual)
}

func TestCompilerContextRecordsViewDependencies(t *testing.T) {
	c := &compilerContext{}
	views := []string{"db#v1", "db#v2"}

	c.SetViews(views)
	views[0] = "changed"
	actual := c.GetViews()
	actual[1] = "also changed"

	require.Equal(t, []string{"db#v1", "db#v2"}, c.GetViews())
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
