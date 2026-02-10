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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

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

func TestCompilerContextGetRelationFallbackToTempTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	eng := mock_frontend.NewMockEngine(ctrl)
	mainDB := mock_frontend.NewMockDatabase(ctrl)
	tempDB := mock_frontend.NewMockDatabase(ctrl)
	tempRel := mock_frontend.NewMockRelation(ctrl)

	mainErr := moerr.NewNoSuchTable(context.Background(), "testdb", "t1")
	eng.EXPECT().Database(gomock.Any(), "testdb", nil).Return(mainDB, nil)
	mainDB.EXPECT().Relation(gomock.Any(), "t1", nil).Return(nil, mainErr)
	eng.EXPECT().HasTempEngine().Return(true)
	eng.EXPECT().Database(gomock.Any(), defines.TEMPORARY_DBNAME, nil).Return(tempDB, nil)
	tempDB.EXPECT().Relation(gomock.Any(), engine.GetTempTableName("testdb", "t1"), nil).Return(tempRel, nil)

	c := &compilerContext{
		proc:      proc,
		engine:    eng,
		defaultDB: "testdb",
	}

	ctx, rel, err := c.getRelation("", "t1", nil)
	require.NoError(t, err)
	require.NotNil(t, ctx)
	require.Same(t, tempRel, rel)
}

func TestCompilerContextGetRelationReturnsNilWhenTempDbUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	eng := mock_frontend.NewMockEngine(ctrl)
	mainDB := mock_frontend.NewMockDatabase(ctrl)

	mainErr := moerr.NewNoSuchTable(context.Background(), "testdb", "t1")
	eng.EXPECT().Database(gomock.Any(), "testdb", nil).Return(mainDB, nil)
	mainDB.EXPECT().Relation(gomock.Any(), "t1", nil).Return(nil, mainErr)
	eng.EXPECT().HasTempEngine().Return(true)
	eng.EXPECT().Database(gomock.Any(), defines.TEMPORARY_DBNAME, nil).Return(nil, moerr.NewInternalErrorNoCtx("temp db not found"))

	c := &compilerContext{
		proc:      proc,
		engine:    eng,
		defaultDB: "testdb",
	}

	ctx, rel, err := c.getRelation("testdb", "t1", nil)
	require.NoError(t, err)
	require.Nil(t, ctx)
	require.Nil(t, rel)
}

func TestCompilerContextGetRelationPropagatesNonNoSuchTableError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	eng := mock_frontend.NewMockEngine(ctrl)
	mainDB := mock_frontend.NewMockDatabase(ctrl)

	relationErr := moerr.NewInternalErrorNoCtx("relation failed")
	eng.EXPECT().Database(gomock.Any(), "testdb", nil).Return(mainDB, nil)
	mainDB.EXPECT().Relation(gomock.Any(), "t1", nil).Return(nil, relationErr)

	c := &compilerContext{
		proc:      proc,
		engine:    eng,
		defaultDB: "testdb",
	}

	ctx, rel, err := c.getRelation("testdb", "t1", nil)
	require.Error(t, err)
	require.Nil(t, ctx)
	require.Nil(t, rel)
}
