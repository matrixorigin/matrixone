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

package compile

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type testCompileContext struct {
	original  *plan.TableDef
	db        string
	info      *plan.CreateTable
	prepare   bool
	build     bool
	publish   bool
	runErr    error
	dropErr   error
	createErr error
	built     int
	runSQL    []string
	created   bool
}

func (c *testCompileContext) Ctx() compileplugin.Context       { return nil }
func (c *testCompileContext) Database() engine.Database        { return nil }
func (c *testCompileContext) QryDatabase() string              { return c.db }
func (c *testCompileContext) OriginalTableDef() *plan.TableDef { return c.original }
func (c *testCompileContext) IndexInfo() *plan.CreateTable     { return c.info }
func (c *testCompileContext) MainTableID() uint64              { return 0 }
func (c *testCompileContext) MainExtra() *api.SchemaExtra      { return nil }
func (c *testCompileContext) RunSql(sql string) error {
	c.runSQL = append(c.runSQL, sql)
	return c.runErr
}
func (c *testCompileContext) BuildIndexTable(*plan.TableDef) error            { c.built++; return c.runErr }
func (c *testCompileContext) ResolveVariable(string, bool, bool) (any, error) { return int64(0), nil }
func (c *testCompileContext) IsFrontend() bool                                { return false }
func (c *testCompileContext) IsTableClone() bool                              { return false }
func (c *testCompileContext) IsExperimentalEnabled(string) (bool, error)      { return true, nil }
func (c *testCompileContext) IsCCPRTaskTransaction() bool                     { return false }
func (c *testCompileContext) IsTableFromPublication(_ *plan.TableDef) bool    { return false }
func (c *testCompileContext) SinkerTypeFromAlgo(string) int8                  { return 0 }
func (c *testCompileContext) CreateIndexCdcTask(_, _ string, _ uint64, _ string, _ int8, _ bool, _ string, _ *plan.TableDef) error {
	c.created = true
	return c.createErr
}
func (c *testCompileContext) DropIndexCdcTask(_ *plan.TableDef, _, _, _ string) error {
	return c.dropErr
}
func (c *testCompileContext) RunSqlWithResult(string) (executor.Result, error) {
	return executor.Result{}, nil
}
func (c *testCompileContext) RegisterIdxcronUpdate(uint64, string, string, string, string, []byte) error {
	return nil
}
func (c *testCompileContext) IsAlterCopyPrepare() bool     { return c.prepare }
func (c *testCompileContext) IsAlterCopyIndexBuild() bool  { return c.build }
func (c *testCompileContext) IsAlterCopyPublication() bool { return c.publish }

func fulltextTestDefs(async string) map[string]*plan.IndexDef {
	return map[string]*plan.IndexDef{"": {
		IndexName: "ft", IndexTableName: "__mo_ft", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(),
		IndexAlgoParams: async, Parts: []string{"body"},
	}}
}

func fulltextTestTable() *plan.TableDef {
	return &plan.TableDef{Name: "t", TblId: 7, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"}}
}

func TestFulltextAlterCopyPhases(t *testing.T) {
	base := func() *testCompileContext { return &testCompileContext{db: "db", original: fulltextTestTable()} }

	// The child CREATE only materializes its hidden table. It must not populate
	// data or register a task before the replacement relation is published.
	ctx := base()
	ctx.prepare = true
	ctx.info = &plan.CreateTable{IndexTables: []*plan.TableDef{{Name: "__mo_ft"}}}
	require.NoError(t, (Hooks{}).HandleCreateIndex(ctx, fulltextTestDefs("")))
	require.Equal(t, 1, ctx.built)
	require.Empty(t, ctx.runSQL)
	require.False(t, ctx.created)

	// Synchronous publication only exposes the already-built physical rows.
	ctx = base()
	ctx.publish = true
	require.NoError(t, (Hooks{}).HandleCreateIndex(ctx, fulltextTestDefs("")))
	require.False(t, ctx.created)

	// An asynchronous index is left empty during preparation and receives its
	// one CDC task after the final relation identity is known.
	ctx = base()
	ctx.prepare = true
	require.NoError(t, (Hooks{}).HandleCreateIndex(ctx, fulltextTestDefs(`{"async":"true"}`)))
	require.Empty(t, ctx.runSQL)
	ctx = base()
	ctx.publish = true
	require.NoError(t, (Hooks{}).HandleCreateIndex(ctx, fulltextTestDefs(`{"async":"true"}`)))
	require.True(t, ctx.created)
}

func TestFulltextAlterCopyPublicationErrorsAreReturned(t *testing.T) {
	ctx := &testCompileContext{db: "db", original: fulltextTestTable(), publish: true}
	ctx.dropErr = errors.New("drop task failed")
	require.ErrorIs(t, (Hooks{}).HandleCreateIndex(ctx, fulltextTestDefs(`{"async":"true"}`)), ctx.dropErr)

	ctx = &testCompileContext{db: "db", original: fulltextTestTable(), publish: true, createErr: errors.New("create task failed")}
	require.ErrorIs(t, (Hooks{}).HandleCreateIndex(ctx, fulltextTestDefs(`{"async":"true"}`)), ctx.createErr)
}

func TestFulltextHookValidation(t *testing.T) {
	err := (Hooks{}).HandleCreateIndex(&testCompileContext{}, map[string]*plan.IndexDef{})
	require.Error(t, err)
	require.Error(t, (Hooks{}).HandleReindex(nil, nil, false, false))
	require.Equal(t, "SELECT 1", func() string { _, sql, _ := (Hooks{}).RestoreInitSQL(nil, nil); return sql }())
}
