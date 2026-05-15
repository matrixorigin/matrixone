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
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/idxcron"
	compileplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/compile"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	// Blank-import vector-index plugins so their init() registrations fire
	// any time this package is loaded (production via cmd/mo-service and
	// every test that exercises compile).
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin"
)

// pluginCompileCtx adapts a *Scope + *Compile to compileplugin.CompileContext
// so vector-index plugin hooks can drive DDL without importing this package.
type pluginCompileCtx struct {
	scope            *Scope
	c                *Compile
	mainTableID      uint64
	mainExtra        *api.SchemaExtra
	dbSource         engine.Database
	qryDatabase      string
	originalTableDef *plan.TableDef
	indexInfo        *plan.CreateTable
}

func newPluginCompileCtx(
	scope *Scope,
	c *Compile,
	mainTableID uint64,
	mainExtra *api.SchemaExtra,
	dbSource engine.Database,
	qryDatabase string,
	originalTableDef *plan.TableDef,
	indexInfo *plan.CreateTable,
) *pluginCompileCtx {
	return &pluginCompileCtx{
		scope:            scope,
		c:                c,
		mainTableID:      mainTableID,
		mainExtra:        mainExtra,
		dbSource:         dbSource,
		qryDatabase:      qryDatabase,
		originalTableDef: originalTableDef,
		indexInfo:        indexInfo,
	}
}

// newPluginCompileCtxForSync is a minimal CompileContext for plugin hooks
// that only need the process (ResolveVariable / Ctx). Used by
// CreateAllIndexUpdateTasks to invoke IdxcronMetadata.
func newPluginCompileCtxForSync(c *Compile) *pluginCompileCtx {
	return &pluginCompileCtx{c: c}
}

func (p *pluginCompileCtx) Ctx() compileplugin.Context { return p.c.proc.Ctx }

func (p *pluginCompileCtx) Database() engine.Database      { return p.dbSource }
func (p *pluginCompileCtx) QryDatabase() string            { return p.qryDatabase }
func (p *pluginCompileCtx) OriginalTableDef() *plan.TableDef { return p.originalTableDef }
func (p *pluginCompileCtx) IndexInfo() *plan.CreateTable   { return p.indexInfo }
func (p *pluginCompileCtx) MainTableID() uint64            { return p.mainTableID }
func (p *pluginCompileCtx) MainExtra() *api.SchemaExtra    { return p.mainExtra }
func (p *pluginCompileCtx) RunSql(sql string) error        { return p.c.runSql(sql) }

func (p *pluginCompileCtx) BuildIndexTable(def *plan.TableDef) error {
	return indexTableBuild(p.c, p.mainTableID, p.mainExtra, def, p.dbSource)
}

func (p *pluginCompileCtx) ResolveVariable(name string, isSystemVar, isGlobalVar bool) (any, error) {
	return p.c.proc.GetResolveVariableFunc()(name, isSystemVar, isGlobalVar)
}

func (p *pluginCompileCtx) IsExperimentalEnabled(flag string) (bool, error) {
	return p.scope.isExperimentalEnabled(p.c, flag)
}

func (p *pluginCompileCtx) IsCCPRTaskTransaction() bool {
	return p.c.isCCPRTaskTransaction()
}

func (p *pluginCompileCtx) IsTableFromPublication(tableDef *plan.TableDef) bool {
	return isTableFromPublication(tableDef)
}

func (p *pluginCompileCtx) SinkerTypeFromAlgo(algo string) int8 {
	return getSinkerTypeFromAlgo(algo)
}

func (p *pluginCompileCtx) CreateIndexCdcTask(dbName, tableName string, tableID uint64, indexName string,
	sinkerType int8, startFromNow bool, sql string, tableDef *plan.TableDef) error {
	return CreateIndexCdcTask(p.c, dbName, tableName, tableID, indexName, sinkerType, startFromNow, sql, tableDef)
}

func (p *pluginCompileCtx) DropIndexCdcTask(tableDef *plan.TableDef, dbName, tableName, indexName string) error {
	return DropIndexCdcTask(p.c, tableDef, dbName, tableName, indexName)
}

// RunSqlWithResult forwards to runSqlWithResult using NoAccountId
// (matches the legacy ddl_index_algo.go:handleIndexColCount call
// site). Callers must Close() the returned Result.
func (p *pluginCompileCtx) RunSqlWithResult(sql string) (executor.Result, error) {
	return p.c.runSqlWithResult(sql, NoAccountId)
}

func (p *pluginCompileCtx) RegisterIdxcronUpdate(
	tableID uint64, dbName, tableName, indexName, action string, metadata []byte,
) error {
	return idxcron.RegisterUpdate(
		p.c.proc.Ctx,
		p.c.proc.GetService(),
		p.c.proc.GetTxnOperator(),
		tableID, dbName, tableName, indexName, action, string(metadata),
	)
}
