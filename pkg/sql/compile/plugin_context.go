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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/maintenance"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/idxcron"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	// Blank-import the central plugin registration list so every
	// vector-index plugin's init() fires whenever compile is loaded
	// (production via cmd/mo-service and every test that exercises compile).
	_ "github.com/matrixorigin/matrixone/pkg/indexplugin/all"

	// And the parallel ISCP-hook wiring (kept in a separate sub-package
	// to avoid the pkg/iscp ↔ pkg/sql/plan ↔ indexplugin/all cycle —
	// see pkg/indexplugin/iscp/import.go for the rationale).
	_ "github.com/matrixorigin/matrixone/pkg/indexplugin/iscp"
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

func (p *pluginCompileCtx) Database() engine.Database        { return p.dbSource }
func (p *pluginCompileCtx) QryDatabase() string              { return p.qryDatabase }
func (p *pluginCompileCtx) OriginalTableDef() *plan.TableDef { return p.originalTableDef }
func (p *pluginCompileCtx) IndexInfo() *plan.CreateTable     { return p.indexInfo }
func (p *pluginCompileCtx) MainTableID() uint64              { return p.mainTableID }
func (p *pluginCompileCtx) MainExtra() *api.SchemaExtra      { return p.mainExtra }
func (p *pluginCompileCtx) RunSql(sql string) error          { return p.c.runSql(sql) }

func (p *pluginCompileCtx) RunIndexMaintenanceSQL(sql, function, indexName string) error {
	if p.c == nil || p.c.proc == nil || p.originalTableDef == nil || p.dbSource == nil || indexName == "" {
		return moerr.NewInvalidInputNoCtx("missing index maintenance compiler owner")
	}
	defs := p.originalTableDef.Indexes
	if p.indexInfo != nil && p.indexInfo.TableDef != nil {
		defs = p.indexInfo.TableDef.Indexes
	}
	group := make(map[string]*plan.IndexDef)
	var algorithm string
	for _, def := range defs {
		if def == nil || def.IndexName != indexName {
			continue
		}
		if def.IndexAlgoTableType == "" || group[def.IndexAlgoTableType] != nil || (algorithm != "" && !strings.EqualFold(algorithm, def.IndexAlgo)) {
			return moerr.NewInvalidInputNoCtx("ambiguous index maintenance group")
		}
		algorithm = def.IndexAlgo
		group[def.IndexAlgoTableType] = def
	}
	plugin, ok := indexplugin.Get(algorithm)
	if !ok {
		return moerr.NewInvalidInputNoCtx("missing index maintenance plugin")
	}
	descriptor, ok := plugin.Compile().(compileplugin.IndexMaintenanceDescriptor)
	if !ok {
		return moerr.NewInvalidInputNoCtx("missing index maintenance descriptor")
	}
	expected, err := descriptor.IndexMaintenanceRoles(function, group)
	if err != nil {
		return err
	}
	selected := maintenance.SelectDefinitions(group, expected)
	if selected == nil {
		return moerr.NewInvalidInputNoCtx("index maintenance roles do not match the owner")
	}

	spec, err := compileplugin.ResolveIndexMaintenanceSpec(p.c.proc.Ctx, p.qryDatabase, p.dbSource, p.originalTableDef.Name, p.mainTableID, function, selected)
	if err != nil {
		return err
	}
	res, err := p.c.runSqlWithResultAndMaintenance(sql, NoAccountId, executor.StatementOption{}, spec, descriptor)
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

// RunWithSourceReadCacheSkip runs fn with SkipMemoryCacheWrites attached to the
// compile context, so block reads performed inside fn — notably an index build's
// source-table scans (kmeans sample + entry assignment) — do NOT populate the
// fileservice memory cache. The build reads the whole source once; queries read only
// the candidate rows of it, so caching the build's reads would just evict the blocks
// the queries hit. Mirrors the policy LOAD DATA
// (external) already uses for one-shot bulk reads. runSqlWithResultAndOptions
// reads c.proc.Ctx for the sub-execution, so
// attaching the policy here propagates to every read in the build; it is restored
// afterward. The build runs synchronously within this compile, so the temporary
// swap is single-threaded.
func (p *pluginCompileCtx) RunWithSourceReadCacheSkip(fn func() error) error {
	if p.c == nil || p.c.proc == nil {
		return fn()
	}
	prev := p.c.proc.Ctx
	p.c.proc.Ctx = fileservice.WithFileServicePolicy(prev, fileservice.SkipMemoryCacheWrites)
	defer func() { p.c.proc.Ctx = prev }()
	return fn()
}

func (p *pluginCompileCtx) BuildIndexTable(def *plan.TableDef) error {
	return indexTableBuild(p.c, p.mainTableID, p.mainExtra, def, p.dbSource)
}

func (p *pluginCompileCtx) ResolveVariable(name string, isSystemVar, isGlobalVar bool) (any, error) {
	// Routed through resolveVariableOrDefault for nil-safety: when no
	// resolver is attached fall back to gSysVarsDefs defaults rather
	// than panicking. Background detection no longer rides on whether
	// this errors — callers that need to distinguish frontend from
	// background should call IsFrontend() instead.
	return resolveVariableOrDefault(p.c.proc, name, isSystemVar, isGlobalVar)
}

func (p *pluginCompileCtx) IsFrontend() bool {
	return p.c.proc.Base.IsFrontend
}

// IsTableClone reports whether this compile runs inside a table-clone scope
// (`create table … clone`) — which is how snapshot/restore replays a table,
// and which `IsFrontend` cannot distinguish (it is true for the restore
// backExec too). This is the same signal the experimental-flag gate keys on
// (`isExperimentalEnabled`, ddl_index_algo.go). It lets an index plugin
// restore a prebuilt model verbatim instead of rebuilding it. The sync-create
// variant has no scope, so it reports false.
func (p *pluginCompileCtx) IsTableClone() bool {
	return p.scope != nil && p.scope.IsTableClone()
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
