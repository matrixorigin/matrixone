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
	compileplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/compile"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	// Blank-import vector-index plugins so their init() registrations fire
	// any time this package is loaded (production via cmd/mo-service and
	// every test that exercises compile).
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
