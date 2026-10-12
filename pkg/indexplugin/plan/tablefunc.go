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

package plan

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// TableFuncBuilder is the signature a vector-index plugin's table-function
// builder (e.g. ivfpq_create / ivfpq_search) must satisfy. Construct and
// append the FUNCTION_SCAN node; return its node ID. Use the PlanBuilder
// facade for any bind-tag / node-assembly primitives.
type TableFuncBuilder func(pb PlanBuilder, tbl *tree.TableFunction, ctx BindContext, exprs []*plan.Expr, children []int32) (int32, error)

var (
	tableFuncMu sync.RWMutex
	tableFuncs  = map[string]tableFuncRegistration{}
)

type tableFuncRegistration struct {
	builder         TableFuncBuilder
	coordinatorOnly bool
}

// RegisterTableFunc installs a per-name table-function builder. Called from
// plugin init(). Panics on duplicate registration.
//
// pkg/sql/plan/query_builder.go consults this registry in its
// table-function dispatch switch (default arm) so per-algorithm builders
// can live entirely inside the algo's plugin package.
func RegisterTableFunc(name string, b TableFuncBuilder) {
	registerTableFunc(name, b, false)
}

// RegisterCoordinatorTableFunc registers a builder whose execution must share
// the initiating CN's transaction workspace. Its input scans may still run on
// other CNs, but APPLY must gather them before invoking this function.
func RegisterCoordinatorTableFunc(name string, b TableFuncBuilder) {
	registerTableFunc(name, b, true)
}

func registerTableFunc(name string, b TableFuncBuilder, coordinatorOnly bool) {
	tableFuncMu.Lock()
	defer tableFuncMu.Unlock()
	if _, ok := tableFuncs[name]; ok {
		panic("planplugin: duplicate RegisterTableFunc for " + name)
	}
	tableFuncs[name] = tableFuncRegistration{builder: b, coordinatorOnly: coordinatorOnly}
}

// TableFunc returns the registered builder for name, or (nil, false) if
// none is registered.
func TableFunc(name string) (TableFuncBuilder, bool) {
	tableFuncMu.RLock()
	defer tableFuncMu.RUnlock()
	registration, ok := tableFuncs[name]
	return registration.builder, ok
}

// TableFuncRequiresCoordinator reports the registered execution placement
// requirement. Unknown functions retain the default distributed behavior.
func TableFuncRequiresCoordinator(name string) bool {
	tableFuncMu.RLock()
	defer tableFuncMu.RUnlock()
	return tableFuncs[name].coordinatorOnly
}
