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

package vectorplan

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
	tableFuncs  = map[string]TableFuncBuilder{}
)

// RegisterTableFunc installs a per-name table-function builder. Called from
// plugin init(). Panics on duplicate registration.
//
// pkg/sql/plan/query_builder.go consults this registry in its
// table-function dispatch switch (default arm) so per-algorithm builders
// can live entirely inside the algo's plugin package.
func RegisterTableFunc(name string, b TableFuncBuilder) {
	tableFuncMu.Lock()
	defer tableFuncMu.Unlock()
	if _, ok := tableFuncs[name]; ok {
		panic("vectorplan: duplicate RegisterTableFunc for " + name)
	}
	tableFuncs[name] = b
}

// TableFunc returns the registered builder for name, or (nil, false) if
// none is registered.
func TableFunc(name string) (TableFuncBuilder, bool) {
	tableFuncMu.RLock()
	defer tableFuncMu.RUnlock()
	b, ok := tableFuncs[name]
	return b, ok
}
