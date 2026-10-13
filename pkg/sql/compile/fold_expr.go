// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Fold ID 属于 Compile 而非 selector。注册可能扩容 slice，求值也会修改
// executor，故两者必须在同一独占边界内；表达式仍由各 scope 单独持有。
// 锁不跨 datasource 的存储访问、runtime-filter 等待或 pipeline 启停/join。
func (c *Compile) replaceFoldExpr(proc *process.Process, expr *plan.Expr) error {
	c.filterExprMu.Lock()
	defer c.filterExprMu.Unlock()
	_, err := plan2.ReplaceFoldExpr(proc, expr, &c.filterExprExes)
	return err
}

func (c *Compile) evalFoldExpr(proc *process.Process, expr *plan.Expr) error {
	c.filterExprMu.Lock()
	defer c.filterExprMu.Unlock()
	return plan2.EvalFoldExpr(proc, expr, &c.filterExprExes)
}

// Reset/Free 仍要求旧执行的所有 scope 已退出；此锁不替代代际 join。
func (c *Compile) resetFilterExprExecutors() {
	c.filterExprMu.Lock()
	defer c.filterExprMu.Unlock()
	for _, executor := range c.filterExprExes {
		executor.ResetForNextQuery()
	}
}

func (c *Compile) freeFilterExprExecutors() {
	c.filterExprMu.Lock()
	defer c.filterExprMu.Unlock()
	for _, executor := range c.filterExprExes {
		executor.Free()
	}
	c.filterExprExes = nil
}
