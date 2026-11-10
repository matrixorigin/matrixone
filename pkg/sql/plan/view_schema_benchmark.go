//go:build viewmetadatabench

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ViewSchemaBenchmarkStats 只在显式基准构建中存在。计数属于单个 request，
// 不含候选 SQL/授权 SQL 的目录扫描，不代表磁盘/RPC 次数或全进程内存。
// ViewBinds 包含根推导及真正进入绑定的嵌套 View，不含 memo 替代的绑定。
type ViewSchemaBenchmarkStats struct {
	ViewBinds, RelationLookups, DatabaseLookups uint64
	CompletedRootBinds, MemoHits                uint64
	Roots, LogicalWork                          int
}

type viewSchemaBenchmarkState struct {
	enabled                                 bool
	binds, relationLookups, databaseLookups uint64
}

func (s *viewSchemaBenchmarkState) bind() {
	if s.enabled {
		s.binds++
	}
}
func (s *viewSchemaBenchmarkState) resolve() {
	if s.enabled {
		s.relationLookups++
	}
}
func (s *viewSchemaBenchmarkState) resolveDatabase() {
	if s.enabled {
		s.databaseLookups++
	}
}

// ConfigureBenchmark 必须在首次工作之前调用，不是生产激活/会话开关。
// collect=false 保留同一执行路径，用于检验计量的结果等价性与成本。
func (r *ViewSchemaRequest) ConfigureBenchmark(disableMemo, collect bool) error {
	select {
	case <-r.gate:
		defer func() { r.gate <- struct{}{} }()
	default:
		return ErrViewSchemaBusy
	}
	if err := context.Cause(r.ctx); err != nil {
		return err
	}
	if r.workCtx != nil {
		return moerr.NewInvalidStateNoCtx("benchmark configuration must precede view schema work")
	}
	r.memoDisabled = disableMemo
	r.benchmark.enabled = collect
	return nil
}

// BenchmarkStats 与 binder 使用同一 gate；调用者必须先结束消费者工作。
// Close 后仍可读计数，但不保留 compiler、事务、结果或 memo。
func (r *ViewSchemaRequest) BenchmarkStats() (ViewSchemaBenchmarkStats, error) {
	select {
	case <-r.gate:
		defer func() { r.gate <- struct{}{} }()
	default:
		return ViewSchemaBenchmarkStats{}, ErrViewSchemaBusy
	}
	return ViewSchemaBenchmarkStats{
		ViewBinds: r.benchmark.binds, RelationLookups: r.benchmark.relationLookups,
		DatabaseLookups: r.benchmark.databaseLookups, CompletedRootBinds: r.binds,
		MemoHits: r.hits, Roots: r.roots, LogicalWork: r.work,
	}, nil
}

func (r *ViewMetadataRequest) ConfigureBenchmark(disableMemo, collect bool) error {
	return r.schema.ConfigureBenchmark(disableMemo, collect)
}

func (r *ViewMetadataRequest) BenchmarkStats() (ViewSchemaBenchmarkStats, error) {
	return r.schema.BenchmarkStats()
}
