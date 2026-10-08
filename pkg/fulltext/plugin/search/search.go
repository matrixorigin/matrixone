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

// Package search runs a classic fulltext IndexSearchScan: a MATCH over a
// classic fulltext index. Scan holds the search itself.
package search

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	ftplan "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/plan"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Hooks is the classic fulltext IndexSearchScan reader factory.
type Hooks struct{}

var _ searchplugin.Hooks = Hooks{}
var _ searchplugin.EmptyScanHooks = Hooks{}

// NewReader returns the reader of one classic fulltext MATCH search.
func (Hooks) NewReader(proc *process.Process, spec *plan.IndexSearchScan, req searchplugin.Request) (engine.Reader, error) {
	if proc == nil {
		return nil, moerr.NewInvalidStateNoCtx("fulltext index search requires a process")
	}
	if spec == nil || spec.Index == nil {
		return nil, moerr.NewInvalidInputNoCtx("fulltext index search is missing its index metadata")
	}
	opts, err := ftplan.DecodeScanOptions(spec.GetAlgoOptions())
	if err != nil {
		return nil, err
	}
	scan := NewScan(req.ResultLimit)
	if err := scan.SetParam([]byte(spec.Index.IndexAlgoParams)); err != nil {
		return nil, err
	}
	return &reader{proc: proc, spec: spec, req: req, opts: opts, scan: scan}, nil
}

// EmptyScan checks a search that does not run, in the order a running search
// checks it: the zero-relevance guard, then the pattern.
func (Hooks) EmptyScan(proc *process.Process, _ *plan.IndexSearchScan, req searchplugin.Request) error {
	return checkGuardAndPattern(proc.Ctx, req)
}

// checkGuardAndPattern runs the zero-relevance guard for a MATCH score threshold
// only known at EXECUTE (a prepared '?') before the pattern is validated, so the
// guard decides the outcome whatever the search term binds to. The refusal it
// restates is a plan-time property of the threshold alone; letting a NULL or
// empty pattern report its own error first would answer `AGAINST(NULL) > ?`
// differently from the identical literal, which the planner refuses outright.
func checkGuardAndPattern(ctx context.Context, req searchplugin.Request) error {
	guard, _ := req.AlgoValue(fulltext.ZeroRelevanceGuardExpr)
	if err := fulltext.CheckZeroRelevanceGuard(ctx, guard); err != nil {
		return err
	}
	if req.QueryIsNull {
		return moerr.NewInvalidInput(ctx, "fulltext search pattern must not be NULL")
	}
	if len(req.QueryPayload) == 0 {
		return moerr.NewInvalidInput(ctx, "fulltext search pattern must not be empty")
	}
	return nil
}

// reader emits the (doc_id, score) rows of one classic fulltext search.
type reader struct {
	proc    *process.Process
	spec    *plan.IndexSearchScan
	req     searchplugin.Request
	opts    ftplan.ScanOptions
	scan    *Scan
	started bool
	closed  bool
}

var _ engine.Reader = (*reader)(nil)
var _ engine.ExplainDiagnosticReader = (*reader)(nil)

// start runs the index match of the search.
func (r *reader) start() error {
	proc := r.proc
	r.scan.ResetRowState(proc)
	r.scan.SetScanSnapshot(r.spec.GetScanSnapshot())

	// A subscribed source runs the internal SQL in the publisher account.
	var sourceRef, indexRef *plan.ObjectRef
	if source := r.spec.GetSourceTable(); source.GetPubInfo() != nil {
		sourceRef = source
		if hidden := r.spec.GetHiddenTables(); len(hidden) > 0 {
			indexRef = hidden[0].GetObject()
		}
	}
	sourceTable, indexTable, err := r.scan.ResolveExecutionTarget(
		proc.Ctx, sourceRef, indexRef, r.opts.SourceTable, r.opts.IndexTable)
	if err != nil {
		return err
	}

	if err := checkGuardAndPattern(proc.Ctx, r.req); err != nil {
		return err
	}
	pattern := string(r.req.QueryPayload)

	scoreAlgo, err := fulltext.GetScoreAlgo(proc)
	if err != nil {
		return err
	}

	// The unique-join-keys runtime filter (pre-filter pushdown) restricts the doc_ids
	// the index table reads return.
	if r.req.HasMembershipFilter && len(r.req.MembershipFilter) > 0 {
		payload, err := fulltext.BuildMembershipFilter(proc, r.req.MembershipFilter)
		if err != nil {
			return err
		}
		r.scan.SetMembershipFilter(payload)
	}

	return r.scan.Match(proc, sourceTable, indexTable, pattern, r.opts.Mode, r.spec.Index.IndexAlgoParams, scoreAlgo)
}

// Read emits the next batch of the search result into out; it returns true at
// the end of the result.
func (r *reader) Read(_ context.Context, _ []string, _ *plan.Expr, _ *mpool.MPool, out *batch.Batch) (bool, error) {
	if r.closed {
		return true, nil
	}
	if !r.started {
		r.started = true
		if err := r.start(); err != nil {
			return false, err
		}
	}
	result, err := r.scan.Call(r.proc, out)
	if err != nil {
		return false, err
	}
	if result.Status == vm.ExecStop || result.Batch == nil || result.Batch.RowCount() == 0 {
		return true, nil
	}
	return false, nil
}

// TakeExplainDiagnostics returns the logical plans of the internal SQL.
func (r *reader) TakeExplainDiagnostics() []*plan.Query {
	return r.scan.TakeBackgroundQueries()
}

// Close releases the search. It is idempotent.
func (r *reader) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	r.scan.Free(r.proc)
	return nil
}

func (*reader) SetOrderBy([]*plan.OrderBySpec)       {}
func (*reader) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (*reader) SetIndexParam(*plan.IndexReaderParam) {}
func (*reader) SetFilterZM(objectio.ZoneMap)         {}
