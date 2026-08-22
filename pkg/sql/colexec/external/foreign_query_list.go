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

package external

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// DeriveForeignQueryList derives the candidate query-text list of an ESQL/SQL
// foreign external table scan from the scan's filter conjuncts — the foreign
// analogue of enumerating a directory for __mo_filepath (which has a file
// system to list; __mo_query does not, so candidates come from the predicate).
//
// Candidates are produced by query-level conjuncts of these shapes:
//
//	__mo_query =  <expr>            -> one candidate
//	__mo_query IN (<e1>, <e2>, ...) -> several
//	OR of the above                 -> the union
//
// where <expr> is compile-time evaluable (literals, prepare params, session
// variables, foldable functions — the same safety rules the classifier
// applies). Other query-level conjuncts (e.g. LIKE) generate nothing here but
// still prune the candidate list in FilterFileList afterwards. Duplicates are
// removed; a query runs once. If no conjunct generates a candidate, the
// caller falls back to the table's 'query' option or errors.
func DeriveForeignQueryList(ctx context.Context, node *plan.Node, proc *process.Process) ([]string, error) {
	var out []string
	seen := make(map[string]bool)
	add := func(vals []string) {
		for _, v := range vals {
			if !seen[v] {
				seen[v] = true
				out = append(out, v)
			}
		}
	}
	for _, conjunct := range node.FilterList {
		if !isFileLevelFilter(node, conjunct) {
			continue
		}
		vals, err := deriveQueryCandidates(ctx, node, proc, conjunct)
		if err != nil {
			return nil, err
		}
		add(vals)
	}
	return out, nil
}

// deriveQueryCandidates extracts candidates from one query-level conjunct.
// A nil result (no error) means the conjunct generates no candidates.
func deriveQueryCandidates(ctx context.Context, node *plan.Node, proc *process.Process, expr *plan.Expr) ([]string, error) {
	fn, ok := expr.Expr.(*plan.Expr_F)
	if !ok || fn.F == nil || fn.F.Func == nil {
		return nil, nil
	}
	switch fn.F.Func.ObjName {
	case "=":
		if len(fn.F.Args) != 2 {
			return nil, nil
		}
		// one side is the hidden column, the other the candidate text
		for i := 0; i < 2; i++ {
			if col, ok := fn.F.Args[i].Expr.(*plan.Expr_Col); ok && isFileLevelColumn(node, col.Col) {
				return evalQueryTexts(ctx, proc, fn.F.Args[1-i])
			}
		}
		return nil, nil
	case "in":
		if len(fn.F.Args) != 2 {
			return nil, nil
		}
		if col, ok := fn.F.Args[0].Expr.(*plan.Expr_Col); !ok || !isFileLevelColumn(node, col.Col) {
			return nil, nil
		}
		if list, ok := fn.F.Args[1].Expr.(*plan.Expr_List); ok && list.List != nil {
			var out []string
			for _, item := range list.List.List {
				vals, err := evalQueryTexts(ctx, proc, item)
				if err != nil {
					return nil, err
				}
				out = append(out, vals...)
			}
			return out, nil
		}
		// a folded IN list arrives as a vector literal; evaluate it whole
		return evalQueryTexts(ctx, proc, fn.F.Args[1])
	case "or":
		var out []string
		for _, arg := range fn.F.Args {
			vals, err := deriveQueryCandidates(ctx, node, proc, arg)
			if err != nil {
				return nil, err
			}
			if vals == nil {
				// one OR branch that generates nothing (e.g. a LIKE) makes
				// the whole disjunction non-generating: the branch could
				// match query texts we cannot enumerate.
				return nil, nil
			}
			out = append(out, vals...)
		}
		return out, nil
	default:
		return nil, nil
	}
}

// evalQueryTexts evaluates a column-free, compile-time-safe expression (the
// conjunct it came from already passed the classifier) and returns its string
// value(s). NULL values are skipped: NULL never equals anything.
func evalQueryTexts(ctx context.Context, proc *process.Process, expr *plan.Expr) ([]string, error) {
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	if vec == nil || vec.IsConstNull() {
		return nil, nil
	}
	if !vec.GetType().IsVarlen() {
		return nil, moerr.NewInvalidInput(ctx, "__mo_query must compare against a string value")
	}
	n := vec.Length()
	if vec.IsConst() {
		n = 1
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if vec.GetNulls().Contains(uint64(i)) {
			continue
		}
		out = append(out, vec.GetStringAt(i))
	}
	return out, nil
}
