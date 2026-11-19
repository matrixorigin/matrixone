// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"os"
	"os/exec"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

// Recursive CTE anchors that reference their own declaration re-enter
// bindCte while the declaration is still being bound as the initial
// statement, before it is published; without a guard that recursion grows
// without bound until the planner dies of a stack overflow. Every case below
// therefore runs in a child process with a bounded stack, a bounded deadline,
// and a terminated-and-reaped run, so an unfixed planner produces a recorded
// child failure instead of dying in-process.

const (
	recursiveReentryCaseEnv      = "MO_TEST_PLAN_RECURSIVE_REENTRY_CASE"
	recursiveReentrySequenceEnv  = "MO_TEST_PLAN_RECURSIVE_REENTRY_SEQUENCE"
	recursiveReentryMsgFragment  = "initial statement of Recursive Common Table Expression"
	recursiveReentryChildStack   = 128 << 20
	recursiveReentryChildTimeout = 180 * time.Second
	recursiveReentryParentWait   = 240 * time.Second
)

type recursiveReentryCase struct {
	name string
	sql  string
	// kind:
	//   "anchorError" - must fail with a typed ParseError naming the
	//                   initial statement of the recursive CTE.
	//   "plan"        - must build a plan (legal shapes that must keep working).
	//   "guard"       - must keep failing with the pre-existing guard message
	//                   carried in `mustContain`.
	kind        string
	mustContain string
}

var recursiveReentryCases = []recursiveReentryCase{
	// A recursive reference inside the initial statement: the whole crash class.
	{
		name: "exact-issue-anchor-refs-qn",
		sql:  "with recursive qn as (select 1 from qn union all select 1 from qn) select * from qn",
		kind: "anchorError",
	},
	{
		name: "three-union-all-arms",
		sql:  "with recursive qn as (select 1 from qn union all select 1 from t1 union all select 1 from qn) select * from qn",
		kind: "anchorError",
	},
	{
		name: "union-distinct-duplicate",
		sql:  "with recursive qn as (select 1 from qn union select 1 from qn) select * from qn",
		kind: "anchorError",
	},
	{
		name: "multiple-ctes-bad-anchors",
		sql:  "with recursive a as (select 1 from a union all select 1 from a), b as (select 1 from b union all select 1 from b) select * from a",
		kind: "anchorError",
	},
	{
		name: "derived-and-outer-union",
		sql:  "select * from (with recursive qn as (select 1 from qn union all select 1 from qn) select * from qn) as d union all select 2",
		kind: "anchorError",
	},
	{
		name: "insert-select-bad-anchor",
		sql:  "insert into t1 (a) with recursive qn as (select 1 from qn union all select 1 from qn) select 1 from qn",
		kind: "anchorError",
	},

	// Legal shapes whose references are NOT the recursion feed of their own
	// initial statement: a valid anchor, legal multiple recursive members, a
	// derived-table anchor, a same-word inner CTE with a distinct declaration,
	// a same-word derived alias, and a schema-qualified real table.
	{
		name: "valid-anchor-single-arm",
		sql:  "with recursive qn(n) as (select 1 union all select n + 1 from qn where n < 3) select * from qn",
		kind: "plan",
	},
	{
		name: "valid-anchor-two-legal-arms",
		sql:  "with recursive c as (select a from t1 union all select a + 1 from c where a < 3 union all select a + 1 from c where a < 4) select * from c",
		kind: "plan",
	},
	{
		name: "derived-table-anchor",
		sql:  "with recursive qn(n) as (select n from (select 1 as n) as seed union all select n + 1 from qn where n < 3) select * from qn",
		kind: "plan",
	},
	{
		name: "inner-cte-same-word-distinct-declaration",
		sql:  "with recursive qn as ((with qn as (select 7) select * from qn) union all select 1 from qn) select * from qn",
		kind: "plan",
	},
	{
		name: "inner-alias-same-word",
		sql:  "with recursive qn as (select 1 from (select 1) as qn union all select 1 from qn) select * from qn",
		kind: "plan",
	},
	{
		name: "schema-qualified-real-table",
		sql:  "with recursive t1 as (select a from cte_test.t1 union all select a + 1 from t1 where a < 3) select * from t1",
		kind: "plan",
	},

	// Existing guards must keep rejecting with their own diagnostics.
	{
		name:        "self-join-in-recursive-member",
		sql:         "with recursive c as (select a from t1 union all select a from c as x join c as y on x.a = y.a) select * from c",
		kind:        "guard",
		mustContain: "unsupport multiple recursive table expr in recursive CTE",
	},
	{
		name:        "recursive-reference-inside-subquery",
		sql:         "with recursive c as (select a from t1 union all select a + 1 from c where a in (select a from c)) select * from c",
		kind:        "guard",
		mustContain: "the recursive table must be referenced only once, and not in any subquery",
	},
	{
		name:        "mixing-union-modes",
		sql:         "with recursive c as (select a from t1 union all select a + 1 from c where a < 3 union select a + 1 from c where a < 2) select * from c",
		kind:        "guard",
		mustContain: "mixing UNION ALL and UNION DISTINCT in recursive cte is not supported",
	},
	{
		name:        "projection-column-count",
		sql:         "with recursive c as (select a from t1 union all select a, a from c) select * from c",
		kind:        "guard",
		mustContain: "recursive cte c projection error",
	},
}

const (
	recursiveReentrySequenceValidSQL   = "with recursive qn(n) as (select 1 union all select n + 1 from qn where n < 3) select * from qn"
	recursiveReentrySequenceBadSQL     = "with recursive qn as (select 1 from qn union all select 1 from qn) select * from qn"
	recursiveReentrySequencePrepareSQL = "prepare p1 from 'with recursive qn(n) as (select 1 union all select n + 1 from qn where n < 3) select * from qn'"
)

func buildRecursiveReentryCasePlan(t *testing.T, sql string) (*Plan, error) {
	t.Helper()
	opt := NewMockOptimizer(false, newPlanTestProcess(t))
	stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	if len(stmts) != 1 {
		t.Fatalf("parse %q: got %d statements, want 1", sql, len(stmts))
	}
	defer stmts[0].Free()
	return BuildPlan(opt.CurrentContext(), stmts[0], false, nil)
}

func runRecursiveReentryCase(t *testing.T, c recursiveReentryCase) {
	t.Helper()
	switch c.kind {
	case "anchorError":
		_, err := buildRecursiveReentryCasePlan(t, c.sql)
		require.Error(t, err, "case %s: the bad anchor-reference shape must be rejected", c.name)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrParseError),
			"case %s: want typed ParseError, got %v", c.name, err)
		require.ErrorContains(t, err, recursiveReentryMsgFragment,
			"case %s: unexpected error for %q", c.name, c.sql)
	case "plan":
		_, err := buildRecursiveReentryCasePlan(t, c.sql)
		require.NoError(t, err, "case %s must keep building a plan", c.name)
	case "guard":
		_, err := buildRecursiveReentryCasePlan(t, c.sql)
		require.Error(t, err, "case %s: existing guard must keep rejecting", c.name)
		require.ErrorContains(t, err, c.mustContain, "case %s: existing guard diagnostic changed", c.name)
	default:
		t.Fatalf("case %s: unknown kind %q", c.name, c.kind)
	}
}

// TestRecursiveCteAnchorReentrySubprocess is the child of
// TestRecursiveCteAnchorReentry; it is a no-op unless one of the private
// environment variables selects a case or the sequential same-context probe.
func TestRecursiveCteAnchorReentrySubprocess(t *testing.T) {
	caseIdx, hasCase := os.LookupEnv(recursiveReentryCaseEnv)
	_, hasSeq := os.LookupEnv(recursiveReentrySequenceEnv)
	if !hasCase && !hasSeq {
		return
	}
	// Bound the stack: the unfixed planner dies here within seconds instead of
	// exhausting the default 1 GiB stack, so the parent records a fast,
	// deterministic child failure.
	debug.SetMaxStack(recursiveReentryChildStack)

	if hasSeq {
		opts := NewMockOptimizer(false, newPlanTestProcess(t))
		_, err := runOneStmt(opts, t, recursiveReentrySequenceValidSQL)
		require.NoError(t, err, "valid recursive query before the rejected one")

		_, err = runOneStmt(opts, t, recursiveReentrySequenceBadSQL)
		require.Error(t, err, "bad anchor reference through the same compiler context")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrParseError),
			"want typed ParseError from the same compiler context, got %v", err)
		require.ErrorContains(t, err, recursiveReentryMsgFragment)

		_, err = runOneStmt(opts, t, recursiveReentrySequenceValidSQL)
		require.NoError(t, err, "next query on the same compiler context must still work")

		_, err = runOneStmt(opts, t, recursiveReentrySequencePrepareSQL)
		require.NoError(t, err, "prepared binding on the same compiler context must still work")
		return
	}

	idx, err := strconv.Atoi(caseIdx)
	if err != nil || idx < 0 || idx >= len(recursiveReentryCases) {
		t.Fatalf("invalid case index %q", caseIdx)
	}
	runRecursiveReentryCase(t, recursiveReentryCases[idx])
}

// childFatalHeader surfaces the fatal-error header of a crashed child from the
// full output; the bounded tail alone can end deep inside the goroutine dump.
func childFatalHeader(out []byte) string {
	var headers []string
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "fatal error") ||
			strings.Contains(line, "goroutine stack exceeds") ||
			strings.Contains(line, "stack overflow") {
			headers = append(headers, line)
			if len(headers) >= 3 {
				break
			}
		}
	}
	return strings.Join(headers, "\n")
}

func runRecursiveReentryChild(t *testing.T, env []string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), recursiveReentryParentWait)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^TestRecursiveCteAnchorReentrySubprocess$",
		"-test.timeout="+recursiveReentryChildTimeout.String(),
		"-test.v")
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	rc := -1
	if cmd.ProcessState != nil {
		rc = cmd.ProcessState.ExitCode()
	}
	// Keep the tail bounded so a stack-overflow trace stays readable in logs,
	// and prepend the fatal header that sits before the tail window.
	const tailLen = 4096
	tail := string(out)
	if len(tail) > tailLen {
		tail = "..." + tail[len(tail)-tailLen:]
	}
	if header := childFatalHeader(out); header != "" {
		tail = header + "\n" + tail
	}
	if ctx.Err() != nil {
		t.Fatalf("child hit the parent wait deadline and was killed (rc=%d, err=%v); child output tail:\n%s",
			rc, err, tail)
	}
	return rc, tail
}

func TestRecursiveCteAnchorReentry(t *testing.T) {
	for i, c := range recursiveReentryCases {
		t.Run(c.name, func(t *testing.T) {
			rc, tail := runRecursiveReentryChild(t, []string{
				recursiveReentryCaseEnv + "=" + strconv.Itoa(i),
			})
			if rc != 0 {
				t.Fatalf("case %q: child planner exited rc=%d; child output tail:\n%s",
					c.name, rc, tail)
			}
		})
	}

	t.Run("same-compiler-context-across-rejection", func(t *testing.T) {
		rc, tail := runRecursiveReentryChild(t, []string{recursiveReentrySequenceEnv + "=1"})
		if rc != 0 {
			t.Fatalf("sequence child exited rc=%d; child output tail:\n%s", rc, tail)
		}
	})
}
