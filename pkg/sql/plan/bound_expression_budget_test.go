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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Observe the real migration context rather than substituting the binder or
// folder. Relative visits make generous/exact/one-less traces comparable.
type boundMigrationTraceContext struct {
	CompilerContext
	proc    *process.Process
	budget  *tableDumpBindingBudget
	initial int
	events  *[]string
}

func (c boundMigrationTraceContext) GetContext() context.Context {
	session := c.proc.GetSessionInfo()
	*c.events = append(*c.events, fmt.Sprintf("%d/%d/%d/%s/%s", c.budget.visits-c.initial, c.budget.nodes, c.budget.text, session.TimeZone, session.LCTimeNames))
	return c.CompilerContext.GetContext()
}

func boundMigrationMemoSnapshot(budget *tableDumpBindingBudget) []string {
	var snapshot []string
	for source, memo := range budget.probeMemo.sources {
		snapshot = append(snapshot, fmt.Sprintf("source:%d:%x", source.mode, source.source))
		for session, result := range memo.results {
			snapshot = append(snapshot, fmt.Sprintf("result:%d:%x:%s:%s:%x:%x", source.mode, source.source, session.zone, session.locale, result.data, result.value))
		}
	}
	for _, hint := range budget.sessionHints {
		snapshot = append(snapshot, fmt.Sprintf("hint:%s:%s", hint.zone, hint.locale))
	}
	sort.Strings(snapshot)
	return snapshot
}

func TestProjectedBoundExpressionCumulativeBudget(t *testing.T) {
	creationZone := time.FixedZone("creation", 8*3600)
	compiler, source := legacyFloatScopeTable(t, "create table t(a double,g varchar(200) default(concat(a,from_unixtime(0))),h varchar(200) default(concat(a,from_unixtime(0))))", 4, creationZone)
	before, err := proto.Marshal(source)
	require.NoError(t, err)
	// Every run starts with a fresh proof owner, memo and session-hint list.
	// Both expressions share that owner, just as table migration does.
	type cost struct{ visits, nodes, text, entries, bytes, hints int }
	run := func(initial int, fail bool) ([]*planpb.Expr, []cost) {
		proof := newTableDumpProofProcess(compiler)
		defer proof.Free()
		proofCtx := tableDumpBindContext{CompilerContext: compiler, ctx: compiler.GetContext(), proc: proof}
		budget := &tableDumpBindingBudget{
			visits: initial,
			// Search fairness depends on remaining headroom. Keep this budget
			// boundary fixture independent of Local and the host's named zones,
			// while retaining a rejected zone and a real session witness.
			probeTimeZones: func(yield func(*time.Location) bool) {
				for _, zone := range []*time.Location{time.UTC, creationZone} {
					if !yield(zone) {
						return
					}
				}
			},
		}
		var events []string
		traced := boundMigrationTraceContext{CompilerContext: proofCtx, proc: proof, budget: budget, initial: initial, events: &events}
		var results []*planpb.Expr
		var costs []cost
		for item := range persistedTableExpressions(source) {
			if !hasLegacyFloatStringCast(item.expr) {
				continue
			}
			repaired, err := rebindLegacyFloatStringExpression(traced, source, item, budget)
			require.Same(t, time.UTC, proof.GetSessionInfo().TimeZone)
			require.Equal(t, "en_US", proof.GetSessionInfo().LCTimeNames)
			costs = append(costs, cost{budget.visits - initial, budget.nodes, budget.text, budget.probeMemo.entries, budget.probeMemo.bytes, len(budget.sessionHints)})
			var wire []byte
			var marshalErr error
			if repaired != nil {
				wire, marshalErr = proto.Marshal(repaired)
				require.NoError(t, marshalErr)
			}
			observation := struct {
				Initial, Item                       int
				Tree                                []byte
				Error                               string
				Visits, Nodes, Text, Entries, Bytes int
				Memo, SessionTrace                  []string
			}{
				initial, len(costs) - 1, wire, fmt.Sprintf("%T:%v", err, err), budget.visits - initial, budget.nodes, budget.text, budget.probeMemo.entries, budget.probeMemo.bytes, boundMigrationMemoSnapshot(budget), append([]string(nil), events...),
			}
			observationJSON, marshalErr := json.Marshal(observation)
			require.NoError(t, marshalErr)
			t.Logf("migration-observation %s", observationJSON)
			if err != nil {
				require.True(t, fail)
				require.ErrorContains(t, err, "binding exceeds limit")
				require.Nil(t, repaired)
				require.Greater(t, budget.visits, tableDumpMaxCandidateVisits)
				return results, costs
			}
			require.NotNil(t, repaired)
			results = append(results, repaired)
		}
		require.False(t, fail, "one less unit of headroom must reject")
		require.Len(t, results, 2)
		return results, costs
	}
	expected, costs := run(0, false)
	require.Positive(t, costs[0].entries, "the fixture must exercise the real session memo")
	require.Positive(t, costs[0].hints, "the second expression must reuse a session witness")
	require.Greater(t, costs[1].visits, costs[0].visits, "expressions accumulate instead of resetting the budget")
	for i, e := range expected {
		wire, err := proto.Marshal(e)
		require.NoError(t, err)
		t.Logf("baseline item=%d cumulative=%+v repaired_sha256=%x", i, costs[i], sha256.Sum256(wire))
	}
	total := costs[1].visits
	exact, exactCosts := run(tableDumpMaxCandidateVisits-total, false)
	require.Equal(t, costs, exactCosts)
	for i := range expected {
		require.True(t, proto.Equal(expected[i], exact[i]), "complete repaired tree must match")
	}
	partial, failedCosts := run(tableDumpMaxCandidateVisits-total+1, true)
	require.Len(t, partial, 1, "the first expression must retain its successful prefix")
	require.Equal(t, costs[0], failedCosts[0])
	t.Logf("one-less cumulative=%+v", failedCosts)
	after, err := proto.Marshal(source)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Same(t, time.UTC, compiler.GetProcess().GetSessionInfo().TimeZone)
	require.Equal(t, "en_US", compiler.GetProcess().GetSessionInfo().LCTimeNames)
}
