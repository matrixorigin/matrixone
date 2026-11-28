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
	"context"
	"testing"

	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// buildPaginationStagePlan builds the recursive CTE whose enclosing wrapper
// selects each add one pagination stage around the union body, so the same
// finite witness covers 1, 2 and 4 admitted stages deterministically.
func buildPaginationStagePlan(t *testing.T, stages int) string {
	t.Helper()
	inner := "(SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 3)"
	for i := 0; i < stages; i++ {
		inner = "(" + inner + " LIMIT 2 OFFSET 1)"
	}
	sql := "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) " + inner + ") SELECT n FROM q"
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	defer stmt.Free()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	query, err := mock.Optimize(stmt)
	require.NoError(t, err, sql)
	return query.String()
}

func TestRecursiveWrapperPaginationPlanGrowsLinearly(t *testing.T) {
	one := len(buildPaginationStagePlan(t, 1))
	two := len(buildPaginationStagePlan(t, 2))
	four := len(buildPaginationStagePlan(t, 4))
	// Every admitted stage contributes one constant-size cluster of skip,
	// width and count arithmetic to the optimized plan: adding two stages may
	// not exceed a small multiple of adding one. The previous absolute-
	// coordinate composition re-embedded every earlier stage's accumulated
	// arithmetic inside each stage's candidate end, so its serialized plan
	// grew quadratically with the stage count - the blowup this bound
	// rejects.
	first := two - one
	second := four - two
	require.Positive(t, first, "a second wrapper stage must add a measurable amount of plan text")
	require.Less(t, second, 4*first+64,
		"adding two stages may not exceed a small multiple of adding one; the optimized plan must grow linearly with the number of pagination stages")
}

// buildPaginationWireQuery builds the optimized plan for the staged witness
// with DISTINCT prepared LIMIT/OFFSET placeholders per stage (two param slots
// per stage, statement order), then binds it through the production
// prepared/plain optimizer just as the engine does.
func buildPaginationWireQuery(t *testing.T, stages int, prepared bool) *pbplan.Query {
	t.Helper()
	inner := "(SELECT a FROM x UNION ALL SELECT n + 1 FROM q WHERE n < 3 LIMIT 3)"
	for i := 0; i < stages; i++ {
		if prepared {
			inner = "(" + inner + " LIMIT ? OFFSET ?)"
		} else {
			inner = "(" + inner + " LIMIT 2 OFFSET 1)"
		}
	}
	sql := "WITH RECURSIVE q(n) AS (WITH x AS (SELECT 1 AS a) " + inner + ") SELECT n FROM q"
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	require.Len(t, stmts, 1, sql)
	defer stmts[0].Free()
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	opt := NewBaseOptimizer(ctx)
	if prepared {
		opt = NewPrepareOptimizer(ctx)
	}
	query, oerr := opt.Optimize(stmts[0], prepared, nil)
	require.NoError(t, oerr, sql)
	return query
}

// collectWireParamMultiplicity walks an expression tree and records how many
// times each bound parameter position appears, so duplicate expansions of a
// prior stage's marker are observable rather than hidden in a union.
func collectWireParamMultiplicity(e *pbplan.Expr, into map[int32]int) {
	if e == nil {
		return
	}
	switch v := e.Expr.(type) {
	case *pbplan.Expr_P:
		if v.P != nil {
			into[v.P.Pos]++
		}
	case *pbplan.Expr_F:
		for _, arg := range v.F.GetArgs() {
			collectWireParamMultiplicity(arg, into)
		}
	case *pbplan.Expr_List:
		for _, arg := range v.List.List {
			collectWireParamMultiplicity(arg, into)
		}
	}
}

// recursiveSink returns the optimized plan's output sink node: the recursive
// graph does not guarantee the sink sorts last after optimization, so locate
// the node carrying the composed pagination channels by type.
func recursiveSink(t *testing.T, query *pbplan.Query) *pbplan.Node {
	t.Helper()
	for _, node := range query.Nodes {
		if node.NodeType == pbplan.Node_SINK_SCAN {
			return node
		}
	}
	t.Fatalf("the optimized plan carries no SINK_SCAN output node")
	return nil
}

func TestRecursiveWrapperPaginationWireGrowsAtMostLinearly(t *testing.T) {
	// The real wire measurement per stage count and per channel: the total
	// marshaled plan bytes plus the separate encoded sizes of the sink's
	// OFFSET and LIMIT channels, printed once per examined case so the
	// receipts carry the actual numbers.
	for _, prepared := range []bool{false, true} {
		total := map[int]int{}
		offsetCh := map[int]int{}
		limitCh := map[int]int{}
		for _, stages := range []int{1, 2, 4} {
			query := buildPaginationWireQuery(t, stages, prepared)
			wire, merr := query.Marshal()
			require.NoError(t, merr)
			total[stages] = len(wire)
			sink := recursiveSink(t, query)
			ow, oerr := sink.Offset.Marshal()
			require.NoError(t, oerr)
			offsetCh[stages] = len(ow)
			lw, lerr := sink.Limit.Marshal()
			require.NoError(t, lerr)
			limitCh[stages] = len(lw)
			t.Logf("stages=%d prepared=%v total_wire_bytes=%d offset_channel_bytes=%d limit_channel_bytes=%d",
				stages, prepared, total[stages], offsetCh[stages], limitCh[stages])
		}
		// Aggregated wire bound retained from the accepted phase-66 test...
		require.Less(t, total[4]-total[2], 4*(total[2]-total[1])+1024,
			"prepared=%v: total wire must grow linearly", prepared)
		require.Less(t, offsetCh[4]-offsetCh[2], 4*(offsetCh[2]-offsetCh[1])+512,
			"prepared=%v: the OFFSET channel must grow linearly on its own", prepared)
		require.Less(t, limitCh[4]-limitCh[2], 4*(limitCh[2]-limitCh[1])+512,
			"prepared=%v: the LIMIT channel must grow linearly on its own", prepared)
	}
	// Round-trip + channel isolation + placement at the examined prepared
	// 2-stage representation: the A/B lexical roles keep their statement-
	// order ordinals (A.limit=1 A.offset=2 B.limit=3 B.offset=4) and each
	// OFFSET slot reaches the OFFSET channel exactly once per channel, so a
	// prior stage's marker can never be re-expanded.
	query2 := buildPaginationWireQuery(t, 2, true)
	wire2, merr2 := query2.Marshal()
	require.NoError(t, merr2)
	fresh := &pbplan.Query{}
	require.NoError(t, fresh.Unmarshal(wire2), "the marshaled plan must unmarshal through the production codec")
	sink := recursiveSink(t, fresh)
	require.NotNil(t, sink.Offset, "the sink's independent OFFSET channel survives the wire round-trip")
	require.NotNil(t, sink.Limit, "the sink's independent LIMIT channel survives the wire round-trip")
	offsets := map[int32]int{}
	limits := map[int32]int{}
	collectWireParamMultiplicity(sink.Offset, offsets)
	collectWireParamMultiplicity(sink.Limit, limits)
	t.Logf("prepared 2-stage placement: offset_channel=%v limit_channel=%v", offsets, limits)
	require.Equal(t, 1, offsets[2], "stage A's OFFSET slot reaches the OFFSET channel exactly once")
	require.Equal(t, 1, offsets[4], "stage B's OFFSET slot reaches the OFFSET channel exactly once")
	require.Equal(t, map[int32]int{2: 1, 4: 1}, offsets,
		"the OFFSET channel carries exactly the two OFFSET slots, once each")
	for _, limitSlot := range []int32{1, 3} {
		require.Positive(t, limits[limitSlot],
			"LIMIT slot %d reaches the LIMIT channel", limitSlot)
	}
	require.Equal(t, 1, limits[2], "stage A's OFFSET slot enters the width arithmetic once, not re-expanded")
	require.Equal(t, 1, limits[4], "stage B's OFFSET slot enters the width arithmetic once, not re-expanded")
	// The enclosing placeholders must never leak into the recursion's own
	// feedback cover: the recursive emission bound stays literal-only.
	var cover *pbplan.Node
	for _, node := range fresh.Nodes {
		if node.NodeType == pbplan.Node_RECURSIVE_CTE {
			cover = node
			break
		}
	}
	require.NotNil(t, cover, "the plan carries the recursive feedback node")
	coverParams := map[int32]int{}
	collectWireParamMultiplicity(cover.Limit, coverParams)
	collectWireParamMultiplicity(cover.Offset, coverParams)
	require.Empty(t, coverParams,
		"the recursive feedback prefix must stay free of the enclosing prepared slots")
}
