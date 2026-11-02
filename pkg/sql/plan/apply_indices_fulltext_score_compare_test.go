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

package plan

import (
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// A direct score comparison that a relevance of 0 can satisfy (`= 0`, `< c`, `>= 0`, `!= c`)
// cannot drive an INNER index stream. It must be answered like its `OR false` form: LEFT-attach
// the complete score stream and compare the coalesced score, instead of leaving the MATCH
// unresolved for execution to raise 20105 (#31604).
func TestFullTextDirectScoreComparisonUsesComposition(t *testing.T) {
	leftJoins := func(query *planpb.Query) int {
		n := 0
		for _, node := range booleanFullTextNodes(query, query.Steps[0]) {
			if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_LEFT {
				n++
			}
		}
		return n
	}
	for _, ft2 := range []bool{false, true} {
		suffix := map[bool]string{false: "/classic", true: "/fulltext2"}[ft2]
		for _, sql := range []string{
			"select id from ft where match(title, body) against('hello') = 0",
			"select id from ft where 0 = match(title, body) against('hello')",
			"select id from ft where match(title, body) against('hello') < 0.5",
			"select id from ft where match(title, body) against('hello') >= 0",
			"select id from ft where match(title, body) against('hello') != 1",
			"select id from ft where match(title, body) against('hello') = 0 and base_id = 'b1'",
			"select id, match(title, body) against('hello') from ft where match(title, body) against('hello') <= 0",
		} {
			t.Run(sql+suffix, func(t *testing.T) {
				query := buildBooleanFullTextPublicPlan(t, sql, ft2, false)
				require.Zero(t, countReachableFullTextMatches(query), "every MATCH must be served")
				require.Greater(t, leftJoins(query), 0, "rows the index did not return must survive with score 0")
			})
		}
		// Controls: membership-implying shapes keep the existing INNER driver path.
		for _, sql := range []string{
			"select id from ft where match(title, body) against('hello')",
			"select id from ft where match(title, body) against('hello') > 0",
			"select id from ft where match(title, body) against('hello') and match(title, body) against('hello') < 0.5",
		} {
			t.Run("control "+sql+suffix, func(t *testing.T) {
				query := buildBooleanFullTextPublicPlan(t, sql, ft2, false)
				require.Zero(t, countReachableFullTextMatches(query))
				require.Zero(t, leftJoins(query), "a driven MATCH must not switch to composition")
			})
		}
	}
}
