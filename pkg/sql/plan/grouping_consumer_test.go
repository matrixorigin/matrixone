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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGroupingQuantifiedSubqueryInputs(t *testing.T) {
	for _, comparison := range []string{
		"n_nationkey in (select r_regionkey from region)",
		"n_nationkey not in (select r_regionkey from region)",
		"n_nationkey = any(select r_regionkey from region)",
		"n_nationkey < all(select r_regionkey from region)",
		"n_nationkey+1 in (select r_regionkey from region)",
		"(n_nationkey,n_regionkey) in (select r_regionkey,r_regionkey from region)",
	} {
		for _, prefix := range []string{"", "create table quantified_group as "} {
			t.Run(prefix+comparison, func(t *testing.T) {
				_, err := buildSingleStmt(NewMockOptimizer(false), t,
					prefix+"select n_nationkey,"+comparison+" flag from nation group by n_nationkey,n_regionkey with rollup")
				require.NoError(t, err, "the quantified operand must reference the combined grouping output")
			})
		}
	}
}

func TestGroupingDistinctSubqueryProjection(t *testing.T) {
	for _, query := range []string{
		"select distinct (select 7) k from nation group by n_nationkey with rollup",
		"select distinct (select r_regionkey from region where r_regionkey=1) k from nation group by n_nationkey with rollup",
		"select distinct coalesce((select 7),9) k from nation group by grouping sets ((n_nationkey),(n_nationkey))",
		"select distinct n_nationkey in (select r_regionkey from region) flag from nation group by n_nationkey with rollup",
		"select distinct (select 7) k from nation group by n_nationkey with rollup order by k limit 1",
		"select distinct count(*) over() n,(select 7) k from nation group by n_nationkey with rollup",
	} {
		for _, prefix := range []string{"", "create table distinct_subquery_group as "} {
			t.Run(prefix+query, func(t *testing.T) {
				p, err := buildSingleStmt(NewMockOptimizer(false), t, prefix+query)
				require.NoError(t, err, "a DISTINCT key projection cannot contain an unexpanded subquery")
				if prefix == "" {
					for _, node := range p.GetQuery().Nodes {
						for _, project := range node.ProjectList {
							require.False(t, hasSubquery(project),
								"published projections must be executable, not merely bind successfully")
						}
					}
				}
			})
		}
	}
}

func TestGroupingDistinctSubqueryError(t *testing.T) {
	for _, suffix := range []string{"", " group by n_nationkey with rollup"} {
		_, err := buildSingleStmt(NewMockOptimizer(false), t,
			"select distinct (select 1) in (select r_regionkey from region) flag from nation"+suffix)
		require.ErrorContains(t, err, "a quantified subquery's left operand can't contain subquery",
			"DISTINCT must preserve the ordinary subquery flattener's rejection")
	}
}

func TestGroupingSampleTransport(t *testing.T) {
	for _, query := range []string{
		"select n_regionkey,sample(n_nationkey,100 percent) k from nation group by grouping sets ((n_regionkey),(n_regionkey))",
		"select n_nationkey,n_regionkey,sample(n_comment,100 percent) k from nation group by grouping sets ((),())",
		"select n_regionkey,sample(n_nationkey,n_name,100 percent) from nation group by grouping sets ((n_regionkey),(n_regionkey))",
		"select sample(n_nationkey,100 percent) k from nation group by grouping sets ((),())",
		"select distinct n_regionkey,sample(n_nationkey,100 percent) k from nation group by grouping sets ((n_regionkey),(n_regionkey))",
		"select n_regionkey,sample(n_nationkey,10 rows) k from nation group by grouping sets ((n_regionkey),(n_regionkey)) order by k limit 2",
	} {
		for _, prefix := range []string{"", "create table sampled_group as "} {
			t.Run(prefix+query, func(t *testing.T) {
				_, err := buildSingleStmt(NewMockOptimizer(false), t, prefix+query)
				require.NoError(t, err, "all columns produced by SAMPLE must cross the grouping UNION boundary")
			})
		}
	}
}
