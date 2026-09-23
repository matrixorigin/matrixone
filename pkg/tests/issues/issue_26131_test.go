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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

const issue26131Q15 = `
with revenue0 as (
    select l_suppkey as supplier_no,
           sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from lineitem
    where l_shipdate >= date '1995-12-01'
      and l_shipdate < date '1995-12-01' + interval '3' month
    group by l_suppkey
)
select s_suppkey, s_name, total_revenue
from supplier, revenue0
where s_suppkey = supplier_no
  and total_revenue = (select max(total_revenue) from revenue0)
order by s_suppkey`

const nestedSharedCTEQ15 = `
with base_lineitem as (
    select l_suppkey, l_extendedprice, l_discount, l_shipdate
    from lineitem
), revenue0 as (
    select l_suppkey as supplier_no,
           sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from base_lineitem
    where l_shipdate >= date '1995-12-01'
      and l_shipdate < date '1995-12-01' + interval '3' month
    group by l_suppkey
)
select s_suppkey, s_name, total_revenue
from supplier, revenue0
where s_suppkey = supplier_no
  and total_revenue = (select max(total_revenue) from revenue0)
order by s_suppkey`

const predicateAwareSharedCTE = `
with supplier_totals as (
    select l_suppkey,
           max(l_comment) as comment,
           sum(l_extendedprice) as total
    from lineitem
    group by l_suppkey
)
select a.l_suppkey, a.comment, a.total, b.total
from supplier_totals a join supplier_totals b
  on a.l_suppkey = b.l_suppkey
where a.l_suppkey between 1 and 42
  and b.l_suppkey between 1 and 100
  and a.total > 0
order by a.l_suppkey
limit 10`

func TestIssue26131Q15SharedCTEExecutesBothConsumers(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const database = "issue_26131"
		execSQLMaybe(t, ctx, db, "drop database if exists "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "create database "+database)
		execSQLRequire(t, ctx, db, "use "+database)
		execSQLRequire(t, ctx, db, "create table supplier (s_suppkey int primary key, s_name varchar(32))")
		execSQLRequire(t, ctx, db, "create table lineitem (l_suppkey int, l_extendedprice decimal(15,2), l_discount decimal(15,2), l_shipdate date, l_comment varchar(32))")
		execSQLRequire(t, ctx, db, "create table cte_error_rows (k int, raw varchar(32), payload varchar(32))")
		execSQLRequire(t, ctx, db, "create table cte_domain_rows (k int, x varchar(32), raw varchar(32), payload varchar(32))")
		execSQLRequire(t, ctx, db, "create table cte_join_fact (region int, k int, raw varchar(32), payload varchar(32))")
		execSQLRequire(t, ctx, db, "create table cte_join_d1 (k int primary key)")
		execSQLRequire(t, ctx, db, "create table cte_join_d2 (k int primary key)")
		execSQLRequire(t, ctx, db, "create table cte_probe_rows (k int, raw varchar(32))")
		execSQLRequire(t, ctx, db, "create table cte_filter_risk (k int primary key, raw varchar(32))")
		execSQLRequire(t, ctx, db, "create table cte_having_risk (k int, raw varchar(32))")
		execSQLRequire(t, ctx, db, "create table cte_empty_dim_1 (k int primary key, flag int)")
		execSQLRequire(t, ctx, db, "create table cte_empty_dim_2 (k int primary key, flag int)")
		execSQLRequire(t, ctx, db, "insert into supplier values (1, 'supplier-1'), (42, 'supplier-42')")
		// Two rows per group preserve real SUM accumulation. The shared-CTE
		// topology is asserted from EXPLAIN below and does not depend on volume.
		execSQLRequire(t, ctx, db, `insert into lineitem values
			(1, -1, 0, '1995-12-15', 'supplier-1-a'), (1, -1, 0, '1995-12-15', 'supplier-1-b'),
			(42, 2, 0, '1995-12-15', 'supplier-42-a'), (42, 2, 0, '1995-12-15', 'supplier-42-b')`)
		execSQLRequire(t, ctx, db, "insert into cte_error_rows values (1, '10', 'a'), (2, 'not-an-integer', 'bb')")
		execSQLRequire(t, ctx, db, "insert into cte_domain_rows values (1, '1', '10', 'a'), (1, '0', 'bad', 'hidden'), (2, '1', '20', 'bb')")
		execSQLRequire(t, ctx, db, "insert into cte_join_fact values (1, 1, '10', 'a'), (1, 9, 'bad', 'hidden'), (2, 2, '20', 'bb')")
		execSQLRequire(t, ctx, db, "insert into cte_join_d1 values (1)")
		execSQLRequire(t, ctx, db, "insert into cte_join_d2 values (2)")
		execSQLRequire(t, ctx, db, "insert into cte_probe_rows select result, 'not-an-integer' from generate_series(1, 10000) g")
		execSQLRequire(t, ctx, db, "insert into cte_filter_risk select result, if(result = 10000, 'not-an-integer', '1') from generate_series(1, 10000) g")
		execSQLRequire(t, ctx, db, "insert into cte_having_risk select if(result = 10000, 2, 1), if(result = 10000, 'not-an-integer', '1') from generate_series(1, 10000) g")
		execSQLRequire(t, ctx, db, "insert into cte_empty_dim_1 values (1, 1)")
		execSQLRequire(t, ctx, db, "insert into cte_empty_dim_2 values (2, 1)")
		execSQLRequire(t, ctx, db, "analyze table supplier, lineitem")
		execSQLRequire(t, ctx, db, "analyze table cte_probe_rows, cte_filter_risk, cte_having_risk, cte_empty_dim_1, cte_empty_dim_2")

		planText := explainSQL(t, ctx, db, "explain "+issue26131Q15)
		require.Equal(t, 1, strings.Count(planText, ".lineitem"),
			"the shared CTE must have exactly one lineitem producer:\n%s", planText)
		require.Equal(t, 2, strings.Count(planText, "Sink Scan"),
			"the join and scalar MAX must be the only shared-CTE consumers:\n%s", planText)

		rows, err := db.QueryContext(ctx, issue26131Q15)
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		var supplierKey int
		var supplierName string
		var revenue float64
		require.NoError(t, rows.Scan(&supplierKey, &supplierName, &revenue))
		require.Equal(t, 42, supplierKey)
		require.Equal(t, "supplier-42", supplierName)
		require.Equal(t, 4.0, revenue)
		require.False(t, rows.Next(), "both consumers must drain to one terminal result")
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		nestedPlanText := explainSQL(t, ctx, db, "explain "+nestedSharedCTEQ15)
		require.Equal(t, 1, strings.Count(nestedPlanText, ".lineitem"),
			"a shared CTE containing another CTE must keep one lineitem producer:\n%s", nestedPlanText)
		require.Equal(t, 2, strings.Count(finalExplainPlan(nestedPlanText), "Sink Scan"),
			"the outer shared CTE must still serve both consumers:\n%s", nestedPlanText)

		nestedRows, err := db.QueryContext(ctx, nestedSharedCTEQ15)
		require.NoError(t, err)
		defer nestedRows.Close()
		require.True(t, nestedRows.Next())
		require.NoError(t, nestedRows.Scan(&supplierKey, &supplierName, &revenue))
		require.Equal(t, 42, supplierKey)
		require.Equal(t, "supplier-42", supplierName)
		require.Equal(t, 4.0, revenue)
		require.False(t, nestedRows.Next())
		require.NoError(t, nestedRows.Err())
		require.NoError(t, nestedRows.Close())

		predicatePlanText := explainSQL(t, ctx, db, "explain "+predicateAwareSharedCTE)
		require.Equal(t, 1, strings.Count(predicatePlanText, ".lineitem"),
			"consumer predicates must bound one variable-width CTE producer:\n%s", predicatePlanText)
		require.Equal(t, 2, strings.Count(predicatePlanText, "Sink Scan"),
			"both filtered consumers must read the shared producer:\n%s", predicatePlanText)
		require.Contains(t, predicatePlanText, "l_suppkey BETWEEN 1 AND 42 or lineitem.l_suppkey BETWEEN 1 AND 100",
			"the producer must retain the union of both consumer predicates:\n%s", predicatePlanText)

		predicateRows, err := db.QueryContext(ctx, predicateAwareSharedCTE)
		require.NoError(t, err)
		defer predicateRows.Close()
		require.True(t, predicateRows.Next())
		var leftRevenue, rightRevenue float64
		require.NoError(t, predicateRows.Scan(&supplierKey, &supplierName, &leftRevenue, &rightRevenue))
		require.Equal(t, 42, supplierKey)
		require.Equal(t, "supplier-42-b", supplierName)
		require.Equal(t, 4.0, leftRevenue)
		require.Equal(t, 4.0, rightRevenue)
		require.False(t, predicateRows.Next())
		require.NoError(t, predicateRows.Err())
		require.NoError(t, predicateRows.Close())

		const fallibleOutputQuery = `with c as (
			select k, cast(raw as bigint) as risky, payload from cte_error_rows
		)
		select sum(risky) from c where k = 1
		union all
		select sum(length(payload)) from c where k = 2`
		falliblePlan := explainSQL(t, ctx, db, "explain "+fallibleOutputQuery)
		require.NotContains(t, falliblePlan, "Sink Scan",
			"sharing must not expand evaluation of a consumer-only fallible cast")
		fallibleRows, err := db.QueryContext(ctx, fallibleOutputQuery)
		require.NoError(t, err)
		defer fallibleRows.Close()
		var fallibleResults []int64
		for fallibleRows.Next() {
			var value int64
			require.NoError(t, fallibleRows.Scan(&value))
			fallibleResults = append(fallibleResults, value)
		}
		require.NoError(t, fallibleRows.Err())
		require.ElementsMatch(t, []int64{10, 2}, fallibleResults)

		const fallibleRowDomainQuery = `with c as (
			select k, x, cast(max(raw) as bigint) as risky, max(payload) as payload
			from cte_domain_rows group by k, x
		)
		select sum(risky), max(length(payload)) from c where k = 1 and x = '1'
		union all
		select sum(risky), max(length(payload)) from c where k = 2`
		fallibleDomainPlan := explainSQL(t, ctx, db, "explain "+fallibleRowDomainQuery)
		require.NotContains(t, fallibleDomainPlan, "Sink Scan",
			"sharing must not weaken the row domain of a fallible CTE output")
		fallibleDomainRows, err := db.QueryContext(ctx, fallibleRowDomainQuery)
		require.NoError(t, err)
		defer fallibleDomainRows.Close()
		type domainResult struct {
			sum    int64
			length int64
		}
		var fallibleDomainResults []domainResult
		for fallibleDomainRows.Next() {
			var result domainResult
			require.NoError(t, fallibleDomainRows.Scan(&result.sum, &result.length))
			fallibleDomainResults = append(fallibleDomainResults, result)
		}
		require.NoError(t, fallibleDomainRows.Err())
		require.ElementsMatch(t, []domainResult{{sum: 10, length: 1}, {sum: 20, length: 2}},
			fallibleDomainResults)

		const omittedFalliblePredicateQuery = `with c as (
			select k, x, cast(max(raw) as bigint) as risky, max(payload) as payload
			from cte_domain_rows group by k, x
		)
		select sum(risky), max(length(payload)) from c
		where k = 1 and cast(x as bigint) > 0
		union all
		select sum(risky), max(length(payload)) from c where k = 2`
		omittedPredicatePlan := explainSQL(t, ctx, db, "explain "+omittedFalliblePredicateQuery)
		require.NotContains(t, omittedPredicatePlan, "Sink Scan",
			"an omitted fallible consumer predicate must keep the CTE inline")
		omittedPredicateRows, err := db.QueryContext(ctx, omittedFalliblePredicateQuery)
		require.NoError(t, err)
		defer omittedPredicateRows.Close()
		var omittedPredicateResults []domainResult
		for omittedPredicateRows.Next() {
			var result domainResult
			require.NoError(t, omittedPredicateRows.Scan(&result.sum, &result.length))
			omittedPredicateResults = append(omittedPredicateResults, result)
		}
		require.NoError(t, omittedPredicateRows.Err())
		require.ElementsMatch(t, []domainResult{{sum: 10, length: 1}, {sum: 20, length: 2}},
			omittedPredicateResults)

		const consumerJoinDomainQuery = `with c as (
			select region, k, cast(max(raw) as bigint) as risky, max(payload) as payload
			from cte_join_fact group by region, k
		)
		select sum(c.risky), max(length(c.payload))
		from c join cte_join_d1 d1 on c.k = d1.k where c.region = 1
		union all
		select sum(c.risky), max(length(c.payload))
		from c join cte_join_d2 d2 on c.k = d2.k where c.region = 2`
		consumerJoinPlan := explainSQL(t, ctx, db, "explain "+consumerJoinDomainQuery)
		require.NotContains(t, consumerJoinPlan, "Sink Scan",
			"consumer joins must keep a fallible CTE output inline")
		consumerJoinRows, err := db.QueryContext(ctx, consumerJoinDomainQuery)
		require.NoError(t, err)
		defer consumerJoinRows.Close()
		var consumerJoinResults []domainResult
		for consumerJoinRows.Next() {
			var result domainResult
			require.NoError(t, consumerJoinRows.Scan(&result.sum, &result.length))
			consumerJoinResults = append(consumerJoinResults, result)
		}
		require.NoError(t, consumerJoinRows.Err())
		require.ElementsMatch(t, []domainResult{{sum: 10, length: 1}, {sum: 20, length: 2}},
			consumerJoinResults)

		const consumerTopNDomainQuery = `with c as (
			select region, k, cast(max(raw) as bigint) as risky, max(payload) as payload
			from cte_join_fact group by region, k
		)
		select sum(risky), max(length(payload)) from (
			select risky, payload from c where region = 1 order by k limit 1
		) a
		union all
		select sum(risky), max(length(payload)) from (
			select risky, payload from c where region = 2 order by k limit 1
		) b`
		consumerTopNPlan := explainSQL(t, ctx, db, "explain "+consumerTopNDomainQuery)
		require.NotContains(t, consumerTopNPlan, "Sink Scan",
			"consumer Top-N must keep a fallible CTE output inline")
		consumerTopNRows, err := db.QueryContext(ctx, consumerTopNDomainQuery)
		require.NoError(t, err)
		defer consumerTopNRows.Close()
		var consumerTopNResults []domainResult
		for consumerTopNRows.Next() {
			var result domainResult
			require.NoError(t, consumerTopNRows.Scan(&result.sum, &result.length))
			consumerTopNResults = append(consumerTopNResults, result)
		}
		require.NoError(t, consumerTopNRows.Err())
		require.ElementsMatch(t, []domainResult{{sum: 10, length: 1}, {sum: 20, length: 2}},
			consumerTopNResults)

		const tagFreePredicateQuery = `with c as (
			select k as region, cast(max(raw) as bigint) as risky, max(payload) as payload
			from cte_domain_rows group by k, x
		)
		select coalesce(sum(risky), 0), coalesce(max(length(payload)), 0)
		from c where region = 1 and rand() < 0
		union all
		select coalesce(sum(risky), 0), coalesce(max(length(payload)), 0)
		from c where region = 2 and rand() < 0`
		tagFreePredicatePlan := explainSQL(t, ctx, db, "explain "+tagFreePredicateQuery)
		require.NotContains(t, tagFreePredicatePlan, "Sink Scan",
			"an omitted tag-free consumer predicate must keep the CTE inline")
		tagFreePredicateRows, err := db.QueryContext(ctx, tagFreePredicateQuery)
		require.NoError(t, err)
		defer tagFreePredicateRows.Close()
		var tagFreePredicateResults []domainResult
		for tagFreePredicateRows.Next() {
			var result domainResult
			require.NoError(t, tagFreePredicateRows.Scan(&result.sum, &result.length))
			tagFreePredicateResults = append(tagFreePredicateResults, result)
		}
		require.NoError(t, tagFreePredicateRows.Err())
		require.ElementsMatch(t, []domainResult{{sum: 0, length: 0}, {sum: 0, length: 0}},
			tagFreePredicateResults)

		const fallibleProducerPredicateQuery = `with c as (
			select k from cte_filter_risk where cast(raw as bigint) > 0
		)
		(select count(*) from (select * from c) d where k = 1)
		union all
		(select k from c limit 0)`
		fallibleProducerPlan := explainSQL(t, ctx, db, "explain "+fallibleProducerPredicateQuery)
		require.NotContains(t, fallibleProducerPlan, "Sink Scan",
			"sharing must not expand a fallible producer predicate beyond consumer row domains")
		require.Equal(t, []int64{1},
			queryInt64Column(t, ctx, db, fallibleProducerPredicateQuery))

		const fallibleHavingQuery = `with c as (
			select k from cte_having_risk group by k
			having cast(max(raw) as bigint) > 0
		)
		(select count(*) from c where k = 1)
		union all
		(select k from c limit 0)`
		fallibleHavingPlan := explainSQL(t, ctx, db, "explain "+fallibleHavingQuery)
		require.NotContains(t, fallibleHavingPlan, "Sink Scan",
			"sharing must not expand a fallible HAVING predicate beyond consumer group domains")
		require.Equal(t, []int64{1}, queryInt64Column(t, ctx, db, fallibleHavingQuery))

		const fallibleGroupingKeyQuery = `with c as (
			select k from cte_having_risk group by k, cast(raw as bigint)
		)
		(select count(*) from c where k = 1)
		union all
		(select k from c limit 0)`
		fallibleGroupingPlan := explainSQL(t, ctx, db, "explain "+fallibleGroupingKeyQuery)
		require.NotContains(t, fallibleGroupingPlan, "Sink Scan",
			"sharing must not expand a fallible grouping key beyond consumer row domains")
		require.Equal(t, []int64{1}, queryInt64Column(t, ctx, db, fallibleGroupingKeyQuery))

		const emptyBuildProbeQuery = `with c as (
			select k from cte_probe_rows where cast(raw as bigint) > 0
		)
		select count(*) from c
		join cte_empty_dim_1 d1 on c.k = d1.k and d1.flag = 0
		union all
		select count(*) from c
		join cte_empty_dim_2 d2 on c.k = d2.k and d2.flag = 0`
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "set session optimizer_hints = ''")
		}()
		execSQLRequire(t, ctx, db,
			"set session optimizer_hints = 'joinOrdering=1,sharedComputation=1'")
		legacyCounts := queryInt64Column(t, ctx, db, emptyBuildProbeQuery)
		require.ElementsMatch(t, []int64{0, 0}, legacyCounts)

		execSQLRequire(t, ctx, db, "set session optimizer_hints = 'joinOrdering=1'")
		emptyBuildPlan := explainSQL(t, ctx, db, "explain "+emptyBuildProbeQuery)
		require.NotContains(t, emptyBuildPlan, "Sink Scan",
			"a fixed INNER probe is not a complete-evaluation witness")
		require.ElementsMatch(t, legacyCounts,
			queryInt64Column(t, ctx, db, emptyBuildProbeQuery))
	})
}

func queryInt64Column(t *testing.T, ctx context.Context, db *sql.DB, statement string) []int64 {
	t.Helper()
	rows, err := db.QueryContext(ctx, statement)
	require.NoError(t, err)
	defer rows.Close()
	var values []int64
	for rows.Next() {
		var value int64
		require.NoError(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.NoError(t, rows.Err())
	return values
}

func finalExplainPlan(planText string) string {
	if finalPlan := strings.LastIndex(planText, "\nPlan "); finalPlan >= 0 {
		return planText[finalPlan+1:]
	}
	return planText
}

func explainSQL(t *testing.T, ctx context.Context, db *sql.DB, statement string) string {
	t.Helper()
	rows, err := db.QueryContext(ctx, statement)
	require.NoError(t, err)
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	var lines []string
	for rows.Next() {
		values := make([]sql.RawBytes, len(columns))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		require.NoError(t, rows.Scan(dest...))
		for _, value := range values {
			lines = append(lines, string(value))
		}
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n")
}
