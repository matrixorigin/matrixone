// Copyright 2021 - 2022 Matrix Origin
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

package explain

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"testing"
)

func TestSimpleQueryToJson(t *testing.T) {
	//input := "select * from part where p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')"
	//input := "SELECT DISTINCT N_NAME FROM NATION"
	//input := "SELECT l.L_ORDERKEY a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10"
	sqls := []string{
		"SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC",                                                                                  //test alias
		"SELECT N_NAME, count(distinct N_REGIONKEY) FROM NATION group by N_NAME",                                                                                          //test distinct agg function
		"SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10",                                                                        //test agg
		"SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20", //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",                                                    //test more expr
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY",
		"SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 0",
		"SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE NATION2.R_REGIONKEY > 0",
		"select n_name from nation intersect all select n_name from nation2",
		"select n_name from nation minus select n_name from nation2",
		"select 1 union select 2 intersect select 2 union all select 1.1 minus select 22222",
		"SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10",
		"SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 0",
		"select col1 from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a(col1, col2) where col2 > 0 order by col1",
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
	}

	mock := plan.NewMockOptimizer()
	buildPlanMarshalTest(mock, t, sqls)
}

func TestSingleTableQueryToJson(t *testing.T) {
	sqls := []string{
		"SELECT '1900-01-01 00:00:00' + INTERVAL 2147483648 SECOND",
		"SELECT N_NAME, N_REGIONKEY FROM NATION WHERE N_REGIONKEY > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10, 20",
		"SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_REGIONKEY > 0 ORDER BY a DESC", //test alias
		"SELECT NATION.N_NAME FROM NATION",                                       //test alias
		"SELECT * FROM NATION",                                                   //test star
		"SELECT a.* FROM NATION a",                                               //test star
		"SELECT count(*) FROM NATION",                                            //test star
		"SELECT count(*) FROM NATION group by N_NAME",                            //test star
		"SELECT N_NAME, count(distinct N_REGIONKEY) FROM NATION group by N_NAME", //test distinct agg function
		"SELECT N_NAME, MAX(N_REGIONKEY) FROM NATION GROUP BY N_NAME HAVING MAX(N_REGIONKEY) > 10", //test agg
		"SELECT DISTINCT N_NAME FROM NATION", //test distinct
		"select sum(n_nationkey) as s from nation order by s",
		"select date_add(date '2001-01-01', interval 1 day) as a",
		"select date_sub(date '2001-01-01', interval '1' day) as a",
		"select date_add('2001-01-01', interval '1' day) as a",
		"select n_name, count(*) from nation group by n_name order by 2 asc",
		"select count(distinct 12)",
		"select nullif(n_name, n_comment), ifnull(n_comment, n_name) from nation",

		"select 18446744073709551500",
		"select 0xffffffffffffffff",
		"select 0xffff",

		"SELECT N_REGIONKEY + 2 as a, N_REGIONKEY/2, N_REGIONKEY* N_NATIONKEY, N_REGIONKEY % N_NATIONKEY, N_REGIONKEY - N_NATIONKEY FROM NATION WHERE -N_NATIONKEY < -20", //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY >= N_NATIONKEY or (N_NAME like '%ddd' and N_REGIONKEY >0.5)",                                                    //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY between 2 and 2 OR N_NATIONKEY not between 3 and 10",                                                            //test more expr
		// "SELECT N_REGIONKEY FROM NATION where N_REGIONKEY is null and N_NAME is not null",
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY IN (1, 2)",  //test more expr
		"SELECT N_REGIONKEY FROM NATION where N_REGIONKEY NOT IN (1)", //test more expr
		"select N_REGIONKEY from nation group by N_REGIONKEY having abs(nation.N_REGIONKEY - 1) >10",

		//"SELECT -1",
		//"select date_add('1997-12-31 23:59:59',INTERVAL 100000 SECOND)",
		//"select date_sub('1997-12-31 23:59:59',INTERVAL 2 HOUR)",
		//"select @str_var, @int_var, @bool_var, @float_var, @null_var",
		//"select @str_var, @@global.int_var, @@session.bool_var",
		//"select n_name from nation where n_name != @str_var and n_regionkey > @int_var",
		//"select n_name from nation where n_name != @@global.str_var and n_regionkey > @@session.int_var",
		//"select distinct(n_name), ((abs(n_regionkey))) from nation",
		//"SET @var = abs(-1), @@session.string_var = 'aaa'",
		//"SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci'",
		//"SELECT DISTINCT N_NAME FROM NATION ORDER BY N_NAME", //test distinct with order by
		//
		//"prepare stmt1 from select * from nation",
		//"prepare stmt1 from select * from nation where n_name = ?",
		//"prepare stmt1 from 'select * from nation where n_name = ?'",
		//"prepare stmt1 from 'update nation set n_name = ? where n_nationkey > ?'",
		//"prepare stmt1 from 'delete from nation where n_nationkey > ?'",
		//"prepare stmt1 from 'insert into nation select * from nation2 where n_name = ?'",
		//"prepare stmt1 from 'select * from nation where n_name = ?'",
		//"prepare stmt1 from 'drop table if exists t1'",
		//"prepare stmt1 from 'create table t1 (a int)'",
		//"prepare stmt1 from select N_REGIONKEY from nation group by N_REGIONKEY having abs(nation.N_REGIONKEY - ?) > ?",
		//"execute stmt1",
		//"execute stmt1 using @str_var, @@global.int_var",
		//"deallocate prepare stmt1",
		//"drop prepare stmt1",
		//"select count(n_name) from nation",
		//"select l_shipdate + interval '1' day from lineitem",
		//"select interval '1' day + l_shipdate  from lineitem",
		//"select interval '1' day + cast('2022-02-02 00:00:00' as datetime)",
		//"select cast('2022-02-02 00:00:00' as datetime) + interval '1' day",
		//"delete from nation",
		//"delete nation, nation2 from nation join nation2 on nation.n_name = nation2.n_name",
	}
	mock := plan.NewMockOptimizer()
	buildPlanMarshalTest(mock, t, sqls)
}

func TestJoinQueryToJson(t *testing.T) {
	sqls := []string{
		"SELECT N_NAME,N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY",
		"SELECT N_NAME, N_REGIONKEY FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 0",
		"SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY) WHERE NATION2.R_REGIONKEY > 0",
		"SELECT N_NAME, NATION2.R_REGIONKEY FROM NATION2 NATURAL JOIN REGION WHERE NATION2.R_REGIONKEY > 0",
		"SELECT N_NAME FROM NATION NATURAL JOIN REGION",                                                                                                     //have no same column name but it's ok
		"SELECT N_NAME,N_REGIONKEY FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",                                    //test alias
		"SELECT l.L_ORDERKEY a FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY and o.O_ORDERKEY < 10", //join three tables
		"SELECT c.* FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",                                  //test star
		"SELECT * FROM CUSTOMER c, ORDERS o, LINEITEM l WHERE c.C_CUSTKEY = o.O_CUSTKEY and l.L_ORDERKEY = o.O_ORDERKEY",                                    //test star
		"SELECT a.* FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",                                                   //test star
		"SELECT * FROM NATION a join REGION b on a.N_REGIONKEY = b.R_REGIONKEY WHERE a.N_REGIONKEY > 0",
		"SELECT N_NAME, R_REGIONKEY FROM NATION2 join REGION using(R_REGIONKEY)",
		"select nation.n_name from nation join nation2 on nation.n_name !='a' join region on nation.n_regionkey = region.r_regionkey",
		"select n_name from nation intersect select n_name from nation2",
		"select n_name from nation minus select n_name from nation2",
	}
	mock := plan.NewMockOptimizer()
	buildPlanMarshalTest(mock, t, sqls)
}

func TestNestedQueryToJson(t *testing.T) {
	sqls := []string{
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION)",                                 // unrelated
		"SELECT * FROM NATION where N_REGIONKEY in (select max(R_REGIONKEY) from REGION)",                                // unrelated
		"SELECT * FROM NATION where N_REGIONKEY not in (select max(R_REGIONKEY) from REGION)",                            // unrelated
		"SELECT * FROM NATION where exists (select max(R_REGIONKEY) from REGION)",                                        // unrelated
		"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY = N_REGIONKEY)", // related
		//"SELECT * FROM NATION where N_REGIONKEY > (select max(R_REGIONKEY) from REGION where R_REGIONKEY < N_REGIONKEY)", // related
		//"DELETE FROM NATION WHERE N_NATIONKEY > 10",
		`select
		sum(l_extendedprice) / 7.0 as avg_yearly
	from
		lineitem,
		part
	where
		p_partkey = l_partkey
		and p_brand = 'Brand#54'
		and p_container = 'LG BAG'
		and l_quantity < (
			select
				0.2 * avg(l_quantity)
			from
				lineitem
			where
				l_partkey = p_partkey
		);`, //tpch q17
	}
	mock := plan.NewMockOptimizer()
	buildPlanMarshalTest(mock, t, sqls)
}

func TestCollectionQueryToJson(t *testing.T) {
	sqls := []string{
		"select 1 union select 2",
		"select 1 union (select 2 union select 3)",
		"(select 1 union select 2) union select 3 intersect select 4 order by 1",
		"select 1 union select null",
		"select n_name from nation intersect select n_name from nation2",
		"select n_name from nation minus select n_name from nation2",
		"select 1 union select 2 intersect select 2 union all select 1.1 minus select 22222",
		"select 1 as a union select 2 order by a limit 1",
		"select n_name from nation union select n_comment from nation order by n_name",
		"with qn (foo, bar) as (select 1 as col, 2 as coll union select 4, 5) select qn1.bar from qn qn1",
		"select n_name, n_comment from nation union all select n_name, n_comment from nation2",
		"select n_name from nation intersect all select n_name from nation2",
		"SELECT distinct(l.L_ORDERKEY) FROM LINEITEM AS l WHERE l.L_SHIPINSTRUCT='DELIVER IN PERSON' UNION SELECT distinct(l.L_ORDERKEY) FROM LINEITEM AS l WHERE l.L_SHIPMODE='AIR' OR  l.L_SHIPMODE='AIR REG'",
		"SELECT distinct(l.L_ORDERKEY) FROM LINEITEM AS l WHERE l.L_SHIPMODE IN ('AIR','AIR REG') EXCEPT SELECT distinct(l.L_ORDERKEY) FROM LINEITEM AS l WHERE l.L_SHIPINSTRUCT='DELIVER IN PERSON'",
	}

	mock := plan.NewMockOptimizer()
	buildPlanMarshalTest(mock, t, sqls)
}

func TestDerivedTableQueryToJson(t *testing.T) {
	// should pass
	sqls := []string{
		"select c_custkey from (select c_custkey from CUSTOMER ) a",
		"select c_custkey from (select c_custkey from CUSTOMER group by c_custkey ) a",
		"select col1 from (select c_custkey from CUSTOMER group by c_custkey ) a(col1)",
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a where ff > 0 order by c_custkey",
		"select col1 from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a(col1, col2) where col2 > 0 order by col1",
		"select c_custkey from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"select a.* from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
		"select * from (select c_custkey, count(C_NATIONKEY) ff from CUSTOMER group by c_custkey ) a join NATION b on a.c_custkey = b.N_REGIONKEY where b.N_NATIONKEY > 10",
	}
	mock := plan.NewMockOptimizer()
	buildPlanMarshalTest(mock, t, sqls)
}

func TestDMLToJson(t *testing.T) {
	sqls := []string{
		"INSERT INTO NATION SELECT * FROM NATION2",
		"UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=N_REGIONKEY+2 WHERE N_NATIONKEY > 10 LIMIT 20",
		"UPDATE NATION,NATION2 SET NATION.N_NAME ='U1',NATION2.N_NATIONKEY=15 WHERE NATION.N_NATIONKEY = NATION2.N_NATIONKEY",
		"DELETE FROM NATION WHERE N_NATIONKEY > 10",
		"DELETE FROM a1, a2 USING NATION AS a1 INNER JOIN NATION2 AS a2 WHERE a1.N_NATIONKEY=a2.N_NATIONKEY",
	}
	mock := plan.NewMockOptimizer()
	buildPlanMarshalTest(mock, t, sqls)
}

func buildPlanMarshalTest(opt plan.Optimizer, t *testing.T, sqls []string) {
	for _, sql := range sqls {
		t.Logf("sql: %s \n", sql)
		mock := plan.NewMockOptimizer()
		plan, err := runSingleSql(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		queryPlan := plan.GetQuery()
		for _, node := range queryPlan.Nodes {
			info := plan2.AnalyzeInfo{
				InputRows:    12,
				OutputRows:   12,
				TimeConsumed: 5,
				InputSize:    24,
				OutputSize:   24,
				MemorySize:   10,
			}
			node.AnalyzeInfo = &info
		}
		// generator query explain
		explainQuery := NewExplainQueryImpl(queryPlan)
		options := &ExplainOptions{
			Verbose: true,
			Analyze: true,
			Format:  EXPLAIN_FORMAT_TEXT,
		}

		marshalPlan := explainQuery.BuildJsonPlan(uuid.New(), options)
		//marshal, err := json.Marshal(marshalPlan)

		buffer := &bytes.Buffer{}
		encoder := json.NewEncoder(buffer)
		encoder.SetEscapeHTML(false)
		err = encoder.Encode(marshalPlan)

		if err != nil {
			panic(err)
		}
		t.Logf("SQL plan to json : %s\n", buffer.String())

	}
}

func runSingleSql(opt plan.Optimizer, t *testing.T, sql string) (*plan.Plan, error) {
	stmts, err := mysql.Parse(sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	// this sql always return one stmt
	ctx := opt.CurrentContext()
	return plan.BuildPlan(ctx, stmts[0])
}
