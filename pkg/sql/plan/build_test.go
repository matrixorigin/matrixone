// Copyright 2021 Matrix Origin
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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine"
	"log"
	"testing"
)

var querys = []string{
	"select * from R join S on R.uid = S.uid",
	"select sum(R.price) from R join S on R.uid = S.uid",
	"select * from R join S on R.uid = S.uid group by R.uid",
	"select sum(R.price) from R join S on R.uid = S.uid group by R.uid",
	"SELECT userID, MIN(score) FROM t1 GROUP BY userID;",
	"SELECT userID, MIN(score) FROM t1 GROUP BY userID ORDER BY userID asc;",
	"SELECT userID, SUM(score) FROM t1 GROUP BY userID ORDER BY userID desc;",
	"SELECT userID as a, MIN(score) as b FROM t1 GROUP BY userID;",
	"SELECT userID as user, MAX(score) as max FROM t1 GROUP BY userID order by user;",
	"SELECT userID as user, MAX(score) as max FROM t1 GROUP BY userID order by max desc;",
	"select userID,count(score) from t1 group by userID having count(score)>1;",
	"select userID,count(score) from t1 where userID>2 group by userID having count(score)>1;",
	"select userID,count(score) from t1 group by userID having count(score)>1;",
	"SELECT distinct userID, count(score) FROM t1 GROUP BY userID;",
	"select distinct spID,userID from t1;",
	"select distinct spID,userID from t1 where score>2;",
	"select distinct spID,userID from t1 where score>2 order by spID asc;",
	"select distinct spID,userID from t1 where spID>2 order by userID desc;",
	"select distinct sum(spID) from t1 group by userID;",
	"select distinct sum(spID) as sum from t1 group by userID order by sum asc;",
	"select distinct sum(spID) as sum from t1 where score>1 group by userID order by sum asc;",
	"select userID,MAX(score) from t1 where userID between 2 and 3 group by userID;",
	"select userID,MAX(score) from t1 where userID not between 2 and 3 group by userID order by userID desc;",
	"select spID,userID,score from t1 limit 2,1;",
	"select spID,userID,score from t1 limit 2 offset 1;",
	"select sum(score) as sum from t1 where spID=6 group by score order by sum desc;",
	"select userID, userID DIV 2 as user_dir, userID%2 as user_percent, userID MOD 2 as user_mod from t1;",
	// "select userID,MAX(score) max_score from t1 where userID <2 || userID > 3 group by userID order by max_score;",
	"select userID, userID DIV 2 as user_dir, userID%2 as user_percent, userID MOD 2 as user_mod from t1 WHERE userID > 3 ;",
	"select CAST(userID AS CHAR) userid_cast, userID from t1 where CAST(spID AS CHAR)='1';",
	"select CAST(userID AS DOUBLE) cast_double, CAST(userID AS FLOAT(3)) cast_float , CAST(userID AS REAL) cast_real, CAST(userID AS SIGNED) cast_signed, CAST(userID AS UNSIGNED) cast_unsigned from t1 limit 2;",
	// "select * from t1 where spID>2 AND userID <2 || userID >=2 OR userID < 2 limit 3;",
	"select * from t1 where (spID >2  or spID <= 2) && score <> 1 AND userID/2>2;",
	// "select * from t1 where spID >2  || spID <= 2 && score !=1 limit 3;",

	`select
		sum(lo_revenue) as revenue
	 from lineorder join dates on lo_orderdate = d_datekey
	 where d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25;`,

	`select
		sum(lo_revenue) as revenue
	 from lineorder
	 join dates on lo_orderdate = d_datekey
	 where d_yearmonthnum = 199401
	 and lo_discount between 4 and 6
	 and lo_quantity between 26 and 35;`,

	`select
		sum(lo_revenue) as revenue
	 from lineorder
     join dates on lo_orderdate = d_datekey
	 where d_weeknuminyear = 6 and d_year = 1994
	 and lo_discount between 5 and 7
	 and lo_quantity between 26 and 35;`,

	`select sum(lo_revenue) as lo_revenue, d_year, p_brand
	 from lineorder
	 join dates on lo_orderdate = d_datekey
     join part on lo_partkey = p_partkey
     join supplier on lo_suppkey = s_suppkey
     where p_category = 'MFGR#12' and s_region = 'AMERICA'
     group by d_year, p_brand
     order by d_year, p_brand;`,

	`select sum(lo_revenue) as lo_revenue, d_year, p_brand
	 from lineorder
     join dates on lo_orderdate = d_datekey
     join part on lo_partkey = p_partkey
     join supplier on lo_suppkey = s_suppkey
     where p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA'
     group by d_year, p_brand
     order by d_year, p_brand;`,

	`select sum(lo_revenue) as lo_revenue, d_year, p_brand
     from lineorder
     join dates on lo_orderdate = d_datekey
     join part on lo_partkey = p_partkey
     join supplier on lo_suppkey = s_suppkey
     where p_brand = 'MFGR#2239' and s_region = 'EUROPE'
     group by d_year, p_brand
     order by d_year, p_brand;`,

	`select c_nation, s_nation, d_year, sum(lo_revenue) as lo_revenue
     from lineorder
     join dates on lo_orderdate = d_datekey
     join customer on lo_custkey = c_custkey
     join supplier on lo_suppkey = s_suppkey
     where c_region = 'ASIA' and s_region = 'ASIA'and d_year >= 1992 and d_year <= 1997
     group by c_nation, s_nation, d_year
     order by d_year asc, lo_revenue desc;`,

	`select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
     from lineorder
     join dates on lo_orderdate = d_datekey
     join customer on lo_custkey = c_custkey
     join supplier on lo_suppkey = s_suppkey
     where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES'
     and d_year >= 1992 and d_year <= 1997
     group by c_city, s_city, d_year
     order by d_year asc, lo_revenue desc;`,

	`select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
     from lineorder
     join dates on lo_orderdate = d_datekey
     join customer on lo_custkey = c_custkey
     join supplier on lo_suppkey = s_suppkey
     where (c_city='UNITED KI1' or c_city='UNITED KI5')
     and (s_city='UNITED KI1' or s_city='UNITED KI5')
     and d_year >= 1992 and d_year <= 1997
     group by c_city, s_city, d_year
     order by d_year asc, lo_revenue desc;`,

	`select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
     from lineorder
     join dates on lo_orderdate = d_datekey
     join customer on lo_custkey = c_custkey
     join supplier on lo_suppkey = s_suppkey
     where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_yearmonth = 'Dec1997'
     group by c_city, s_city, d_year
     order by d_year asc, lo_revenue desc;`,

	`select d_year, c_nation, sum(lo_revenue) - sum(lo_supplycost) as profit
     from lineorder
     join dates on lo_orderdate = d_datekey
     join customer on lo_custkey = c_custkey
     join supplier on lo_suppkey = s_suppkey
     join part on lo_partkey = p_partkey
     where c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
     group by d_year, c_nation
     order by d_year, c_nation;`,

	`select d_year, s_nation, p_category, sum(lo_revenue) - sum(lo_supplycost) as profit
     from lineorder
     join dates on lo_orderdate = d_datekey
     join customer on lo_custkey = c_custkey
     join supplier on lo_suppkey = s_suppkey
     join part on lo_partkey = p_partkey
     where c_region = 'AMERICA'and s_region = 'AMERICA'
     and (d_year = 1997 or d_year = 1998)
     and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
     group by d_year, s_nation, p_category
     order by d_year, s_nation, p_category;`,

	`select d_year, s_city, p_brand, sum(lo_revenue) - sum(lo_supplycost) as profit
     from lineorder
     join dates on lo_orderdate = d_datekey
     join customer on lo_custkey = c_custkey
     join supplier on lo_suppkey = s_suppkey
     join part on lo_partkey = p_partkey
     where c_region = 'AMERICA'and s_nation = 'UNITED STATES'
     and (d_year = 1997 or d_year = 1998)
     and p_category = 'MFGR#14'
     group by d_year, s_city, p_brand
     order by d_year, s_city, p_brand;`,
}

func TestBuild(t *testing.T) {
	e := memEngine.NewTestEngine()
	for _, query := range querys {
		processQuery(query, e)
	}
}

func processQuery(query string, e engine.Engine) {
	stmts, err := parsers.Parse(dialect.MYSQL, query)
	if err != nil {
		log.Fatal(err)
	}
	b := New("test", query, e)
	for _, stmt := range stmts {
		fmt.Printf("%s\n", query)
		qry, err := b.BuildStatement(stmt)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", qry)
	}

}
