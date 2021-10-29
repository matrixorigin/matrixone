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

package build

import (
	"fmt"
	"log"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/memEngine"
	"testing"
)

var querys = []string{
	"select * from R join S using(uid)",
	"select * from R join S on R.uid = S.uid",
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
	"select userID,MAX(score) max_score from t1 where userID <2 || userID > 3 group by userID order by max_score;",
	"select userID, userID DIV 2 as user_dir, userID%2 as user_percent, userID MOD 2 as user_mod from t1 WHERE userID > 3 ;",
	"select CAST(userID AS CHAR) userid_cast, userID from t1 where CAST(spID AS CHAR)='1';",
	"select CAST(userID AS DOUBLE) cast_double, CAST(userID AS FLOAT(3)) cast_float , CAST(userID AS REAL) cast_real, CAST(userID AS SIGNED) cast_signed, CAST(userID AS UNSIGNED) cast_unsigned from t1 limit 2;",
	"select * from t1 where spID>2 AND userID <2 || userID >=2 OR userID < 2 limit 3;",
	"select * from t1 where (spID >2  or spID <= 2) && score <> 1 AND userID/2>2;",
	"select * from t1 where spID >2  || spID <= 2 && score !=1 limit 3;",
}

func TestBuild(t *testing.T) {
	e := memEngine.NewTestEngine()
	for _, query := range querys {
		processQuery(query, e)
	}
}

func processQuery(query string, e engine.Engine) {
	stmts, err := tree.NewParser().Parse(query)
	if err != nil {
		log.Fatal(err)
	}
	b := New("test", query, e)
	for _, stmt := range stmts {
		qry, err := b.BuildStatement(stmt)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", qry)
	}

}
