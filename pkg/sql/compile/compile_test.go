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

package compile

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"log"
	"testing"
)

var querys = []string{
	"CREATE DATABASE db;",
	"CREATE DATABASE IF NOT EXISTS db;",
	"CREATE TABLE table1(a int);",
	"CREATE TABLE IF NOT EXISTS table1(a int);",
	"CREATE INDEX idx on table1(a);",
	// "DROP INDEX idx on table1;",
	"SHOW DATABASES;",
	"SHOW TABLES;",
	"SHOW COLUMNS FROM table1;",
	"SHOW CREATE TABLE table1;",
	// "SHOW CREATE DATABASE db;",
	"INSERT INTO table1 values(12);",
	"DROP TABLE table1;",
	"DROP DATABASE IF EXISTS db;",
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
	"select distinct sum(spID) from t1 group by userID;",
	"select distinct sum(spID) as sum from t1 group by userID order by sum asc;",
	"select distinct sum(spID) as sum from t1 where score>1 group by userID order by sum asc;",
	"select userID,MAX(score) from t1 where userID between 2 and 3 group by userID;",
	"select userID,MAX(score) from t1 where userID not between 2 and 3 group by userID order by userID desc;",
	"select sum(score) as sum from t1 where spID=6 group by score order by sum desc;",
	// "select userID,MAX(score) max_score from t1 where userID <2 || userID > 3 group by userID order by max_score;",
}

func TestCompile(t *testing.T) {
	InitAddress("127.0.0.1")
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	proc := process.New(mheap.New(gm))
	e := memEngine.NewTestEngine()
	for _, query := range querys {
		processQuery(query, e, proc)
	}
}

func sqlOutput(_ interface{}, bat *batch.Batch) error {
	fmt.Printf("%v\n", bat.Zs)
	fmt.Printf("%v\n", bat)
	return nil
}

func processQuery(query string, e engine.Engine, proc *process.Process) {
	c := New("test", query, "", e, proc)
	es, err := c.Build()
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range es {
		fmt.Printf("%s\n", query)
		if err := e.Compile(nil, sqlOutput); err != nil {
			log.Fatal(err)
		}
		attrs := e.Columns()
		fmt.Printf("result:\n")
		for i, attr := range attrs {
			fmt.Printf("\t[%v] = %v:%v\n", i, attr.Name, attr.Typ)
		}
		if err := e.Run(0); err != nil {
			log.Fatal(err)
		}
	}
}
