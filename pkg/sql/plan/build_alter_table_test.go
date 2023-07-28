// Copyright 2023 Matrix Origin
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

import "testing"

func TestAlterTable1(t *testing.T) {
	//sql := "ALTER TABLE t1 ADD (d TIMESTAMP, e INT not null);"
	//sql := "ALTER TABLE t1 ADD d INT NOT NULL PRIMARY KEY;"
	sql := "ALTER TABLE t1 MODIFY b INT;"
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, true, t)
}

func TestAlterTableAddColumns(t *testing.T) {
	mock := NewMockOptimizer(false)
	// CREATE TABLE t1 (a INTEGER, b CHAR(10));
	sqls := []string{
		`ALTER TABLE t1 ADD d TIMESTAMP;`,
		//`ALTER TABLE t1 ADD (d TIMESTAMP, e INT not null);`,
		`ALTER TABLE t1 ADD c INT PRIMARY KEY;`,
		`ALTER TABLE t1 ADD c INT PRIMARY KEY PRIMARY KEY;`,
		`ALTER TABLE t1 ADD c INT PRIMARY KEY PRIMARY KEY PRIMARY KEY;`,
	}
	runTestShouldPass(mock, t, sqls, false, false)
}
