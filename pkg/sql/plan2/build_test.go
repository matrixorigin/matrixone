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

package plan2

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

func outPutQuery(query *Query, toFile bool, t *testing.T) {
	b, err := json.Marshal(query)
	if err != nil {
		t.Logf("%+v", query)
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "    ")
	if err != nil {
		t.Logf("%+v", query)
	}
	if toFile {
		err := ioutil.WriteFile("/Users/ouyuanning/test.json", []byte(out.String()), 777)
		if err != nil {
			t.Logf("%+v", err)
		}
	} else {
		t.Logf(out.String())
	}
}

func TestSqlBuilder(t *testing.T) {
	mock := newMockOptimizer()

	sql := "SELECT N_NAME,N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0"
	stmts, err := mysql.Parse(sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	for _, stmt := range stmts {
		query, err := mock.Optimize(stmt)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutQuery(query, false, t)
	}
}
