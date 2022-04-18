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
