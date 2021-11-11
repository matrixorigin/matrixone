package main

import (
	"fmt"

	"matrixone/pkg/sql/parsers"
	"matrixone/pkg/sql/parsers/dialect"
	"matrixone/pkg/sql/parsers/tree"
)

func main() {
	sql := `select u.a, (select t.a from sa.t, u) from u, (select t.a, u.a from sa.t, u where t.a = u.a) as t where (u.a, u.b, u.c) in (select t.a, u.a, t.b * u.b as tubb from t)`

	ast, err := parsers.ParseOne(dialect.MYSQL, sql)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(tree.String(ast, dialect.MYSQL))
	fmt.Printf("\n %#v \n", ast)
}
