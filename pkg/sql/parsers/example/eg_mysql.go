// Copyright 2022 Matrix Origin
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

package main

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func main() {
	sql := `select u.a, (select t.a from sa.t, u) from u, (select t.a, u.a from sa.t, u where t.a = u.a) as t where (u.a, u.b, u.c) in (select t.a, u.a, t.b * u.b as tubb from t)`

	ast, err := parsers.ParseOne(context.TODO(), dialect.MYSQL, sql, 1, 0)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(tree.String(ast, dialect.MYSQL))
	fmt.Printf("\n %#v \n", ast)
}
