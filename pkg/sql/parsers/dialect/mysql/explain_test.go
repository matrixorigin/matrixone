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

package mysql

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"testing"
)

func TestExplain(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		output string
	}{
		{
			name:   "test01",
			input:  "explain select * from emp",
			output: "explain select * from emp",
		},
		{
			name:   "test02",
			input:  "explain verbose select * from emp",
			output: "explain (verbose) select * from emp",
		},
		{
			name:   "test03",
			input:  "explain analyze select * from emp",
			output: "explain (analyze) select * from emp",
		},
		{
			name:   "test04",
			input:  "explain analyze verbose select * from emp",
			output: "explain (analyze,verbose) select * from emp",
		},
		{
			name:   "test05",
			input:  "explain (analyze true,verbose false) select * from emp",
			output: "explain (analyze true,verbose false) select * from emp",
		},
		{
			name:   "test06",
			input:  "explain (analyze true,verbose false,format JSON) select * from emp",
			output: "explain (analyze true,verbose false,format JSON) select * from emp",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.output == "" {
				c.output = c.input
			}
			ast, err := ParseOne(c.input)
			if err != nil {
				t.Errorf("Parse [%v] with sql [%q] occur error: %v", c.name, c.input, err)
			} else {
				out := tree.String(ast, dialect.MYSQL)
				fmt.Printf("Test[%v], out result is: %v\n", c.name, out)
				if c.output != out {
					t.Errorf("Parsing failed. \n Expected/Got:\n%s\n%s", c.output, out)
				}
			}

		})
	}
}

func TestExplainTolerance(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{
			name:  "test01",
			input: "explain analyze verbose true select * from emp",
		},
		{
			name:  "test02",
			input: "explain analyze verbose false select * from emp",
		},
		{
			name:  "test03",
			input: "explain verbose true select * from emp",
		},
		{
			name:  "test04",
			input: "explain analyze true select * from emp",
		},
		{
			name:  "test05",
			input: "explain analyze (verbose) select * from emp",
		},
		{
			name:  "test06",
			input: "explain format JSON select * from emp",
		},
		{
			name:  "test07",
			input: "explain analyze verbose true true select * from emp",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseOne(c.input)
			if err == nil {
				t.Errorf("Parse [%v] with sql [%q] should report parse error!", c.name, c.input)
			}
		})
	}
}
