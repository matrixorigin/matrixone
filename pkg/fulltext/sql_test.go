// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fulltext

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSqlPhrase(t *testing.T) {
	tests := []TestCase{
		{
			pattern: "\"Matrix Origin\"",
			expect:  "(phrase (text 0 0 matrix) (text 1 7 origin))",
		},
		{
			pattern: "\"Matrix\"",
			expect:  "(phrase (text 0 0 matrix))",
		},
		{
			pattern: "\"    Matrix     \"",
			expect:  "(phrase (text 0 0 matrix))",
		},
		{
			pattern: "\"Matrix     Origin\"",
			expect:  "(phrase (text 0 0 matrix) (text 1 11 origin))",
		},
		{
			pattern: "\"  你好嗎? Hello World  在一起  Happy  再见  \"",
			expect:  "(phrase (text 0 0 你好嗎?) (text 1 11 hello) (text 2 17 world) (text 3 24 在一起) (text 4 35 happy) (text 5 42 再见))",
		},
	}

	for _, c := range tests {
		s, err := NewSearchAccum("src", "index", c.pattern, int64(tree.FULLTEXT_BOOLEAN), "")
		require.Nil(t, err)
		result, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_BOOLEAN), "`__mo_index_secondary_0193b0b9-07c2-782d-aa6f-fc892c464561`")
		require.Nil(t, err)
		fmt.Println(result)
		assert.Equal(t, c.expect, result)
	}
}

func TestSqlBoolean(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "Matrix Origin",
			expect:  "(text 0 matrix) (text 1 origin)",
		},
		{
			pattern: "+Matrix Origin",
			expect:  "(+ (text 0 matrix)) (text 1 origin)",
		},
		{
			pattern: "+Matrix -Origin",
			expect:  "(+ (text 0 matrix)) (- (text 1 origin))",
		},
		{
			pattern: "Matrix ~Origin",
			expect:  "(text 0 matrix) (~ (text 1 origin))",
		},
		{
			pattern: "Matrix +(<Origin >One)",
			expect:  "(+ (group (< (text 0 origin)) (> (text 1 one)))) (text 2 matrix)",
		},
		{
			pattern: "+Matrix +Origin",
			expect:  "(join 0 (+ (text 0 matrix)) (+ (text 0 origin)))",
		},
		{
			pattern: "\"Matrix origin\"",
			expect:  "(phrase (text 0 matrix) (text 1 origin))",
		},
		{
			pattern: "Matrix Origin*",
			expect:  "(text 0 matrix) (* 1 origin*)",
		},
		{
			pattern: "+Matrix +(Origin (One Two))",
			expect:  "(+ (text 0 matrix)) (+ (group (text 1 origin) (group (text 2 one) (text 3 two))))",
		},
		{
			pattern: "+读写汉字 -学中文",
			expect:  "(+ (text 0 读写汉字)) (- (text 1 学中文))",
		},
	}

	for _, c := range tests {
		s, err := NewSearchAccum("src", "index", c.pattern, int64(tree.FULLTEXT_BOOLEAN), "")
		require.Nil(t, err)
		result, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_BOOLEAN), "`__mo_index_secondary_0193b0b9-07c2-782d-aa6f-fc892c464561`")
		fmt.Println(PatternListToStringWithPosition(s.Pattern))
		require.Nil(t, err)
		fmt.Println(result)
		assert.Equal(t, c.expect, result)
	}
}

func TestSqlNL(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "Matrix Origin",
			expect:  "(text 0 matrix) (text 1 origin)",
		},
		{
			pattern: "读写汉字 学中文",
			expect:  "(text 0 读写汉) (text 1 写汉字) (text 2 汉字) (text 3 字) (text 4 学中文) (text 5 中文) (text 6 文)",
		},
		{
			pattern: "读写",
			expect:  "(* 0 读写*)",
		},
	}

	for _, c := range tests {
		s, err := NewSearchAccum("src", "index", c.pattern, int64(tree.FULLTEXT_NL), "")
		require.Nil(t, err)
		result, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_NL), "`__mo_index_secondary_0193b0b9-07c2-782d-aa6f-fc892c464561`")
		require.Nil(t, err)
		fmt.Println(result)
		assert.Equal(t, c.expect, result)
	}
}
