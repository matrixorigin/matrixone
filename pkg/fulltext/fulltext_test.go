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

type TestCase struct {
	pattern string
	expect  string
}

func TestPatternPhrase(t *testing.T) {
	tests := []TestCase{
		{
			pattern: "\"Ma'trix     Origin\"",
			expect:  "(phrase (text 0 0 ma) (text 1 3 trix) (text 2 12 origin))",
		},
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
			expect:  "(phrase (text 0 0 你好嗎) (text 1 11 hello) (text 2 17 world) (text 3 24 在一起) (text 4 35 happy) (* 5 42 再见*))",
		},
	}

	for _, c := range tests {
		result, err := PatternToStringWithPosition(c.pattern, int64(tree.FULLTEXT_BOOLEAN))
		require.Nil(t, err)
		assert.Equal(t, c.expect, result)
	}
}

func TestPatternBoolean(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "Ma'trix Origin",
			expect:  "(text 0 ma'trix) (text 1 origin)",
		},
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
		{
			pattern: "+读书会 +提效 +社群 +案例 +运营",
			expect:  "(join 0 (+ (text 0 读书会)) (+ (text 0 提效)) (+ (text 0 社群)) (+ (text 0 案例)) (+ (text 0 运营)))",
		},
	}

	for _, c := range tests {
		result, err := PatternToString(c.pattern, int64(tree.FULLTEXT_BOOLEAN))
		require.Nil(t, err)
		assert.Equal(t, c.expect, result)
	}
}

func TestPatternNL(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "Ma'trix Origin",
			expect:  "(* 0 0 ma*) (text 1 3 trix) (text 2 8 origin)",
		},
		{
			pattern: "Matrix Origin",
			expect:  "(text 0 0 matrix) (text 1 7 origin)",
		},
		{
			pattern: "读写汉字 学中文",
			expect:  "(text 0 0 读写汉) (text 1 3 写汉字) (text 2 13 学中文)",
		},
		{
			pattern: "读写",
			expect:  "(* 0 0 读写*)",
		},
		{
			pattern: "肥胖的原因都是因为摄入脂肪多导致的吗",
			expect:  "(text 0 0 肥胖的) (text 1 9 原因都) (text 2 18 是因为) (text 3 27 摄入脂) (text 4 36 肪多导) (text 5 45 致的吗)",
		},
		{
			pattern: "肥胖的原因都是因为摄入fat多导致的吗",
			expect:  "(text 0 0 肥胖的) (text 1 9 原因都) (text 2 18 是因为) (text 3 24 为摄入) (text 4 33 fat) (text 5 36 多导致) (text 6 42 致的吗)",
		},
	}

	for _, c := range tests {
		result, err := PatternToStringWithPosition(c.pattern, int64(tree.FULLTEXT_NL))
		require.Nil(t, err)
		assert.Equal(t, c.expect, result)
	}
}

func TestPatternQueryExpansion(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "Matrix Origin",
			expect:  "(text 0 matrix) (text 1 origin)",
		},
		{
			pattern: "读写汉字 学中文",
			expect:  "(+ (text 0 读写汉字)) (- (text 1 学中文))",
		},
	}

	for _, c := range tests {
		_, err := PatternToString(c.pattern, int64(tree.FULLTEXT_QUERY_EXPANSION))
		require.NotNil(t, err)
	}
}

func TestPatternFail(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "Matrix Origin( ",
		},
		{
			pattern: "(+Matrix Origin",
		},
		{
			pattern: "++Matrix -Origin",
		},
		{
			pattern: "Matrix ~~Origin",
		},
		{
			pattern: "Matrix +(<(+Origin -apple) >One)",
		},
		{
			pattern: "+Matrix --Origin",
		},
	}

	for _, c := range tests {
		_, err := PatternToString(c.pattern, int64(tree.FULLTEXT_BOOLEAN))
		require.NotNil(t, err)
	}
}

func TestPatternNLFail(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "+[[[",
		},
		{
			pattern: "+''",
		},
	}

	for _, c := range tests {
		_, err := PatternToString(c.pattern, int64(tree.FULLTEXT_NL))
		require.NotNil(t, err)
	}
}

func TestFullTextNL(t *testing.T) {

	pattern := "apple banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_NL), "", ALGO_TFIDF)
	require.Nil(t, err)

	sql, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_NL), "indextbl", "", ALGO_TFIDF)
	require.Nil(t, err)

	fmt.Println(sql)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "apple"
	//apple_idx := word2idx["apple"]
	//banna_idx := word2idx["banna"]
	agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	aggcnt[0] = 2
	aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
		}

		if len(result) > 0 {
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, true)
	_, ok = test_result[1]
	assert.Equal(t, ok, true)
	_, ok = test_result[11]
	assert.Equal(t, ok, true)
	_, ok = test_result[12]
	assert.Equal(t, ok, true)

}

func TestFullTextOr(t *testing.T) {

	pattern := "apple banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	aggcnt[0] = 2
	aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
		}

		if len(result) > 0 {
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, true)
	_, ok = test_result[1]
	assert.Equal(t, ok, true)
	_, ok = test_result[11]
	assert.Equal(t, ok, true)
	_, ok = test_result[12]
	assert.Equal(t, ok, true)

}

func TestFullTextPlusPlus(t *testing.T) {

	pattern := "+apple -orange"
	//pattern := "+apple +banana -orange"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	//fmt.Printf("PATTERN %v\n", s.Pattern)
	// [(join 0 (+ (text 0 apple)) (+ (text 0 banana))) (- (text orange))]
	agghtab[0] = []uint8{uint8(2), uint8(2)}  // join
	agghtab[1] = []uint8{uint8(3), uint8(0)}  // join
	agghtab[11] = []uint8{uint8(0), uint8(3)} // orange
	agghtab[12] = []uint8{uint8(0), uint8(4)} // ornage

	aggcnt[0] = 2
	aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, false)
	_, ok = test_result[1]
	assert.Equal(t, ok, true)
	_, ok = test_result[11]
	assert.Equal(t, ok, false)
	_, ok = test_result[12]
	assert.Equal(t, ok, false)
}

func TestFullTextPlusOr(t *testing.T) {

	pattern := "+apple banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	aggcnt[0] = 2
	aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, true)
	_, ok = test_result[1]
	assert.Equal(t, ok, true)
	_, ok = test_result[11]
	assert.Equal(t, ok, false)
	_, ok = test_result[12]
	assert.Equal(t, ok, false)
}

func TestFullTextMinus(t *testing.T) {

	pattern := "+apple -banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	aggcnt[0] = 2
	aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, false)
	_, ok = test_result[1]
	assert.Equal(t, ok, true)
	_, ok = test_result[11]
	assert.Equal(t, ok, false)
	_, ok = test_result[12]
	assert.Equal(t, ok, false)

}

func TestFullTextTilda(t *testing.T) {

	pattern := "+apple ~banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	aggcnt[0] = 2
	aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, true)
	_, ok = test_result[1]
	assert.Equal(t, ok, true)
	_, ok = test_result[11]
	assert.Equal(t, ok, false)
	_, ok = test_result[12]
	assert.Equal(t, ok, false)
}

func TestFullText1(t *testing.T) {

	pattern := "we aRe so Happy"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	// {we, are, so, happy}
	// we
	agghtab[0] = []uint8{uint8(2), uint8(0), uint8(0), uint8(0)} // we
	agghtab[1] = []uint8{uint8(3), uint8(0), uint8(0), uint8(0)} // we

	// are
	agghtab[10] = []uint8{uint8(0), uint8(2), uint8(0), uint8(0)} // are
	agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5), uint8(0)}
	agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	// so
	agghtab[30] = []uint8{uint8(0), uint8(0), uint8(0), uint8(1)}
	agghtab[31] = []uint8{uint8(0), uint8(0), uint8(0), uint8(2)}
	agghtab[32] = []uint8{uint8(0), uint8(0), uint8(0), uint8(3)}
	agghtab[33] = []uint8{uint8(0), uint8(0), uint8(0), uint8(4)}

	aggcnt[0] = 2
	aggcnt[1] = 3
	aggcnt[2] = 4
	aggcnt[3] = 4

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
			//fmt.Printf("result %v\n", result)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	ids := []int{0, 1, 10, 11, 12, 20, 21, 22, 23, 30, 31, 32, 33}

	for _, id := range ids {
		_, ok = test_result[id]
		assert.Equal(t, ok, true)
	}
}

func TestFullText2(t *testing.T) {

	pattern := "+we +aRe +so +Happy"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	// {we, are, so, happy}
	// we
	agghtab[0] = []uint8{uint8(2), uint8(2), uint8(2), uint8(2)} // we, are
	agghtab[1] = []uint8{uint8(3), uint8(0), uint8(0), uint8(0)} // we

	// are
	agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	// so
	agghtab[31] = []uint8{uint8(0), uint8(0), uint8(0), uint8(2)}
	agghtab[32] = []uint8{uint8(0), uint8(0), uint8(0), uint8(3)}
	agghtab[33] = []uint8{uint8(0), uint8(0), uint8(0), uint8(4)}

	aggcnt[0] = 2
	aggcnt[1] = 3
	aggcnt[2] = 3
	aggcnt[3] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
			//fmt.Printf("result %v\n", result)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, true)

}

func TestFullText3(t *testing.T) {

	pattern := "+we -aRe -so -Happy"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	// {we, are, so, happy}
	// we
	agghtab[0] = []uint8{uint8(2), uint8(2), uint8(0), uint8(0)} // we, are
	agghtab[1] = []uint8{uint8(3), uint8(0), uint8(0), uint8(0)} // we

	// are
	agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5), uint8(0)}
	agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	// happy
	agghtab[30] = []uint8{uint8(0), uint8(0), uint8(0), uint8(1)}
	agghtab[31] = []uint8{uint8(0), uint8(0), uint8(0), uint8(2)}
	agghtab[32] = []uint8{uint8(0), uint8(0), uint8(0), uint8(3)}
	agghtab[33] = []uint8{uint8(0), uint8(0), uint8(0), uint8(4)}

	aggcnt[0] = 2
	aggcnt[1] = 3
	aggcnt[2] = 4
	aggcnt[3] = 4

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
			//fmt.Printf("result %v\n", result)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[1]
	assert.Equal(t, ok, true)
}

func TestFullText5(t *testing.T) {

	pattern := "we aRe so +Happy"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	// [(+ (text 0 happy)) (text 1 we) (text 2 are) (text 3 so)]
	// {we, are, so, happy}
	// we
	agghtab[0] = []uint8{uint8(0), uint8(2), uint8(0), uint8(0)} // we
	agghtab[1] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // we

	// are
	agghtab[11] = []uint8{uint8(0), uint8(0), uint8(3), uint8(0)} // are
	agghtab[12] = []uint8{uint8(0), uint8(0), uint8(4), uint8(0)} // are

	aggcnt[1] = 2
	aggcnt[2] = 2

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
			//fmt.Printf("result %v\n", result)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	assert.Equal(t, len(test_result), 0)

}

func TestFullTextGroup(t *testing.T) {

	pattern := "+we +(<are >so)"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	// {we, are, so, happy}
	// we
	agghtab[0] = []uint8{uint8(2), uint8(2), uint8(0), uint8(0)} // we, are
	agghtab[1] = []uint8{uint8(3), uint8(0), uint8(5), uint8(0)} // we, so

	// are
	agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5), uint8(0)}
	agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	aggcnt[0] = 2
	aggcnt[1] = 3
	aggcnt[2] = 6

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
			//fmt.Printf("result %v\n", result)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, true)
	_, ok = test_result[1]
	assert.Equal(t, ok, true)
}

func TestFullTextJoinGroupTilda(t *testing.T) {

	pattern := "+we +also ~(<are >so)"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	// {(we, also), are, so}
	// (we, also)
	agghtab[0] = []uint8{uint8(2), uint8(2), uint8(0)} // (we, also), are
	agghtab[1] = []uint8{uint8(3), uint8(0), uint8(5)} // (we, also), so

	// are
	agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0)} // are
	agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0)} // are

	// so
	agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5)}
	agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6)}
	agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7)}
	agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8)}

	aggcnt[0] = 2
	aggcnt[1] = 3
	aggcnt[2] = 6

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
			//fmt.Printf("result %v\n", result)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, true)
	_, ok = test_result[1]
	assert.Equal(t, ok, true)
	assert.Equal(t, 2, len(test_result))
}

func TestFullTextGroupTilda(t *testing.T) {

	pattern := "+we ~(<are >so)"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	// {we, are, so}
	// we
	agghtab[0] = []uint8{uint8(2), uint8(2), uint8(0)} // we, are
	agghtab[1] = []uint8{uint8(3), uint8(0), uint8(5)} // we, so

	// are
	agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0)} // are
	agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0)} // are

	// so
	agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5)}
	agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6)}
	agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7)}
	agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8)}

	aggcnt[0] = 2
	aggcnt[1] = 3
	aggcnt[2] = 6

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
			//fmt.Printf("result %v\n", result)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, true)
	_, ok = test_result[1]
	assert.Equal(t, ok, true)
	assert.Equal(t, 2, len(test_result))
}

func TestFullTextStar(t *testing.T) {

	pattern := "apple*"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))
	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	// {apple*}
	// apple*
	agghtab[0] = []uint8{uint8(2), uint8(2), uint8(0), uint8(0)} // we, are
	agghtab[1] = []uint8{uint8(3), uint8(0), uint8(5), uint8(0)} // we, so

	aggcnt[0] = 2

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
			//fmt.Printf("result %v\n", result)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, true)
	_, ok = test_result[1]
	assert.Equal(t, ok, true)

}

func TestFullTextPhrase(t *testing.T) {

	pattern := "\"we aRe so Happy\""
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.Nil(t, err)

	sql, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_BOOLEAN), "idxtbl", "", ALGO_TFIDF)
	require.Nil(t, err)
	fmt.Println(sql)

	agghtab := make(map[any][]uint8)
	aggcnt := make([]int64, 64)

	// {we, are, so, happy}
	// we
	agghtab[0] = []uint8{uint8(2), uint8(2), uint8(2), uint8(2)} // we
	agghtab[1] = []uint8{uint8(3), uint8(2), uint8(0), uint8(0)} // we

	// are
	agghtab[10] = []uint8{uint8(0), uint8(2), uint8(0), uint8(0)} // are
	agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5), uint8(0)}
	agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	// so
	agghtab[30] = []uint8{uint8(0), uint8(0), uint8(0), uint8(1)}
	agghtab[31] = []uint8{uint8(0), uint8(0), uint8(0), uint8(2)}
	agghtab[32] = []uint8{uint8(0), uint8(0), uint8(0), uint8(3)}
	agghtab[33] = []uint8{uint8(0), uint8(0), uint8(0), uint8(4)}

	aggcnt[0] = 2
	aggcnt[1] = 5
	aggcnt[2] = 5
	aggcnt[3] = 5

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range agghtab {
		var result []float32
		docvec := agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, 0, aggcnt, float32(1.0), result)
			require.Nil(t, err)
			//fmt.Printf("result %v\n", result)
		}

		if len(result) > 0 {
			//fmt.Printf("result %v %f\n", key, result[0])
			test_result[key] = result[0]
		}
		i++
	}

	var ok bool
	_, ok = test_result[0]
	assert.Equal(t, ok, true)
}

func TestFullTextCombine(t *testing.T) {
	p := &Pattern{}

	s1 := []float32{1}
	s2 := []float32{2}

	result, err := p.Combine(nil, nil, nil, s1, s2)
	require.Nil(t, err)

	assert.Equal(t, result[0], float32(2))
}

// #29271: a quoted BOOLEAN phrase on the ""/default/ngram parser must be decomposed by SimpleTokenizer
// -- the same tokenizer the index build uses -- not looked up as the raw whole string, which is never
// an indexed token. Covers the three reported shapes: a CJK phrase (stored as trigrams), a Latin run
// longer than MAX_TOKEN_SIZE=23 (stored truncated), and a hyphen/apostrophe phrase (SimpleTokenizer
// breaks on both). A short CJK tail stays a prefix STAR (苹果 is only ever a prefix of a stored
// trigram like 苹果甜); a short Latin word is stored whole so it matches exactly.
func TestBooleanPhraseDecomposesLikeIndex29271(t *testing.T) {
	tests := []TestCase{
		// CJK phrase: overlapping trigrams positioned as the index stored them.
		{pattern: `"苹果香蕉"`, expect: "(phrase (text 0 0 苹果香) (text 1 3 果香蕉))"},
		// short CJK phrase: prefix, so it reaches 苹果甜 in 红苹果甜.
		{pattern: `"苹果"`, expect: "(phrase (* 0 0 苹果*))"},
		// hyphen and apostrophe are breakers at index time, so the phrase splits on them too.
		{pattern: `"full-text search"`, expect: "(phrase (text 0 0 full) (text 1 5 text) (text 2 10 search))"},
		// a Latin run over 23 bytes is stored truncated to 23; the phrase looks up the same 23 bytes.
		{pattern: `"bbbbbbbbbbbbbbbbbbbbbbbbbb"`, expect: "(phrase (text 0 0 bbbbbbbbbbbbbbbbbbbbbbb))"},
		// a short Latin word is stored whole, so it stays an exact TEXT (not a prefix).
		{pattern: `"is not red"`, expect: "(phrase (text 0 0 is) (text 1 3 not) (text 2 7 red))"},
		// a whole short Latin phrase is matched exactly (not a prefix, which would false-match a
		// longer word since a single-word phrase has no positional anchor).
		{pattern: `"is"`, expect: "(phrase (text 0 0 is))"},
		{pattern: `"a"`, expect: "(phrase (text 0 0 a))"},
	}
	for _, c := range tests {
		result, err := PatternToStringWithPosition(c.pattern, int64(tree.FULLTEXT_BOOLEAN))
		require.Nil(t, err, c.pattern)
		assert.Equal(t, c.expect, result, c.pattern)
	}
}

// A phrase whose body tokenizes to nothing (only breakers/punctuation) is rejected rather than
// looked up as the raw string, matching natural-language mode on the same input (#29271).
func TestBooleanPhraseRejectsEmptyTokenization29271(t *testing.T) {
	for _, p := range []string{`"---"`, `"-.-"`} {
		_, err := PatternToStringWithPosition(p, int64(tree.FULLTEXT_BOOLEAN))
		require.NotNil(t, err, p)
		require.Contains(t, err.Error(), "converted to empty pattern", p)
	}
}

// Validate accepts a STAR child in a PHRASE (SqlPhrase resolves it as a positional prefix_eq) but
// still rejects a compound child (#29271).
func TestPhraseValidateAllowsStarRejectsCompound29271(t *testing.T) {
	ok := &Pattern{Operator: PHRASE, Children: []*Pattern{
		{Operator: TEXT, Text: "abc"},
		{Operator: STAR, Text: "de*"},
	}}
	require.Nil(t, ok.Validate())

	bad := &Pattern{Operator: PHRASE, Children: []*Pattern{
		{Operator: PLUS, Children: []*Pattern{{Operator: TEXT, Text: "x"}}},
	}}
	err := bad.Validate()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "PHRASE can only have text or star Pattern")
}
