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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestCase struct {
	pattern string
	expect  string
}

func PatternListToString(ps []*Pattern) string {
	ss := make([]string, 0, len(ps))
	for _, p := range ps {
		ss = append(ss, p.String())
	}

	return strings.Join(ss, " ")
}

func PatternToString(pattern string, mode int64) (string, error) {
	ps, err := ParsePattern(pattern, mode)
	if err != nil {
		return "", err
	}

	return PatternListToString(ps), nil
}

func PatternListToStringWithPosition(ps []*Pattern) string {
	ss := make([]string, 0, len(ps))
	for _, p := range ps {
		ss = append(ss, p.StringWithPosition())
	}

	return strings.Join(ss, " ")
}

func PatternToStringWithPosition(pattern string, mode int64) (string, error) {
	ps, err := ParsePattern(pattern, mode)
	if err != nil {
		return "", err
	}

	return PatternListToStringWithPosition(ps), nil
}

func TestPatternPhrase(t *testing.T) {
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
		result, err := PatternToStringWithPosition(c.pattern, int64(tree.FULLTEXT_BOOLEAN))
		require.Nil(t, err)
		assert.Equal(t, c.expect, result)
	}
}

func TestPatternBoolean(t *testing.T) {

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
			expect:  "(+ (group (< (text 1 origin)) (> (text 2 one)))) (text 0 matrix)",
		},
		{
			pattern: "+Matrix +Origin",
			expect:  "(+ (text 0 matrix)) (+ (text 1 origin))",
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
		result, err := PatternToString(c.pattern, int64(tree.FULLTEXT_BOOLEAN))
		require.Nil(t, err)
		assert.Equal(t, c.expect, result)
	}
}

func TestPatternNL(t *testing.T) {

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
		result, err := PatternToString(c.pattern, int64(tree.FULLTEXT_NL))
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

func TestFullTextNL(t *testing.T) {

	pattern := "apple banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_NL), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
		//fmt.Printf("idx %d, text %s\n", indexes[i], kw)
	}

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "apple"
	//apple_idx := word2idx["apple"]
	//banna_idx := word2idx["banna"]
	s.Agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	s.Agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	s.Agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	s.Agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	s.Agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	s.Agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	s.Agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	s.Agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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

	pattern := "+apple +banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	s.Agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	s.Agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	s.Agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	s.Agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
	assert.Equal(t, ok, false)
	_, ok = test_result[11]
	assert.Equal(t, ok, false)
	_, ok = test_result[12]
	assert.Equal(t, ok, false)
}

func TestFullTextPlusOr(t *testing.T) {

	pattern := "+apple banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	s.Agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	s.Agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	s.Agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	s.Agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	s.Agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	s.Agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	s.Agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	s.Agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	s.Agghtab[0] = []uint8{uint8(2), uint8(2)}  // apple, banna
	s.Agghtab[1] = []uint8{uint8(3), uint8(0)}  // apple
	s.Agghtab[11] = []uint8{uint8(0), uint8(3)} // banna
	s.Agghtab[12] = []uint8{uint8(0), uint8(4)} // banna

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 4)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	// {we, are, so, happy}
	// we
	s.Agghtab[0] = []uint8{uint8(2), uint8(0), uint8(0), uint8(0)} // we
	s.Agghtab[1] = []uint8{uint8(3), uint8(0), uint8(0), uint8(0)} // we

	// are
	s.Agghtab[10] = []uint8{uint8(0), uint8(2), uint8(0), uint8(0)} // are
	s.Agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	s.Agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	s.Agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5), uint8(0)}
	s.Agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	s.Agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	s.Agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	// so
	s.Agghtab[30] = []uint8{uint8(0), uint8(0), uint8(0), uint8(1)}
	s.Agghtab[31] = []uint8{uint8(0), uint8(0), uint8(0), uint8(2)}
	s.Agghtab[32] = []uint8{uint8(0), uint8(0), uint8(0), uint8(3)}
	s.Agghtab[33] = []uint8{uint8(0), uint8(0), uint8(0), uint8(4)}

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3
	s.Aggcnt[2] = 4
	s.Aggcnt[3] = 4

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	// {we, are, so, happy}
	// we
	s.Agghtab[0] = []uint8{uint8(2), uint8(2), uint8(2), uint8(2)} // we, are
	s.Agghtab[1] = []uint8{uint8(3), uint8(0), uint8(0), uint8(0)} // we

	// are
	s.Agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	s.Agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	s.Agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	s.Agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	s.Agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	// so
	s.Agghtab[31] = []uint8{uint8(0), uint8(0), uint8(0), uint8(2)}
	s.Agghtab[32] = []uint8{uint8(0), uint8(0), uint8(0), uint8(3)}
	s.Agghtab[33] = []uint8{uint8(0), uint8(0), uint8(0), uint8(4)}

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3
	s.Aggcnt[2] = 3
	s.Aggcnt[3] = 3

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	// {we, are, so, happy}
	// we
	s.Agghtab[0] = []uint8{uint8(2), uint8(2), uint8(0), uint8(0)} // we, are
	s.Agghtab[1] = []uint8{uint8(3), uint8(0), uint8(0), uint8(0)} // we

	// are
	s.Agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	s.Agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	s.Agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5), uint8(0)}
	s.Agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	s.Agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	s.Agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	// happy
	s.Agghtab[30] = []uint8{uint8(0), uint8(0), uint8(0), uint8(1)}
	s.Agghtab[31] = []uint8{uint8(0), uint8(0), uint8(0), uint8(2)}
	s.Agghtab[32] = []uint8{uint8(0), uint8(0), uint8(0), uint8(3)}
	s.Agghtab[33] = []uint8{uint8(0), uint8(0), uint8(0), uint8(4)}

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3
	s.Aggcnt[2] = 4
	s.Aggcnt[3] = 4

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	// {we, are, so, happy}
	// we
	s.Agghtab[0] = []uint8{uint8(2), uint8(0), uint8(0), uint8(0)} // we
	s.Agghtab[1] = []uint8{uint8(3), uint8(0), uint8(0), uint8(0)} // we

	// are
	s.Agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	s.Agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 2

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	// {we, are, so, happy}
	// we
	s.Agghtab[0] = []uint8{uint8(2), uint8(2), uint8(0), uint8(0)} // we, are
	s.Agghtab[1] = []uint8{uint8(3), uint8(0), uint8(5), uint8(0)} // we, so

	// are
	s.Agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	s.Agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	s.Agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5), uint8(0)}
	s.Agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	s.Agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	s.Agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3
	s.Aggcnt[2] = 6

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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

func TestFullTextGroupTilda(t *testing.T) {

	pattern := "+we ~(<are >so)"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	// {we, are, so, happy}
	// we
	s.Agghtab[0] = []uint8{uint8(2), uint8(2), uint8(0), uint8(0)} // we, are
	s.Agghtab[1] = []uint8{uint8(3), uint8(0), uint8(5), uint8(0)} // we, so

	// are
	s.Agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	s.Agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	s.Agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5), uint8(0)}
	s.Agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	s.Agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	s.Agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 3
	s.Aggcnt[2] = 6

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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

func TestFullTextStar(t *testing.T) {

	pattern := "apple*"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))
	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}
	for _, p := range s.Pattern {
		keywords, indexes = GetStarFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	// {apple*}
	// apple*
	s.Agghtab[0] = []uint8{uint8(2), uint8(2), uint8(0), uint8(0)} // we, are
	s.Agghtab[1] = []uint8{uint8(3), uint8(0), uint8(5), uint8(0)} // we, so

	s.Aggcnt[0] = 2

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	var keywords []string
	var indexes []int32
	word2idx := make(map[string]int32)

	for _, p := range s.Pattern {
		keywords, indexes = GetTextFromPattern(p, keywords, indexes)
	}

	//fmt.Println(keywords)
	//fmt.Println(indexes)

	for i, kw := range keywords {
		word2idx[kw] = indexes[i]
	}

	// {we, are, so, happy}
	// we
	s.Agghtab[0] = []uint8{uint8(2), uint8(2), uint8(2), uint8(2)} // we
	s.Agghtab[1] = []uint8{uint8(3), uint8(2), uint8(0), uint8(0)} // we

	// are
	s.Agghtab[10] = []uint8{uint8(0), uint8(2), uint8(0), uint8(0)} // are
	s.Agghtab[11] = []uint8{uint8(0), uint8(3), uint8(0), uint8(0)} // are
	s.Agghtab[12] = []uint8{uint8(0), uint8(4), uint8(0), uint8(0)} // are

	// so
	s.Agghtab[20] = []uint8{uint8(0), uint8(0), uint8(5), uint8(0)}
	s.Agghtab[21] = []uint8{uint8(0), uint8(0), uint8(6), uint8(0)}
	s.Agghtab[22] = []uint8{uint8(0), uint8(0), uint8(7), uint8(0)}
	s.Agghtab[23] = []uint8{uint8(0), uint8(0), uint8(8), uint8(0)}

	// so
	s.Agghtab[30] = []uint8{uint8(0), uint8(0), uint8(0), uint8(1)}
	s.Agghtab[31] = []uint8{uint8(0), uint8(0), uint8(0), uint8(2)}
	s.Agghtab[32] = []uint8{uint8(0), uint8(0), uint8(0), uint8(3)}
	s.Agghtab[33] = []uint8{uint8(0), uint8(0), uint8(0), uint8(4)}

	s.Aggcnt[0] = 2
	s.Aggcnt[1] = 5
	s.Aggcnt[2] = 5
	s.Aggcnt[3] = 5

	s.Nrow = 100

	test_result := make(map[any]float32, 13)
	// eval
	i := 0
	for key := range s.Agghtab {
		var result []float32
		docvec := s.Agghtab[key]
		//fmt.Printf("docvec %v %v\n", key, docvec)
		for _, p := range s.Pattern {
			result, err = p.Eval(s, docvec, s.Aggcnt, float32(1.0), result)
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
