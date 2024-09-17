package table_function

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
	var ss []string
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

func TestPatternBoolean(t *testing.T) {

	tests := []TestCase{
		TestCase{
			pattern: "Matrix Origin",
			expect:  "(text matrix) (text origin)",
		},
		TestCase{
			pattern: "+Matrix Origin",
			expect:  "(+ (text matrix)) (text origin)",
		},
		TestCase{
			pattern: "+Matrix -Origin",
			expect:  "(+ (text matrix)) (- (text origin))",
		},
		TestCase{
			pattern: "Matrix ~Origin",
			expect:  "(text matrix) (~ (text origin))",
		},
		TestCase{
			pattern: "Matrix +(<Origin >One)",
			expect:  "(+ (group (< (text origin)) (> (text one)))) (text matrix)",
		},
		TestCase{
			pattern: "+Matrix +Origin",
			expect:  "(+ (text matrix)) (+ (text origin))",
		},
		TestCase{
			pattern: "\"Matrix origin\"",
			expect:  "(phrase (text matrix) (text origin))",
		},
		TestCase{
			pattern: "Matrix Origin*",
			expect:  "(text matrix) (* origin*)",
		},
		TestCase{
			pattern: "+Matrix +(Origin (One Two))",
			expect:  "(+ (text matrix)) (+ (group (text origin) (group (text one) (text two))))",
		},
		TestCase{
			pattern: "+读写汉字 -学中文",
			expect:  "(+ (text 读写汉字)) (- (text 学中文))",
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
		TestCase{
			pattern: "Matrix Origin",
			expect:  "(text matrix) (text origin)",
		},
		TestCase{
			pattern: "读写汉字 学中文",
			expect:  "(text 读写汉) (text 写汉字) (text 汉字) (text 字) (text 学中文) (text 中文) (text 文)",
		},
		TestCase{
			pattern: "读写",
			expect:  "(* 读写*)",
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
		TestCase{
			pattern: "Matrix Origin",
			expect:  "(text matrix) (text origin)",
		},
		TestCase{
			pattern: "读写汉字 学中文",
			expect:  "(+ (text 读写汉字)) (- (text 学中文))",
		},
	}

	for _, c := range tests {
		_, err := PatternToString(c.pattern, int64(tree.FULLTEXT_QUERY_EXPANSION))
		require.NotNil(t, err)
	}
}

func TestPatternFail(t *testing.T) {

	tests := []TestCase{
		TestCase{
			pattern: "Matrix Origin( ",
		},
		TestCase{
			pattern: "(+Matrix Origin",
		},
		TestCase{
			pattern: "++Matrix -Origin",
		},
		TestCase{
			pattern: "Matrix ~~Origin",
		},
		TestCase{
			pattern: "Matrix +(<(+Origin -apple) >One)",
		},
		TestCase{
			pattern: "+Matrix --Origin",
		},
	}

	for _, c := range tests {
		_, err := PatternToString(c.pattern, int64(tree.FULLTEXT_BOOLEAN))
		require.NotNil(t, err)
	}
}

func TestCalcDocCount(t *testing.T) {

	pattern := "we aRe so Happy"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_NL), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "we"
	word := "we"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "are"
	word = "are"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[10] = &Word{DocId: 10, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	// init the word "so"
	word = "so"
	s.WordAccums[word] = &WordAccum{Id: 2, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[20] = &Word{DocId: 20, Position: []int64{0, 4, 6}, DocCount: 5}
	s.WordAccums[word].Words[21] = &Word{DocId: 21, Position: []int64{0, 4, 6}, DocCount: 6}
	s.WordAccums[word].Words[22] = &Word{DocId: 22, Position: []int64{0, 4, 6}, DocCount: 7}
	s.WordAccums[word].Words[23] = &Word{DocId: 23, Position: []int64{0, 4, 6}, DocCount: 8}

	// init the word "happy"
	word = "happy"
	s.WordAccums[word] = &WordAccum{Id: 3, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[30] = &Word{DocId: 30, Position: []int64{0, 4, 6}, DocCount: 1}
	s.WordAccums[word].Words[31] = &Word{DocId: 31, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[32] = &Word{DocId: 32, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[33] = &Word{DocId: 33, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	//fmt.Println(s.SumDocCount)

	assert.Equal(t, s.SumDocCount["we"], int32(5))
	assert.Equal(t, s.SumDocCount["are"], int32(9))
	assert.Equal(t, s.SumDocCount["so"], int32(26))
	assert.Equal(t, s.SumDocCount["happy"], int32(10))
}

func TestFullTextOr(t *testing.T) {

	pattern := "apple banana"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_NL), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "apple"
	word := "apple"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "banana"
	word = "banana"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[0]
	assert.Equal(t, ok, true)
	_, ok = result[1]
	assert.Equal(t, ok, true)
	_, ok = result[11]
	assert.Equal(t, ok, true)
	_, ok = result[12]
	assert.Equal(t, ok, true)

}

func TestFullTextPlusPlus(t *testing.T) {

	pattern := "+apple +banana"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "apple"
	word := "apple"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "banana"
	word = "banana"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[0]
	assert.Equal(t, ok, true)
}

func TestFullTextPlusOr(t *testing.T) {

	pattern := "+apple banana"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "apple"
	word := "apple"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "banana"
	word = "banana"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[0]
	assert.Equal(t, ok, true)
	_, ok = result[1]
	assert.Equal(t, ok, true)
}

func TestFullTextMinus(t *testing.T) {

	pattern := "-banana +apple"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "apple"
	word := "apple"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "banana"
	word = "banana"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[1]
	assert.Equal(t, ok, true)
}

func TestFullTextTilda(t *testing.T) {

	pattern := "+apple ~banana"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "apple"
	word := "apple"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "banana"
	word = "banana"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[0]
	assert.Equal(t, ok, true)
	_, ok = result[1]
	assert.Equal(t, ok, true)
}

func TestFullText1(t *testing.T) {

	pattern := "we aRe so Happy"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "we"
	word := "we"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "are"
	word = "are"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[10] = &Word{DocId: 10, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	// init the word "so"
	word = "so"
	s.WordAccums[word] = &WordAccum{Id: 2, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[20] = &Word{DocId: 20, Position: []int64{0, 4, 6}, DocCount: 5}
	s.WordAccums[word].Words[21] = &Word{DocId: 21, Position: []int64{0, 4, 6}, DocCount: 6}
	s.WordAccums[word].Words[22] = &Word{DocId: 22, Position: []int64{0, 4, 6}, DocCount: 7}
	s.WordAccums[word].Words[23] = &Word{DocId: 23, Position: []int64{0, 4, 6}, DocCount: 8}

	// init the word "happy"
	word = "happy"
	s.WordAccums[word] = &WordAccum{Id: 3, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[30] = &Word{DocId: 30, Position: []int64{0, 4, 6}, DocCount: 1}
	s.WordAccums[word].Words[31] = &Word{DocId: 31, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[32] = &Word{DocId: 32, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[33] = &Word{DocId: 33, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	ids := []int{0, 1, 10, 11, 12, 20, 21, 22, 23, 30, 31, 32, 33}

	for _, id := range ids {
		_, ok = result[id]
		assert.Equal(t, ok, true)
	}

}

func TestFullText2(t *testing.T) {

	pattern := "+we +aRe +so +Happy"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "we"
	word := "we"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "are"
	word = "are"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	// init the word "so"
	word = "so"
	s.WordAccums[word] = &WordAccum{Id: 2, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[21] = &Word{DocId: 21, Position: []int64{0, 4, 6}, DocCount: 6}
	s.WordAccums[word].Words[22] = &Word{DocId: 22, Position: []int64{0, 4, 6}, DocCount: 7}
	s.WordAccums[word].Words[23] = &Word{DocId: 23, Position: []int64{0, 4, 6}, DocCount: 8}

	// init the word "happy"
	word = "happy"
	s.WordAccums[word] = &WordAccum{Id: 3, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[31] = &Word{DocId: 31, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[32] = &Word{DocId: 32, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[33] = &Word{DocId: 33, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[0]
	assert.Equal(t, ok, true)
}

func TestFullText3(t *testing.T) {

	pattern := "+we -aRe -so -Happy"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "we"
	word := "we"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "are"
	word = "are"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	// init the word "so"
	word = "so"
	s.WordAccums[word] = &WordAccum{Id: 2, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[20] = &Word{DocId: 20, Position: []int64{0, 4, 6}, DocCount: 5}
	s.WordAccums[word].Words[21] = &Word{DocId: 21, Position: []int64{0, 4, 6}, DocCount: 6}
	s.WordAccums[word].Words[22] = &Word{DocId: 22, Position: []int64{0, 4, 6}, DocCount: 7}
	s.WordAccums[word].Words[23] = &Word{DocId: 23, Position: []int64{0, 4, 6}, DocCount: 8}

	// init the word "happy"
	word = "happy"
	s.WordAccums[word] = &WordAccum{Id: 3, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[30] = &Word{DocId: 30, Position: []int64{0, 4, 6}, DocCount: 1}
	s.WordAccums[word].Words[31] = &Word{DocId: 31, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[32] = &Word{DocId: 32, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[33] = &Word{DocId: 33, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[1]
	assert.Equal(t, ok, true)
}

func TestFullText4(t *testing.T) {

	pattern := "we -aRe so Happy"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// no words found

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	assert.Equal(t, len(result), int(0))
}

func TestFullText5(t *testing.T) {

	pattern := "we aRe so +Happy"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "we"
	word := "we"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "are"
	word = "are"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[10] = &Word{DocId: 10, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[0]
	assert.Equal(t, ok, true)
	_, ok = result[1]
	assert.Equal(t, ok, true)
}

func TestFullTextGroup(t *testing.T) {

	pattern := "+we +(<are >so)"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "we"
	word := "we"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "are"
	word = "are"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[11] = &Word{DocId: 11, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	// init the word "so"
	word = "so"
	s.WordAccums[word] = &WordAccum{Id: 2, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 5}
	s.WordAccums[word].Words[21] = &Word{DocId: 21, Position: []int64{0, 4, 6}, DocCount: 6}
	s.WordAccums[word].Words[22] = &Word{DocId: 22, Position: []int64{0, 4, 6}, DocCount: 7}
	s.WordAccums[word].Words[23] = &Word{DocId: 23, Position: []int64{0, 4, 6}, DocCount: 8}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[0]
	assert.Equal(t, ok, true)
	_, ok = result[1]
	assert.Equal(t, ok, true)
}

func TestFullTextStar(t *testing.T) {

	pattern := "apple*"
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "apple"
	word := "apple*"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[0]
	assert.Equal(t, ok, true)
	_, ok = result[1]
	assert.Equal(t, ok, true)
}

func TestFullTextPhrase(t *testing.T) {

	pattern := "\"we aRe so Happy\""
	s, err := NewSearchAccum("index", pattern, int64(tree.FULLTEXT_BOOLEAN), "")
	require.Nil(t, err)

	//fmt.Println(PatternListToString(s.Pattern))

	// pretend adding records from database
	// init the word "we"
	word := "we"
	s.WordAccums[word] = &WordAccum{Id: 0, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}

	// init the word "are"
	word = "are"
	s.WordAccums[word] = &WordAccum{Id: 1, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[12] = &Word{DocId: 12, Position: []int64{0, 4, 6}, DocCount: 4}

	// init the word "so"
	word = "so"
	s.WordAccums[word] = &WordAccum{Id: 2, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 5}
	s.WordAccums[word].Words[1] = &Word{DocId: 1, Position: []int64{0, 4, 6}, DocCount: 6}
	s.WordAccums[word].Words[22] = &Word{DocId: 22, Position: []int64{0, 4, 6}, DocCount: 7}
	s.WordAccums[word].Words[23] = &Word{DocId: 23, Position: []int64{0, 4, 6}, DocCount: 8}

	// init the word "happy"
	word = "happy"
	s.WordAccums[word] = &WordAccum{Id: 3, Mode: 0, Words: make(map[any]*Word)}
	s.WordAccums[word].Words[0] = &Word{DocId: 0, Position: []int64{0, 4, 6}, DocCount: 1}
	s.WordAccums[word].Words[31] = &Word{DocId: 31, Position: []int64{0, 4, 6}, DocCount: 2}
	s.WordAccums[word].Words[32] = &Word{DocId: 32, Position: []int64{0, 4, 6}, DocCount: 3}
	s.WordAccums[word].Words[33] = &Word{DocId: 33, Position: []int64{0, 4, 6}, DocCount: 4}

	s.Nrow = 100
	s.calculateDocCount()

	// eval
	var result map[any]float32
	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		require.Nil(t, err)
	}

	var ok bool
	_, ok = result[0]
	assert.Equal(t, ok, true)
}
