package table_function

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestCase struct {
	pattern string
	expect  string
}

func PatternToString(pattern string, mode int64) (string, error) {
	ps, err := ParsePattern(pattern, mode)
	if err != nil {
		return "", err
	}

	var ss []string
	for _, p := range ps {
		ss = append(ss, p.String())
	}

	return strings.Join(ss, " "), nil
}

func TestPattern(t *testing.T) {

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
			expect:  "(text matrix) (+ (group (< (text origin)) (> (text one))))",
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
	}

	for _, c := range tests {
		result, err := PatternToString(c.pattern, int64(0))
		require.Nil(t, err)
		assert.Equal(t, c.expect, result)
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
		_, err := PatternToString(c.pattern, int64(0))
		require.NotNil(t, err)
	}
}

func TestCalcDocCount(t *testing.T) {

	pattern := "we aRe so Happy"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(s.SumDocCount)

	assert.Equal(t, s.SumDocCount["we"], int32(5))
	assert.Equal(t, s.SumDocCount["are"], int32(9))
	assert.Equal(t, s.SumDocCount["so"], int32(26))
	assert.Equal(t, s.SumDocCount["happy"], int32(10))
}

func TestFullTextOr(t *testing.T) {

	pattern := "apple banana"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}

func TestFullTextPlusPlus(t *testing.T) {

	pattern := "+apple +banana"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}

func TestFullTextPlusOr(t *testing.T) {

	pattern := "+apple banana"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}

func TestFullTextMinus(t *testing.T) {

	pattern := "+apple -banana"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}

func TestFullTextTilda(t *testing.T) {

	pattern := "+apple ~banana"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}

func TestFullText1(t *testing.T) {

	pattern := "we aRe so Happy"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}

func TestFullText2(t *testing.T) {

	pattern := "+we +aRe +so +Happy"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}

func TestFullText3(t *testing.T) {

	pattern := "+we -aRe -so -Happy"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}

func TestFullTextGroup(t *testing.T) {

	pattern := "+we +(<are >so)"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}

func TestFullTextStar(t *testing.T) {

	pattern := "apple*"
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}

func TestFullTextPhrase(t *testing.T) {

	pattern := "\"we aRe so Happy\""
	s, err := NewSearchAccum("index", pattern, 0, "")
	require.Nil(t, err)

	for _, p := range s.Pattern {
		fmt.Printf("%v", p)
	}

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

	fmt.Println(result)
}
