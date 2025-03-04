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

func TestFullTextNLBM25(t *testing.T) {

	pattern := "apple banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_NL), "", ALGO_BM25)
	require.Nil(t, err)

	sql, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_NL), "indextbl", "", ALGO_BM25)
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

func TestFullTextOrBM25(t *testing.T) {

	pattern := "apple banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullTextPlusPlusBM25(t *testing.T) {

	pattern := "+apple -orange"
	//pattern := "+apple +banana -orange"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullTextPlusOrBM25(t *testing.T) {

	pattern := "+apple banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullTextMinusBM25(t *testing.T) {

	pattern := "+apple -banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullTextTildaBM25(t *testing.T) {

	pattern := "+apple ~banana"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullText1BM25(t *testing.T) {

	pattern := "we aRe so Happy"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullText2BM25(t *testing.T) {

	pattern := "+we +aRe +so +Happy"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullText3BM25(t *testing.T) {

	pattern := "+we -aRe -so -Happy"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullText5BM25(t *testing.T) {

	pattern := "we aRe so +Happy"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullTextGroupBM25(t *testing.T) {

	pattern := "+we +(<are >so)"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullTextJoinGroupTildaBM25(t *testing.T) {

	pattern := "+we +also ~(<are >so)"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullTextGroupTildaBM25(t *testing.T) {

	pattern := "+we ~(<are >so)"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullTextStarBM25(t *testing.T) {

	pattern := "apple*"
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
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

func TestFullTextPhraseBM25(t *testing.T) {

	pattern := "\"we aRe so Happy\""
	s, err := NewSearchAccum("src", "index", pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
	require.Nil(t, err)

	sql, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_BOOLEAN), "idxtbl", "", ALGO_BM25)
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
