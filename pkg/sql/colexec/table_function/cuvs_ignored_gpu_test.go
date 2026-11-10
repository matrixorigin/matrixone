//go:build gpu

// Copyright 2026 Matrix Origin
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

package table_function

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	cuvsfilter "github.com/matrixorigin/matrixone/pkg/cuvs/filter"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// recordingBuilder records the rows a cuvs build receives, starting a new sub-index every
// capacity rows as CagraBuild and IvfpqBuild do.
type recordingBuilder struct {
	capacity int
	subs     [][]int64
	filters  []string
}

func (b *recordingBuilder) AddRow(id int64, vecBytes []byte) error {
	if len(b.subs) == 0 || (b.capacity > 0 && len(b.subs[len(b.subs)-1]) >= b.capacity) {
		b.subs = append(b.subs, nil)
	}
	b.subs[len(b.subs)-1] = append(b.subs[len(b.subs)-1], id)
	return nil
}

func (b *recordingBuilder) SetFilterColumns(string) {}

func (b *recordingBuilder) AddFilterChunk(colIdx uint32, data []byte, nullBitmap []uint32, nrows uint64) error {
	b.filters = append(b.filters, fmt.Sprintf("%d:%d:%v", colIdx, int32(binary.LittleEndian.Uint32(data)), nullBitmap != nil))
	return nil
}

// feed routes the storable rows ids as the create table functions do.
func feed(t *testing.T, g *cuvsIgnoredRows, b *recordingBuilder, ids []int64) {
	for _, id := range ids {
		vec := []float32{float32(id), 1}
		skip, err := checkCuvsRow(g, id, vec)
		require.NoError(t, err)
		require.False(t, skip)
		require.NoError(t, g.route(b, nil, nil, 0, id, types.ArrayToBytes(vec)))
	}
}

func seq(from, to int64) []int64 {
	var ids []int64
	for i := from; i <= to; i++ {
		ids = append(ids, i)
	}
	return ids
}

func newTestCuvsIgnoredRows(t *testing.T, limit int64, capacity, threshold int64) *cuvsIgnoredRows {
	g := newCuvsIgnoredRows(vectorindex.IndexTableConfig{DbName: "db", IgnoredTable: "ign", MaxBadRecords: limit}, t.TempDir(), true)
	require.NotNil(t, g)
	g.setPlan(capacity, threshold)
	t.Cleanup(g.close)
	return g
}

func TestCuvsIgnoredRowsWithoutTable(t *testing.T) {
	require.Nil(t, newCuvsIgnoredRows(vectorindex.IndexTableConfig{}, "", true))
	// close of a nil tracker is a no-op
	(*cuvsIgnoredRows)(nil).close()
}

// Every sub-index the builder receives has at least threshold rows: a short last one is held and
// goes to the tail.
func TestCuvsIgnoredRowsHoldsShortSubIndex(t *testing.T) {
	// capacity 5, threshold 3: 12 rows make 5 + 5 + 2, and the last 2 go to the tail
	g := newTestCuvsIgnoredRows(t, 0, 5, 3)
	b := &recordingBuilder{capacity: 5}
	feed(t, g, b, seq(1, 12))
	require.Equal(t, [][]int64{seq(1, 5), seq(6, 10)}, b.subs)
	tail := make([]int64, 0, len(g.tail()))
	for _, r := range g.tail() {
		tail = append(tail, r.Pkid)
	}
	require.Equal(t, []int64{11, 12}, tail)

	// 13 rows make 5 + 5 + 3: the last sub-index reaches the threshold
	g = newTestCuvsIgnoredRows(t, 0, 5, 3)
	b = &recordingBuilder{capacity: 5}
	feed(t, g, b, seq(1, 13))
	require.Equal(t, [][]int64{seq(1, 5), seq(6, 10), seq(11, 13)}, b.subs)
	require.Empty(t, g.tail())
	// a flush releases the vectors it held
	require.Nil(t, g.held[:cap(g.held)][0].Vec)

	// capacity 0 is one sub-index
	g = newTestCuvsIgnoredRows(t, 0, 0, 3)
	b = &recordingBuilder{}
	feed(t, g, b, seq(1, 7))
	require.Equal(t, [][]int64{seq(1, 7)}, b.subs)

	// a threshold equal to the capacity flushes whole sub-indexes
	g = newTestCuvsIgnoredRows(t, 0, 3, 3)
	b = &recordingBuilder{capacity: 3}
	feed(t, g, b, seq(1, 7))
	require.Equal(t, [][]int64{seq(1, 3), seq(4, 6)}, b.subs)
	require.Len(t, g.tail(), 1)
}

func TestCuvsIgnoredRowsLimit(t *testing.T) {
	g := newTestCuvsIgnoredRows(t, 2, 0, 1)
	for _, id := range []int64{1, 2} {
		skip, err := checkCuvsRow(g, id, []float32{100000, 1})
		require.NoError(t, err)
		require.True(t, skip)
	}
	skip, err := checkCuvsRow(g, 3, []float32{1, 1})
	require.NoError(t, err)
	require.False(t, skip)
	// one row more than the limit fails the build
	_, err = checkCuvsRow(g, 4, []float32{1, 1e20})
	require.Error(t, err)
	code, _ := moerr.GetMoErrCode(err)
	require.Equal(t, moerr.ErrIndexTooManyBadRecords, code)
	require.NoError(t, g.allBad())

	var sqls []string
	require.NoError(t, g.sqls(vectorindex.IndexTableConfig{DbName: "db", IgnoredTable: "ign"}, 9, func(s string) error {
		sqls = append(sqls, s)
		return nil
	}))
	require.Equal(t, []string{fmt.Sprintf(
		"REPLACE INTO `db`.`ign` (`__mo_index_pk`, `__mo_index_code`, `__mo_index_reason`, `__mo_index_ts`) VALUES (1, %d, NULL, 9), (2, %d, NULL, 9)",
		moerr.ErrIndexVectorOutOfRange, moerr.ErrIndexVectorOutOfRange)}, sqls)
}

// A build that can store none of its rows fails within the limit.
func TestCuvsIgnoredRowsAllBad(t *testing.T) {
	g := newTestCuvsIgnoredRows(t, 10, 0, 1)
	for _, id := range []int64{1, 2} {
		skip, err := checkCuvsRow(g, id, []float32{-70000, 1})
		require.NoError(t, err)
		require.True(t, skip)
	}
	err := g.allBad()
	require.Error(t, err)
	code, _ := moerr.GetMoErrCode(err)
	require.Equal(t, moerr.ErrIndexAllRowsBad, code)
	require.True(t, strings.Contains(err.Error(), "-70000"), err.Error())
}

// Held rows give the builder their INCLUDE values, NULL included.
func TestAppendFilterRowFromInclude(t *testing.T) {
	cols := []cuvsfilter.ColumnMeta{{Name: "a", TypeOid: cuvsfilter.ColTypeInt32}, {Name: "b", TypeOid: cuvsfilter.ColTypeInt32}}
	inc := make([]byte, includeBytesPerRowFromCols(cols))
	binary.LittleEndian.PutUint32(inc[0:4], uint32(7))
	inc[8] = 1 << 1 // b is NULL
	b := &recordingBuilder{}
	require.NoError(t, appendFilterRowFromInclude(b, cols, inc))
	require.Equal(t, []string{"0:7:false", "1:0:true"}, b.filters)
	require.Error(t, appendFilterRowFromInclude(b, cols, inc[:4]))
	require.NoError(t, appendFilterRowFromInclude(b, nil, nil))
}
