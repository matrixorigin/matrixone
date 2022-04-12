package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testGenerator struct {
	rows []int
	idx  int
}

func (g *testGenerator) HasNext() bool {
	return g.idx < len(g.rows)
}

func (g *testGenerator) Next() uint32 {
	row := g.rows[g.idx]
	g.idx++
	return uint32(row)
}

func TestDeleteRows(t *testing.T) {
	cnt := 10000000
	rows := make([]int, 0)
	deleteCnt := 1
	for i := 0; i < deleteCnt; i++ {
		rows = append(rows, cnt/2-i*2-1)
	}
	generator := new(testGenerator)
	generator.rows = rows

	buf := make([]int32, cnt)
	for i := range buf {
		buf[i] = int32(i)
	}

	now := time.Now()
	buf = InplaceDeleteRows(buf, generator).([]int32)
	t.Log(time.Since(now))
	t.Log(len(buf))
	assert.Equal(t, cnt, deleteCnt+len(buf))
}
