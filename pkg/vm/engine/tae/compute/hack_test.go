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

package compute

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
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
	defer testutils.AfterTest(t)()
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
