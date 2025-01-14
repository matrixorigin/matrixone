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

package logservicedriver

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SkipCmd(t *testing.T) {
	cmd := NewSkipCmd(3)
	skipMap := map[uint64]uint64{
		uint64(3): uint64(1),
		uint64(2): uint64(2),
		uint64(1): uint64(3),
	}
	i := 0
	for k, v := range skipMap {
		cmd.Set(i, k, v)
		i++
	}
	cmd.Sort()
	dsns := cmd.GetDSNSlice()
	psns := cmd.GetPSNSlice()

	assert.Equal(t, []uint64{1, 2, 3}, dsns)
	assert.Equal(t, []uint64{3, 2, 1}, psns)
}

func Test_LogEntry1(t *testing.T) {
	e := NewLogEntry()
	assert.Equal(t, EmptyLogEntrySize, len(e))
	assert.Equal(t, uint32(0), e.GetEntryCount())
	assert.Equal(t, uint64(0), e.GetStartDSN())
	assert.Equal(t, uint32(0), e.GetFooterOffset())

	footer := e.GetFooter()
	assert.Equal(t, uint32(0), footer.GetEntryCount())
	t.Log(footer.ShortString())
	t.Log(e.String())

	var entries [][]byte

	writer := NewLogEntryWriter()

	for i := 0; i < 10; i++ {
		entries = append(entries, []byte(fmt.Sprintf("entry %d", i)))
		writer.Append(entries[i])
	}
	dsn := uint64(100)
	e = writer.Finish(dsn)
	assert.Equal(t, uint32(10), e.GetEntryCount())
	assert.Equal(t, dsn, e.GetStartDSN())
	footer = e.GetFooter()
	assert.Equal(t, uint32(10), footer.GetEntryCount())

	t.Log(e.String())
	for i := 0; i < 10; i++ {
		assert.Equal(t, entries[i], e.GetEntry(i))
		t.Log(string(e.GetEntry(i)))
	}

	writer.Reset()

	for i := 0; i < 10; i++ {
		writer.Append(entries[i])
	}
	dsn = uint64(200)

	assert.False(t, writer.IsFinished())
	e = writer.Finish(dsn)
	assert.True(t, writer.IsFinished())

	assert.Equal(t, uint32(10), e.GetEntryCount())
	assert.Equal(t, dsn, e.GetStartDSN())
	footer = e.GetFooter()
	assert.Equal(t, uint32(10), footer.GetEntryCount())
	for i := 0; i < 10; i++ {
		assert.Equal(t, entries[i], e.GetEntry(i))
		t.Log(string(e.GetEntry(i)))
	}
	writer.Close()
}

// func Test_LogEntry2(t *testing.T) {
// 	e := NewLogEntry()
// }
