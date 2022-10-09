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

package entry

// import (
// 	"bytes"
// 	"testing"
// 	"time"

// 	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
// 	"github.com/stretchr/testify/assert"
// )

// func TestBase(t *testing.T) {
// 	now := time.Now()
// 	var buffer bytes.Buffer
// 	buffer.WriteString("helloworld")
// 	buffer.WriteString("helloworld")
// 	buffer.WriteString("helloworld")
// 	buf := buffer.Bytes()
// 	for i := 0; i < 100000; i++ {
// 		e := GetBase()
// 		e.SetType(ETFlush)
// 		n := common.LogAllocator.Alloc(30)
// 		copy(n.GetBuf(), buf)
// 		err := e.UnmarshalFromNode(n, true)
// 		assert.Nil(t, err)
// 		e.Free()
// 	}
// 	t.Logf("takes %s", time.Since(now))
// 	t.Log(common.LogAllocator.String())
// }

// func TestInfoMarshal1(t *testing.T) {
// 	info := &Info{
// 		Group: GTCKp,
// 		TxnId: 35,
// 		Checkpoints: []CkpRanges{
// 			{
// 				Group: GTNoop,
// 				Ranges: &common.ClosedIntervals{
// 					Intervals: []*common.ClosedInterval{{Start: 1, End: 5}, {Start: 7, End: 7}}}},
// 			{
// 				Group: GTUncommit,
// 				Ranges: &common.ClosedIntervals{
// 					Intervals: []*common.ClosedInterval{{Start: 3, End: 5}, {Start: 6, End: 7}}},
// 				Command: map[uint64]CommandInfo{3: {
// 					CommandIds: []uint32{2, 4, 1},
// 					Size:       5,
// 				}},
// 			}},
// 		Uncommits: 5,
// 		GroupLSN:  1,
// 	}
// 	buf := info.Marshal()
// 	info2 := Unmarshal(buf)
// 	checkInfoEqual(t, info, info2)
// }
// func TestInfoMarshal2(t *testing.T) {
// 	info := &Info{}
// 	buf := info.Marshal()
// 	info2 := Unmarshal(buf)
// 	checkInfoEqual(t, info, info2)
// }

// func TestInfoMarshal3(t *testing.T) {
// 	info := &Info{
// 		Group: GTCKp,
// 		TxnId: 35,
// 		Checkpoints: []CkpRanges{
// 			{
// 				Group: GTNoop,
// 				Ranges: &common.ClosedIntervals{
// 					Intervals: []*common.ClosedInterval{{Start: 1, End: 5}, {Start: 7, End: 7}}}},
// 			{
// 				Group: GTUncommit}},
// 		// Ranges: &common.ClosedIntervals{}}},
// 		Uncommits: 5,
// 		GroupLSN:  1,
// 	}
// 	buf := info.Marshal()
// 	info2 := Unmarshal(buf)
// 	checkInfoEqual(t, info, info2)
// }

// func checkInfoEqual(t *testing.T, info, info2 *Info) {
// 	assert.Equal(t, info.Group, info2.Group)
// 	assert.Equal(t, info.TxnId, info2.TxnId)
// 	assert.Equal(t, len(info.Checkpoints), len(info2.Checkpoints))
// 	for i, ckps1 := range info.Checkpoints {
// 		ckps2 := info2.Checkpoints[i]
// 		assert.Equal(t, ckps1.Group, ckps2.Group)
// 		if ckps1.Ranges == nil {
// 			assert.Equal(t, len(ckps2.Ranges.Intervals), 0)
// 		} else {
// 			assert.Equal(t, len(ckps1.Ranges.Intervals), len(ckps2.Ranges.Intervals))
// 			for j, interval1 := range ckps1.Ranges.Intervals {
// 				interval2 := ckps2.Ranges.Intervals[j]
// 				assert.Equal(t, interval1.Start, interval2.Start)
// 				assert.Equal(t, interval1.End, interval2.End)
// 			}
// 		}
// 		assert.Equal(t, len(ckps1.Command), len(ckps2.Command))
// 		for lsn, cmdInfo := range ckps1.Command {
// 			cmdInfo2 := ckps2.Command[lsn]
// 			assert.Equal(t, len(cmdInfo.CommandIds), len(cmdInfo2.CommandIds))
// 			for k, cmdid := range cmdInfo.CommandIds {
// 				assert.Equal(t, cmdid, cmdInfo2.CommandIds[k])
// 			}
// 			assert.Equal(t, cmdInfo.Size, cmdInfo2.Size)
// 		}
// 	}
// 	assert.Equal(t, info.Uncommits, info2.Uncommits)
// 	assert.Equal(t, info.GroupLSN, info2.GroupLSN)
// }
