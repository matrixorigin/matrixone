package entry

import (
	"bytes"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
)

func TestBase(t *testing.T) {
	now := time.Now()
	var buffer bytes.Buffer
	buffer.WriteString("helloworld")
	buffer.WriteString("helloworld")
	buffer.WriteString("helloworld")
	buf := buffer.Bytes()
	for i := 0; i < 100000; i++ {
		e := GetBase()
		e.SetType(ETFlush)
		n := common.GPool.Alloc(30)
		copy(n.GetBuf(), buf)
		e.UnmarshalFromNode(n, true)
		e.Free()
	}
	t.Logf("takes %s", time.Since(now))
	t.Log(common.GPool.String())
}

func TestInfoMarshal1(t *testing.T) {
	info := &Info{
		Group:    GTCKp,
		CommitId: 20,
		TxnId:    35,
		Checkpoints: []CkpRanges{
			{
				Group: GTNoop,
				Ranges: &common.ClosedIntervals{
					Intervals: []*common.ClosedInterval{{Start: 1, End: 5}, {Start: 7, End: 7}}}},
			{
				Group: GTUncommit,
				Ranges: &common.ClosedIntervals{
					Intervals: []*common.ClosedInterval{{Start: 3, End: 5}, {Start: 6, End: 7}}}}},
		Uncommits: []Tid{{
			Group: GTCKp,
			Tid:   12,
		}, {
			Group: GTInvalid,
			Tid:   25,
		}},
		GroupLSN: 1,
	}
	buf := info.Marshal()
	info2 := Unmarshal(buf)
	checkInfoEqual(t, info, info2)
}
func TestInfoMarshal2(t *testing.T) {
	info := &Info{}
	buf := info.Marshal()
	info2 := Unmarshal(buf)
	checkInfoEqual(t, info, info2)
}

func TestInfoMarshal3(t *testing.T) {
	info := &Info{
		Group:    GTCKp,
		CommitId: 20,
		TxnId:    35,
		Checkpoints: []CkpRanges{
			{
				Group: GTNoop,
				Ranges: &common.ClosedIntervals{
					Intervals: []*common.ClosedInterval{{Start: 1, End: 5}, {Start: 7, End: 7}}}},
			{
				Group: GTUncommit}},
		// Ranges: &common.ClosedIntervals{}}},
		Uncommits: []Tid{{
			Group: GTCKp,
			Tid:   12,
		}, {
			Group: GTInvalid,
			Tid:   25,
		}},
		GroupLSN: 1,
	}
	buf := info.Marshal()
	info2 := Unmarshal(buf)
	checkInfoEqual(t, info, info2)
}

func checkInfoEqual(t *testing.T, info, info2 *Info) {
	assert.Equal(t, info.Group, info2.Group)
	assert.Equal(t, info.CommitId, info2.CommitId)
	assert.Equal(t, info.TxnId, info2.TxnId)
	assert.Equal(t, info.GroupLSN, info2.GroupLSN)
	assert.Equal(t, len(info.Checkpoints), len(info2.Checkpoints))
	assert.Equal(t, len(info.Uncommits), len(info2.Uncommits))
	for i, ckps1 := range info.Checkpoints {
		ckps2 := info2.Checkpoints[i]
		assert.Equal(t, ckps1.Group, ckps2.Group)
		if ckps1.Ranges == nil {
			assert.Equal(t, len(ckps2.Ranges.Intervals), 0)
		} else {
			assert.Equal(t, len(ckps1.Ranges.Intervals), len(ckps2.Ranges.Intervals))
			for j, interval1 := range ckps1.Ranges.Intervals {
				interval2 := ckps2.Ranges.Intervals[j]
				assert.Equal(t, interval1.Start, interval2.Start)
				assert.Equal(t, interval1.End, interval2.End)
			}
		}
	}
	for i, tid1 := range info.Uncommits {
		tid2 := info2.Uncommits[i]
		assert.Equal(t, tid1.Group, tid2.Group)
		assert.Equal(t, tid1.Tid, tid2.Tid)
	}
}
