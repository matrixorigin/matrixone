package metadata

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBase(t *testing.T) {
	ts1 := NewTimeStamp()
	time.Sleep(time.Microsecond)
	assert.False(t, ts1.IsDeleted(NowMicro()))
	assert.True(t, ts1.IsCreated(NowMicro()))
	assert.True(t, ts1.Select(NowMicro()))
	assert.Nil(t, ts1.Delete(NowMicro()))
	assert.True(t, ts1.IsDeleted(NowMicro()))
	assert.False(t, ts1.Select(NowMicro()))
	assert.NotNil(t, ts1.Delete(NowMicro()))
	t.Log(ts1.String())
	//assert.Equal(t, )
}

func TestBoundState(t *testing.T) {
	bs := Standalone
	assert.Equal(t, Standalone, bs.GetBoundState())
	assert.Nil(t, bs.Attach())
	assert.NotNil(t, bs.Attach())
	assert.Nil(t, bs.Detach())
	assert.NotNil(t, bs.Detach())
}

func TestSequence(t *testing.T) {
	seq1 := Sequence{
		NextBlockID:     0,
		NextSegmentID:   0,
		NextPartitionID: 0,
		NextTableID:     0,
		NextIndexID:     0,
	}
	assert.Equal(t, uint64(1), seq1.GetBlockID())
	assert.Equal(t, uint64(1), seq1.GetIndexID())
	assert.Equal(t, uint64(1), seq1.GetPartitionID())
	assert.Equal(t, uint64(1), seq1.GetSegmentID())
	assert.Equal(t, uint64(1), seq1.GetTableID())
}


