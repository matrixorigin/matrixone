package operator

import (
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDispatchAndRemoveOperator(t *testing.T) {
	c := NewController()
	operator1 := &Operator{shardID: 1}
	operator2 := &Operator{shardID: 1}
	operator3 := &Operator{shardID: 2}

	c.Dispatch([]*Operator{operator1}, pb.LogState{}, pb.DNState{})
	assert.Equal(t, []*Operator{operator1}, c.operators[1])

	c.Dispatch([]*Operator{operator2}, pb.LogState{}, pb.DNState{})
	assert.Equal(t, []*Operator{operator1, operator2}, c.operators[1])

	c.Dispatch([]*Operator{operator3}, pb.LogState{}, pb.DNState{})
	assert.Equal(t, []*Operator{operator3}, c.operators[2])

	c.RemoveOperator(operator1)
	assert.Equal(t, []*Operator{operator2}, c.operators[1])

	c.RemoveOperator(operator2)
	assert.Equal(t, []*Operator(nil), c.operators[1])

	c.RemoveOperator(operator3)
	assert.Equal(t, []*Operator(nil), c.operators[2])
}

func TestRemoveFinishedOperator(t *testing.T) {
	c := NewController()
	op1 := NewOperator("", 1, 1, AddLogService{
		Target:    "a",
		StoreID:   "d",
		ShardID:   1,
		ReplicaID: 4,
	})
	logState := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {
			ShardID:  1,
			Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
			Epoch:    0,
		}},
	}

	c.Dispatch([]*Operator{op1}, logState, pb.DNState{})
	assert.Equal(t, []*Operator{op1}, c.GetOperators(1))

	logState = pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: {
			ShardID:  1,
			Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d"},
			Epoch:    0,
		}},
	}
	c.RemoveFinishedOperator(logState, pb.DNState{})
	assert.Equal(t, []*Operator(nil), c.GetOperators(1))
}
