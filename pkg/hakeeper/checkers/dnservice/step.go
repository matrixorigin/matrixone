package dnservice

// FIXME: placeholder
type Step interface {
	ShardID() uint64
	ReplicaID() uint64
	Target() StoreID
	Command() CommandType
}

// CommandType is a bit field to identify operator types.
type CommandType uint32

const (
	RemoveReplica CommandType = 1 << iota
	AddReplica
)

type dnStep struct {
	command   CommandType
	target    StoreID
	shardID   uint64
	replicaID uint64
}

func newLaunchStep(shardID, replicaID uint64, target StoreID) Step {
	return &dnStep{
		command:   AddReplica,
		target:    target,
		shardID:   shardID,
		replicaID: replicaID,
	}
}

func newStopStep(shardID, replicaID uint64, target StoreID) Step {
	return &dnStep{
		command:   RemoveReplica,
		target:    target,
		shardID:   shardID,
		replicaID: replicaID,
	}
}

func (s *dnStep) ShardID() uint64 {
	return s.shardID
}

func (s *dnStep) Target() StoreID {
	return s.target
}

func (s *dnStep) Command() CommandType {
	return s.command
}

func (s *dnStep) ReplicaID() uint64 {
	return s.replicaID
}
