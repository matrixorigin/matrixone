package txnbase

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

const (
	MaxNodeRows uint32 = 10000
)

type NodeEntry entry.Entry

type NodeState = int32

const (
	TransientNode NodeState = iota
	PersistNode
)

type NodeType int8
type Node interface {
	base.INode
	Type() NodeType
	ToTransient()
	Close() error
}
