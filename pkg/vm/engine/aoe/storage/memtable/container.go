package memtable

import (
	"errors"
	"fmt"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	"sync"
	// log "github.com/sirupsen/logrus"
)

type Container interface {
	GetCapacity() uint64
	Allocate() error
	Pin() error
	Unpin()
	Close() error
	IsPined() bool
}

type StaticContainer struct {
	sync.RWMutex
	BufMgr  bmgrif.IBufferManager
	BaseID  layout.ID
	Nodes   map[layout.ID]nif.INodeHandle
	Handles map[layout.ID]nif.IBufferHandle
	Pined   bool
	Impl    Container
}

type DynamicContainer struct {
	// Container
	StaticContainer
	StepSize uint64
}

func NewDynamicContainer(bmgr bmgrif.IBufferManager, id layout.ID, step uint64) Container {
	con := &DynamicContainer{
		StepSize: step,
		StaticContainer: StaticContainer{
			BufMgr:  bmgr,
			BaseID:  id,
			Nodes:   make(map[layout.ID]nif.INodeHandle),
			Handles: make(map[layout.ID]nif.IBufferHandle),
			Pined:   true,
		},
	}
	con.Impl = con
	return con
}

func (con *StaticContainer) Allocate() error {
	if con.Impl != nil {
		return con.Impl.Allocate()
	}
	return errors.New("not supported")
}

func (con *StaticContainer) IsPined() bool {
	con.RLock()
	defer con.RUnlock()
	return con.Pined
}

func (con *StaticContainer) GetCapacity() uint64 {
	con.RLock()
	defer con.RUnlock()
	ret := uint64(0)
	for _, n := range con.Nodes {
		ret += n.GetCapacity()
	}
	return ret
}

func (con *StaticContainer) Pin() error {
	con.Lock()
	defer con.Unlock()
	if con.Pined {
		return nil
	}
	for id, n := range con.Nodes {
		h := con.BufMgr.Pin(n)
		if h == nil {
			con.Handles = make(map[layout.ID]nif.IBufferHandle)
			return errors.New(fmt.Sprintf("Cannot pin node %v", id))
		}
		con.Handles[id] = h
	}
	con.Pined = true
	return nil
}

func (con *StaticContainer) Unpin() {
	con.Lock()
	defer con.Unlock()
	if !con.Pined {
		return
	}
	for _, h := range con.Handles {
		err := h.Close()
		if err != nil {
			panic(fmt.Sprintf("logic error: %v", err))
		}
	}
	con.Handles = make(map[layout.ID]nif.IBufferHandle)
	con.Pined = false
	return
}

func (con *StaticContainer) Close() error {
	con.Lock()
	defer con.Unlock()
	for _, h := range con.Handles {
		h.Close()
	}
	con.Handles = make(map[layout.ID]nif.IBufferHandle)
	for _, n := range con.Nodes {
		n.Close()
	}
	con.Nodes = make(map[layout.ID]nif.INodeHandle)
	return nil
}

func (con *DynamicContainer) Allocate() error {
	con.Lock()
	defer con.Unlock()
	if !con.Pined {
		panic("logic error")
	}
	id := con.BaseID
	id.PartID = uint32(len(con.Nodes))
	node := con.BufMgr.RegisterSpillableNode(con.StepSize, id)
	if node == nil {
		return errors.New(fmt.Sprintf("Cannot allocate %d from buffer manager", con.StepSize))
	}
	handle := con.BufMgr.Pin(node)
	if handle == nil {
		node.Close()
		return errors.New(fmt.Sprintf("Cannot pin node %v", id))
	}
	con.Nodes[id] = node
	con.Handles[id] = handle
	return nil
}
