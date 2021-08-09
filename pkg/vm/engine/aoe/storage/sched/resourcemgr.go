package sched

import (
	"errors"
	// log "github.com/sirupsen/logrus"
)

var (
	ErrDuplicateResource = errors.New("aoe: duplicate resource")
)

type BaseResourceMgr struct {
	typeindex map[ResourceType][]int
	nameindex map[string]int
	resources []Resource
	handler   EventHandler
}

func NewBaseResourceMgr(handler EventHandler) *BaseResourceMgr {
	return &BaseResourceMgr{
		typeindex: make(map[ResourceType][]int),
		nameindex: make(map[string]int),
		resources: make([]Resource, 0),
		handler:   handler,
	}
}

func (mgr *BaseResourceMgr) Start() {
	for _, res := range mgr.resources {
		res.Start()
	}
	mgr.handler.Start()
}

func (mgr *BaseResourceMgr) Close() {
	mgr.handler.Close()
	for _, res := range mgr.resources {
		res.Close()
	}
}

func (mgr *BaseResourceMgr) Add(res Resource) error {
	if _, ok := mgr.nameindex[res.Name()]; ok {
		return ErrDuplicateResource
	}
	pos := len(mgr.resources)
	mgr.nameindex[res.Name()] = pos
	mgr.resources = append(mgr.resources, res)
	_, ok := mgr.typeindex[res.Type()]
	if !ok {
		mgr.typeindex[res.Type()] = make([]int, 0)
	}
	mgr.typeindex[res.Type()] = append(mgr.typeindex[res.Type()], pos)

	return nil
}

func (mgr *BaseResourceMgr) GetResource(name string) Resource {
	pos, ok := mgr.nameindex[name]
	if !ok {
		return nil
	}
	return mgr.resources[pos]
}

func (mgr *BaseResourceMgr) ResourceCount() int {
	return len(mgr.resources)
}

func (mgr *BaseResourceMgr) ResourceCountByType(t ResourceType) int {
	pool, ok := mgr.typeindex[t]
	if !ok {
		return 0
	}
	return len(pool)
}

func (mgr *BaseResourceMgr) ExecuteEvent(e Event) {
	mgr.handler.ExecuteEvent(e)
}

func (mgr *BaseResourceMgr) Enqueue(e Event) {
	mgr.handler.Enqueue(e)
}
