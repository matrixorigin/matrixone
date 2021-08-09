package sched

import "errors"

var (
	ErrDuplicateResource = errors.New("aoe: duplicate resource")
)

type BaseResourceMgr struct {
	typeindex map[ResourceType][]int
	nameindex map[string]int
	resources []Resource
	scheduler Scheduler
}

func NewBaseResourceMgr(scheduler Scheduler) *BaseResourceMgr {
	return &BaseResourceMgr{
		typeindex: make(map[ResourceType][]int),
		nameindex: make(map[string]int),
		resources: make([]Resource, 0),
		scheduler: scheduler,
	}
}

func (mgr *BaseResourceMgr) Start() {
	if mgr.scheduler != nil {
		mgr.scheduler.Start()
	}
	// for _, res := range mgr.resources {
	// 	res.Start()
	// }
}

func (mgr *BaseResourceMgr) Stop() {
	if mgr.scheduler != nil {
		mgr.scheduler.Stop()
	}
	// for _, res := range mgr.resources {
	// 	res.Stop()
	// }
}

func (mgr *BaseResourceMgr) Add(res Resource) error {
	if _, ok := mgr.nameindex[res.Name()]; ok {
		return ErrDuplicateResource
	}
	pos := len(mgr.resources)
	mgr.nameindex[res.Name()] = pos
	mgr.resources = append(mgr.resources, res)
	indices, ok := mgr.typeindex[res.Type()]
	if !ok {
		indices = make([]int, 0)
		mgr.typeindex[res.Type()] = indices
	}
	indices = append(indices, pos)
	return nil
}

func (mgr *BaseResourceMgr) GetResource(name string) Resource {
	pos, ok := mgr.nameindex[name]
	if !ok {
		return nil
	}
	return mgr.resources[pos]
}
