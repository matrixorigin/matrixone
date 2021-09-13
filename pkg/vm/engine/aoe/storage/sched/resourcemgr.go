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

func (mgr *BaseResourceMgr) Close() error {
	mgr.handler.Close()
	for _, res := range mgr.resources {
		res.Close()
	}
	return nil
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
