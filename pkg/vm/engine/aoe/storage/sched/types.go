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
	"io"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops/base"
	// log "github.com/sirupsen/logrus"
)

type IDAllocFunc func() uint64

type Scheduler interface {
	Start()
	Stop()
	Schedule(Event) error
}

type Dispatcher interface {
	io.Closer
	Dispatch(Event)
}

type Event interface {
	iops.IOp
	AttachID(uint64)
	ID() uint64
	Type() EventType
	Cancel() error
}

type EventHandler interface {
	Start()
	io.Closer
	Enqueue(Event)
	ExecuteEvent(Event)
}

type ResourceMgr interface {
	EventHandler
	GetResource(name string) Resource
	ResourceCount() int
	ResourceCountByType(ResourceType) int
	Add(Resource) error
}

type Resource interface {
	EventHandler
	Type() ResourceType
	Name() string
}
