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
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type MetaEvent interface {
	sched.Event
	GetScope() (common.ID, bool)
}

type metaEvent struct {
	sched.BaseEvent
	scope    common.ID
	scopeAll bool
}

func NewMetaEvent(scope common.ID, scopeAll bool, t sched.EventType, waitable bool) *metaEvent {
	e := &metaEvent{
		scope:    scope,
		scopeAll: scopeAll,
	}
	e.BaseEvent = *sched.NewBaseEvent(e, t, nil, waitable)
	return e
}

func (e *metaEvent) GetScope() (common.ID, bool) {
	return e.scope, e.scopeAll
}
