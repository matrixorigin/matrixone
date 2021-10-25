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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type precommitBlockEvent struct {
	BaseEvent
	// CommonID of the committed block meta
	Id common.ID
}

func NewPrecommitBlockEvent(ctx *Context, id common.ID) *precommitBlockEvent {
	e := &precommitBlockEvent{
		Id: id,
	}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.PrecommitBlkMetaTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *precommitBlockEvent) Execute() error {
	return nil
}
