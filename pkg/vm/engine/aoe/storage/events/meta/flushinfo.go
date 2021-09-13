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

package meta

import (
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type flushInfoEvent struct {
	dbsched.BaseEvent
	Info *md.MetaInfo
}

func NewFlushInfoEvent(ctx *dbsched.Context, info *md.MetaInfo) *flushInfoEvent {
	e := new(flushInfoEvent)
	e.Info = info
	e.BaseEvent = dbsched.BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.StatelessEvent, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *flushInfoEvent) Execute() (err error) {
	ck := e.Ctx.Opts.Meta.CKFactory.Create()
	err = ck.PreCommit(e.Info)
	if err != nil {
		return err
	}
	err = ck.Commit(e.Info)
	if err != nil {
		return err
	}
	e.Ctx.Opts.Meta.Info.UpdateCheckpointTime(e.Info.CkpTime)

	return err
}
