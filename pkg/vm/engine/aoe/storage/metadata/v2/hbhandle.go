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

package metadata

import (
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/logstore"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	ob "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"sync/atomic"
)

var (
	DefaultCheckpointDelta uint64 = 10000
)

type checkpointOp struct {
	ops.Op
	handle *hbHandle
}

func newCheckpointOp(handle *hbHandle, waitable bool) *checkpointOp {
	op := &checkpointOp{
		handle: handle,
	}
	op.Op = ops.Op{
		Impl:   op,
		Worker: handle.catalog.archiver,
	}
	if !waitable {
		op.DoneCB = op.onDone
	} else {
		op.ErrorC = make(chan error)
	}
	return op
}

func (op *checkpointOp) onDone(iop ob.IOp) {
	if err := iop.GetError(); err != nil {
		logutil.Warn(err.Error())
	}
	logutil.Infof("Checkpointed %d", op.handle.catalog.GetCheckpointId())
	op.handle.onCheckpointDone()
}

func (op *checkpointOp) Execute() error {
	if err := op.handle.catalog.Checkpoint(); err != nil {
		return err
	}
	op.handle.catalog.Store.TryCompact()
	return nil
}

type hbHandle struct {
	catalog   *Catalog
	delta     uint64
	scheduled int32
}

func (h *hbHandle) OnExec() {
	if atomic.LoadInt32(&h.scheduled) == 0 {
		ckId := h.catalog.GetCheckpointId()
		cId := h.catalog.GetSafeCommitId()
		if cId > ckId && cId-ckId >= h.delta {
			atomic.AddInt32(&h.scheduled, int32(1))
			op := newCheckpointOp(h, false)
			op.Push()
		}
	}
	if err := h.catalog.Store.Sync(); err != nil {
		logutil.Warn(err.Error())
	}
}

func (h *hbHandle) onCheckpointDone() {
	v := atomic.AddInt32(&h.scheduled, int32(-1))
	if v < 0 {
		panic("logic error")
	}
}

func (h *hbHandle) OnStopped() {
	op := newCheckpointOp(h, true)
	op.Push()
	op.WaitDone()
	logutil.Infof("Last synced id: %d ", h.catalog.Store.GetSyncedId())
}

type hbHandleFactory struct {
	catalog *Catalog
}

func (factory *hbHandleFactory) builder(_ logstore.BufferedStore) base.IHBHandle {
	return &hbHandle{
		delta:   DefaultCheckpointDelta,
		catalog: factory.catalog,
	}
}
