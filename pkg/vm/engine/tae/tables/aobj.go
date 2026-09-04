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

package tables

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type aobject struct {
	*baseObject
	frozen     atomic.Bool
	freezelock sync.Mutex
	reserved   atomic.Uint32
}

func newAObject(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
	isTombstone bool,
) *aobject {
	obj := &aobject{}
	obj.baseObject = newBaseObject(obj, meta, rt)
	if meta.IsForcePNode() || obj.meta.Load().HasDropCommitted() {
		// A recovered persisted object does not carry the complete in-memory
		// AppendNode history. Its max commit bound must remain unknown.
		obj.appendMVCC.MarkAppendHistoryIncomplete()
		pnode := newPersistedNode(obj.baseObject)
		node := NewNode(pnode)
		node.Ref()
		obj.node.Store(node)
		obj.FreezeAppend()
	} else {
		mnode := newMemoryNode(obj.baseObject, isTombstone)
		node := NewNode(mnode)
		node.Ref()
		obj.node.Store(node)
	}
	rows, _ := obj.Rows()
	obj.reserved.Store(uint32(rows))
	return obj
}

func (obj *aobject) FreezeAppend() {
	obj.frozen.Store(true)
}

// SealAppend permanently closes the object's AppendNode set. Unlike the
// legacy FreezeAppend flag, sealing also enables a stable max-commit
// timestamp once all already attached append transactions finish.
func (obj *aobject) SealAppend() {
	obj.freezelock.Lock()
	defer obj.freezelock.Unlock()
	obj.frozen.Store(true)
	obj.appendMVCC.Seal()
}

func (obj *aobject) GetAppendMaxCommitTS() (types.TS, bool) {
	return obj.appendMVCC.GetMaxCommitTS()
}

func (obj *aobject) IsAppendFrozen() bool {
	return obj.frozen.Load()
}

func (obj *aobject) IsAppendable() bool {
	if obj.IsAppendFrozen() {
		return false
	}
	node := obj.PinNode()
	defer node.Unref()
	if node.IsPersisted() {
		return false
	}
	rows, _ := node.Rows()
	return rows < obj.meta.Load().GetSchema().Extra.BlockMaxRows
}

func (obj *aobject) PrepareCompactInfo() (result bool, reason string) {
	if n := obj.RefCount(); n > 0 {
		reason = fmt.Sprintf("entering refcount %d", n)
		return
	}
	obj.SealAppend()
	if !obj.meta.Load().PrepareCompact() || !obj.appendMVCC.PrepareCompact() {
		if !obj.meta.Load().PrepareCompact() {
			reason = "meta preparecomp false"
		} else {
			reason = "mvcc preparecomp false"
		}
		return
	}

	if n := obj.RefCount(); n != 0 {
		reason = fmt.Sprintf("ending refcount %d", n)
		return
	}
	return obj.RefCount() == 0, reason
}

func (obj *aobject) PrepareCompact() bool {
	if obj.RefCount() > 0 {
		if obj.meta.Load().CheckPrintPrepareCompactLocked(1 * time.Second) {
			if !obj.meta.Load().HasPrintedPrepareComapct.Load() {
				logutil.Infof("object ref count is %d", obj.RefCount())
			}
			obj.meta.Load().PrintPrepareCompactDebugLog()
		}
		return false
	}
	// see more notes in flushtabletail.go
	obj.SealAppend()

	droppedCommitted := obj.meta.Load().HasDropCommitted()

	checkDuration := 10 * time.Minute
	if obj.GetRuntime().Options.CheckpointCfg.FlushInterval < 50*time.Millisecond {
		checkDuration = 8 * time.Second
	}
	if droppedCommitted {
		if !obj.meta.Load().PrepareCompactLocked() {
			return false
		}
	} else {
		if !obj.meta.Load().PrepareCompactLocked() {
			return false
		}
		if !obj.appendMVCC.PrepareCompact() /* all appends are committed */ {
			return false
		}
	}
	prepareCompact := obj.RefCount() == 0
	if !prepareCompact && obj.meta.Load().CheckPrintPrepareCompactLocked(checkDuration) {
		logutil.Infof("obj %v, data ref count is %d", obj.meta.Load().ID().String(), obj.RefCount())
	}
	return prepareCompact
}

func (obj *aobject) Pin() *common.PinnedItem[*aobject] {
	obj.Ref()
	return &common.PinnedItem[*aobject]{
		Val: obj,
	}
}

// check if all rows are committed before the specified ts
// here we assume that the ts is greater equal than the block's
// create ts and less than the block's delete ts
// it is a coarse-grained check
func (obj *aobject) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
	maxCommit, finalized := obj.GetAppendMaxCommitTS()
	return finalized && maxCommit.LT(&ts)
}

func (obj *aobject) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	keysZM index.ZM,
	from, to types.TS,
	rowIDs containers.Vector,
	mp *mpool.MPool,
) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Debugf("BatchDedup obj-%s: %v", obj.meta.Load().ID().String(), err)
		}
	}()
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		// Build the prepare-range selection from the immutable COW snapshot
		// before GetDuplicatedRows takes the object lock for ART and conflict
		// checks. PrePrepare assigns/publishes predecessor PrepareTS values in
		// the same serialized worker before this read; later active appends are
		// still handled by the newest-owner conflict check under the lock.
		selection, snapshot := obj.appendMVCC.GetRowSelectionAfterWithSnapshot(from, to)
		fn := func() (index.RowSelection, error) {
			// PrepareCommit also takes the object lock. If it published a new
			// snapshot between the lock-free traversal above and this callback,
			// rebuild from the now-stable snapshot while holding the lock.
			if !obj.appendMVCC.IsPrepareSnapshotCurrent(snapshot) {
				return obj.appendMVCC.GetRowSelectionAfter(from, to), nil
			}
			return selection, nil
		}
		return node.GetDuplicatedRows(
			ctx,
			txn,
			fn,
			keys,
			keysZM,
			rowIDs,
			mp,
		)
	} else {
		return obj.persistedGetDuplicatedRows(
			ctx,
			txn,
			from, to,
			keys,
			keysZM,
			rowIDs,
			true,
			mp,
		)
	}
}

func (obj *aobject) ApplyDebugBatch(bat *containers.Batch, txn txnif.AsyncTxn) (ans []txnif.TxnEntry, err error) {
	node := obj.PinNode()
	defer node.Unref()
	if node.IsPersisted() {
		return
	}
	mnode := node.MustMNode()

	commitTSVec := bat.Vecs[len(bat.Vecs)-1]
	commitTSs := vector.MustFixedColNoTypeCheck[types.TS](commitTSVec.GetDownstreamVector())
	prevTS := commitTSs[0]
	anodeStart := 0
	ans = make([]txnif.TxnEntry, 0)
	for i, ts := range commitTSs {
		if !ts.EQ(&prevTS) {
			an, create := obj.appendMVCC.AddAppendNodeLocked(txn, uint32(anodeStart), uint32(i))
			if create {
				ans = append(ans, an)
			}
			anodeStart = i
			prevTS = ts
		}
	}
	an, create := obj.appendMVCC.AddAppendNodeLocked(txn, uint32(anodeStart), uint32(bat.Length()))
	if create {
		ans = append(ans, an)
	}
	tmpAttrs := bat.Attrs
	bat.Attrs = tmpAttrs[:len(tmpAttrs)-1]
	if _, err = mnode.ApplyAppendLocked(bat); err != nil {
		return
	}
	bat.Attrs = tmpAttrs
	return
}
func (obj *aobject) Contains(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	keysZM index.ZM,
	mp *mpool.MPool,
) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Debugf("BatchDedup obj-%s: %v", obj.meta.Load().ID().String(), err)
		}
	}()
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return node.Contains(
			ctx,
			keys,
			keysZM,
			txn,
			mp,
		)
	} else {
		return obj.persistedContains(
			ctx,
			txn,
			keys,
			keysZM,
			true,
			mp,
		)
	}
}

func (obj *aobject) OnReplayAppend(node txnif.AppendNode) (err error) {
	obj.Lock()
	defer obj.Unlock()
	an := node.(*updates.AppendNode)
	obj.appendMVCC.OnReplayAppendNode(an)
	if max := an.GetMaxRow(); max > obj.reserved.Load() {
		obj.reserved.Store(max)
	}
	return
}

func (obj *aobject) OnReplayAppendPayload(bat *containers.Batch, offset uint32) (err error) {
	// MakeAppender returns a lightweight view and does not acquire an object
	// reference.  Replay owns the appender only for this call, so acquire the
	// matching reference before Close releases it.
	obj.Ref()
	appender, err := obj.MakeAppender()
	if err != nil {
		obj.Unref()
		return
	}
	defer appender.Close()
	err = appender.ApplyAppendAt(bat, offset, nil)
	if err == nil {
		obj.meta.Load().GetTable().AddRows(uint64(bat.Length()))
	}
	return
}

func (obj *aobject) MakeAppender() (appender data.ObjectAppender, err error) {
	if obj == nil {
		err = moerr.GetOkExpectedEOB()
		return
	}
	appender = newAppender(obj)
	return
}

func (obj *aobject) Init() (err error) { return }

func (obj *aobject) EstimateMemSize() int {
	node := obj.PinNode()
	defer node.Unref()
	obj.RLock()
	defer obj.RUnlock()
	size := obj.appendMVCC.EstimateMemSizeLocked()
	if !node.IsPersisted() {
		size += node.MustMNode().EstimateMemSizeLocked()
	}
	return size
}

func (obj *aobject) GetRowsOnReplay() uint64 {
	if obj.meta.Load().HasDropCommitted() {
		return uint64(obj.meta.Load().
			ObjectStats.Rows())
	}
	return uint64(obj.appendMVCC.GetTotalRow())
}
