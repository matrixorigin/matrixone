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
	"math"
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
}

func newAObject(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
	isTombstone bool,
) *aobject {
	obj := &aobject{}
	obj.baseObject = newBaseObject(obj, meta, rt)
	if meta.IsForcePNode() || obj.meta.Load().HasDropCommitted() {
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
	return obj
}

func (obj *aobject) FreezeAppend() {
	obj.frozen.Store(true)
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
	obj.FreezeAppend()
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
	obj.freezelock.Lock()
	obj.FreezeAppend()
	obj.freezelock.Unlock()

	droppedCommitted := obj.meta.Load().HasDropCommitted()

	checkDuration := 10 * time.Minute
	if obj.GetRuntime().Options.CheckpointCfg.FlushInterval < 50*time.Millisecond {
		checkDuration = 8 * time.Second
	}
	if droppedCommitted {
		if !obj.meta.Load().PrepareCompactLocked() {
			if obj.meta.Load().CheckPrintPrepareCompactLocked(checkDuration) {
				obj.meta.Load().PrintPrepareCompactDebugLog()
			}
			return false
		}
	} else {
		if !obj.meta.Load().PrepareCompactLocked() {
			if obj.meta.Load().CheckPrintPrepareCompactLocked(checkDuration) {
				obj.meta.Load().PrintPrepareCompactDebugLog()
			}
			return false
		}
		if !obj.appendMVCC.PrepareCompact() /* all appends are committed */ {
			if obj.meta.Load().CheckPrintPrepareCompactLocked(checkDuration) {
				logutil.Infof("obj %v, data prepare compact failed", obj.meta.Load().ID().String())
				if !obj.meta.Load().HasPrintedPrepareComapct.Load() {
					obj.meta.Load().HasPrintedPrepareComapct.Store(true)
					logutil.Infof("append MVCC %v", obj.appendMVCC.StringLocked())
				}
			}
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
	// if the block is not frozen, always return false
	if !obj.IsAppendFrozen() {
		return false
	}

	node := obj.PinNode()
	defer node.Unref()

	// if the block is in memory, check with the in-memory node
	// it is a fine-grained check if the block is in memory
	if !node.IsPersisted() {
		return node.MustMNode().allRowsCommittedBefore(ts)
	}

	// always return false for if the block is persisted
	// it is a coarse-grained check
	return false
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
		fn := func() (minv, maxv int32, err error) {
			obj.RUnlock()
			defer obj.RLock()
			if maxv, err = obj.GetMaxRowByTS(to); err != nil {
				return
			}
			minv, err = obj.GetMaxRowByTS(from)
			return
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

func (obj *aobject) GetMaxRowByTS(ts types.TS) (int32, error) {
	if ts.IsEmpty() {
		return -1, nil
	}
	maxTS := types.MaxTs()
	if ts.EQ(&maxTS) {
		return math.MaxInt32, nil
	}
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		obj.RLock()
		defer obj.RUnlock()
		return int32(obj.appendMVCC.GetMaxRowByTSLocked(ts)), nil
	} else {
		vec, err := obj.LoadPersistedCommitTS(0)
		if err != nil {
			return 0, err
		}
		defer vec.Close()
		tsVec := vector.MustFixedColNoTypeCheck[types.TS](
			vec.GetDownstreamVector())
		for i := range tsVec {
			if tsVec[i].GT(&ts) {
				return int32(i), nil
			}
		}
		return int32(vec.Length()), nil
	}
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
	an := node.(*updates.AppendNode)
	obj.appendMVCC.OnReplayAppendNode(an)
	return
}

func (obj *aobject) OnReplayAppendPayload(bat *containers.Batch) (err error) {
	appender, err := obj.MakeAppender()
	if err != nil {
		return
	}
	_, err = appender.ReplayAppend(bat, nil)
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
