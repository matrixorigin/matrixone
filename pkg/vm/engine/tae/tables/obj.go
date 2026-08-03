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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type object struct {
	*baseObject
}

func newObject(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) *object {
	obj := &object{}
	obj.baseObject = newBaseObject(obj, meta, rt)
	pnode := newPersistedNode(obj.baseObject)
	node := NewNode(pnode)
	node.Ref()
	obj.node.Store(node)
	return obj
}

func (obj *object) Init() (err error) {
	return
}

func (obj *object) PrepareCompact() bool {
	prepareCompact := obj.meta.Load().PrepareCompact()
	if !prepareCompact && obj.meta.Load().CheckPrintPrepareCompact() {
		obj.meta.Load().PrintPrepareCompactDebugLog()
	}
	return prepareCompact
}

func (obj *object) PrepareCompactInfo() (result bool, reason string) {
	return obj.meta.Load().PrepareCompact(), ""
}

func (obj *object) FreezeAppend() {}

func (obj *object) Pin() *common.PinnedItem[*object] {
	obj.Ref()
	return &common.PinnedItem[*object]{
		Val: obj,
	}
}

func (obj *object) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
	creatTS := obj.meta.Load().GetCreatedAt()
	return creatTS.LT(&ts)
}

func (obj *object) GetDuplicatedRows(
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
			logutil.Infof("BatchDedup %s (%v)obj-%s: %v",
				obj.meta.Load().GetTable().GetLastestSchemaLocked(false).Name,
				obj.IsAppendable(),
				obj.meta.Load().ID().String(),
				err)
		}
	}()
	// A TN rewrite's CreatedAt is a physical flush/merge timestamp, not the
	// logical commit timestamp of every row it carries. Inspect its hidden
	// commit-TS column only after zonemap/BF and exact-PK matching. This keeps
	// old rows out of the incremental window while preserving genuinely new
	// conflicts that were flushed before pre-prepare.
	//
	// Legacy objects without that column are handled by policy in the row
	// filter: incremental dedup remains best-effort, while strict policies stay
	// conservative. CN-created objects and later TN rewrites that inherit null
	// row timestamps follow those same policy rules; no lineage or upgrade
	// protocol is added for this fallback path.
	filterByCommitTS := !from.IsEmpty() &&
		!obj.meta.Load().GetObjectStats().GetCNCreated()
	return obj.persistedGetDuplicatedRows(
		ctx,
		txn,
		from, to,
		keys,
		keysZM,
		rowIDs,
		filterByCommitTS,
		mp,
	)
}
func (obj *object) GetMaxRowByTS(ts types.TS) (uint32, error) {
	panic("not support")
}
func (obj *object) ApplyDebugBatch(bat *containers.Batch, txn txnif.AsyncTxn) (ans []txnif.TxnEntry, err error) {
	return
}
func (obj *object) Contains(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	keysZM index.ZM,
	mp *mpool.MPool) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup %s (%v)obj-%s: %v",
				obj.meta.Load().GetTable().GetLastestSchemaLocked(false).Name,
				obj.IsAppendable(),
				obj.meta.Load().ID().String(),
				err)
		}
	}()
	return obj.persistedContains(
		ctx,
		txn,
		keys,
		keysZM,
		false,
		mp,
	)
}

func (obj *object) EstimateMemSize() int {
	return 0
}

func (obj *object) GetRowsOnReplay() uint64 {
	return uint64(obj.meta.Load().Rows())
}
