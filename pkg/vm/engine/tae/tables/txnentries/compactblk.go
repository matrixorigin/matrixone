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

package txnentries

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type compactBlockEntry struct {
	sync.RWMutex
	txn       txnif.AsyncTxn
	from      handle.Block
	to        handle.Block
	scheduler tasks.TaskScheduler
	deletes   *nulls.Bitmap
}

func NewCompactBlockEntry(
	txn txnif.AsyncTxn,
	from, to handle.Block,
	scheduler tasks.TaskScheduler,
	sortIdx []int32,
	deletes *nulls.Bitmap,
) *compactBlockEntry {

	page := model.NewTransferHashPage(from.Fingerprint(), time.Now())
	if to != nil {
		toId := to.Fingerprint()
		offsetMapping := compute.GetOffsetMapBeforeApplyDeletes(deletes)
		if !deletes.IsEmpty() {
			delCnt := deletes.GetCardinality()
			for i, idx := range sortIdx {
				if int(idx) < len(offsetMapping) {
					sortIdx[i] = int32(offsetMapping[idx])
				} else {
					sortIdx[i] = idx + int32(delCnt)
				}
			}
		}
		for i, idx := range sortIdx {
			rowid := objectio.NewRowid(&toId.BlockID, uint32(i))
			page.Train(uint32(idx), *rowid)
		}
		_ = scheduler.AddTransferPage(page)
	}
	return &compactBlockEntry{
		txn:       txn,
		from:      from,
		to:        to,
		scheduler: scheduler,
		deletes:   deletes,
	}
}

func (entry *compactBlockEntry) IsAborted() bool { return false }
func (entry *compactBlockEntry) PrepareRollback() (err error) {
	// TODO: remove block file? (should be scheduled and executed async)
	_ = entry.scheduler.DeleteTransferPage(entry.from.Fingerprint())
	var fs fileservice.FileService
	var fromName, toName string

	fromBlockEntry := entry.from.GetMeta().(*catalog.BlockEntry)
	fs = fromBlockEntry.GetBlockData().GetFs().Service

	// do not delete nonappendable `from` block file because it can be compacted again if it has deletes
	if entry.from.IsAppendableBlock() {
		seg := fromBlockEntry.ID.Segment()
		num, _ := fromBlockEntry.ID.Offsets()
		fromName = objectio.BuildObjectName(seg, num).String()
	}

	// it is totally safe to delete the brand new `to` block file
	if entry.to != nil {
		toBlockEntry := entry.to.GetMeta().(*catalog.BlockEntry)
		seg := toBlockEntry.ID.Segment()
		num, _ := toBlockEntry.ID.Offsets()
		toName = objectio.BuildObjectName(seg, num).String()
	}

	entry.scheduler.ScheduleScopedFn(&tasks.Context{}, tasks.IOTask, fromBlockEntry.AsCommonID(), func() error {
		// TODO: variable as timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		if fromName != "" {
			_ = fs.Delete(ctx, fromName)
		}
		if toName != "" {
			_ = fs.Delete(ctx, toName)
		}
		// logutil.Infof("rollback unfinished compact file %q and %q", fromName, toName)
		return nil
	})
	return
}
func (entry *compactBlockEntry) ApplyRollback() (err error) {
	//TODO:?
	return
}
func (entry *compactBlockEntry) ApplyCommit() (err error) {
	_ = entry.from.GetMeta().(*catalog.BlockEntry).GetBlockData().TryUpgrade()
	entry.from.GetMeta().(*catalog.BlockEntry).GetBlockData().GCInMemeoryDeletesByTS(entry.txn.GetCommitTS())
	return
}

func (entry *compactBlockEntry) MakeCommand(csn uint32) (cmd txnif.TxnCmd, err error) {
	to := &common.ID{}
	if entry.to != nil {
		to = entry.to.Fingerprint()
	}
	cmd = newCompactBlockCmd((*common.ID)(entry.from.Fingerprint()), to, entry.txn, csn)
	return
}

func (entry *compactBlockEntry) Set1PC()     {}
func (entry *compactBlockEntry) Is1PC() bool { return false }
func (entry *compactBlockEntry) PrepareCommit() (err error) {
	dataBlock := entry.from.GetMeta().(*catalog.BlockEntry).GetBlockData()
	if dataBlock.HasDeleteIntentsPreparedIn(entry.txn.GetStartTS().Next(), types.MaxTs()) {
		err = moerr.NewTxnWWConflictNoCtx()
	}
	return
}
