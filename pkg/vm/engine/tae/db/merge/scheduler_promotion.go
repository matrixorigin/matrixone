// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"container/heap"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

// CheckPromotionReady rejects a scheduler that has run or accepted messages.
// The caller must exclude concurrent message producers during promotion.
func (a *MergeScheduler) CheckPromotionReady() error {
	if !a.stopped.Load() {
		return moerr.NewInternalErrorNoCtx("merge scheduler already running")
	}
	if generation := a.generation.Load(); generation != nil {
		select {
		case <-generation.stopCh:
			return moerr.NewInternalErrorNoCtx("merge scheduler has already run")
		default:
		}
	}
	if len(a.msgChan) != 0 {
		return moerr.NewInternalErrorNoCtx("merge scheduler has queued messages before promotion")
	}
	return nil
}

// PreparePromotion reconciles the original stopped scheduler with the catalog
// after WAL replay. It leaves scheduling paused until ResumePromotion succeeds.
func (a *MergeScheduler) PreparePromotion(
	source catalog.CatalogEventSource,
	settings map[uint64]*MMsgTaskTrigger,
) error {
	if err := a.CheckPromotionReady(); err != nil {
		return err
	}
	active := make(map[uint64]catalog.MergeTable)
	for table := range source.InitSource() {
		active[table.ID()] = table
	}
	for id, supp := range a.supps {
		if table, ok := active[id]; ok {
			supp.todo.table = table
		} else {
			heap.Remove(&a.pq, supp.todo.index)
			delete(a.supps, id)
		}
	}
	for id, table := range active {
		if a.supps[id] == nil {
			a.handleAddTable(table)
		}
	}
	for id, supp := range a.supps {
		supp.baseTrigger = settings[id]
	}
	a.bootstrapMsg = nil
	a.allPaused = true
	return nil
}

// ResumePromotion waits until the scheduler has applied the resume message.
func (a *MergeScheduler) ResumePromotion(ctx context.Context) error {
	generation := a.generation.Load()
	if a.stopped.Load() || generation == nil {
		return ErrMergeSchedulerStopped
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case a.msgChan <- &MMsg{
		Kind:       MMsgKindSwitch,
		Value:      MMsgSwitch{On: true},
		generation: generation,
	}:
	case <-generation.stopCh:
		return ErrMergeSchedulerStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	// Sending resume is the commit point: the scheduler may launch a merge
	// before the caller's context is canceled. Wait for the query barrier even
	// after cancellation, then report the completed transition.
	_, err := a.Query(context.WithoutCancel(ctx), nil)
	return err
}
