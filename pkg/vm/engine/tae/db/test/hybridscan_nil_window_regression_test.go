// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	config "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
)

// TestHybridScanNilWindowAndDeleteAll proves, deterministically and without any
// production hook, the exact contract gap behind the TestDelete3 coverage
// panic: an appendable object that has been published by a committed
// transaction carryies no registered AppendNode until the first real append,
// and in that state a committed reader's HybridScan returns (err=nil,
// view=nil). The shared DeleteAll consumer must treat that successful-nil scan
// as an empty object instead of dereferencing the nil batch. The empty carrier
// is created through the real handle API (Relation.CreateObject) and the same
// object is then shown non-nil after the first AppendNode registers.
func TestHybridScanNilWindowAndDeleteAll(t *testing.T) {
	ctx := context.Background()
	tae := testutil.NewTestEngine(ctx, ModuleName, t, config.WithLongScanAndCKPOpts(nil))
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, 2)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	rowIDIdx := schema.GetColIdx(catalog.PhyAddrColumnName)
	pkIdx := schema.GetPrimaryKey().Idx

	// Stage 1: seal the first object so later appends must use a new carrier,
	// then free the primary-key range through a committed delete.
	bat10 := catalog.MockBatch(schema, int(schema.Extra.BlockMaxRows))
	// MockBatch allocates caller-owned vectors (common.DefaultAllocator):
	// neither CreateRelAndAppend nor rel.Append takes the caller batch's
	// ownership, so close the inputs on every path.
	defer bat10.Close()
	tae.CreateRelAndAppend(bat10, true)
	tae.CheckRowsByScan(int(schema.Extra.BlockMaxRows), true)
	if err := tae.DeleteAll(true); err != nil {
		t.Fatalf("fixture DeleteAll: %v", err)
	}
	tae.CheckRowsByScan(0, true)

	// Stage 2 (the producer): create and commit an EMPTY appendable carrier
	// through the real catalog/transaction handle API. Rel.CreateObject
	// publishes the carrier's catalog entry via its own independently
	// committed allocation (AllocateAndPublishCommitTS) — it is not the outer
	// commit that creates it — so the committed carrier is reader-visible with
	// no registered AppendNode.
	txnC, relC := tae.GetRelation()
	carrier, err := relC.CreateObject(false)
	if err != nil {
		t.Fatalf("CreateObject: %v", err)
	}
	carrierID := carrier.GetID().String()
	carrier.Close()
	if err := txnC.Commit(ctx); err != nil {
		t.Fatalf("carrier txn commit: %v", err)
	}

	// scanAll is assertion-based: any HybridScan error is fatal (with the
	// object identity captured before the pooled handle is closed and any
	// partially produced batch released), and the reader transaction's commit
	// result is checked explicitly.
	scanAll := func(stage string) (nilViews, nonNilViews []string) {
		txn, rel := tae.GetRelation()
		defer func() {
			if cerr := txn.Commit(ctx); cerr != nil {
				t.Errorf("reader txn commit (%s): %v", stage, cerr)
			}
		}()
		it := rel.MakeObjectIt(false)
		defer it.Close()
		for it.Next() {
			obj := it.GetObject()
			blkCnt := uint16(obj.BlkCnt())
			for i := uint16(0); i < blkCnt; i++ {
				var view *containers.Batch
				err := obj.HybridScan(ctx, &view, i, []int{rowIDIdx, pkIdx}, common.DefaultAllocator)
				if err != nil {
					if view != nil {
						view.Close()
					}
					objID := obj.GetID().String()
					obj.Close()
					t.Fatalf("%s: HybridScan object=%v blk=%d err=%v", stage, objID, i, err)
				}
				card := lenOrNil(view)
				if view == nil {
					nilViews = append(nilViews, obj.GetID().String())
				} else {
					nonNilViews = append(nonNilViews, obj.GetID().String())
					view.Close()
				}
				t.Logf("%s stage=%s object=%v blk=%d/%d err=nil view_nil=%t card=%d",
					t.Name(), stage, obj.GetID().String(), i, blkCnt-1, view == nil, card)
			}
			obj.Close()
		}
		return
	}

	// Stage 2 cont (the producer assertion): the freshly committed carrier must
	// scan with the successful-nil shape (err=nil, view=nil).
	nilW1, _ := scanAll("committed_empty_carrier")
	found := false
	for _, id := range nilW1 {
		if id == carrierID {
			found = true
		}
	}
	if !found {
		t.Fatalf("the committed empty carrier %v did not produce a successful-nil scan (nil views: %v)", carrierID, nilW1)
	}

	// Stage 3 (the consumer): DeleteAll in the exact empty-carrier state must
	// survive the nil scan, skip it as empty, and leave the row state intact.
	if err := tae.DeleteAll(true); err != nil {
		t.Fatalf("DeleteAll with the committed empty carrier: %v", err)
	}
	tae.CheckRowsByScan(0, true)

	// Stage 4 (the same-object visible control): a real append commits into
	// the SAME carrier (it is the table's only appendable object), registering
	// its first AppendNode; the identical object must then scan non-nil with
	// the committed rows.
	bat4 := catalog.MockBatch(schema, 4)
	defer bat4.Close()
	txnW, relW := tae.GetRelation()
	if err := relW.Append(ctx, bat4); err != nil {
		_ = txnW.Rollback(ctx)
		t.Fatalf("Append into the carrier: %v", err)
	}
	if err := txnW.Commit(ctx); err != nil {
		t.Fatalf("append commit: %v", err)
	}

	_, nonNilW2 := scanAll("after_first_append_node")
	match := ""
	for _, id := range nonNilW2 {
		if id == carrierID {
			match = id
		}
	}
	if match == "" {
		t.Fatalf("the carrier %v did not scan non-nil after its first AppendNode registered (non-nil views: %v)", carrierID, nonNilW2)
	}
	tae.CheckRowsByScan(4, true)
}

func lenOrNil(b *containers.Batch) int {
	if b == nil {
		return -1
	}
	return b.Length()
}
