// Copyright 2026 Matrix Origin
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

package iscp

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext/wand"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// TestWandSqlWriterAccumulate checks the writer maps extracted CDC rows to the
// right WandCdc events (pk/text column resolution, op tracking, defensive pk
// copy) and that ToSql round-trips them. Fields are set directly to bypass the
// tabledef/indexdef-dependent constructor.
func TestWandSqlWriterAccumulate(t *testing.T) {
	pkType := int32(types.T_int64)
	w := &WandSqlWriter{
		pkType:   pkType,
		pkPos:    0,
		textPos:  1,
		capacity: 100,
		cdc:      wand.NewWandCdc(pkType),
	}
	ctx := context.Background()

	if !w.Empty() {
		t.Fatal("fresh writer should be empty")
	}
	if !w.CheckLastOp(vectorindex.CDC_INSERT) {
		t.Fatal("empty writer should accept any op")
	}

	// pk in col 0, text (varchar → []byte) in col 1; delete carries pk in col 0.
	if err := w.Insert(ctx, []any{int64(1), []byte("营养 早餐")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Upsert(ctx, []any{int64(2), "视频"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Delete(ctx, []any{int64(3)}); err != nil {
		t.Fatal(err)
	}

	if w.Empty() {
		t.Fatal("writer should have buffered events")
	}
	if w.lastOp != vectorindex.CDC_DELETE || w.CheckLastOp(vectorindex.CDC_INSERT) {
		t.Fatalf("lastOp tracking wrong: %q", w.lastOp)
	}

	blob, err := w.ToSql()
	if err != nil {
		t.Fatal(err)
	}
	cdc, err := wand.DecodeWandCdc(blob)
	if err != nil {
		t.Fatal(err)
	}
	if len(cdc.Events) != 3 {
		t.Fatalf("want 3 events, got %d", len(cdc.Events))
	}
	if cdc.Events[0].Pk.(int64) != 1 || cdc.Events[0].Text != "营养 早餐" ||
		cdc.Events[1].Pk.(int64) != 2 || cdc.Events[1].Text != "视频" ||
		cdc.Events[2].Pk.(int64) != 3 {
		t.Fatalf("events wrong: %+v", cdc.Events)
	}

	w.Reset()
	if !w.Empty() || w.lastOp != "" || w.ndata != 0 {
		t.Fatal("Reset should clear the writer")
	}
}
