// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// sharedStates is shared among all PartitionStates
type sharedStates struct {
	sync.Mutex
}

// RowEntry represents a version of a row
type RowEntry struct {
	BlockID objectio.Blockid // we need to iter by block id, so put it first to allow faster iteration
	RowID   objectio.Rowid
	Time    types.TS

	ID                int64 // a unique version id, for primary index building and validating
	Deleted           bool
	Batch             *batch.Batch
	Offset            int64
	PrimaryIndexBytes []byte
}

func (r RowEntry) String() string {
	t, _ := types.Unpack(r.PrimaryIndexBytes)
	return fmt.Sprintf("RID(%s)-EID(%d)-DEL(%v)-OFF(%d)-TIME(%s)-PK(%s | %v)",
		r.RowID.String(),
		r.ID,
		r.Deleted,
		r.Offset,
		r.Time.ToString(),
		t.SQLStrings(nil),
		r.PrimaryIndexBytes)
}

func (r *RowEntry) Less(than *RowEntry) bool {
	// asc
	if cmp := r.BlockID.Compare(&than.BlockID); cmp != 0 {
		return cmp < 0
	}

	// asc
	if cmp := r.RowID.Compare(&than.RowID); cmp != 0 {
		return cmp < 0
	}

	// desc
	return r.Time.Compare(&than.Time) > 0
}

type PrimaryIndexEntry struct {
	Bytes      []byte
	RowEntryID int64

	// fields for validating
	BlockID objectio.Blockid
	RowID   objectio.Rowid
	Time    types.TS
	Deleted bool
}

func (p *PrimaryIndexEntry) Less(than *PrimaryIndexEntry) bool {
	if res := bytes.Compare(p.Bytes, than.Bytes); res != 0 {
		return res < 0
	}

	// desc
	if res := p.Time.Compare(&than.Time); res != 0 {
		return res > 0
	}

	// desc
	return p.RowEntryID > than.RowEntryID
}

type ObjectIndexByTSEntry struct {
	Time         types.TS // insert or delete time
	ShortObjName objectio.ObjectNameShort

	IsDelete     bool
	IsAppendable bool
}

func (b ObjectIndexByTSEntry) Less(than ObjectIndexByTSEntry) bool {
	// asc
	if cmp := b.Time.Compare(&than.Time); cmp != 0 {
		return cmp < 0
	}

	// asc
	return bytes.Compare(b.ShortObjName[:], than.ShortObjName[:]) < 0
}

var nextRowEntryID = int64(1)
