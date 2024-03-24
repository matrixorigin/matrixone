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

package model

import (
	"bytes"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type HashPageTable = TransferTable[*TransferHashPage]

type TransferHashPage struct {
	common.RefHelper
	bornTS      time.Time
	id          *common.ID // not include blk offset
	hashmap     map[uint32]types.Rowid
	isTransient bool
}

func NewTransferHashPage(id *common.ID, ts time.Time, isTransient bool) *TransferHashPage {
	page := &TransferHashPage{
		bornTS:      ts,
		id:          id,
		hashmap:     make(map[uint32]types.Rowid),
		isTransient: isTransient,
	}
	page.OnZeroCB = page.Close
	return page
}

func (page *TransferHashPage) ID() *common.ID    { return page.id }
func (page *TransferHashPage) BornTS() time.Time { return page.bornTS }

func (page *TransferHashPage) TTL(now time.Time, ttl time.Duration) bool {
	if page.isTransient {
		ttl /= 2
	}
	return now.After(page.bornTS.Add(ttl))
}

func (page *TransferHashPage) Close() {
	logutil.Debugf("Closing %s", page.String())
	page.hashmap = make(map[uint32]types.Rowid)
}

func (page *TransferHashPage) Length() int {
	return len(page.hashmap)
}

func (page *TransferHashPage) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("hashpage[%s][%s][Len=%d]",
		page.id.BlockString(),
		page.bornTS.String(),
		len(page.hashmap)))
	return w.String()
}

func (page *TransferHashPage) Pin() *common.PinnedItem[*TransferHashPage] {
	page.Ref()
	return &common.PinnedItem[*TransferHashPage]{
		Val: page,
	}
}

func (page *TransferHashPage) Train(from uint32, to types.Rowid) {
	page.hashmap[from] = to
}

func (page *TransferHashPage) Transfer(from uint32) (dest types.Rowid, ok bool) {
	dest, ok = page.hashmap[from]
	return
}
