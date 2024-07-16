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
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type TransDels struct {
	// any txn starts after this ts won't check this table
	noUseAfter types.TS
	bornTime   time.Time
	// row -> commit ts
	Mapping map[int]types.TS
}

func NewTransDels(endts types.TS) *TransDels {
	return &TransDels{
		noUseAfter: endts,
		bornTime:   time.Now(),
		Mapping:    make(map[int]types.TS),
	}
}

func (t *TransDels) PPString() string {
	var w bytes.Buffer
	for row, ts := range t.Mapping {
		_, _ = w.WriteString(strconv.Itoa(row))
		_, _ = w.WriteString(":")
		_, _ = w.WriteString(ts.ToString())
		_, _ = w.WriteString(", ")
	}
	return w.String()
}

type TransDelsForBlks struct {
	sync.RWMutex
	dels map[types.Blockid]*TransDels
}

func NewTransDelsForBlks() *TransDelsForBlks {
	return &TransDelsForBlks{
		dels: make(map[types.Blockid]*TransDels),
	}
}

func (t *TransDelsForBlks) GetDelsForBlk(blkid types.Blockid) *TransDels {
	t.RLock()
	defer t.RUnlock()
	return t.dels[blkid]
}

func (t *TransDelsForBlks) SetDelsForBlk(blkid types.Blockid, rowOffset int, endTS, ts types.TS) {
	t.Lock()
	defer t.Unlock()
	dels, ok := t.dels[blkid]
	if !ok {
		dels = NewTransDels(endTS)
		t.dels[blkid] = dels
	}
	dels.Mapping[rowOffset] = ts
}

func (t *TransDelsForBlks) DeleteDelsForBlk(blkid types.Blockid) {
	t.Lock()
	defer t.Unlock()
	delete(t.dels, blkid)
}

func (t *TransDelsForBlks) Prune(gap time.Duration) {
	t.Lock()
	defer t.Unlock()
	for blkid, del := range t.dels {
		if time.Since(del.bornTime) > gap {
			delete(t.dels, blkid)
		}
	}
}
