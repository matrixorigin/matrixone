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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type HashPageTable = TransferTable[*TransferHashPage]

// BlockRead Using blockio directly will cause cycle import
type BlockRead interface {
	LoadTableByBlock(loc objectio.Location, fs fileservice.FileService) (bat *batch.Batch, release func(), err error)
}

var (
	RD     BlockRead
	FS     fileservice.FileService
	fsOnce sync.Once
)

func SetFileService(fs fileservice.FileService) {
	fsOnce.Do(func() {
		FS = fs
	})
}

type TransferHashPageParams struct {
	TTL     time.Duration
	DiskTTL time.Duration
}

type Option func(*TransferHashPageParams)

func WithTTL(ttl time.Duration) Option {
	return func(params *TransferHashPageParams) {
		params.TTL = ttl
	}
}

func WithDiskTTL(diskTTL time.Duration) Option {
	return func(params *TransferHashPageParams) {
		params.DiskTTL = diskTTL
	}
}

type TransferHashPage struct {
	common.RefHelper
	latch       sync.RWMutex
	bornTS      time.Time
	id          *common.ID // not include blk offset
	hashmap     api.HashPageMap
	loc         atomic.Pointer[objectio.Location]
	params      TransferHashPageParams
	isTransient bool
	isPersisted int32 // 0 in memory, 1 on disk
}

func NewTransferHashPage(id *common.ID, ts time.Time, pageSize int, isTransient bool, opts ...Option) *TransferHashPage {
	params := TransferHashPageParams{
		TTL:     5 * time.Second,
		DiskTTL: 10 * time.Minute,
	}
	for _, opt := range opts {
		opt(&params)
	}

	page := &TransferHashPage{
		bornTS:      ts,
		id:          id,
		hashmap:     api.HashPageMap{M: make(map[uint32][]byte, pageSize)},
		params:      params,
		isTransient: isTransient,
		isPersisted: 0,
	}
	page.OnZeroCB = page.Close

	return page
}

func (page *TransferHashPage) ID() *common.ID    { return page.id }
func (page *TransferHashPage) BornTS() time.Time { return page.bornTS }

func (page *TransferHashPage) TTL() uint8 {
	if time.Now().After(page.bornTS.Add(page.params.DiskTTL)) {
		return 2
	}
	if time.Now().After(page.bornTS.Add(page.params.TTL)) {
		return 1
	}
	return 0
}

func (page *TransferHashPage) Close() {
	logutil.Debugf("Closing %s", page.String())
	page.hashmap = api.HashPageMap{M: make(map[uint32][]byte)}
}

func (page *TransferHashPage) Length() int {
	page.latch.RLock()
	defer page.latch.RUnlock()
	return len(page.hashmap.M)
}

func (page *TransferHashPage) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("hashpage[%s][%s][Len=%d]",
		page.id.BlockString(),
		page.bornTS.String(),
		page.Length()))
	return w.String()
}

func (page *TransferHashPage) Pin() *common.PinnedItem[*TransferHashPage] {
	page.Ref()
	return &common.PinnedItem[*TransferHashPage]{
		Val: page,
	}
}

func (page *TransferHashPage) Clear(clearDisk bool) {
	page.ClearTable()
	if clearDisk {
		page.ClearPersistTable()
	}
}

func (page *TransferHashPage) Train(from uint32, to types.Rowid) {
	page.latch.Lock()
	defer page.latch.Unlock()
	page.hashmap.M[from] = to[:]
	v2.TransferPageRowHistogram.Observe(1)
}

func (page *TransferHashPage) Transfer(from uint32) (dest types.Rowid, ok bool) {
	v2.TransferPageSinceBornDurationHistogram.Observe(time.Since(page.bornTS).Seconds())
	if atomic.LoadInt32(&page.isPersisted) == 1 {
		diskStart := time.Now()
		page.loadTable()
		v2.TransferPageDiskHitHistogram.Observe(1)
		diskDuration := time.Since(diskStart)
		v2.TransferDiskLatencyHistogram.Observe(diskDuration.Seconds())
	} else {
		v2.TransferPageMemHitHistogram.Observe(1)
	}
	v2.TransferPageTotalHitHistogram.Observe(1)

	memStart := time.Now()
	var m []byte
	page.latch.RLock()
	m, ok = page.hashmap.M[from]
	page.latch.RUnlock()
	if ok {
		dest = types.Rowid(m)
	}
	memDuration := time.Since(memStart)
	v2.TransferMemLatencyHistogram.Observe(memDuration.Seconds())
	return
}

func (page *TransferHashPage) Marshal() []byte {
	page.latch.RLock()
	defer page.latch.RUnlock()
	data, _ := proto.Marshal(&page.hashmap)
	return data
}

func (page *TransferHashPage) Unmarshal(data []byte) error {
	page.latch.Lock()
	defer page.latch.Unlock()
	err := proto.Unmarshal(data, &page.hashmap)
	return err
}

func (page *TransferHashPage) SetLocation(location objectio.Location) {
	page.loc.Store(&location)
}

func (page *TransferHashPage) ClearTable() {
	if atomic.LoadInt32(&page.isPersisted) == 1 {
		return
	}
	atomic.StoreInt32(&page.isPersisted, 1)
	v2.TaskMergeTransferPageSizeGauge.Sub(float64(page.Length()))
	page.latch.Lock()
	page.hashmap.M = make(map[uint32][]byte)
	page.latch.Unlock()
}

func (page *TransferHashPage) loadTable() {
	if page.loc.Load() == nil {
		return
	}

	var bat *batch.Batch
	var release func()
	bat, release, err := RD.LoadTableByBlock(*page.loc.Load(), FS)
	defer release()
	if err != nil {
		logutil.Errorf("[TransferHashPage] load table failed, %v", err)
		return
	}
	err = page.Unmarshal(bat.Vecs[0].GetBytesAt(0))
	if err != nil {
		return
	}

	v2.TaskMergeTransferPageSizeGauge.Add(float64(page.Length()))

	atomic.StoreInt32(&page.isPersisted, 0)
}

func (page *TransferHashPage) ClearPersistTable() {
	if page.loc.Load() == nil {
		return
	}
	FS.Delete(context.Background(), page.loc.Load().Name().String())
}

func (page *TransferHashPage) IsPersist() int32 {
	return atomic.LoadInt32(&page.isPersisted)
}
