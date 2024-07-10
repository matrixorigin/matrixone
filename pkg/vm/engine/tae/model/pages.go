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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type HashPageTable = TransferTable[*TransferHashPage]

var (
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

type Path struct {
	Name   string
	Offset int64
	Size   int64
}

type TransferHashPage struct {
	common.RefHelper
	latch       sync.RWMutex
	bornTS      time.Time
	id          *common.ID // not include blk offset
	hashmap     api.HashPageMap
	path        atomic.Pointer[Path]
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

// 0 no need to clear
// 1 clear memory hashmap
// 2 clear disk hashmap
func (page *TransferHashPage) TTL() uint8 {
	now := time.Now()
	if now.After(page.bornTS.Add(page.params.DiskTTL)) {
		return 2
	}
	if now.After(page.bornTS.Add(page.params.TTL)) {
		return 1
	}
	return 0
}

func (page *TransferHashPage) Close() {
	logutil.Debugf("Closing %s", page.String())
	page.hashmap.M = make(map[uint32][]byte)
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

func (page *TransferHashPage) Train(m map[uint32][]byte) {
	page.latch.Lock()
	defer page.latch.Unlock()
	for k, v := range m {
		page.hashmap.M[k] = v
	}
	v2.TransferPageRowHistogram.Observe(float64(len(m)))
}

func (page *TransferHashPage) Transfer(from uint32) (dest types.Rowid, ok bool) {
	v2.TransferPageSinceBornDurationHistogram.Observe(time.Since(page.bornTS).Seconds())
	if atomic.LoadInt32(&page.isPersisted) == 1 {
		diskStart := time.Now()
		page.loadTable()
		diskDuration := time.Since(diskStart)
		v2.TransferDiskLatencyHistogram.Observe(diskDuration.Seconds())
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

func (page *TransferHashPage) SetPath(path *Path) {
	page.path.Store(path)
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
	if page.path.Load() == nil {
		return
	}

	path := page.path.Load()
	name, offset, size := path.Name, path.Offset, path.Size

	ioVector := fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
	}

	entry := fileservice.IOEntry{
		Offset: offset,
		Size:   size,
	}
	ioVector.Entries = append(ioVector.Entries, entry)
	err := FS.Read(context.Background(), &ioVector)
	if err != nil {
		return
	}

	err = page.Unmarshal(ioVector.Entries[0].Data)
	if err != nil {
		return
	}

	v2.TaskMergeTransferPageSizeGauge.Add(float64(page.Length()))

	atomic.StoreInt32(&page.isPersisted, 0)
}

func (page *TransferHashPage) ClearPersistTable() {
	if page.path.Load() == nil {
		return
	}
	FS.Delete(context.Background(), page.path.Load().Name)
}

func (page *TransferHashPage) IsPersist() int32 {
	return atomic.LoadInt32(&page.isPersisted)
}
