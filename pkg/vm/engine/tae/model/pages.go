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
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type HashPageTable = TransferTable[*TransferHashPage]

var (
	ttl      = 5 * time.Second
	diskTTL  = 3 * time.Minute
	ttlLatch sync.RWMutex
)

func SetTTL(t time.Duration) {
	ttlLatch.Lock()
	defer ttlLatch.Unlock()
	ttl = t
}

func SetDiskTTL(t time.Duration) {
	ttlLatch.Lock()
	defer ttlLatch.Unlock()
	diskTTL = t
}

func GetTTL() time.Duration {
	ttlLatch.RLock()
	defer ttlLatch.RUnlock()
	return ttl
}

func GetDiskTTL() time.Duration {
	ttlLatch.RLock()
	defer ttlLatch.RUnlock()
	return diskTTL
}

type Path struct {
	Name   string
	Offset int64
	Size   int64
}

type TransferHashPage struct {
	common.RefHelper
	bornTS      time.Time
	id          *common.ID // not include blk offset
	hashmap     atomic.Pointer[api.HashPageMap]
	path        Path
	isTransient bool
	fs          fileservice.FileService
	ttl         time.Duration
	diskTTL     time.Duration
}

func NewTransferHashPage(id *common.ID, ts time.Time, isTransient bool, fs fileservice.FileService, ttl, diskTTL time.Duration) *TransferHashPage {

	page := &TransferHashPage{
		bornTS:      ts,
		id:          id,
		isTransient: isTransient,
		fs:          fs,
		ttl:         ttl,
		diskTTL:     diskTTL,
	}

	m := api.HashPageMap{M: make(map[uint32][]byte)}
	page.hashmap.Store(&m)
	page.OnZeroCB = page.Close

	return page
}

func (page *TransferHashPage) ID() *common.ID         { return page.id }
func (page *TransferHashPage) BornTS() time.Time      { return page.bornTS }
func (page *TransferHashPage) SetBornTS(ts time.Time) { page.bornTS = ts }

const (
	notClear    = uint8(0)
	clearMemory = uint8(1)
	clearDisk   = uint8(2)
)

func (page *TransferHashPage) TTL() uint8 {
	now := time.Now()
	if now.After(page.bornTS.Add(page.diskTTL)) {
		return clearDisk
	}
	if now.After(page.bornTS.Add(page.ttl)) {
		return clearMemory
	}
	return notClear
}

func (page *TransferHashPage) Close() {
	logutil.Debugf("Closing %s", page.String())
	page.ClearPersistTable()
	page.hashmap.Store(nil)
}

func (page *TransferHashPage) Length() int {
	m := page.hashmap.Load()
	if m == nil {
		return 0
	}
	return len(m.M)
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

func (page *TransferHashPage) Clear() {
	m := page.hashmap.Load()
	if m == nil {
		return
	}

	page.hashmap.Store(nil)
	v2.TaskMergeTransferPageLengthGauge.Sub(float64(len(m.M)))
	m.M = make(map[uint32][]byte)
}

func (page *TransferHashPage) Train(m map[uint32][]byte) {
	hashmap := api.HashPageMap{M: m}
	page.hashmap.Store(&hashmap)
	v2.TransferPageRowHistogram.Observe(float64(len(m)))
}

func (page *TransferHashPage) Transfer(from uint32) (dest types.Rowid, ok bool) {
	v2.TransferPageSinceBornDurationHistogram.Observe(time.Since(page.bornTS).Seconds())
	m := page.hashmap.Load()
	if m == nil {
		diskStart := time.Now()
		m = page.loadTable()
		diskDuration := time.Since(diskStart)
		v2.TransferDiskLatencyHistogram.Observe(diskDuration.Seconds())
	}
	v2.TransferPageTotalHitHistogram.Observe(1)

	memStart := time.Now()
	var data []byte
	if m == nil {
		ok = false
		return
	}
	data, ok = m.M[from]
	if ok {
		dest = types.Rowid(data)
	}
	memDuration := time.Since(memStart)
	v2.TransferMemLatencyHistogram.Observe(memDuration.Seconds())
	return
}

func (page *TransferHashPage) Marshal() []byte {
	m := page.hashmap.Load()
	if m == nil {
		panic("empty hashmap")
	}
	data, _ := proto.Marshal(m)
	return data
}

func (page *TransferHashPage) Unmarshal(data []byte) (*api.HashPageMap, error) {
	m := api.HashPageMap{}
	err := proto.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (page *TransferHashPage) SetPath(path Path) {
	page.path = path
}

func (page *TransferHashPage) loadTable() *api.HashPageMap {
	if page.path.Name == "" {
		return nil
	}

	path := page.path
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
	m := page.hashmap.Load()
	if m != nil {
		return m
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := page.fs.Read(ctx, &ioVector)
	if err != nil {
		logutil.Errorf("[TransferPage] read persist table %v: %v", page.path.Name, err)
		return nil
	}
	defer ioVector.Release()

	m = page.hashmap.Load()
	if m != nil {
		return m
	}
	m, err = page.Unmarshal(ioVector.Entries[0].Data)
	if err != nil {
		return nil
	}
	page.hashmap.Store(m)
	logutil.Infof("[TransferPage] load transfer page %v", page.String())
	v2.TaskMergeTransferPageLengthGauge.Add(float64(page.Length()))
	return m
}

func (page *TransferHashPage) ClearPersistTable() {
	if page.path.Name == "" {
		return
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := page.fs.Delete(ctx, page.path.Name)
	if err != nil {
		logutil.Errorf("[TransferPage] clear transfer table %v: %v", page.path.Name, err)
	}
}

func (page *TransferHashPage) IsPersist() bool {
	return page.hashmap.Load() == nil
}

func InitTransferPageIO() *fileservice.IOVector {
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	return &fileservice.IOVector{
		FilePath: path.Join("transfer", "transfer-"+name.String()),
	}
}

func AddTransferPage(page *TransferHashPage, ioVector *fileservice.IOVector) error {
	data := page.Marshal()
	le := len(ioVector.Entries)
	offset := int64(0)
	if le > 0 {
		offset = ioVector.Entries[le-1].Offset +
			ioVector.Entries[le-1].Size
	}
	ioEntry := fileservice.IOEntry{
		Offset: offset,
		Size:   int64(len(data)),
		Data:   data,
	}
	ioVector.Entries = append(ioVector.Entries, ioEntry)

	return nil
}

func WriteTransferPage(ctx context.Context, fs fileservice.FileService, pages []*TransferHashPage, ioVector fileservice.IOVector) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := fs.Write(ctx, ioVector)
	if err != nil {
		for _, page := range pages {
			page.SetBornTS(page.BornTS().Add(time.Minute))
		}
		logutil.Errorf("[TransferPage] write transfer page error, page count %v", len(pages))
	}
	for i, page := range pages {
		path := Path{
			Name:   ioVector.FilePath,
			Offset: ioVector.Entries[i].Offset,
			Size:   ioVector.Entries[i].Size,
		}
		page.SetPath(path)
	}
}
