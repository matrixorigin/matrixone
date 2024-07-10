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
	bornTS      time.Time
	id          *common.ID // not include blk offset
	hashmap     atomic.Pointer[api.HashPageMap]
	path        Path
	params      TransferHashPageParams
	isTransient bool
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
		params:      params,
		isTransient: isTransient,
	}

	m := api.HashPageMap{M: make(map[uint32][]byte, pageSize)}
	page.hashmap.Store(&m)
	page.OnZeroCB = page.Close

	return page
}

func (page *TransferHashPage) ID() *common.ID    { return page.id }
func (page *TransferHashPage) BornTS() time.Time { return page.bornTS }

const (
	notClear    = uint8(0)
	clearMemory = uint8(1)
	clearDisk   = uint8(2)
)

func (page *TransferHashPage) TTL() uint8 {
	now := time.Now()
	if now.After(page.bornTS.Add(page.params.DiskTTL)) {
		return clearDisk
	}
	if now.After(page.bornTS.Add(page.params.TTL)) {
		return clearMemory
	}
	return notClear
}

func (page *TransferHashPage) Close() {
	logutil.Debugf("Closing %s", page.String())
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

func (page *TransferHashPage) Clear(clearDisk bool) {
	page.ClearTable()
	if clearDisk {
		page.ClearPersistTable()
	}
}

func (page *TransferHashPage) Train(m map[uint32][]byte) {
	hashmap := api.HashPageMap{M: m}
	page.hashmap.Store(&hashmap)
	v2.TransferPageRowHistogram.Observe(float64(len(m)))
}

func (page *TransferHashPage) Transfer(from uint32) (dest types.Rowid, ok bool) {
	v2.TransferPageSinceBornDurationHistogram.Observe(time.Since(page.bornTS).Seconds())
	var m *api.HashPageMap
	if page.hashmap.Load() == nil {
		diskStart := time.Now()
		m = page.loadTable()
		diskDuration := time.Since(diskStart)
		v2.TransferDiskLatencyHistogram.Observe(diskDuration.Seconds())
	}
	v2.TransferPageTotalHitHistogram.Observe(1)

	memStart := time.Now()
	var data []byte
	if m == nil {
		m = page.hashmap.Load()
	}
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
		return nil
	}
	data, _ := proto.Marshal(m)
	return data
}

func (page *TransferHashPage) Unmarshal(data []byte) (*api.HashPageMap, error) {
	if page.hashmap.Load() != nil {
		return nil, nil
	}
	m := api.HashPageMap{}
	err := proto.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	page.hashmap.Store(&m)
	return &m, nil
}

func (page *TransferHashPage) SetPath(path Path) {
	page.path = path
}

func (page *TransferHashPage) ClearTable() {
	m := page.hashmap.Load()
	if m == nil {
		return
	}

	page.hashmap.Store(nil)
	v2.TaskMergeTransferPageSizeGauge.Sub(float64(len(m.M)))
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
	err := FS.Read(context.Background(), &ioVector)
	if err != nil {
		return nil
	}

	m, err2 := page.Unmarshal(ioVector.Entries[0].Data)
	if err2 != nil {
		return nil
	}

	logutil.Infof("load transfer page %v", page.String())
	v2.TaskMergeTransferPageSizeGauge.Add(float64(page.Length()))
	return m
}

func (page *TransferHashPage) ClearPersistTable() {
	if page.path.Name == "" {
		return
	}
	FS.Delete(context.Background(), page.path.Name)
}

func (page *TransferHashPage) IsPersist() bool {
	return page.hashmap.Load() == nil
}
