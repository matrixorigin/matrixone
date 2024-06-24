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
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type HashPageTable = TransferTable[*TransferHashPage]

type BlockRead interface {
	LoadTableByBlock(loc objectio.Location, fs fileservice.FileService) (bat *batch.Batch, release func(), err error)
}

var (
	RD BlockRead
	//FS     fileservice.FileService
	rdOnce sync.Once
	fsOnce sync.Once
)

func SetBlockRead(rd BlockRead) {
	rdOnce.Do(func() {
		RD = rd
	})
	logutil.Infof("[TransferHashPage] RD init %v", RD != nil)
}

//func SetFileService(fs fileservice.FileService) {
//	fsOnce.Do(func() {
//		FS = fs
//	})
//	logutil.Infof("[TransferHashPage] FS init %v", FS != nil)
//}

type TransferHashPageParams struct {
	FS      fileservice.FileService
	TTL     time.Duration
	DiskTTL time.Duration
}

type Option func(*TransferHashPageParams)

func WithFileService(fs fileservice.FileService) Option {
	return func(params *TransferHashPageParams) {
		params.FS = fs
	}
}

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
	latch sync.Mutex
	common.RefHelper
	bornTS      time.Time
	id          *common.ID // not include blk offset
	hashmap     map[uint32]types.Rowid
	loc         objectio.Location
	params      TransferHashPageParams
	isTransient bool
	isPersisted bool
}

func NewTransferHashPage(id *common.ID, ts time.Time, isTransient bool, opts ...Option) *TransferHashPage {
	params := TransferHashPageParams{
		TTL:     10 * time.Second,
		DiskTTL: 10 * time.Minute,
	}
	for _, opt := range opts {
		opt(&params)
	}

	page := &TransferHashPage{
		bornTS:      ts,
		id:          id,
		hashmap:     make(map[uint32]types.Rowid),
		params:      params,
		isTransient: isTransient,
		isPersisted: false,
	}
	page.OnZeroCB = page.Close

	go func(page *TransferHashPage) {
		time.Sleep(params.TTL)
		page.clearTable()
	}(page)

	go func(page *TransferHashPage) {
		time.Sleep(params.DiskTTL)
		page.clearPersistTable()
	}(page)

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
	v2.TransferRowTotalCounter.Inc()
}

func (page *TransferHashPage) Transfer(from uint32) (dest types.Rowid, ok bool) {
	page.latch.Lock()
	if page.isPersisted && page.loc == nil {
		logutil.Infof("[TransferHashPage] persist table is cleared")
		page.latch.Unlock()
		return types.Rowid{}, false
	}
	if page.isPersisted {
		diskStart := time.Now()
		page.loadTable()
		v2.TransferDiskHitCounter.Inc()
		diskDuration := time.Since(diskStart)
		v2.TransferDurationDiskHistogram.Observe(diskDuration.Seconds())
	} else {
		v2.TransferMemoryHitCounter.Inc()
	}
	v2.TransferTotalHitCounter.Inc()
	page.latch.Unlock()

	memstart := time.Now()
	dest, ok = page.hashmap[from]
	memduration := time.Since(memstart)
	v2.TransferDurationMemoryHistogram.Observe(memduration.Seconds())

	var str string
	if ok {
		str = "succeeded"
	} else {
		str = "failed"
	}
	logutil.Infof("[TransferHashPage] get transfer %v", str)
	return
}

func (page *TransferHashPage) Marshal() []byte {
	m := make(map[uint32][]byte)
	for k, v := range page.hashmap {
		m[k] = v[:]
	}
	mapping := &api.HashPageMap{M: m}
	data, _ := proto.Marshal(mapping)
	return data
}

func (page *TransferHashPage) Unmarshal(data []byte) error {
	var mapping api.HashPageMap
	err := proto.Unmarshal(data, &mapping)
	if err != nil {
		return err
	}

	for key, value := range mapping.M {
		var v [24]byte
		copy(v[:], value)
		page.hashmap[key] = v
	}
	return nil
}

func (page *TransferHashPage) SetLocation(location objectio.Location) {
	page.latch.Lock()
	defer page.latch.Unlock()
	page.loc = location
}

func (page *TransferHashPage) clearTable() {
	page.latch.Lock()
	defer page.latch.Unlock()
	logutil.Infof("[TransferHashPage] clear hash table")
	clear(page.hashmap)
	page.isPersisted = true
}

func (page *TransferHashPage) loadTable() {
	logutil.Infof("[TransferHashPage] load persist table, objectname: %v", page.loc.Name().String())
	if page.loc == nil {
		return
	}

	logutil.Infof("[TransferHashPage] loc %v, rd %v, fs %v", page.loc.Name().String(), RD, page.params.FS)

	var bat *batch.Batch
	var release func()
	bat, release, err := RD.LoadTableByBlock(page.loc, page.params.FS)
	defer release()
	if err != nil {
		logutil.Errorf("[TransferHashPage] load table failed, %v", err)
		return
	}
	err = page.Unmarshal(bat.Vecs[0].GetBytesAt(0))
	if err != nil {
		return
	}

	v2.TransferRowHitCounter.Add(float64(len(page.hashmap)))

	page.isPersisted = false

	go func(page *TransferHashPage) {
		time.Sleep(page.params.TTL)
		page.clearTable()
	}(page)
}

func (page *TransferHashPage) clearPersistTable() {
	page.latch.Lock()
	defer page.latch.Unlock()
	if page.loc == nil {
		return
	}
	logutil.Infof("[TransferHashPage] clear persist table, objectname: %v", page.loc.Name().String())
	page.params.FS.Delete(context.Background(), page.loc.Name().String())
	page.loc = nil
}

func (page *TransferHashPage) IsPersist() bool {
	page.latch.Lock()
	defer page.latch.Unlock()
	return page.isPersisted
}
