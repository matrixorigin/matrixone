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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type HashPageTable = TransferTable[*TransferHashPage]

type BlockRead interface {
	LoadTableByBlock(loc objectio.Location, fs fileservice.FileService) (bat *batch.Batch, release func(), err error)
}

type TransferHashPage struct {
	common.RefHelper
	bornTS      time.Time
	id          *common.ID // not include blk offset
	hashmap     map[uint32]types.Rowid
	location    objectio.Location
	fs          fileservice.FileService
	rd          BlockRead
	isTransient bool
	isPersisted bool
}

func NewTransferHashPage(id *common.ID, ts time.Time, isTransient bool, fs fileservice.FileService, rd BlockRead) *TransferHashPage {
	page := &TransferHashPage{
		bornTS:      ts,
		id:          id,
		hashmap:     make(map[uint32]types.Rowid),
		fs:          fs,
		rd:          rd,
		isTransient: isTransient,
		isPersisted: false,
	}
	page.OnZeroCB = page.Close

	go func(page *TransferHashPage) {
		time.Sleep(10 * time.Second)
		page.clearTable()
	}(page)

	go func(page *TransferHashPage) {
		time.Sleep(10 * time.Minute)
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
}

func (page *TransferHashPage) Transfer(from uint32) (dest types.Rowid, ok bool) {
	flag := page.isPersisted

	if flag && page.location == nil {
		return types.Rowid{}, false
	}

	if page.isPersisted {
		logutil.Infof("transfer loadding table")
		page.loadTable()
	}

	dest, ok = page.hashmap[from]
	return
}

func toProtoRowid(rowid [24]byte) api.RowId {
	return api.RowId{Id: rowid[:]}
}

func fromProtoRowid(protoRowid api.RowId) [24]byte {
	var rowid [24]byte
	copy(rowid[:], protoRowid.Id)
	return rowid
}

func (page *TransferHashPage) Marshal() []byte {
	m := make(map[uint32]api.RowId)
	for k, v := range page.hashmap {
		m[k] = toProtoRowid(v)
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
		page.hashmap[key] = fromProtoRowid(value)
	}
	return nil
}

func (page *TransferHashPage) SetLocation(location objectio.Location) {
	page.location = location
}

func (page *TransferHashPage) clearTable() {
	logutil.Infof("transfer clear hash table")
	clear(page.hashmap)
	page.isPersisted = true
}

func (page *TransferHashPage) loadTable() {
	logutil.Infof("transfer load persist table, objectname: %v", page.location.Name().String())
	if page.location == nil {
		return
	}
	page.isPersisted = false

	var bat *batch.Batch
	var release func()
	bat, release, err := page.rd.LoadTableByBlock(page.location, page.fs)
	if err != nil {
		return
	}
	err = page.Unmarshal(bat.Vecs[0].GetBytesAt(0))
	if err != nil {
		release()
		return
	}

	go func(page *TransferHashPage) {
		time.Sleep(10 * time.Second)
		page.clearTable()
	}(page)
}

func (page *TransferHashPage) clearPersistTable() {
	if page.location == nil {
		return
	}
	logutil.Infof("transfer clear persist table, objectname: %v", page.location.Name().String())
	page.fs.Delete(context.Background(), page.location.Name().String())
	page.location = nil
}
