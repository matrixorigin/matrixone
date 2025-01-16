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

package logservicedriver

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var ErrRecordNotFound = moerr.NewInternalErrorNoCtx("driver read cache: lsn not found")
var ErrAllRecordsRead = moerr.NewInternalErrorNoCtx("driver read cache: all records are read")

type readCache struct {
	psns []uint64
	// PSN -> Record mapping
	records map[uint64]LogEntry
}

func newReadCache() readCache {
	return readCache{
		psns:    make([]uint64, 0),
		records: make(map[uint64]LogEntry),
	}
}

func (c *readCache) clear() {
	c.psns = c.psns[:0]
	for psn := range c.records {
		delete(c.records, psn)
	}
}

func (c *readCache) removeRecord(psn uint64) {
	delete(c.records, psn)
}

func (c *readCache) addRecord(
	psn uint64, e LogEntry,
) (updated bool) {
	if _, ok := c.records[psn]; ok {
		return
	}
	// logutil.Infof("add record %d:%v", psn, e.addr)
	c.records[psn] = e
	c.psns = append(c.psns, psn)
	updated = true
	return
}

func (c *readCache) isEmpty() bool {
	return len(c.records) == 0
}

func (c *readCache) getRecord(psn uint64) (r LogEntry, err error) {
	var ok bool
	if r, ok = c.records[psn]; !ok {
		err = ErrRecordNotFound
	}
	return
}
