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

package gcreqs

import (
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops"
)

var (
	stopSign = errors.New("stop sign")
)

type catalogCompactionRequest struct {
	gc.BaseRequest
	catalog    *metadata.Catalog
	interval   time.Duration
	lastExecTS int64
}

func NewCatalogCompactionRequest(catalog *metadata.Catalog, interval time.Duration) *catalogCompactionRequest {
	req := new(catalogCompactionRequest)
	req.catalog = catalog
	req.interval = interval
	req.Op = ops.Op{
		Impl:   req,
		ErrorC: make(chan error),
	}
	return req
}

func (req *catalogCompactionRequest) IncIteration() {}

func (req *catalogCompactionRequest) updateExecTS() { req.lastExecTS = time.Now().UnixMilli() }
func (req *catalogCompactionRequest) checkInterval() bool {
	now := time.Now().UnixMilli()
	return now-req.lastExecTS >= req.interval.Milliseconds()
}

func (req *catalogCompactionRequest) hardDeleteDatabase(db *metadata.Database) {
	processor := new(metadata.LoopProcessor)
	processor.TableFn = func(t *metadata.Table) error {
		t.RLock()
		defer t.RUnlock()
		if !t.IsHardDeletedLocked() || !t.HasCommittedLocked() {
			return stopSign
		}
		return nil
	}
	var canDelete bool
	db.RLock()
	err := db.LoopLocked(processor)
	if err != nil {
		db.RUnlock()
		return
	}
	if db.CanHardDeleteLocked() {
		canDelete = true
	}
	db.RUnlock()
	if canDelete {
		if err := db.SimpleHardDelete(); err != nil {
			panic(err)
		}
		logutil.Infof("%s | HardDeleted | [GC]", db.Repr())
	}
}

func (req *catalogCompactionRequest) hardDeleteDatabases(dbs []*metadata.Database) {
	for _, db := range dbs {
		req.hardDeleteDatabase(db)
	}
}

func (req *catalogCompactionRequest) Execute() error {
	req.Next = req
	if !req.checkInterval() {
		return nil
	}
	return req.DoRun()
}

func (req *catalogCompactionRequest) DoRun() error {
	deleted := make([]*metadata.Database, 0, 4)
	processor := new(metadata.LoopProcessor)
	processor.DatabaseFn = func(database *metadata.Database) error {
		if database.IsDeleted() {
			if !database.IsHardDeleted() {
				deleted = append(deleted, database)
			}
		}
		return nil
	}
	req.catalog.RLock()
	req.catalog.LoopLocked(processor)
	req.catalog.RUnlock()
	if len(deleted) > 0 {
		req.hardDeleteDatabases(deleted)
	}

	req.catalog.Compact(nil, nil)
	req.updateExecTS()
	return nil
}
