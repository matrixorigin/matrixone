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

package db

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker/base"
)

type DBScanner interface {
	base.IHBHandle
	RegisterOp(ScannerOp)
}

type ErrHandler interface {
	OnBlockErr(entry *catalog.BlockEntry, err error) error
	OnSegmentErr(entry *catalog.SegmentEntry, err error) error
	OnTableErr(entry *catalog.TableEntry, err error) error
	OnDatabaseErr(entry *catalog.DBEntry, err error) error
}

type NoopErrHandler struct{}

func (h *NoopErrHandler) OnBlockErr(entry *catalog.BlockEntry, err error) error     { return nil }
func (h *NoopErrHandler) OnSegmentErr(entry *catalog.SegmentEntry, err error) error { return nil }
func (h *NoopErrHandler) OnTableErr(entry *catalog.TableEntry, err error) error     { return nil }
func (h *NoopErrHandler) OnDatabaseErr(entry *catalog.DBEntry, err error) error     { return nil }

type dbScanner struct {
	*catalog.LoopProcessor
	db         *DB
	ops        []ScannerOp
	errHandler ErrHandler
	dbmask     *roaring.Bitmap
	tablemask  *roaring.Bitmap
	segmask    *roaring.Bitmap
}

func (scanner *dbScanner) OnStopped() {
	logutil.Infof("DBScanner Stopped")
}

func (scanner *dbScanner) OnExec() {
	scanner.dbmask.Clear()
	scanner.tablemask.Clear()
	scanner.segmask.Clear()
	dbutils.PrintMemStats()
	for _, op := range scanner.ops {
		err := op.PreExecute()
		if err != nil {
			panic(err)
		}
	}
	if err := scanner.db.Catalog.RecurLoop(scanner); err != nil {
		logutil.Errorf("DBScanner Execute: %v", err)
	}
	for _, op := range scanner.ops {
		err := op.PostExecute()
		if err != nil {
			panic(err)
		}
	}
}

func NewDBScanner(db *DB, errHandler ErrHandler) *dbScanner {
	if errHandler == nil {
		errHandler = new(NoopErrHandler)
	}
	scanner := &dbScanner{
		LoopProcessor: new(catalog.LoopProcessor),
		db:            db,
		ops:           make([]ScannerOp, 0),
		errHandler:    errHandler,
		dbmask:        roaring.New(),
		tablemask:     roaring.New(),
		segmask:       roaring.New(),
	}
	scanner.BlockFn = scanner.onBlock
	scanner.SegmentFn = scanner.onSegment
	scanner.PostSegmentFn = scanner.onPostSegment
	scanner.TableFn = scanner.onTable
	scanner.DatabaseFn = scanner.onDatabase
	return scanner
}

func (scanner *dbScanner) RegisterOp(op ScannerOp) {
	scanner.ops = append(scanner.ops, op)
}

func (scanner *dbScanner) onBlock(entry *catalog.BlockEntry) (err error) {
	for _, op := range scanner.ops {
		err = op.OnBlock(entry)
		if err = scanner.errHandler.OnBlockErr(entry, err); err != nil {
			break
		}
	}
	return
}

func (scanner *dbScanner) onPostSegment(entry *catalog.SegmentEntry) (err error) {
	for _, op := range scanner.ops {
		err = op.OnPostSegment(entry)
		if err = scanner.errHandler.OnSegmentErr(entry, err); err != nil {
			break
		}
	}
	return
}

func (scanner *dbScanner) onSegment(entry *catalog.SegmentEntry) (err error) {
	scanner.segmask.Clear()
	for i, op := range scanner.ops {
		if scanner.tablemask.Contains(uint32(i)) {
			scanner.segmask.Add(uint32(i))
			continue
		}
		err = op.OnSegment(entry)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			scanner.segmask.Add(uint32(i))
		}
		if err = scanner.errHandler.OnSegmentErr(entry, err); err != nil {
			break
		}
	}
	if scanner.segmask.GetCardinality() == uint64(len(scanner.ops)) {
		err = moerr.GetOkStopCurrRecur()
	}
	return
}

func (scanner *dbScanner) onTable(entry *catalog.TableEntry) (err error) {
	if entry.IsVirtual() {
		err = moerr.GetOkStopCurrRecur()
		return
	}
	scanner.tablemask.Clear()
	for i, op := range scanner.ops {
		// If the specified op was masked OnDatabase. skip it
		if scanner.dbmask.Contains(uint32(i)) {
			scanner.tablemask.Add(uint32(i))
			continue
		}
		err = op.OnTable(entry)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			scanner.tablemask.Add(uint32(i))
		}
		if err = scanner.errHandler.OnTableErr(entry, err); err != nil {
			break
		}
	}
	if scanner.tablemask.GetCardinality() == uint64(len(scanner.ops)) {
		err = moerr.GetOkStopCurrRecur()
	}
	return
}

func (scanner *dbScanner) onDatabase(entry *catalog.DBEntry) (err error) {
	// if entry.IsSystemDB() {
	// 	err = catalog.ErrStopCurrRecur
	// 	return
	// }
	scanner.dbmask.Clear()
	for i, op := range scanner.ops {
		err = op.OnDatabase(entry)
		if moerr.IsMoErrCode(err, moerr.OkStopCurrRecur) {
			scanner.dbmask.Add(uint32(i))
		}
		if err = scanner.errHandler.OnDatabaseErr(entry, err); err != nil {
			break
		}
	}
	if scanner.dbmask.GetCardinality() == uint64(len(scanner.ops)) {
		err = moerr.GetOkStopCurrRecur()
	}
	return
}
