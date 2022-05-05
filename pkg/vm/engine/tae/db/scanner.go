package db

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/worker/base"
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
}

func (scanner *dbScanner) OnStopped() {
	logutil.Infof("DBScanner Stopped")
}

func (scanner *dbScanner) OnExec() {
	for _, op := range scanner.ops {
		op.PreExecute()
	}
	if err := scanner.db.Catalog.RecurLoop(scanner); err != nil {
		logutil.Errorf("DBScanner Execute: %v", err)
	}
	for _, op := range scanner.ops {
		op.PostExecute()
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
	}
	scanner.BlockFn = scanner.onBlock
	scanner.SegmentFn = scanner.onSegment
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

func (scanner *dbScanner) onSegment(entry *catalog.SegmentEntry) (err error) {
	for _, op := range scanner.ops {
		err = op.OnSegment(entry)
		if err = scanner.errHandler.OnSegmentErr(entry, err); err != nil {
			break
		}
	}
	return
}

func (scanner *dbScanner) onTable(entry *catalog.TableEntry) (err error) {
	for _, op := range scanner.ops {
		err = op.OnTable(entry)
		if err = scanner.errHandler.OnTableErr(entry, err); err != nil {
			break
		}
	}
	return
}

func (scanner *dbScanner) onDatabase(entry *catalog.DBEntry) (err error) {
	for _, op := range scanner.ops {
		err = op.OnDatabase(entry)
		if err = scanner.errHandler.OnDatabaseErr(entry, err); err != nil {
			break
		}
	}
	return
}
