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

package txnif

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type TxnMemo struct {
	*model.Tree
	IsFlushOrMerge     bool
	isCatalogChanged   bool
	logicalDMLTracking bool
	logicalDMLKnown    bool
	logicalDML         map[uint64]uint64
}

func NewTxnMemo() *TxnMemo {
	return &TxnMemo{
		Tree:            model.NewTree(),
		logicalDMLKnown: true,
	}
}

func (memo *TxnMemo) AddCatalogChange() {
	memo.isCatalogChanged = true
}

func (memo *TxnMemo) HasAnyTableDataChanges() bool {
	return memo.TableCount() > 0
}

func (memo *TxnMemo) HasTableDataChanges(id uint64) bool {
	return memo.HasTable(id)
}

func (memo *TxnMemo) HasCatalogChanges() bool {
	return memo.isCatalogChanged
}

func (memo *TxnMemo) GetDirty() *model.Tree {
	return memo.Tree
}

// EnableLogicalDMLTracking opts this transaction into the V6 WAL extension.
// Enabling a decoded legacy memo does not invent knowledge of its old writes.
func (memo *TxnMemo) EnableLogicalDMLTracking()       { memo.logicalDMLTracking = true }
func (memo *TxnMemo) LogicalDMLTrackingEnabled() bool { return memo.logicalDMLTracking }
func (memo *TxnMemo) LogicalDMLKnown() bool           { return memo.logicalDMLTracking && memo.logicalDMLKnown }

func (memo *TxnMemo) Reset() {
	memo.Tree.Reset()
	memo.isCatalogChanged = false
	memo.logicalDML = nil
	memo.logicalDMLKnown = true
}

// LogicalDMLTables is a read-only view owned by this non-concurrent memo.
func (memo *TxnMemo) LogicalDMLTables() map[uint64]uint64 { return memo.logicalDML }

func (memo *TxnMemo) RecordLogicalDML(database, table uint64) error {
	if !memo.logicalDMLTracking || !memo.logicalDMLKnown {
		return moerr.NewInternalErrorNoCtx("logical DML tracking is not active or known")
	}
	record := memo.GetTable(table)
	if database == 0 || table == 0 || record == nil || record.DbID != database {
		return moerr.NewInternalErrorNoCtx("logical DML identity is not in the dirty memo")
	}
	if memo.logicalDML == nil {
		memo.logicalDML = make(map[uint64]uint64)
	}
	memo.logicalDML[table] = database
	return nil
}

func (memo *TxnMemo) LogicalDMLSize() int64 { return 5 + 16*int64(len(memo.logicalDML)) }

// WriteLogicalDMLTo is separate from the historical memo layout. Only V6 uses it.
func (memo *TxnMemo) WriteLogicalDMLTo(w io.Writer) (n int64, err error) {
	if !memo.logicalDMLTracking || !memo.logicalDMLKnown && len(memo.logicalDML) != 0 ||
		uint64(len(memo.logicalDML)) > uint64(memo.TableCount()) || uint64(len(memo.logicalDML)) > uint64(^uint32(0)) {
		return 0, moerr.NewInternalErrorNoCtx("invalid logical DML memo")
	}
	for table, database := range memo.logicalDML {
		record := memo.GetTable(table)
		if database == 0 || table == 0 || record == nil || record.DbID != database {
			return 0, moerr.NewInternalErrorNoCtx("logical DML identity is not in the dirty memo")
		}
	}
	write := func(value []byte) error {
		written, err := w.Write(value)
		n += int64(written)
		if err == nil && written != len(value) {
			return io.ErrShortWrite
		}
		return err
	}
	known := byte(0)
	if memo.logicalDMLKnown {
		known = 1
	}
	if err = write([]byte{known}); err != nil {
		return
	}
	count := uint32(len(memo.logicalDML))
	if err = write(types.EncodeUint32(&count)); err != nil {
		return
	}
	for table, database := range memo.logicalDML {
		if err = write(types.EncodeUint64(&database)); err != nil {
			return
		}
		if err = write(types.EncodeUint64(&table)); err != nil {
			return
		}
	}
	return
}

func (memo *TxnMemo) ReadLogicalDMLFrom(r io.Reader) (n int64, err error) {
	memo.logicalDML, memo.logicalDMLKnown, memo.logicalDMLTracking = nil, false, false
	var header [5]byte
	var read int
	read, err = io.ReadFull(r, header[:])
	n = int64(read)
	if err != nil {
		return
	}
	count := types.DecodeUint32(header[1:])
	if header[0] > 1 || header[0] == 0 && count != 0 || uint64(count) > uint64(memo.TableCount()) {
		return n, moerr.NewInternalErrorNoCtx("invalid logical DML extension header")
	}
	for i := uint32(0); i < count; i++ {
		var pair [16]byte
		read, err = io.ReadFull(r, pair[:])
		n += int64(read)
		if err != nil {
			return
		}
		database, table := types.DecodeUint64(pair[:8]), types.DecodeUint64(pair[8:])
		record := memo.GetTable(table)
		if database == 0 || table == 0 || record == nil || record.DbID != database {
			return n, moerr.NewInternalErrorNoCtx("logical DML identity is not in the dirty memo")
		}
		if _, duplicate := memo.logicalDML[table]; duplicate {
			return n, moerr.NewInternalErrorNoCtx("duplicate logical DML identity")
		}
		if memo.logicalDML == nil {
			memo.logicalDML = make(map[uint64]uint64, count)
		}
		memo.logicalDML[table] = database
	}
	memo.logicalDMLKnown, memo.logicalDMLTracking = header[0] == 1, true
	return
}

func (memo *TxnMemo) ApproxSize() int64 {
	var (
		size int64
	)

	size += memo.Tree.ApproxSize()
	size += 1 // catalog changed
	if memo.logicalDMLTracking {
		size += memo.LogicalDMLSize()
	}

	return size
}

func (memo *TxnMemo) WriteTo(w io.Writer) (n int64, err error) {
	var tmpn int64
	if tmpn, err = memo.Tree.WriteTo(w); err != nil {
		return
	}
	n += tmpn
	isCatalogChanged := int8(0)
	if memo.isCatalogChanged {
		isCatalogChanged = 1
	}
	if _, err = w.Write(types.EncodeInt8(&isCatalogChanged)); err != nil {
		return
	}
	n += 1
	return
}

func (memo *TxnMemo) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) {
	memo.logicalDML, memo.logicalDMLKnown, memo.logicalDMLTracking = nil, false, false
	memo.Tree.Reset()
	var tmpn int64
	if tmpn, err = memo.Tree.ReadFromWithVersion(r, ver); err != nil {
		return
	}
	n += tmpn
	isCatalogChanged := int8(0)
	if _, err = r.Read(types.EncodeInt8(&isCatalogChanged)); err != nil {
		return
	}
	memo.isCatalogChanged = isCatalogChanged == 1
	n += 1
	return
}
