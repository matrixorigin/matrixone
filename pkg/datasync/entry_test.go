// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	"github.com/stretchr/testify/assert"
)

type mockEntry struct {
	entryType   uint16
	version     uint16
	meta        logservicedriver.Meta
	entries     []*driverEntry.Entry
	payloadSize uint64
}

func newMockEntry() *mockEntry {
	return &mockEntry{
		entryType: objectio.IOET_ObjMeta,
	}
}

func (m *mockEntry) appendEntry(e *driverEntry.Entry) {
	m.entries = append(m.entries, e)
	m.meta.AddAddr(e.Lsn, m.payloadSize)
	m.payloadSize += uint64(e.GetSize())
}

func (m *mockEntry) WriteTo(w io.Writer) (int64, error) {
	var n int64
	if _, err := w.Write(types.EncodeUint16(&m.entryType)); err != nil {
		return 0, err
	}
	n += 2
	if _, err := w.Write(types.EncodeUint16(&m.version)); err != nil {
		return 0, err
	}
	n += 2
	nn, err := m.meta.WriteTo(w)
	if err != nil {
		return 0, err
	}
	n += nn
	if m.meta.GetType() == logservicedriver.TNormal {
		for _, e := range m.entries {
			nn, err := e.WriteTo(w)
			if err != nil {
				return 0, err
			}
			n += nn
		}
	}
	return 0, nil
}

func runWithBaseEnv(fn func(cat *catalog.Catalog, txn txnif.AsyncTxn) error) error {
	cat := catalog.MockCatalog()
	defer cat.Close()
	txnMgr := txnbase.NewTxnManager(
		catalog.MockTxnStoreFactory(cat),
		catalog.MockTxnFactory(cat),
		types.NewMockHLCClock(1),
	)
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()
	txn, _ := txnMgr.StartTxn(nil)
	return fn(cat, txn)
}

type parameter struct {
	metaType  logservicedriver.MetaType
	groupType uint32
}

func newParameter() *parameter {
	return &parameter{
		metaType:  logservicedriver.TNormal,
		groupType: wal.GroupPrepare,
	}
}

func (p *parameter) withGroupType(t uint32) *parameter {
	p.groupType = t
	return p
}

func (p *parameter) withMetaType(t logservicedriver.MetaType) *parameter {
	p.metaType = t
	return p
}

func generateCmdPayload(param parameter, loc objectio.Location) ([]byte, error) {
	var ret []byte
	if err := runWithBaseEnv(func(cat *catalog.Catalog, txn txnif.AsyncTxn) error {
		temp := os.TempDir()
		dbName := "db"
		tbName := "tb"
		db, err := cat.CreateDBEntry(dbName, "", "", txn)
		if err != nil {
			return err
		}
		schema := catalog.MockSchema(1, 0)
		schema.Name = tbName
		tb, err := db.CreateTableEntry(schema, txn, nil)
		if err != nil {
			return err
		}

		dataFactory := tables.NewDataFactory(nil, temp)
		obj := catalog.NewObjectEntry(
			tb,
			txn,
			objectio.ZeroObjectStats,
			dataFactory.MakeObjectFactory(),
			false,
		)

		if err := objectio.SetObjectStatsLocation(&obj.ObjectStats, loc); err != nil {
			return err
		}
		if err := objectio.SetObjectStatsSize(&obj.ObjectStats, 10); err != nil {
			return err
		}
		tb.AddEntryLocked(obj)

		cmd, err := obj.MakeCommand(0)
		if err != nil {
			return err
		}
		txnCmd := txnbase.NewTxnCmd(1024 * 10)
		txnCmd.AddCmd(cmd)
		data, err := txnCmd.MarshalBinary()
		if err != nil {
			return err
		}

		baseEntry := entry.GetBase()
		err = baseEntry.SetPayload(data)
		if err != nil {
			return err
		}
		baseEntry.SetType(entry.IOET_WALEntry_Test)
		baseEntry.SetVersion(entry.IOET_WALEntry_V1)
		baseEntry.SetInfo(&entry.Info{
			Group: param.groupType,
		})
		drEntry := driverEntry.NewEntry(baseEntry)
		// prepare write, set the info buf
		drEntry.Entry.PrepareWrite()

		me := newMockEntry()
		me.meta.SetType(param.metaType)
		me.appendEntry(drEntry)
		var buf bytes.Buffer
		_, err = me.WriteTo(&buf)
		if err != nil {
			return err
		}
		ret = buf.Bytes()
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func generateCkpPayload(data []byte) ([]byte, error) {
	var ret []byte
	baseEntry := entry.GetBase()
	err := baseEntry.SetPayload(data)
	if err != nil {
		return nil, err
	}
	baseEntry.SetType(entry.IOET_WALEntry_Test)
	baseEntry.SetVersion(entry.IOET_WALEntry_V1)
	baseEntry.SetInfo(&entry.Info{
		Group: store.GroupFiles,
	})
	drEntry := driverEntry.NewEntry(baseEntry)
	// prepare write, set the info buf
	drEntry.Entry.PrepareWrite()

	me := newMockEntry()
	me.meta.SetType(logservicedriver.TNormal)
	me.appendEntry(drEntry)
	var buf bytes.Buffer
	_, err = me.WriteTo(&buf)
	if err != nil {
		return nil, err
	}
	ret = buf.Bytes()
	return ret, nil
}
func genLocation(
	uid uuid.UUID, num uint16, alg uint8, offset, length, originSize uint32,
) objectio.Location {
	id := objectio.Segmentid(uid)
	objName := objectio.BuildObjectName(&id, num)
	objExtent := objectio.NewExtent(alg, offset, length, originSize)
	return objectio.BuildLocation(objName, objExtent, 10, 1)
}

func genRecord(payload []byte, upstreamLsn uint64) logservice.LogRecord {
	p := make([]byte, len(payload)+12)
	binaryEnc.PutUint32(p, uint32(logservice.UserEntryUpdate))
	binaryEnc.PutUint64(p[headerSize:], upstreamLsn)
	copy(p[12:], payload)
	rec := logservice.LogRecord{
		Data: p,
	}
	return rec
}

func TestEntry_ParseLocation(t *testing.T) {
	t.Run("invalid record type", func(t *testing.T) {
		rec := logservice.LogRecord{
			Type: logservice.Internal,
		}
		assert.Equal(t, []string(nil), getLocations(rec))
	})

	t.Run("invalid header size", func(t *testing.T) {
		rec := logservice.LogRecord{
			Data: make([]byte, 2),
		}
		assert.Equal(t, []string(nil), getLocations(rec))
	})

	t.Run("read buffer error", func(t *testing.T) {
		rec := logservice.LogRecord{
			Data: make([]byte, 22),
		}
		assert.Equal(t, []string(nil), getLocations(rec))
	})

	t.Run("invalid meta type", func(t *testing.T) {
		p, err := generateCmdPayload(
			*newParameter().withMetaType(logservicedriver.TReplay),
			genLocation(uuid.New(), 0, 0, 0, 0, 0),
		)
		assert.NoError(t, err)
		assert.NotNil(t, p)
		assert.Equal(t, []string(nil), getLocations(genRecord(p, 0)))
	})

	t.Run("invalid group type", func(t *testing.T) {
		p, err := generateCmdPayload(
			*newParameter().withGroupType(wal.GroupC),
			genLocation(uuid.New(), 0, 0, 0, 0, 0),
		)
		assert.NoError(t, err)
		assert.NotNil(t, p)
		assert.Equal(t, []string(nil), getLocations(genRecord(p, 0)))
	})

	t.Run("ckp, invalid payload", func(t *testing.T) {
		p, err := generateCmdPayload(
			*newParameter().withGroupType(store.GroupFiles),
			genLocation(uuid.New(), 0, 0, 0, 0, 0),
		)
		assert.NoError(t, err)
		assert.NotNil(t, p)
		assert.Panics(t, func() {
			getLocations(genRecord(p, 0))
		})
	})

	t.Run("ok, common", func(t *testing.T) {
		rec, fileName, err := genRecordWithLocation(8, 0)
		assert.NoError(t, err)
		assert.Equal(t,
			[]string{fmt.Sprintf("%s_0_0_0_0_0_0", fileName)},
			getLocations(rec),
		)
	})

	t.Run("ok, ckp", func(t *testing.T) {
		files := []string{
			"file1.txt",
			"file2.txt",
		}
		rec, err := genRecordWithCkpFiles(files, 0)
		assert.NoError(t, err)
		assert.Equal(t,
			files,
			getLocations(rec),
		)
	})
}

func genRecordWithLocation(num uint16, upstreamLsn uint64) (logservice.LogRecord, string, error) {
	uid := uuid.New()
	var alg uint8 = 0
	var offset uint32 = 0
	var length uint32 = 0
	var originSize uint32 = 0
	p, err := generateCmdPayload(
		*newParameter(),
		genLocation(uid, num, alg, offset, length, originSize),
	)
	if err != nil {
		return logservice.LogRecord{}, "", err
	}
	fileName := fmt.Sprintf("%s_%05d", uid.String(), num)
	return genRecord(p, upstreamLsn), fileName, nil
}

func genRecordWithCkpFiles(files []string, upstreamLsn uint64) (logservice.LogRecord, error) {
	vec := containers.NewVector(types.T_char.ToType())
	for _, file := range files {
		vec.Append([]byte(file), false)
	}
	defer vec.Close()
	buf, err := vec.GetDownstreamVector().MarshalBinary()
	if err != nil {
		return logservice.LogRecord{}, err
	}
	payload, err := generateCkpPayload(buf)
	if err != nil {
		return logservice.LogRecord{}, err
	}
	return genRecord(payload, upstreamLsn), nil
}
