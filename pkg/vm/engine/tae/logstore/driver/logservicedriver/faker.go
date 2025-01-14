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
	"context"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

func makeMockLogRecord(pos int) logservice.LogRecord {
	return logservice.LogRecord{
		Type: pb.UserRecord,
		Lsn:  uint64(pos),
	}
}

func noopAppendSkipCmd(_ context.Context, skipMap map[uint64]uint64) error {
	return nil
}

func mockUnmarshalLogRecordFactor(d *mockDriver) func(r logservice.LogRecord) *recordEntry {
	return func(r logservice.LogRecord) *recordEntry {
		pos := int(r.Lsn)

		// [CmdType,PSN,DSN-S,DSN-E,SAFE]
		spec := d.recordSpecs[pos]
		e := newRecordEntry()
		e.unmarshaled.Store(1)
		e.appended = uint64(spec[4])
		e.cmdType = CmdType(spec[0])
		switch e.cmdType {
		case Cmd_Normal:
			for dsn := spec[2]; dsn <= spec[3]; dsn++ {
				e.addr[dsn] = dsn
				le := entry.NewEmptyEntry()
				le.DSN = dsn
				e.entries = append(e.entries, le)
			}
		case Cmd_SkipDSN:
			e.cmd = NewReplayCmd(d.skipMap[spec[1]])
		}
		return e
	}
}

func mockHandleFactory(fromDSN uint64, onApplyCB func(*entry.Entry)) func(*entry.Entry) (replayEntryState driver.ReplayEntryState) {
	return func(e *entry.Entry) (replayEntryState driver.ReplayEntryState) {
		if e.DSN < fromDSN {
			return driver.RE_Truncate
		}
		if onApplyCB != nil {
			onApplyCB(e)
		}
		return driver.RE_Nomal
	}
}

type mockDriver struct {
	truncatedPSN uint64
	recordSpecs  [][5]uint64
	skipMap      map[uint64]map[uint64]uint64
	maxClient    int
}

func newMockDriver(
	truncatedPSN uint64,
	recordSpecs [][5]uint64,
	maxClient int,
) *mockDriver {
	return &mockDriver{
		truncatedPSN: truncatedPSN,
		recordSpecs:  recordSpecs,
		skipMap:      make(map[uint64]map[uint64]uint64),
		maxClient:    maxClient,
	}
}

func (d *mockDriver) GetMaxClient() int {
	return d.maxClient
}

func (d *mockDriver) addSkipMap(psn, skipDSN, skipPSN uint64) {
	if _, ok := d.skipMap[psn]; !ok {
		d.skipMap[psn] = make(map[uint64]uint64)
	}
	d.skipMap[psn][skipDSN] = skipPSN
}

func (d *mockDriver) getClientForWrite() (*wrappedClient, uint64) {
	return nil, 0
}

func (d *mockDriver) getTruncatedPSNFromBackend(ctx context.Context) (uint64, error) {
	return d.truncatedPSN, nil
}

func (d *mockDriver) readFromBackend(
	ctx context.Context, firstPSN uint64, maxSize int,
) (nextPSN uint64, records []logservice.LogRecord, err error) {
	for i, spec := range d.recordSpecs {
		if spec[1] >= firstPSN {
			records = append(records, makeMockLogRecord(i))
		}
		if len(records) >= maxSize {
			return spec[1] + 1, records, nil
		}
	}
	if len(d.recordSpecs) == 0 {
		nextPSN = firstPSN
	} else {
		nextPSN = d.recordSpecs[len(d.recordSpecs)-1][1] + 1
	}

	return
}
