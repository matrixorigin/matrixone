// Copyright 2026 Matrix Origin
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

package wal

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/stretchr/testify/require"
)

type boundedWALTestDriver struct {
	driver.Driver
	maxSize int
	appends int
}

func (d *boundedWALTestDriver) MaxWALRecordSize() int { return d.maxSize }
func (d *boundedWALTestDriver) Close() error          { return nil }
func (d *boundedWALTestDriver) Append(e *driverEntry.Entry) error {
	d.appends++
	e.DSN = uint64(d.appends)
	e.DoneWithErr(nil)
	return nil
}

func newBoundedWALTestEntry(size int) entry.Entry {
	e := entry.GetBase()
	e.SetType(entry.IOET_WALEntry_Test)
	e.SetInfo(&entry.Info{})
	_ = e.SetPayload(make([]byte, size))
	return e
}

func TestOversizedEntryKeepsLSNContiguous(t *testing.T) {
	driver := &boundedWALTestDriver{maxSize: 256}
	w := NewStore(driver)
	defer func() { require.NoError(t, w.Close()) }()

	oversized := newBoundedWALTestEntry(256)
	lsn, err := w.AppendEntry(GroupUserTxn, oversized)
	var tooLarge *RecordTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Zero(t, lsn)
	require.ErrorAs(t, oversized.WaitDone(), &tooLarge)
	oversized.Free()
	require.Zero(t, w.GetLSNWatermark())
	require.Zero(t, driver.appends)

	accepted := newBoundedWALTestEntry(16)
	lsn, err = w.AppendEntry(GroupUserTxn, accepted)
	require.NoError(t, err)
	require.Equal(t, uint64(1), lsn)
	require.NoError(t, accepted.WaitDone())
	accepted.Free()
	require.Equal(t, 1, driver.appends)
}

func TestLogServiceHandleUsesConfiguredRecordLimit(t *testing.T) {
	_, factory := logservicedriver.NewMockServiceAndClientFactory()
	w := NewLogserviceHandle(factory, 256)
	defer func() { require.NoError(t, w.Close()) }()
	require.Equal(t, 256, w.maxRecordSize)
}
