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

package logservice_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	driverentry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/stretchr/testify/require"
)

func TestRecoveredWALPayloadIsTNReplayable(t *testing.T) {
	const dsn = 100
	wantPayload := []byte("tn-replay-payload")
	tnEntry := driverentry.MockEntryWithPayload(wantPayload)
	tnEntry.DSN = dsn
	defer tnEntry.Entry.Free()

	writer := logservicedriver.NewLogEntryWriter()
	require.NoError(t, writer.AppendEntry(tnEntry))
	writer.SetSafeDSN(dsn - 1)
	raw, err := writer.Finish()
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "wal_data.bin")
	require.NoError(t, logservice.WriteWALDataFileForTNTest(path, []logservice.WALEntry{{
		DSN:        dsn,
		SafeDSN:    dsn - 1,
		RaftIndex:  1,
		RaftTerm:   1,
		EntryCount: 1,
		RawData:    raw,
	}}))

	payloads, err := logservice.ValidateAndLoadWALDataForTNTest(context.Background(), path)
	require.NoError(t, err)
	require.Len(t, payloads, 1)

	decoded, err := logservicedriver.DecodeLogEntry(payloads[0], nil)
	require.NoError(t, err)
	require.NoError(t, decoded.ForEachEntry(func(entry *driverentry.Entry) {
		defer entry.Entry.Free()
		require.Equal(t, uint64(dsn), entry.DSN)
		require.True(t, bytes.Equal(wantPayload, entry.Entry.GetPayload()))
	}))
}
