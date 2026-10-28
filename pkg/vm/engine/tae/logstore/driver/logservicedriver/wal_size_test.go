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

package logservicedriver

import (
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/stretchr/testify/require"
)

func TestCommitGroupsUseEncodedSize(t *testing.T) {
	backend := NewMockBackend()
	first := entry.MockEntryWithPayload(make([]byte, 64))
	second := entry.MockEntryWithPayload(make([]byte, 64))
	require.NoError(t, first.Prepare())
	maxSize := EmptyLogEntrySize + first.GetSize() + OneEntryFooterSize
	cfg := NewConfig("", WithConfigMockClient(backend), WithConfigOptMaxWALRecordSize(maxSize))
	d := NewLogServiceDriver(&cfg)

	d.onCommitIntents(first, second)
	require.NoError(t, first.WaitDone())
	require.NoError(t, second.WaitDone())
	require.NoError(t, d.Close())
	defer first.Entry.Free()
	defer second.Entry.Free()

	backend.RLock()
	defer backend.RUnlock()
	require.Len(t, backend.store, 2)
	dsns := make([]uint64, 0, len(backend.store))
	for _, record := range backend.store {
		walRecord := LogEntry(record.Payload())
		require.Equal(t, maxSize, len(walRecord))
		require.Equal(t, uint32(1), walRecord.GetEntryCount())
		dsns = append(dsns, walRecord.GetStartDSN())
	}
	sort.Slice(dsns, func(i, j int) bool { return dsns[i] < dsns[j] })
	require.Equal(t, []uint64{1, 2}, dsns)
}
