// Copyright 2021 - 2023 Matrix Origin
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

package cache

import (
	"sync"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestNewTableGuard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	g := newTableGuard()
	require.NotNil(t, g)
	require.Equal(t, 0, len(g.mu.deletedTables))
	require.Equal(t, defaultDeletedTableSize, cap(g.mu.deletedTables))
}

func TestPushDeletedTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	g := newTableGuard()
	require.NotNil(t, g)
	item := &TableItem{deleted: true}
	g.pushDeletedTable(item)
	require.Equal(t, 1, len(g.mu.deletedTables))
	item = &TableItem{deleted: false}
	g.pushDeletedTable(item)
	require.Equal(t, 1, len(g.mu.deletedTables))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.pushDeletedTable(&TableItem{deleted: true})
		}()
	}
	wg.Wait()
	require.Equal(t, 51, len(g.mu.deletedTables))
}

func TestGetDeletedTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	g := newTableGuard()
	require.NotNil(t, g)

	for i := 0; i < 10; i++ {
		item := &TableItem{deleted: true, Ts: timestamp.Timestamp{PhysicalTime: 1}}
		g.pushDeletedTable(item)
	}
	require.Equal(t, 10, len(g.mu.deletedTables))

	tables := g.getDeletedTables(3, timestamp.Timestamp{PhysicalTime: 2})
	require.Equal(t, 6, len(tables))

	tables = g.getDeletedTables(11, timestamp.Timestamp{PhysicalTime: 2})
	require.Equal(t, 0, len(tables))

	g.mu.gcCount = 10
	require.Panicsf(t, func() {
		g.getDeletedTables(0, timestamp.Timestamp{PhysicalTime: 2})
	}, "")
}

func TestGetDeletedTableIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	g := newTableGuard()
	require.NotNil(t, g)
	for i := 0; i < 10; i++ {
		item := &TableItem{deleted: true}
		g.pushDeletedTable(item)
	}
	require.Equal(t, 9, g.getDeletedTableIndex())
}

func TestTableGuardGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	g := newTableGuard()
	require.NotNil(t, g)

	for i := 0; i < 10; i++ {
		item := &TableItem{deleted: true, Ts: timestamp.Timestamp{PhysicalTime: int64(i)}}
		g.pushDeletedTable(item)
	}
	require.Equal(t, 9, g.getDeletedTableIndex())

	g.GC(timestamp.Timestamp{PhysicalTime: 3})
	require.Equal(t, 9, g.getDeletedTableIndex())
	require.Equal(t, 7, len(g.mu.deletedTables))
	require.Equal(t, 5, len(g.getDeletedTables(4, timestamp.Timestamp{PhysicalTime: 20})))

	g.GC(timestamp.Timestamp{PhysicalTime: 5})
	require.Equal(t, 9, g.getDeletedTableIndex())
	require.Equal(t, 5, len(g.mu.deletedTables))
	require.Equal(t, 5, len(g.getDeletedTables(4, timestamp.Timestamp{PhysicalTime: 20})))
}
