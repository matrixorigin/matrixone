// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func testConfig(objectMinOSize uint32, maxOneRun int) *BasicPolicyConfig {
	return &BasicPolicyConfig{
		ObjectMinOsize: objectMinOSize,
		MergeMaxOneRun: maxOneRun,
	}
}

func newSortedDataEntryWithTableEntry(t *testing.T, tbl *catalog.TableEntry, txn txnif.AsyncTxn, v1, v2 int32, size uint32) *catalog.ObjectEntry {
	zm := index.NewZM(types.T_int32, 0)
	index.UpdateZM(zm, types.EncodeInt32(&v1))
	index.UpdateZM(zm, types.EncodeInt32(&v2))
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zm))
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, size))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 2))
	entry, err := tbl.CreateObject(txn, &objectio.CreateObjOpt{
		Stats: stats,
	}, nil)
	require.NoError(t, err)
	return entry
}

func newSortedTombstoneEntryWithTableEntry(t *testing.T, tbl *catalog.TableEntry, txn txnif.AsyncTxn, v1, v2 types.Rowid) *catalog.ObjectEntry {
	zm := index.NewZM(types.T_Rowid, 0)
	index.UpdateZM(zm, v1[:])
	index.UpdateZM(zm, v2[:])
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsLocation(stats, objectio.NewRandomLocation(2, 1111)))
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zm))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 2))
	entry := catalog.NewObjectEntry(tbl, txn, *stats, nil, true)
	entry.GetLastMVCCNode().Txn = nil
	tbl.AddEntryLocked(entry)
	return entry
}

func newSortedTestObjectEntry(t *testing.T, v1, v2 int32, size uint32) *catalog.ObjectEntry {
	zm := index.NewZM(types.T_int32, 0)
	index.UpdateZM(zm, types.EncodeInt32(&v1))
	index.UpdateZM(zm, types.EncodeInt32(&v2))
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zm))
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, size))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 2))
	return &catalog.ObjectEntry{
		ObjectMVCCNode: catalog.ObjectMVCCNode{ObjectStats: *stats},
	}
}

func newTestObjectEntryWithRowCnt(t *testing.T, size, rowCnt uint32, isTombstone bool) *catalog.ObjectEntry {
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, size))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, rowCnt))

	return &catalog.ObjectEntry{
		ObjectMVCCNode: catalog.ObjectMVCCNode{ObjectStats: *stats},
		ObjectNode:     catalog.ObjectNode{IsTombstone: isTombstone},
	}
}

func newTestObjectEntry(t *testing.T, size uint32, isTombstone bool) *catalog.ObjectEntry {
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsOriginSize(stats, size))

	return &catalog.ObjectEntry{
		ObjectMVCCNode: catalog.ObjectMVCCNode{ObjectStats: *stats},
		ObjectNode:     catalog.ObjectNode{IsTombstone: isTombstone},
	}
}

func TestPolicyBasic(t *testing.T) {
	common.IsStandaloneBoost.Store(true)
	p := newBasicPolicy()

	// only schedule objects whose size < cfg.objectMinOSize
	p.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{Extra: &api.SchemaExtra{BlockMaxRows: options.DefaultBlockMaxRows}}))
	cfg := testConfig(100, 3)
	require.True(t, p.onObject(newTestObjectEntry(t, 10, false), cfg))
	require.True(t, p.onObject(newTestObjectEntry(t, 20, false), cfg))
	require.False(t, p.onObject(newTestObjectEntry(t, 120, false), cfg))
	result := p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 2, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)

	// only schedule objects less than cfg.maxOneRun
	p.resetForTable(catalog.MockStaloneTableEntry(1, &catalog.Schema{Extra: &api.SchemaExtra{BlockMaxRows: options.DefaultBlockMaxRows}}))
	cfg = testConfig(100, 2)
	require.True(t, p.onObject(newTestObjectEntry(t, 10, false), cfg))
	require.True(t, p.onObject(newTestObjectEntry(t, 20, false), cfg))
	require.False(t, p.onObject(newTestObjectEntry(t, 30, false), cfg))
	result = p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 2, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)

	// basic policy do not schedule tombstones
	p.resetForTable(catalog.MockStaloneTableEntry(2, &catalog.Schema{Extra: &api.SchemaExtra{BlockMaxRows: options.DefaultBlockMaxRows}}))
	cfg = testConfig(100, 2)
	require.False(t, p.onObject(newTestObjectEntry(t, 10, true), cfg))
	require.False(t, p.onObject(newTestObjectEntry(t, 20, true), cfg))
	result = p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 0, len(result))

	// memory limit
	p.resetForTable(catalog.MockStaloneTableEntry(2, &catalog.Schema{Extra: &api.SchemaExtra{BlockMaxRows: options.DefaultBlockMaxRows}}))
	cfg = testConfig(100, 3)
	require.True(t, p.onObject(newTestObjectEntryWithRowCnt(t, 10, 1, false), cfg))
	require.True(t, p.onObject(newTestObjectEntryWithRowCnt(t, 20, 1, false), cfg))
	require.True(t, p.onObject(newTestObjectEntryWithRowCnt(t, 20, 1, false), cfg))
	result = p.revise(0, 36, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 2, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)
}

func TestPolicyTombstone(t *testing.T) {
	common.IsStandaloneBoost.Store(true)
	p := newTombstonePolicy()

	// tombstone policy do not schedule data objects
	p.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{Extra: &api.SchemaExtra{BlockMaxRows: options.DefaultBlockMaxRows}}))
	cfg := testConfig(100, 2)
	require.False(t, p.onObject(newTestObjectEntry(t, 10, false), cfg))
	require.False(t, p.onObject(newTestObjectEntry(t, 20, false), cfg))
	result := p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 0, len(result))

	p.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{Extra: &api.SchemaExtra{BlockMaxRows: options.DefaultBlockMaxRows}}))
	cfg = testConfig(100, 2)
	require.True(t, p.onObject(newTestObjectEntry(t, 10, true), cfg))
	require.True(t, p.onObject(newTestObjectEntry(t, 20, true), cfg))
	result = p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 2, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)

	// only schedule objects less than cfg.maxOneRun
	p.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{Extra: &api.SchemaExtra{BlockMaxRows: options.DefaultBlockMaxRows}}))
	cfg = testConfig(100, 2)
	require.True(t, p.onObject(newTestObjectEntry(t, 10, true), cfg))
	require.True(t, p.onObject(newTestObjectEntry(t, 20, true), cfg))
	require.False(t, p.onObject(newTestObjectEntry(t, 30, true), cfg))
	result = p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 2, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)

	// tombstone do not consider size limit
	p.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{Extra: &api.SchemaExtra{BlockMaxRows: options.DefaultBlockMaxRows}}))
	cfg = testConfig(100, 3)
	require.True(t, p.onObject(newTestObjectEntry(t, 10, true), cfg))
	require.True(t, p.onObject(newTestObjectEntry(t, 20, true), cfg))
	require.True(t, p.onObject(newTestObjectEntry(t, 120, true), cfg))
	result = p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 3, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)
}

func TestPolicyGroup(t *testing.T) {
	common.IsStandaloneBoost.Store(true)
	g := newPolicyGroup(newBasicPolicy(), newTombstonePolicy())
	g.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{Extra: &api.SchemaExtra{BlockMaxRows: options.DefaultBlockMaxRows}}))
	g.config = &BasicPolicyConfig{MergeMaxOneRun: 2, ObjectMinOsize: 100}

	g.onObject(newTestObjectEntry(t, 10, false))
	g.onObject(newTestObjectEntry(t, 20, false))
	g.onObject(newTestObjectEntry(t, 30, false))
	g.onObject(newTestObjectEntry(t, 10, true))
	g.onObject(newTestObjectEntry(t, 20, true))
	g.onObject(newTestObjectEntry(t, 30, true))

	results := g.revise(0, math.MaxInt64)
	require.Equal(t, 2, len(results))
	require.Equal(t, TaskHostDN, results[0].kind)
	require.Equal(t, TaskHostDN, results[1].kind)

	require.Equal(t, 2, len(results[0].objs))
	require.Equal(t, 2, len(results[1].objs))
}

const overlapSizeThreshold = common.DefaultMinOsizeQualifiedMB * common.Const1MBytes

func TestObjOverlap(t *testing.T) {

	// empty policy
	policy := newObjOverlapPolicy()
	objs := policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 0, len(objs))

	policy.resetForTable(nil)

	// no overlap
	entry1 := newSortedTestObjectEntry(t, 1, 2, overlapSizeThreshold)
	entry2 := newSortedTestObjectEntry(t, 3, 4, overlapSizeThreshold)
	require.True(t, policy.onObject(entry1, defaultBasicConfig))
	require.True(t, policy.onObject(entry2, defaultBasicConfig))
	objs = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 0, len(objs))

	policy.resetForTable(nil)

	// overlap
	entry3 := newSortedTestObjectEntry(t, 1, 4, overlapSizeThreshold)
	entry4 := newSortedTestObjectEntry(t, 2, 3, overlapSizeThreshold)
	require.True(t, policy.onObject(entry3, defaultBasicConfig))
	require.True(t, policy.onObject(entry4, defaultBasicConfig))
	objs = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 1, len(objs))
	require.Equal(t, 2, len(objs[0].objs))
	require.Equal(t, TaskHostDN, objs[0].kind)

	policy.resetForTable(nil)

	// entry is not sorted
	entry5 := newTestObjectEntry(t, overlapSizeThreshold, false)
	entry6 := newTestObjectEntry(t, overlapSizeThreshold, false)
	require.False(t, policy.onObject(entry5, defaultBasicConfig))
	require.False(t, policy.onObject(entry6, defaultBasicConfig))
	require.Equal(t, 0, len(policy.objects))
	objs = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 0, len(objs))

	policy.resetForTable(nil)

	// two overlap set:
	// {entry7, entry8}
	// {entry9, entry10, entry11}
	entry7 := newSortedTestObjectEntry(t, 1, 4, overlapSizeThreshold)
	entry8 := newSortedTestObjectEntry(t, 2, 3, overlapSizeThreshold)

	entry9 := newSortedTestObjectEntry(t, 11, 14, overlapSizeThreshold)
	entry10 := newSortedTestObjectEntry(t, 12, 13, overlapSizeThreshold)
	entry11 := newSortedTestObjectEntry(t, 13, 15, overlapSizeThreshold)

	require.True(t, policy.onObject(entry7, defaultBasicConfig))
	require.True(t, policy.onObject(entry8, defaultBasicConfig))
	require.True(t, policy.onObject(entry9, defaultBasicConfig))
	require.True(t, policy.onObject(entry10, defaultBasicConfig))
	require.True(t, policy.onObject(entry11, defaultBasicConfig))

	objs = policy.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 1, len(objs))
	require.Equal(t, 3, len(objs[0].objs))
	require.Equal(t, TaskHostDN, objs[0].kind)

	policy.resetForTable(nil)

	// no enough memory
	entry12 := newSortedTestObjectEntry(t, 1, 4, overlapSizeThreshold)
	entry13 := newSortedTestObjectEntry(t, 2, 3, overlapSizeThreshold)

	require.True(t, policy.onObject(entry12, defaultBasicConfig))
	require.True(t, policy.onObject(entry13, defaultBasicConfig))

	objs = policy.revise(0, 0, defaultBasicConfig)
	require.Equal(t, 0, len(objs))

	policy.resetForTable(nil)
}

func TestMinValue(t *testing.T) {
	require.Equal(t, minValue(types.T_bit), 0)
	require.True(t, types.CompareDecimal64(minValue(types.T_decimal64).(types.Decimal64), 0) < 0)
	require.True(t, types.CompareDecimal128(minValue(types.T_decimal128).(types.Decimal128), types.Decimal128{}) < 0)
}

func TestPolicyCompact(t *testing.T) {
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	p := newObjCompactPolicy(fs)

	cata := catalog.MockCatalog()
	defer cata.Close()
	txnMgr := txnbase.NewTxnManager(catalog.MockTxnStoreFactory(cata), catalog.MockTxnFactory(cata), types.NewMockHLCClock(1))
	txnMgr.Start(context.Background())
	defer txnMgr.Stop()
	txn1, _ := txnMgr.StartTxn(nil)
	db, err := cata.CreateDBEntry("db", "", "", txn1)
	require.NoError(t, err)
	catalog.MockSchema(1, 0)
	tbl, err := db.CreateTableEntry(catalog.MockSchema(1, 0), txn1, nil)
	require.NoError(t, err)
	require.NoError(t, txn1.Commit(context.Background()))

	p.resetForTable(tbl)

	objs := p.revise(0, math.MaxInt64, defaultBasicConfig)
	require.Equal(t, 0, len(objs))

	txn2, _ := txnMgr.StartTxn(nil)
	entry1 := newSortedDataEntryWithTableEntry(t, tbl, txn2, 0, 1, overlapSizeThreshold)
	require.NoError(t, txn2.Commit(context.Background()))
	require.False(t, p.onObject(entry1, defaultBasicConfig))

	txn3, _ := txnMgr.StartTxn(nil)
	newSortedTombstoneEntryWithTableEntry(t, tbl, txn3, types.Rowid{0}, types.Rowid{1})
	require.NoError(t, txn3.Commit(context.Background()))
	require.False(t, p.onObject(entry1, defaultBasicConfig))
}
