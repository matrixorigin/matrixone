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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func testConfig(objectMinOSize uint32, maxOneRun int) *BasicPolicyConfig {
	return &BasicPolicyConfig{
		ObjectMinOsize: objectMinOSize,
		MergeMaxOneRun: maxOneRun,
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
	p.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{BlockMaxRows: options.DefaultBlockMaxRows}))
	cfg := testConfig(100, 3)
	p.onObject(newTestObjectEntry(t, 10, false), cfg)
	p.onObject(newTestObjectEntry(t, 20, false), cfg)
	p.onObject(newTestObjectEntry(t, 120, false), cfg)
	result := p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 2, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)

	// only schedule objects less than cfg.maxOneRun
	p.resetForTable(catalog.MockStaloneTableEntry(1, &catalog.Schema{BlockMaxRows: options.DefaultBlockMaxRows}))
	cfg = testConfig(100, 2)
	p.onObject(newTestObjectEntry(t, 10, false), cfg)
	p.onObject(newTestObjectEntry(t, 20, false), cfg)
	p.onObject(newTestObjectEntry(t, 30, false), cfg)
	result = p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 2, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)

	// basic policy do not schedule tombstones
	p.resetForTable(catalog.MockStaloneTableEntry(2, &catalog.Schema{BlockMaxRows: options.DefaultBlockMaxRows}))
	cfg = testConfig(100, 2)
	p.onObject(newTestObjectEntry(t, 10, true), cfg)
	p.onObject(newTestObjectEntry(t, 20, true), cfg)
	result = p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 0, len(result))

	// memory limit
	p.resetForTable(catalog.MockStaloneTableEntry(2, &catalog.Schema{BlockMaxRows: options.DefaultBlockMaxRows}))
	cfg = testConfig(100, 2)
	p.onObject(newTestObjectEntryWithRowCnt(t, 10, 1, false), cfg)
	p.onObject(newTestObjectEntryWithRowCnt(t, 20, 1, false), cfg)
	p.onObject(newTestObjectEntryWithRowCnt(t, 20, 1, false), cfg)
	result = p.revise(0, 36, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 2, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)
}

func TestPolicyTombstone(t *testing.T) {
	common.IsStandaloneBoost.Store(true)
	p := newTombstonePolicy()

	// tombstone policy do not schedule data objects
	p.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{BlockMaxRows: options.DefaultBlockMaxRows}))
	cfg := testConfig(100, 2)
	p.onObject(newTestObjectEntry(t, 10, false), cfg)
	p.onObject(newTestObjectEntry(t, 20, false), cfg)
	result := p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 0, len(result))

	p.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{BlockMaxRows: options.DefaultBlockMaxRows}))
	cfg = testConfig(100, 2)
	p.onObject(newTestObjectEntry(t, 10, true), cfg)
	p.onObject(newTestObjectEntry(t, 20, true), cfg)
	result = p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 2, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)

	// only schedule objects less than cfg.maxOneRun
	p.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{BlockMaxRows: options.DefaultBlockMaxRows}))
	cfg = testConfig(100, 2)
	p.onObject(newTestObjectEntry(t, 10, true), cfg)
	p.onObject(newTestObjectEntry(t, 20, true), cfg)
	p.onObject(newTestObjectEntry(t, 30, true), cfg)
	result = p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 2, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)

	// tombstone do not consider size limit
	p.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{BlockMaxRows: options.DefaultBlockMaxRows}))
	cfg = testConfig(100, 3)
	p.onObject(newTestObjectEntry(t, 10, true), cfg)
	p.onObject(newTestObjectEntry(t, 20, true), cfg)
	p.onObject(newTestObjectEntry(t, 120, true), cfg)
	result = p.revise(0, math.MaxInt64, cfg)
	require.Equal(t, 1, len(result))
	require.Equal(t, 3, len(result[0].objs))
	require.Equal(t, TaskHostDN, result[0].kind)
}

func TestPolicyGroup(t *testing.T) {
	common.IsStandaloneBoost.Store(true)
	g := newPolicyGroup(newBasicPolicy(), newTombstonePolicy())
	g.resetForTable(catalog.MockStaloneTableEntry(0, &catalog.Schema{BlockMaxRows: options.DefaultBlockMaxRows}))
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
