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

package idxcron

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestReindexOptionForCounts exercises the pure MERGE-vs-REBUILD decision on both
// sides of the RebuildDeadPct (30%) boundary. dead% = 100 - liveRows*100/baseDocs.
func TestReindexOptionForCounts(t *testing.T) {
	const merge = "MERGE"
	const rebuild = "" // full REBUILD

	cases := []struct {
		name     string
		liveRows int64
		baseDocs int64
		want     string
	}{
		// baseDocs guard branches.
		{"zero base", 0, 0, merge},
		{"negative base", 100, -1, merge},

		// dead% low → MERGE. All rows live = 0% dead.
		{"no dead docs", 100, 100, merge},
		// 20% dead (< 30) → MERGE.
		{"20pct dead", 80, 100, merge},
		// Exactly at boundary: dead = 30% → liveRows*100 == baseDocs*70, NOT strictly
		// less-than, so still MERGE (boundary is inclusive of MERGE).
		{"exactly 30pct dead", 70, 100, merge},

		// dead% high → REBUILD. 31% dead → 69*100 < 100*70 → REBUILD.
		{"31pct dead", 69, 100, rebuild},
		{"all dead", 0, 100, rebuild},
		{"half dead", 50, 100, rebuild},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reindexOptionForCounts(tc.liveRows, tc.baseDocs)
			require.Equal(t, tc.want, got, "liveRows=%d baseDocs=%d", tc.liveRows, tc.baseDocs)
		})
	}
}

// TestOptLabel covers the human-readable label mapping used in logging.
func TestOptLabel(t *testing.T) {
	require.Equal(t, "REBUILD", optLabel(""))
	require.Equal(t, "MERGE", optLabel("MERGE"))
}

// TestUpdatableWithinInterval covers the cadence short-circuit in Updatable: when
// LastUpdateAt + Interval is still in the future, Updatable returns (false, reason)
// BEFORE touching the SqlProcess, so it is reachable with plain inputs.
func TestUpdatableWithinInterval(t *testing.T) {
	// LastUpdateAt = now, Interval = 1h → last+interval is in the future → not
	// updatable, no engine needed.
	now := types.UnixToTimestamp(time.Now().Unix())
	in := idxcronplugin.UpdatableInput{
		IndexName:    "idx1",
		TableDef:     &plan.TableDef{DbName: "db", Name: "t"},
		LastUpdateAt: &now,
		Interval:     time.Hour,
	}
	ok, reason, err := Hooks{}.Updatable(in)
	require.NoError(t, err)
	require.False(t, ok)
	require.Contains(t, reason, "within reindex interval")
}

// TestUpdatableNoStorageTable covers the branch where the interval gate has passed
// (LastUpdateAt in the distant past) but the TableDef has no matching fulltext2
// storage index, so Updatable bails out before any SQL against the SqlProcess.
func TestUpdatableNoStorageTable(t *testing.T) {
	// LastUpdateAt far in the past so last+interval is already elapsed.
	past := types.UnixToTimestamp(time.Now().Add(-100 * time.Hour).Unix())
	in := idxcronplugin.UpdatableInput{
		IndexName: "idx_missing",
		TableDef: &plan.TableDef{
			DbName:  "db",
			Name:    "t",
			Indexes: []*plan.IndexDef{}, // no storage index for idx_missing
		},
		LastUpdateAt: &past,
		Interval:     time.Hour,
	}
	ok, reason, err := Hooks{}.Updatable(in)
	require.NoError(t, err)
	require.False(t, ok)
	require.Contains(t, reason, "not a fulltext2 index")
}

// TestReindexOptionNoMetaTable covers the ReindexOption fast-path: when the
// TableDef has no matching metadata index, it returns "MERGE" without consulting
// the SqlProcess.
func TestReindexOptionNoMetaTable(t *testing.T) {
	in := idxcronplugin.UpdatableInput{
		IndexName: "idx_missing",
		TableDef: &plan.TableDef{
			DbName:  "db",
			Name:    "t",
			Indexes: []*plan.IndexDef{}, // no metadata index
		},
	}
	opt, err := Hooks{}.ReindexOption(in)
	require.NoError(t, err)
	require.Equal(t, "MERGE", opt)
}

// TestTailChunkThreshold sanity-checks the default threshold constant is positive
// (the env override path is exercised at package-init time).
func TestTailChunkThreshold(t *testing.T) {
	require.Greater(t, TailChunkThreshold, int64(0))
	require.Equal(t, 30, RebuildDeadPct)
}
