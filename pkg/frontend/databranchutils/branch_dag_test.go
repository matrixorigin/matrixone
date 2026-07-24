// Copyright 2025 Matrix Origin
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

package databranchutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDAGFunctionality(t *testing.T) {

	// --- Test Setup: Define a complex graph with multiple trees (a forest) ---
	// A PTableID of 0 is used to signify a root node.
	//
	// The graph structure being tested remains the same:
	// Tree 1 (Root 100)      Tree 2 (Root 200)      Tree 3 (Root 300)   Tree 4 (Root 400)
	//
	//          100                    200                    300                 400
	//      /    |    \              /      \                  |
	//       110    120   130         210    220              301
	//   /   \       |               |                        |
	// 111   112    121             211                       302
	//           |
	//          1121
	//
	rows := []DataBranchMetadata{
		// Tree 1
		{TableID: 100, PTableID: 0}, // PTableID: 0 indicates a root node
		{TableID: 110, PTableID: 100},
		{TableID: 120, PTableID: 100},
		{TableID: 130, PTableID: 100},
		{TableID: 111, PTableID: 110},
		{TableID: 112, PTableID: 110},
		{TableID: 121, PTableID: 120},
		{TableID: 1121, PTableID: 112},

		// Tree 2
		{TableID: 200, PTableID: 0}, // PTableID: 0 indicates a root node
		{TableID: 210, PTableID: 200},
		{TableID: 220, PTableID: 200},
		{TableID: 211, PTableID: 210},

		// Tree 3 (A simple chain)
		{TableID: 300, PTableID: 0}, // PTableID: 0 indicates a root node
		{TableID: 301, PTableID: 300},
		{TableID: 302, PTableID: 301},

		// Tree 4 (Isolated node)
		{TableID: 400, PTableID: 0}, // PTableID: 0 indicates a root node
	}

	// Assuming your newDAG function has been updated to handle PTableID=0 as a root.
	dag := NewDAG(rows)

	// Sub-test for node existence
	t.Run("Node Existence Checks", func(t *testing.T) {
		if !dag.Exists(1121) {
			t.Error("Expected node 1121 to exist, but it doesn't")
		}
		if !dag.Exists(100) {
			t.Error("Expected root node 100 to exist, but it doesn't")
		}
		if !dag.Exists(400) {
			t.Error("Expected isolated node 400 to exist, but it doesn't")
		}
		if dag.Exists(9999) {
			t.Error("Expected node 9999 to not exist, but it does")
		}
	})

	t.Run("Parent Checks", func(t *testing.T) {
		if !dag.HasParent(111) {
			t.Error("Expected node 111 to have a parent")
		}
		if dag.HasParent(100) {
			t.Error("Expected root node 100 to have no parent")
		}
		if dag.HasParent(9999) {
			t.Error("Expected unknown node 9999 to report no parent")
		}
	})

	// Sub-test for LCA logic - no changes needed here as it tests the final graph structure.
	t.Run("Lowest Common Ancestor Checks", func(t *testing.T) {
		testCases := []struct {
			name     string
			id1, id2 uint64
			wantLCA  uint64
			wantTS1  uint64
			wantTS2  uint64
			wantOK   bool
		}{
			// --- Scenarios within a single complex tree (Tree 1) ---
			{
				name: "Sibling nodes",
				id1:  111, id2: 112,
				wantLCA: 110, wantTS1: 111, wantTS2: 112, wantOK: true,
			},
			{
				name: "Cousin nodes in different branches",
				id1:  111, id2: 121,
				wantLCA: 100, wantTS1: 110, wantTS2: 120, wantOK: true,
			},
			{
				name: "Nodes at different depths",
				id1:  111, id2: 1121,
				wantLCA: 110, wantTS1: 111, wantTS2: 112, wantOK: true,
			},
			{
				name: "Direct ancestor case",
				id1:  110, id2: 1121,
				wantLCA: 110, wantTS1: 110, wantTS2: 112, wantOK: true,
			},
			{
				name: "Root is the direct ancestor",
				id1:  100, id2: 121,
				wantLCA: 100, wantTS1: 100, wantTS2: 120, wantOK: true,
			},

			// --- Scenarios involving multiple trees (Forest) ---
			{
				name: "Nodes in two different trees",
				id1:  111, id2: 211,
				wantOK: false,
			},
			{
				name: "Node from a tree and a chain-like tree",
				id1:  121, id2: 302,
				wantOK: false,
			},
			{
				name: "Node from a tree and an isolated node",
				id1:  220, id2: 400,
				wantOK: false,
			},
			{
				name: "Two different root nodes",
				id1:  100, id2: 200,
				wantOK: false,
			},

			// --- Boundary and Edge Cases ---
			{
				name: "Query with the same node",
				id1:  121, id2: 121,
				wantLCA: 121, wantTS1: 121, wantTS2: 121, wantOK: true,
			},
			{
				name: "Query in a chain-like tree",
				id1:  300, id2: 302,
				wantLCA: 300, wantTS1: 300, wantTS2: 301, wantOK: true,
			},
			{
				name: "One node does not exist",
				id1:  111, id2: 9999,
				wantOK: false,
			},
			{
				name: "Both nodes do not exist",
				id1:  8888, id2: 9999,
				wantOK: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				lcaID, childID1, childID2, ok := dag.FindLCA(tc.id1, tc.id2)

				if ok != tc.wantOK {
					t.Fatalf("FindLCA(%d, %d): expected ok=%v, got ok=%v", tc.id1, tc.id2, tc.wantOK, ok)
				}

				// If we expected success, check all values
				if tc.wantOK {
					if lcaID != tc.wantLCA {
						t.Errorf("FindLCA(%d, %d): wrong LCA ID. got=%d, want=%d", tc.id1, tc.id2, lcaID, tc.wantLCA)
					}
					if childID1 != tc.wantTS1 {
						t.Errorf("FindLCA(%d, %d): wrong child table ID1. got=%d, want=%d", tc.id1, tc.id2, childID1, tc.wantTS1)
					}
					if childID2 != tc.wantTS2 {
						t.Errorf("FindLCA(%d, %d): wrong child table ID2. got=%d, want=%d", tc.id1, tc.id2, childID2, tc.wantTS2)
					}
				}
			})
		}
	})
}

func TestPathFromRoot(t *testing.T) {
	dag := NewDAG([]DataBranchMetadata{
		{TableID: 2, PTableID: 1, CloneTS: 20},
		{TableID: 3, PTableID: 2, CloneTS: 30},
	})

	ids, cloneTSs, ok := dag.PathFromRoot(3)
	if !ok || len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("unexpected root path: ids=%v ok=%v", ids, ok)
	}
	if len(cloneTSs) != 3 || cloneTSs[0] != 0 || cloneTSs[1] != 20 || cloneTSs[2] != 30 {
		t.Fatalf("unexpected clone timestamps: %v", cloneTSs)
	}

	_, _, ok = dag.PathFromRoot(99)
	if ok {
		t.Fatal("missing node unexpectedly had a root path")
	}
}

func TestComputeAlterLineageCompactionPlan(t *testing.T) {
	dag := NewBranchReclaimDag([]DataBranchMetadata{
		{TableID: 2, PTableID: 1, CloneTS: 100, Level: "alter"},
		{TableID: 3, PTableID: 2, CloneTS: 200, Level: "alter"},
	})
	edges := map[uint64]HistoricalLineageEdge{
		2: {
			ChildTableID:  2,
			ParentTableID: 1,
			CloneTS:       100,
			AccountName:   "acc",
			DatabaseName:  "db",
			TableName:     "t",
		},
		3: {
			ChildTableID:  3,
			ParentTableID: 2,
			CloneTS:       200,
			AccountName:   "acc",
			DatabaseName:  "db",
			TableName:     "t",
		},
	}

	require.Equal(t,
		AlterLineageCompactionPlan{
			TableIDs:      []uint64{2, 3},
			SnapshotNames: []string{"__mo_branch_2", "__mo_branch_3"},
		},
		ComputeAlterLineageCompactionPlan(dag, edges, nil),
	)

	sources := []HistoricalSource{{
		Level:        "table",
		AccountName:  "acc",
		DatabaseName: "db",
		TableName:    "t",
		OldestTS:     150,
	}}
	require.Equal(t,
		AlterLineageCompactionPlan{
			TableIDs:      []uint64{2},
			SnapshotNames: []string{"__mo_branch_2"},
		},
		ComputeAlterLineageCompactionPlan(dag, edges, sources),
	)
}

func TestComputeAlterLineageCompactionPlanReclaimsDeletedAlterGenerations(t *testing.T) {
	rows := []DataBranchMetadata{
		{TableID: 2, PTableID: 1, CloneTS: 100, Level: "alter", TableDeleted: true},
		{TableID: 3, PTableID: 2, CloneTS: 200, Level: "alter"},
	}
	edges := map[uint64]HistoricalLineageEdge{
		2: {ChildTableID: 2, ParentTableID: 1, CloneTS: 100},
		3: {ChildTableID: 3, ParentTableID: 2, CloneTS: 200},
	}

	require.Equal(t,
		AlterLineageCompactionPlan{
			TableIDs:      []uint64{2, 3},
			SnapshotNames: []string{"__mo_branch_2", "__mo_branch_3"},
		},
		ComputeAlterLineageCompactionPlan(NewBranchReclaimDag(rows), edges, nil),
	)

	// Plain DROP can reclaim both snapshots while a user snapshot still owns
	// the history. After that owner disappears, the deleted metadata rows
	// must remain reclaimable even though their matching edges are gone.
	rows[1].TableDeleted = true
	require.Equal(t,
		AlterLineageCompactionPlan{
			TableIDs:      []uint64{2, 3},
			SnapshotNames: []string{"__mo_branch_2", "__mo_branch_3"},
		},
		ComputeAlterLineageCompactionPlan(NewBranchReclaimDag(rows), nil, nil),
	)

	covered := []HistoricalSource{{Level: "table", ObjectID: 1, OldestTS: 50}}
	require.Empty(t,
		ComputeAlterLineageCompactionPlan(NewBranchReclaimDag(rows), nil, covered).TableIDs,
		"the dropped current table's historical owner must retain orphaned metadata until owner deletion",
	)
}

func TestComputeAlterLineageCompactionPlanScopeAndOwners(t *testing.T) {
	baseRows := []DataBranchMetadata{
		{TableID: 2, PTableID: 1, CloneTS: 100, Level: "alter"},
	}
	edges := map[uint64]HistoricalLineageEdge{
		2: {
			ChildTableID:  2,
			ParentTableID: 1,
			CloneTS:       100,
			AccountName:   "acc",
			DatabaseName:  "db",
			TableName:     "t",
		},
	}

	for _, source := range []HistoricalSource{
		{Level: "cluster", OldestTS: 100},
		{Level: "account", AccountName: "acc", OldestTS: 100},
		{Level: "database", AccountName: "acc", DatabaseName: "db", OldestTS: 100},
		{Level: "table", AccountName: "acc", DatabaseName: "db", TableName: "t", OldestTS: 100},
		{Level: "table", AccountName: "acc", DatabaseName: "db", TableName: "renamed", ObjectID: 1, OldestTS: 100},
		{Level: "table", AccountName: "acc", DatabaseName: "db", TableName: "t", ObjectID: 999, OldestTS: 100},
	} {
		plan := ComputeAlterLineageCompactionPlan(
			NewBranchReclaimDag(baseRows), edges, []HistoricalSource{source},
		)
		require.Empty(t, plan.TableIDs, "covering source %+v must retain the edge", source)
	}

	uncovered := []HistoricalSource{
		{Level: "account", AccountName: "other", OldestTS: 0},
		{Level: "database", AccountName: "acc", DatabaseName: "other", OldestTS: 0},
		{Level: "table", AccountName: "acc", DatabaseName: "db", TableName: "t", OldestTS: 101},
	}
	plan := ComputeAlterLineageCompactionPlan(
		NewBranchReclaimDag(baseRows), edges, uncovered,
	)
	require.Equal(t, []uint64{2}, plan.TableIDs)

	withLiveLogicalSibling := NewBranchReclaimDag(append(baseRows,
		DataBranchMetadata{TableID: 4, PTableID: 1, CloneTS: 90, Level: "table"},
	))
	plan = ComputeAlterLineageCompactionPlan(withLiveLogicalSibling, edges, nil)
	require.Empty(t, plan.TableIDs)

	withDeletedLogicalSibling := NewBranchReclaimDag(append(baseRows,
		DataBranchMetadata{
			TableID: 4, PTableID: 1, CloneTS: 90, Level: "table", TableDeleted: true,
		},
	))
	plan = ComputeAlterLineageCompactionPlan(withDeletedLogicalSibling, edges, nil)
	require.Equal(t, []uint64{2}, plan.TableIDs)

	// ALTER preserves the logical owner's level after the prefix. Once the
	// copy-and-swap drops the old physical table, this live alter:table row is
	// the only remaining representation of the logical branch and must keep
	// the lineage component alive.
	withInheritedLogicalOwner := NewBranchReclaimDag([]DataBranchMetadata{
		{TableID: 2, PTableID: 1, CloneTS: 100, Level: "table", TableDeleted: true},
		{TableID: 3, PTableID: 2, CloneTS: 200, Level: "alter:table"},
	})
	inheritedEdges := map[uint64]HistoricalLineageEdge{
		3: {
			ChildTableID: 3, ParentTableID: 2, CloneTS: 200,
			AccountName: "acc", DatabaseName: "db", TableName: "t",
		},
	}
	plan = ComputeAlterLineageCompactionPlan(withInheritedLogicalOwner, inheritedEdges, nil)
	require.Empty(t, plan.TableIDs)

	plan = ComputeAlterLineageCompactionPlan(NewBranchReclaimDag(baseRows), nil, nil)
	require.Empty(t, plan.TableIDs, "missing identity must be retained conservatively")
}

func TestComputeAlterLineageCompactionPlanTableOwnerSurvivesRename(t *testing.T) {
	dag := NewBranchReclaimDag([]DataBranchMetadata{
		{TableID: 2, PTableID: 1, CloneTS: 100, Level: "alter"},
		{TableID: 3, PTableID: 2, CloneTS: 200, Level: "alter"},
	})
	edges := map[uint64]HistoricalLineageEdge{
		2: {
			ChildTableID: 2, ParentTableID: 1, CloneTS: 100,
			AccountName: "acc", DatabaseName: "db", TableName: "before_rename",
		},
		3: {
			ChildTableID: 3, ParentTableID: 2, CloneTS: 200,
			AccountName: "acc", DatabaseName: "db", TableName: "after_rename",
		},
	}
	source := HistoricalSource{
		Level: "table", AccountName: "acc", DatabaseName: "db",
		TableName: "before_rename", ObjectID: 1, OldestTS: 50,
	}

	plan := ComputeAlterLineageCompactionPlan(dag, edges, []HistoricalSource{source})
	require.Empty(t, plan.TableIDs,
		"the source object owns the component, including the newer edge across rename")
}

func TestComputeAlterLineageCompactionPlanCycleSafe(t *testing.T) {
	dag := NewBranchReclaimDag([]DataBranchMetadata{
		{TableID: 1, PTableID: 2, CloneTS: 100, Level: "alter"},
		{TableID: 2, PTableID: 1, CloneTS: 200, Level: "alter"},
	})
	edges := map[uint64]HistoricalLineageEdge{
		1: {ChildTableID: 1, ParentTableID: 2, CloneTS: 100},
		2: {ChildTableID: 2, ParentTableID: 1, CloneTS: 200},
	}

	plan := ComputeAlterLineageCompactionPlan(dag, edges, nil)
	require.Equal(t, []uint64{1, 2}, plan.TableIDs)
	require.Equal(t, []string{"__mo_branch_1", "__mo_branch_2"}, plan.SnapshotNames)
}

func TestPitrRetentionLowerBound(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		length int
		unit   string
		want   time.Time
	}{
		{length: 2, unit: "h", want: now.Add(-2 * time.Hour)},
		{length: 2, unit: "d", want: now.AddDate(0, 0, -2)},
		{length: 2, unit: "mo", want: now.AddDate(0, -2, 0)},
		{length: 2, unit: "y", want: now.AddDate(-2, 0, 0)},
	} {
		got, err := PitrRetentionLowerBound(now, tc.length, tc.unit)
		require.NoError(t, err)
		require.Equal(t, tc.want.UnixNano(), got)
	}

	_, err := PitrRetentionLowerBound(now, 1, "week")
	require.Error(t, err)
}

func TestBuildAlterLineageDeleteSQL(t *testing.T) {
	require.Equal(t,
		"delete from mo_catalog.mo_snapshots where kind = 'branch' and sname in ('__mo_branch_2','__mo_branch_3')",
		BuildAlterLineageSnapshotDeleteSQL([]string{"__mo_branch_2", "__mo_branch_3"}),
	)
	require.Equal(t,
		"delete from mo_catalog.mo_branch_metadata where table_id in (2,3) and (level = 'alter' or level like 'alter:%')",
		BuildAlterLineageMetadataDeleteSQL([]uint64{2, 3}),
	)
	require.Empty(t, BuildAlterLineageSnapshotDeleteSQL(nil))
	require.Empty(t, BuildAlterLineageMetadataDeleteSQL(nil))
}
