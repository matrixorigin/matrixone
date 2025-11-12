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

import "testing"

func TestDAGFunctionality(t *testing.T) {

	// --- Test Setup: Define a complex graph with multiple trees (a forest) ---
	// NEW: Since fields cannot be null, we now use value types directly.
	// A PTableID of 0 is used to signify a root node.
	//
	// The graph structure being tested remains the same:
	// Tree 1 (Root 100)      Tree 2 (Root 200)      Tree 3 (Root 300)   Tree 4 (Root 400)
	//
	//       100(ts:100)             200(ts:200)             300(ts:300)         400(ts:400)
	//      /    |    \              /      \                  |
	// 110(110) 120(120) 130(130)  210(210) 220(220)          301(301)
	//   /   \       |               |                        |
	// 111(111) 112(112) 121(121)    211(211)                  302(302)
	//           |
	//        1121(1121)
	//
	rows := []DataBranchMetadata{
		// Tree 1
		{TableID: 100, CloneTS: 100, PTableID: 0}, // PTableID: 0 indicates a root node
		{TableID: 110, CloneTS: 110, PTableID: 100},
		{TableID: 120, CloneTS: 120, PTableID: 100},
		{TableID: 130, CloneTS: 130, PTableID: 100},
		{TableID: 111, CloneTS: 111, PTableID: 110},
		{TableID: 112, CloneTS: 112, PTableID: 110},
		{TableID: 121, CloneTS: 121, PTableID: 120},
		{TableID: 1121, CloneTS: 1121, PTableID: 112},

		// Tree 2
		{TableID: 200, CloneTS: 200, PTableID: 0}, // PTableID: 0 indicates a root node
		{TableID: 210, CloneTS: 210, PTableID: 200},
		{TableID: 220, CloneTS: 220, PTableID: 200},
		{TableID: 211, CloneTS: 211, PTableID: 210},

		// Tree 3 (A simple chain)
		{TableID: 300, CloneTS: 300, PTableID: 0}, // PTableID: 0 indicates a root node
		{TableID: 301, CloneTS: 301, PTableID: 300},
		{TableID: 302, CloneTS: 302, PTableID: 301},

		// Tree 4 (Isolated node)
		{TableID: 400, CloneTS: 400, PTableID: 0}, // PTableID: 0 indicates a root node
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
			wantTS1  int64
			wantTS2  int64
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
				lcaID, ts1, ts2, ok := dag.FindLCA(tc.id1, tc.id2)

				if ok != tc.wantOK {
					t.Fatalf("FindLCA(%d, %d): expected ok=%v, got ok=%v", tc.id1, tc.id2, tc.wantOK, ok)
				}

				// If we expected success, check all values
				if tc.wantOK {
					if lcaID != tc.wantLCA {
						t.Errorf("FindLCA(%d, %d): wrong LCA ID. got=%d, want=%d", tc.id1, tc.id2, lcaID, tc.wantLCA)
					}
					if ts1 != tc.wantTS1 {
						t.Errorf("FindLCA(%d, %d): wrong branch TS1. got=%d, want=%d", tc.id1, tc.id2, ts1, tc.wantTS1)
					}
					if ts2 != tc.wantTS2 {
						t.Errorf("FindLCA(%d, %d): wrong branch TS2. got=%d, want=%d", tc.id1, tc.id2, ts2, tc.wantTS2)
					}
				}
			})
		}
	})
}
