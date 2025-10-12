package databranchutils

type DataBranchMetadata struct {
	TableID      uint64
	CloneTS      int64
	PTableID     uint64
	TableDeleted bool
}

type dagNode struct {
	TableID  uint64
	CloneTS  int64
	ParentID uint64

	Parent *dagNode
	Depth  int
}

type DataBranchDAG struct {
	nodes map[uint64]*dagNode
}

func NewDAG(rows []DataBranchMetadata) *DataBranchDAG {
	dag := &DataBranchDAG{
		nodes: make(map[uint64]*dagNode, len(rows)),
	}

	// --- Pass 1: Create all Node objects ---
	// Iterate through each row and create a corresponding Node object.
	// We store them in the map but do not link them yet, as parent nodes may not have been processed.
	for _, row := range rows {
		var (
			ok    bool
			node1 *dagNode
			node2 *dagNode
		)

		if node1, ok = dag.nodes[row.TableID]; !ok {
			node1 = &dagNode{
				TableID: row.TableID,
				Depth:   -1, // Initialize depth as -1 to mark it as "not calculated".
			}
			dag.nodes[node1.TableID] = node1
		}

		node1.CloneTS = row.CloneTS
		node1.ParentID = row.PTableID

		if node2, ok = dag.nodes[row.PTableID]; !ok {
			node2 = &dagNode{
				TableID: row.PTableID,
				Depth:   -1, // Initialize depth as -1 to mark it as "not calculated".
			}
			dag.nodes[row.PTableID] = node2
		}
	}

	// --- Pass 2: Link Parent pointers ---
	// Now that all nodes exist in the map, we can iterate through them again
	// and set the `Parent` pointer based on the `ParentID`.
	for _, node := range dag.nodes {
		if node.ParentID != 0 {
			if parentNode, ok := dag.nodes[node.ParentID]; ok {
				node.Parent = parentNode
			}
		}
	}

	// --- Pass 3: Calculate the depth for every node ---
	// Depth is crucial for an efficient LCA algorithm. We calculate it once during setup.
	for _, node := range dag.nodes {
		dag.calculateDepth(node)
	}

	return dag
}

// calculateDepth computes the depth of a given dagNode.
// It uses memoization (by checking if `dagNode.Depth != -1`) to avoid re-computation.
func (d *DataBranchDAG) calculateDepth(node *dagNode) int {
	// If depth is already calculated, return it immediately.
	if node.Depth != -1 {
		return node.Depth
	}

	// If a node has no parent, it's a root node, so its depth is 0.
	if node.Parent == nil {
		node.Depth = 0
		return 0
	}

	// Otherwise, the depth is the parent's depth + 1.
	// This recursive call will eventually reach a root or a pre-calculated node.
	node.Depth = d.calculateDepth(node.Parent) + 1
	return node.Depth
}

func (d *DataBranchDAG) Exists(tableID uint64) bool {
	_, ok := d.nodes[tableID]
	return ok
}

func (d *DataBranchDAG) HasParent(tableID uint64) bool {
	node, ok := d.nodes[tableID]
	if !ok {
		return false
	}

	return node.Parent != nil
}

// FindLCA finds the Lowest Common Ancestor (LCA) for two given table IDs.
// It returns:
// 1. lcaTableID: The table_id of the common ancestor.
// 2. branchTS1: The clone_ts of the direct child of the LCA that is on the path to tableID1.
// 3. branchTS2: The clone_ts of the direct child of the LCA that is on the path to tableID2.
// 4. ok: A boolean indicating if a common ancestor was found.
func (d *DataBranchDAG) FindLCA(tableID1, tableID2 uint64) (lcaTableID uint64, branchTS1 int64, branchTS2 int64, ok bool) {
	node1, exists1 := d.nodes[tableID1]
	node2, exists2 := d.nodes[tableID2]

	// Ensure both nodes exist in our DAG.
	if !exists1 || !exists2 {
		return 0, 0, 0, false
	}

	// If it's the same node, it is its own LCA.
	// Both branch timestamps can be defined as the node's own creation timestamp.
	if tableID1 == tableID2 {
		return tableID1, node1.CloneTS, node1.CloneTS, true
	}

	// Keep references to the original nodes for the final step of finding branch timestamps.
	originalNode1 := node1
	originalNode2 := node2

	// --- Step 1: Bring both nodes to the same depth ---
	// Move the deeper node up the tree until it is at the same depth as the shallower node.
	if node1.Depth < node2.Depth {
		for node2.Depth > node1.Depth {
			node2 = node2.Parent
		}
	} else if node2.Depth < node1.Depth {
		for node1.Depth > node2.Depth {
			node1 = node1.Parent
		}
	}

	// --- Step 2: Move both nodes up in lockstep until they meet ---
	// Now that they are at the same depth, we move them up one parent at a time.
	// The first node they both share is the LCA.
	for node1 != node2 {
		// If either path hits a root before they meet, they are in different trees.
		if node1.Parent == nil || node2.Parent == nil {
			return 0, 0, 0, false
		}
		node1 = node1.Parent
		node2 = node2.Parent
	}

	lcaNode := node1
	if lcaNode == nil {
		// Should not happen if they are in the same tree and not both nil roots.
		return 0, 0, 0, false
	}

	lcaTableID = lcaNode.TableID

	// --- Step 3: Find the specific children of the LCA that lead to the original nodes ---

	// Find the branch timestamp for the path to tableID1
	child1 := originalNode1
	if child1 != lcaNode { // If the original node is not the LCA itself
		for child1 != nil && child1.Parent != lcaNode {
			child1 = child1.Parent
		}
		branchTS1 = child1.CloneTS
	} else { // If the original node IS the LCA (ancestor case)
		branchTS1 = lcaNode.CloneTS
	}

	// Find the branch timestamp for the path to tableID2
	child2 := originalNode2
	if child2 != lcaNode { // If the original node is not the LCA itself
		for child2 != nil && child2.Parent != lcaNode {
			child2 = child2.Parent
		}
		branchTS2 = child2.CloneTS
	} else { // If the original node IS the LCA (ancestor case)
		branchTS2 = lcaNode.CloneTS
	}

	ok = true
	return
}
