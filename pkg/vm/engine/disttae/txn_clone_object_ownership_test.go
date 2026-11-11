package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestRegisterCNObjectsPreservesSharedObjectTableOwnership(t *testing.T) {
	txn := &Transaction{cnObjsSummary: map[cnObjectKey]Summary{}}
	var objectID types.Objectid

	txn.registerCNObjects(objectID, 1, 10, 100, nil, "db", "a", 0, false)
	txn.registerCNObjects(objectID, 1, 10, 200, nil, "db", "b", 0, false)

	require.Len(t, txn.cnObjsSummary, 2)
	summaries := make(map[uint64]Summary)
	for _, summary := range txn.cnObjsSummary {
		summaries[summary.tableId] = summary
	}
	require.Equal(t, "a", summaries[100].tbName)
	require.Equal(t, "b", summaries[200].tbName)
}

func TestDeletedBlocksIsolateSharedBlockByTable(t *testing.T) {
	const (
		tableA uint64 = 100
		tableB uint64 = 200
	)
	rowID := types.RandomRowid()
	blockID, _ := rowID.Decode()
	deletes := &deletedBlocks{offsets: map[deletedBlockKey][]int64{}}
	deletes.addDeletedBlocks(tableA, blockID, []int64{1})
	deletes.addDeletedBlocks(tableB, blockID, []int64{2, 3})

	require.Len(t, deletes.offsets, 2)
	readAll := func(tableID uint64) []int64 {
		var offsets []int64
		deletes.getDeletedRowIDs(tableID, func(row types.Rowid) {
			_, offset := row.Decode()
			offsets = append(offsets, int64(offset))
		})
		return offsets
	}
	require.ElementsMatch(t, []int64{1}, readAll(tableA))
	require.ElementsMatch(t, []int64{2, 3}, readAll(tableB))

	var scoped []int64
	deletes.getDeletedRowIDsForBlocks(tableA, []types.Blockid{*blockID}, func(row types.Rowid) {
		_, offset := row.Decode()
		scoped = append(scoped, int64(offset))
	})
	require.Equal(t, []int64{1}, scoped)
}

func TestCloneDeleteOwnershipFlowsToObjectSummary(t *testing.T) {
	txn := &Transaction{cnObjsSummary: map[cnObjectKey]Summary{}}
	var objectID types.Objectid
	txn.registerCNObjects(objectID, 1, 10, 100, nil, "db", "a", 0, false)
	txn.registerCNObjects(objectID, 1, 10, 200, nil, "db", "b", 0, false)

	rowID := types.RandomRowid()
	blockID, _ := rowID.Decode()
	*blockID.Object() = objectID
	txn.deletedBlocks = &deletedBlocks{offsets: map[deletedBlockKey][]int64{}}
	txn.deletedBlocks.addDeletedBlocks(100, blockID, []int64{0})

	var lookups int
	txn.deletedBlocks.iter(func(key deletedBlockKey, offsets []int64) bool {
		lookups++
		summary := txn.cnObjsSummary[cnObjectKey{
			objectID: *key.blockID.Object(),
			tableID:  key.tableID,
		}]
		require.Equal(t, uint64(100), key.tableID)
		require.Equal(t, "a", summary.tbName)
		require.Equal(t, []int64{0}, offsets)
		return true
	})
	require.Equal(t, 1, lookups)
}
