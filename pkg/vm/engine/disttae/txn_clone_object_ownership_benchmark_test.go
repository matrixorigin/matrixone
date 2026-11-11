package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const issue32500BenchObjects = 64

var (
	issue32500BenchSummary Summary
	issue32500BenchRows    int
)

func issue32500BenchIDs() ([]types.Objectid, []types.Blockid) {
	objects := make([]types.Objectid, issue32500BenchObjects)
	blocks := make([]types.Blockid, issue32500BenchObjects)
	for i := range objects {
		for j := 0; j < 8; j++ {
			objects[i][j] = byte((i + 1) >> (8 * j))
		}
		*blocks[i].Object() = objects[i]
	}
	return objects, blocks
}

func BenchmarkIssue32500RegisterSingleTable64(b *testing.B) {
	objects, _ := issue32500BenchIDs()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := issue32500BenchNewTxn()
		for j := range objects {
			txn.registerCNObjects(objects[j], 1, 10, 100, nil, "db", "table", 0, false)
		}
		issue32500BenchSummary = issue32500BenchLookup(txn, objects[i%len(objects)], 100)
	}
}

func BenchmarkIssue32500LookupSingleTable64(b *testing.B) {
	objects, _ := issue32500BenchIDs()
	txn := issue32500BenchNewTxn()
	for j := range objects {
		txn.registerCNObjects(objects[j], 1, 10, 100, nil, "db", "table", 0, false)
	}
	var summaries cnObjectSummaryReader
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range objects {
			issue32500BenchSummary = summaries.get(txn, 100, objects[j])
		}
	}
}

func BenchmarkIssue32500DeletedBlocksSingleTable64(b *testing.B) {
	_, blocks := issue32500BenchIDs()
	offsets := []int64{0, 1, 2, 3}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deletes := issue32500BenchNewDeletedBlocks()
		for j := range blocks {
			issue32500BenchAddDeleted(deletes, 100, &blocks[j], offsets)
		}
		for j := range blocks {
			issue32500BenchGetDeleted(deletes, 100, blocks[j:j+1], func(types.Rowid) {
				issue32500BenchRows++
			})
		}
	}
}

func BenchmarkIssue32500CloneCompactionLookup64x3(b *testing.B) {
	objects, blocks := issue32500BenchIDs()
	txn := issue32500BenchNewTxn()
	deletes := issue32500BenchNewDeletedBlocks()
	for j := range objects {
		for _, tableID := range []uint64{100, 200, 300} {
			txn.registerCNObjects(objects[j], 1, 10, tableID, nil, "db", "table", 0, false)
		}
	}
	for j := range blocks {
		issue32500BenchAddDeleted(deletes, 300, &blocks[j], []int64{0, 1, 2, 3})
	}
	var summaries cnObjectSummaryReader
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range objects {
			issue32500BenchSummary = summaries.get(txn, 300, objects[j])
			issue32500BenchGetDeleted(deletes, 300, blocks[j:j+1], func(types.Rowid) {
				issue32500BenchRows++
			})
		}
	}
}

func BenchmarkIssue32500CloneRegister64x3(b *testing.B) {
	objects, _ := issue32500BenchIDs()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := issue32500BenchNewTxn()
		for j := range objects {
			for _, tableID := range []uint64{100, 200, 300} {
				txn.registerCNObjects(objects[j], 1, 10, tableID, nil, "db", "table", 0, false)
			}
		}
		issue32500BenchSummary = issue32500BenchLookup(txn, objects[i%len(objects)], 300)
	}
}

func issue32500BenchNewTxn() *Transaction {
	return &Transaction{cnObjsSummary: map[uint64]map[types.Objectid]Summary{}}
}

func issue32500BenchNewDeletedBlocks() *deletedBlocks {
	return &deletedBlocks{offsets: map[uint64]map[types.Blockid][]int64{}}
}

func issue32500BenchLookup(txn *Transaction, objectID types.Objectid, tableID uint64) Summary {
	return txn.cnObjsSummary[tableID][objectID]
}

func issue32500BenchAddDeleted(deletes *deletedBlocks, tableID uint64, blockID *types.Blockid, offsets []int64) {
	deletes.addDeletedBlocks(tableID, blockID, offsets)
}

func issue32500BenchGetDeleted(deletes *deletedBlocks, tableID uint64, blocks []types.Blockid, fn func(types.Rowid)) {
	deletes.getDeletedRowIDsForBlocks(tableID, blocks, fn)
}
