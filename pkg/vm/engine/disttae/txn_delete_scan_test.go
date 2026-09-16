// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package disttae

import (
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Sparse workspaces and many persisted delete blocks challenge the cost of
// eligibility checking itself, rather than only measuring its best case.
func BenchmarkDeleteTableWritesSparseWorkspace(b *testing.B) {
	proc := testutil.NewProcess(b)
	var workspace types.Rowid
	workspace.SetSegment(colexec.TxnWorkspaceSegment)
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{objectio.PhysicalAddr_Attr}
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(b, vector.AppendFixed(bat.Vecs[0], workspace, false, proc.Mp()))
	bat.SetRowCount(1)
	b.Cleanup(func() { bat.Clean(proc.Mp()) })
	for _, writes := range []int{0, 1, 1024} {
		for _, blockCount := range []int{1, 256} {
			for _, optimized := range []bool{false, true} {
				b.Run(fmt.Sprintf("writes=%d/blocks=%d/guard=%t", writes, blockCount, optimized), func(b *testing.B) {
					txn := &Transaction{writes: make([]Entry, writes)}
					for i := range txn.writes {
						txn.writes[i] = Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: bat}
					}
					blocks := make(map[types.Blockid]bool, blockCount)
					for i := 0; i < blockCount; i++ {
						id := workspace
						segment := colexec.TxnWorkspaceSegment
						segment[0] = 1
						id.SetSegment(segment)
						id.SetBlkOffset(uint16(i))
						blocks[id.CloneBlockID()] = true
					}
					rows := map[types.Rowid]uint8{}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if optimized {
							txn.deleteTableWrites(7, 42, nil, blocks, 0, math.MaxUint32, rows)
						} else {
							txn.referenceDeleteTableWrites(7, 42, nil, blocks, 0, math.MaxUint32, rows)
						}
					}
				})
			}
		}
	}
}

func BenchmarkDeleteTableWritesGuardBoundary(b *testing.B) {
	proc := testutil.NewProcess(b)
	var workspace types.Rowid
	workspace.SetSegment(colexec.TxnWorkspaceSegment)
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{objectio.PhysicalAddr_Attr}
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(b, vector.AppendFixed(bat.Vecs[0], workspace, false, proc.Mp()))
	bat.SetRowCount(1)
	b.Cleanup(func() { bat.Clean(proc.Mp()) })
	for _, tc := range []struct {
		writes, blocks   int
		sameTable, mixed bool
	}{
		{2, 1, false, false}, {2, 1, true, false},
		{256, 255, false, false}, {256, 255, true, false},
		{256, 256, false, false}, {256, 256, true, false},
		{256, 257, false, false}, {256, 257, true, false},
		{256, 255, false, true}, {256, 255, true, true},
		{1024, 256, false, false},
	} {
		for _, optimized := range []bool{false, true} {
			b.Run(fmt.Sprintf("writes=%d/blocks=%d/same=%t/mixed=%t/guard=%t", tc.writes, tc.blocks, tc.sameTable, tc.mixed, optimized), func(b *testing.B) {
				txn := &Transaction{writes: make([]Entry, tc.writes)}
				if tc.sameTable {
					for i := range txn.writes {
						txn.writes[i] = Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: bat}
					}
				}
				blocks := make(map[types.Blockid]bool, tc.blocks)
				for i := 0; i < tc.blocks; i++ {
					var id types.Rowid
					id.SetBlkOffset(uint16(i))
					if tc.mixed && i == tc.blocks-1 {
						id.SetSegment(colexec.TxnWorkspaceSegment)
						id.SetBlkOffset(math.MaxUint16)
					}
					blocks[id.CloneBlockID()] = true
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if optimized {
						txn.deleteTableWrites(7, 42, nil, blocks, 0, math.MaxUint32, nil)
					} else {
						txn.referenceDeleteTableWrites(7, 42, nil, blocks, 0, math.MaxUint32, nil)
					}
				}
			})
		}
	}
}

// Frozen pre-guard scan from 76e1a9bd9f. It deliberately does not share the
// candidate's namespace predicate; compare both mutation surfaces below.
func (txn *Transaction) referenceDeleteTableWrites(
	databaseId uint64,
	tableId uint64,
	sels []int64,
	deleteBlkId map[types.Blockid]bool,
	min, max uint32,
	mp map[types.Rowid]uint8,
) {
	txn.Lock()
	defer txn.Unlock()
	for _, entry := range txn.writes {
		if entry.tableId != tableId || entry.databaseId != databaseId {
			continue
		}
		if entry.typ == ALTER || entry.typ == DELETE {
			continue
		}
		if entry.bat == nil || entry.bat.RowCount() == 0 {
			continue
		}
		if entry.bat.Attrs[0] == catalog.BlockMeta_BlockInfo {
			continue
		}
		sels = sels[:0]
		rowids := vector.MustFixedColWithTypeCheck[types.Rowid](entry.bat.GetVector(0))
		if len(rowids) == 0 {
			continue
		}
		if !deleteBlkId[rowids[0].CloneBlockID()] {
			continue
		}
		min2 := rowids[0].GetRowOffset()
		max2 := rowids[len(rowids)-1].GetRowOffset()
		if min > max2 || max < min2 {
			continue
		}
		for k, v := range rowids {
			if _, ok := mp[v]; ok {
				sels = append(sels, int64(k))
				mp[v]++
			}
		}
		if len(sels) > 0 {
			txn.addBatchSelectionsLocked(entry.bat, sels)
		}
	}
}

func BenchmarkDeleteTableWritesNamespaceGuard(b *testing.B) {
	proc := testutil.NewProcess(b)
	var workspace types.Rowid
	workspace.SetSegment(colexec.TxnWorkspaceSegment)
	workspace.SetBlkOffset(2)
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{objectio.PhysicalAddr_Attr}
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	require.NoError(b, vector.AppendFixed(bat.Vecs[0], workspace, false, proc.Mp()))
	bat.SetRowCount(1)
	b.Cleanup(func() { bat.Clean(proc.Mp()) })
	var persisted types.Rowid
	segment := colexec.TxnWorkspaceSegment
	segment[0] = 1
	persisted.SetSegment(segment)
	missing := workspace
	missing.SetBlkOffset(3)
	for _, name := range []string{"persisted", "mixed", "workspace"} {
		b.Run(name, func(b *testing.B) {
			kind := name
			for _, optimized := range []bool{false, true} {
				name := "reference"
				if optimized {
					name = "guard"
				}
				b.Run(name, func(b *testing.B) {
					txn := &Transaction{writes: make([]Entry, 1024)}
					for i := range txn.writes {
						txn.writes[i] = Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: bat}
					}
					blocks := make(map[types.Blockid]bool)
					if kind != "workspace" {
						blocks[persisted.CloneBlockID()] = true
					}
					if kind != "persisted" {
						blocks[missing.CloneBlockID()] = true
					}
					rows := map[types.Rowid]uint8{persisted: 0}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if optimized {
							txn.deleteTableWrites(7, 42, nil, blocks, 0, math.MaxUint32, rows)
						} else {
							txn.referenceDeleteTableWrites(7, 42, nil, blocks, 0, math.MaxUint32, rows)
						}
					}
				})
			}
		})
	}
}

func TestDeleteBatchPreservesMixedRowOwnership(t *testing.T) {
	proc := testutil.NewProc(t)
	rt := runtime.ServiceRuntime("")
	previous, existed := rt.GetGlobalVariables(runtime.ColexecServer)
	server := colexec.NewServer("")
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(runtime.ColexecServer, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(runtime.ColexecServer, server)
		}
	})
	op := newTxnOperatorForTest(t)
	txn := &Transaction{proc: proc, op: op, engine: &Engine{},
		batchSelectList: make(map[*batch.Batch][]int64),
		deletedBlocks:   &deletedBlocks{offsets: make(map[types.Blockid][]int64)}}
	var memory, committed, spilled types.Rowid
	memory.SetSegment(colexec.TxnWorkspaceSegment)
	memory.SetRowOffset(1)
	segment := colexec.TxnWorkspaceSegment
	segment[0] = 1
	committed.SetSegment(segment)
	segment[0] = 2
	spilled.SetSegment(segment)
	spilled.SetRowOffset(3)
	server.PutCnSegment(op.Txn().ID, 42, spilled.BorrowSegmentID(), colexec.TxnWorkspaceUnCommitType)
	t.Cleanup(func() { server.DeleteTxnSegmentIds(op.Txn().ID) })
	makeBatch := func(ids ...types.Rowid) *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.Attrs = []string{objectio.PhysicalAddr_Attr}
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(bat.Vecs[0], ids, nil, proc.Mp()))
		bat.SetRowCount(len(ids))
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		return bat
	}
	insert := makeBatch(memory)
	txn.writes = []Entry{{typ: INSERT, databaseId: 7, tableId: 42, bat: insert}}
	for _, ids := range [][]types.Rowid{{committed, spilled}, {committed, spilled, memory}} {
		out := txn.deleteBatch(makeBatch(ids...), 7, 42)
		require.Equal(t, []types.Rowid{committed}, vector.MustFixedColWithTypeCheck[types.Rowid](out.Vecs[0]))
	}
	require.Equal(t, []int64{0}, txn.batchSelectList[insert])
	require.Equal(t, []int64{3, 3}, txn.deletedBlocks.offsets[spilled.CloneBlockID()])
}

func TestDeleteTableWritesNamespaceGuard(t *testing.T) {
	t.Run("empty workspace", func(t *testing.T) {
		txn := &Transaction{}
		rows := map[types.Rowid]uint8{{}: 2}
		txn.deleteTableWrites(7, 42, nil, map[types.Blockid]bool{{}: true}, 0, math.MaxUint32, rows)
		require.Equal(t, map[types.Rowid]uint8{{}: 2}, rows)
		require.Empty(t, txn.batchSelectList)
	})
	rowID := func(workspace bool, block uint16, row uint32) types.Rowid {
		var id types.Rowid
		segment := colexec.TxnWorkspaceSegment
		if !workspace {
			segment[0] = 1
		}
		id.SetSegment(segment)
		id.SetBlkOffset(block)
		id.SetRowOffset(row)
		return id
	}
	hit, miss, persisted := rowID(true, 1, 1), rowID(true, 9, 1), rowID(false, 1, 1)
	for _, tt := range []struct {
		name       string
		keys       []types.Rowid
		falseBlock bool
		min, max   uint32
		want       []int64
	}{
		{"empty", nil, false, 0, math.MaxUint32, []int64{0}},
		{"persisted", []types.Rowid{persisted}, false, 0, math.MaxUint32, []int64{0}},
		{"workspace hit", []types.Rowid{hit}, false, 0, math.MaxUint32, []int64{0, 1}},
		{"workspace miss", []types.Rowid{miss}, false, 0, math.MaxUint32, []int64{0}},
		{"mixed", []types.Rowid{persisted, hit}, false, 0, math.MaxUint32, []int64{0, 1}},
		{"false block", []types.Rowid{hit}, true, 0, math.MaxUint32, []int64{0}},
		{"range miss", []types.Rowid{hit}, false, 10, 20, []int64{0}},
		{"row without workspace block", []types.Rowid{hit}, false, 0, math.MaxUint32, []int64{0}},
		{"workspace block without row", []types.Rowid{persisted}, false, 0, math.MaxUint32, []int64{0}},
		{"blocks equal writes", []types.Rowid{hit}, false, 0, math.MaxUint32, []int64{0, 1}},
		{"blocks exceed writes", []types.Rowid{hit}, false, 0, math.MaxUint32, []int64{0, 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			makeTxn := func() *Transaction {
				txn := &Transaction{batchSelectList: make(map[*batch.Batch][]int64)}
				for i := 0; i < 8; i++ {
					b := batch.NewWithSize(1)
					b.Attrs = []string{objectio.PhysicalAddr_Attr}
					b.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
					require.NoError(t, vector.AppendFixedList(b.Vecs[0], []types.Rowid{rowID(true, 1, 0), hit, rowID(true, 1, 2)}, nil, proc.Mp()))
					b.SetRowCount(3)
					t.Cleanup(func() { b.Clean(proc.Mp()) })
					e := Entry{typ: INSERT, databaseId: 7, tableId: 42, bat: b}
					switch i {
					case 1:
						e.databaseId++
					case 2:
						e.tableId++
					case 3:
						e.typ = DELETE
					case 4:
						e.typ = ALTER
					case 5:
						b.Attrs[0] = catalog.BlockMeta_BlockInfo
					case 6:
						b.SetRowCount(0)
					case 7:
						e.bat = nil
					}
					txn.writes = append(txn.writes, e)
				}
				txn.batchSelectList[txn.writes[0].bat] = []int64{0}
				return txn
			}
			got, ref := makeTxn(), makeTxn()
			blocks := make(map[types.Blockid]bool)
			gotRows, refRows := map[types.Rowid]uint8{}, map[types.Rowid]uint8{}
			for _, id := range tt.keys {
				blocks[id.CloneBlockID()] = !tt.falseBlock
				gotRows[id] = 2
				refRows[id] = 2
			}
			switch tt.name {
			case "row without workspace block":
				blocks = map[types.Blockid]bool{persisted.CloneBlockID(): true}
			case "workspace block without row":
				blocks[hit.CloneBlockID()] = true
			case "blocks equal writes", "blocks exceed writes":
				for i := uint16(1); i < 8; i++ {
					id := rowID(false, i, 0)
					blocks[id.CloneBlockID()] = true
				}
				if tt.name == "blocks exceed writes" {
					id := rowID(false, 8, 0)
					blocks[id.CloneBlockID()] = true
				}
			}
			// Repeated passes must preserve the old counter and selection behavior.
			for repeat := 0; repeat < 2; repeat++ {
				got.deleteTableWrites(7, 42, nil, blocks, tt.min, tt.max, gotRows)
				ref.referenceDeleteTableWrites(7, 42, nil, blocks, tt.min, tt.max, refRows)
				require.Equal(t, refRows, gotRows)
				for i := range got.writes {
					require.Equal(t, ref.batchSelectList[ref.writes[i].bat], got.batchSelectList[got.writes[i].bat])
				}
				require.Equal(t, tt.want, got.batchSelectList[got.writes[0].bat])
				require.Equal(t, []types.Rowid{rowID(true, 1, 0), hit, rowID(true, 1, 2)}, vector.MustFixedColWithTypeCheck[types.Rowid](got.writes[0].bat.Vecs[0]))
			}
		})
	}
}
