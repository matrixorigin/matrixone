// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package multi_update

import (
	"context"
	"math"
	"strings"
	"testing"
	"unsafe"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFlushMetadataBorrowsEncodedPayload(t *testing.T) {
	for _, action := range []actionType{actionInsert, actionDelete} {
		t.Run(map[actionType]string{actionInsert: "insert", actionDelete: "delete"}[action], func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(proc.Free)
			w := &s3WriterDelegate{updateCtxs: []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}}}}
			t.Cleanup(func() { require.NoError(t, w.free(proc)) })
			source := batch.NewOffHeapWithSize(2)
			t.Cleanup(func() { source.Clean(proc.Mp()) })
			name := ""
			frameRows := uint64(17)
			if action == actionDelete {
				source.Attrs = DeleteBatchAttrs
				source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_Rowid.ToType())
				block := types.BuildTestBlockid(1, 1)
				require.NoError(t, vector.AppendFixed(source.Vecs[0], types.NewRowid(&block, 0), false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(source.Vecs[0], types.NewRowid(&block, 1), false, proc.Mp()))
				source.Vecs[1] = testutil.MakeVarcharVector([]string{strings.Repeat("x", 1<<20), ""}, []uint64{1}, proc.Mp())
				source.SetRowCount(2)
				frameRows = uint64(source.RowCount())
				name = objectio.PhysicalAddr_Attr
			} else {
				source.Attrs = []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
				source.Vecs[0] = testutil.MakeTextVector([]string{string(objectio.EncodeBlockInfo(&objectio.BlockInfo{}))}, nil, proc.Mp())
				source.Vecs[1] = vector.NewOffHeapVecWithType(types.T_binary.ToType())
				require.NoError(t, vector.AppendBytes(source.Vecs[1], objectio.ZeroObjectStats[:], false, proc.Mp()))
				source.SetRowCount(1)
			}
			require.NoError(t, w.spoolFrame(proc, action, 0, frameRows, name, source))
			require.NoError(t, w.readMetadataFrame(proc))
			frame := w.outputBat
			require.Len(t, frame.Vecs, 5)
			require.Equal(t, 1, frame.RowCount())
			payload := frame.Vecs[4].GetBytesAt(0)
			start := uintptr(unsafe.Pointer(unsafe.SliceData(payload)))
			end := start + uintptr(len(payload))
			inside := func(ptr uintptr, size int) {
				if size > 0 {
					require.GreaterOrEqual(t, ptr, start)
					require.LessOrEqual(t, ptr+uintptr(size), end)
				}
			}
			observed := false
			inspect := func(decoded *batch.Batch) {
				observed = true
				require.Len(t, decoded.Vecs, 2)
				for _, vec := range decoded.Vecs {
					inside(uintptr(unsafe.Pointer(unsafe.SliceData(vec.GetData()))), len(vec.GetData()))
					inside(uintptr(unsafe.Pointer(unsafe.SliceData(vec.GetArea()))), len(vec.GetArea()))
					ptr, size := vec.GetNulls().RawPtrLen()
					inside(ptr, int((size+63)/64*8))
					require.True(t, vec.NeedDup(), "nested data/area remains borrowed until the engine's synchronous consume/copy completes")
				}
			}
			ctrl := gomock.NewController(t)
			eng := mock_frontend.NewMockEngine(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(99)).Return("test", "metadata_t", rel, nil).Times(1)
			rel.EXPECT().GetTableName().Return("metadata_t").AnyTimes()
			if action == actionDelete {
				rel.EXPECT().Delete(gomock.Any(), gomock.Any(), name).DoAndReturn(func(_ context.Context, decoded *batch.Batch, _ string) error { inspect(decoded); return nil }).Times(1)
			} else {
				rel.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, decoded *batch.Batch) error { inspect(decoded); return nil }).Times(1)
			}
			op := NewArgument()
			t.Cleanup(func() { op.Free(proc, false, nil); op.Release() })
			op.Action, op.Engine = UpdateFlushS3Info, eng
			op.ctr.sources = make(map[uint64]engine.Relation)
			op.ctr.action = action
			op.OpAnalyzer = process.NewAnalyzer(0, false, false, "flush-metadata-no-copy")
			var affected uint64
			op.addAffectedRowsFunc = func(rows uint64) { affected += rows }
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{frame})
			t.Cleanup(func() { child.ResetBatchs(); child.Free(proc, false, nil) }) // Writer owns the borrowed frame.
			op.AppendChild(child)
			before := proc.Mp().AllocatedBytes()
			result, err := op.Call(proc)
			require.NoError(t, err)
			require.Same(t, frame, result.Batch)
			require.True(t, observed)
			require.Equal(t, frameRows, affected)
			require.Equal(t, before, proc.Mp().AllocatedBytes(), "decoder borrows the admitted output frame instead of creating another payload owner")
			require.Len(t, op.ctr.sources, 1)
		})
	}
}

func TestMultiUpdateMetadataBoundAndOverflow(t *testing.T) {
	small, ok := MetadataOutputBound(8<<10, 2, 10)
	require.True(t, ok)
	large, ok := MetadataOutputBound(4<<20, 2, 10)
	require.True(t, ok)
	require.Equal(t, small.ScratchBytes, large.ScratchBytes, "no-copy decoding needs no extra full-payload clone allowance")
	require.Equal(t, 1, large.Rows)
	require.Equal(t, int64(4<<20), large.BatchBytes)
	require.Equal(t, [3]int64{1, 8, 8}, [3]int64{large.ColumnBytes[0], large.ColumnBytes[1], large.ColumnBytes[2]})
	moreTables, ok := MetadataOutputBound(4<<20, 65, 130)
	require.True(t, ok)
	require.Greater(t, moreTables.ScratchBytes, large.ScratchBytes)
	for _, args := range [][3]int64{{0, 1, 1}, {-1, 1, 1}, {8 << 10, 0, 0}, {8 << 10, -1, 1}, {8 << 10, 2, 1}, {8 << 10, math.MaxInt, math.MaxInt}} {
		_, ok := MetadataOutputBound(args[0], int(args[1]), int(args[2]))
		require.False(t, ok)
	}
}
