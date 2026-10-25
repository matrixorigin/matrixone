// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package multi_update

import (
	"encoding/binary"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func metadataProtocolWriter(t *testing.T, contexts []*MultiUpdateCtx) (*process.Process, *s3WriterDelegate) {
	t.Helper()
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	owner := rscthrottler.NewMemThrottler(t.Name(), 1, rscthrottler.WithConstLimit(1<<30))
	ws, err := rscthrottler.ReserveWorkingSet(proc.Ctx, owner, rscthrottler.WorkingSetSpec{UnitBytes: 1,
		Inputs: []rscthrottler.InputSlotSpec{{Name: "csv"}, {Name: "output", Count: 1, Bytes: 64 << 20}}, IOBytes: 1})
	require.NoError(t, err)
	t.Cleanup(ws.Close)
	proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: ws, UnitBytes: 1, OutputBytes: 64 << 20}
	w := &s3WriterDelegate{updateCtxs: contexts}
	t.Cleanup(func() { require.NoError(t, w.free(proc)) })
	return proc, w
}

func TestMultiUpdateInsertOnlyChecksEveryContext(t *testing.T) {
	insert := &MultiUpdateCtx{InsertCols: []int{0}}
	remove := &MultiUpdateCtx{DeleteCols: []int{0, 1}}
	require.True(t, InsertOnlyOutput([]*MultiUpdateCtx{insert, insert}))
	for _, contexts := range [][]*MultiUpdateCtx{nil, {}, {nil}, {insert, nil}, {insert, remove}, {remove, insert}} {
		require.False(t, InsertOnlyOutput(contexts))
	}
}

func TestMultiUpdateInsertMetadataRejectsGlobalSlotSizedFrame(t *testing.T) {
	proc, w := metadataProtocolWriter(t, []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}, InsertCols: []int{0}}})
	require.NoError(t, w.ensureOutputSlot(proc))
	limit, err := colexec.InsertMetadataFrameLimit(1, 1)
	require.NoError(t, err)
	require.Equal(t, limit, w.outputFrameLimit)
	require.Less(t, limit+1, w.outputLease.Capacity()/4)
	w.metadataSpool, err = os.CreateTemp(t.TempDir(), "oversized-fixed-metadata-*")
	require.NoError(t, err)
	require.NoError(t, os.Remove(w.metadataSpool.Name()))
	var header [9]byte
	binary.LittleEndian.PutUint64(header[:8], uint64(limit+1))
	header[8] = byte(actionInsert)
	_, err = w.metadataSpool.Write(header[:])
	require.NoError(t, err)
	w.metadataWritten = limit + 1 + 9
	require.NoError(t, w.metadataSpool.Truncate(w.metadataWritten))
	require.ErrorContains(t, w.readMetadataFrame(proc), "exceeds admitted output capacity")
	require.Zero(t, w.metadataPool().AllocatedBytes(), "reject oversized frame before decoding its backing")
}

func TestMultiUpdateMetadataProtocolPreservesFixedInsertAndWideRawDelete(t *testing.T) {
	for _, rawDelete := range []bool{false, true} {
		t.Run(map[bool]string{false: "fixed-insert", true: "secondary-raw-delete"}[rawDelete], func(t *testing.T) {
			contexts := []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}, InsertCols: []int{0}}}
			idx, action, name := 0, actionInsert, ""
			if rawDelete {
				contexts = append(contexts, &MultiUpdateCtx{TableDef: &plan.TableDef{TblId: 100}, DeleteCols: []int{0, 1}})
				idx, action, name = 1, actionDelete, objectio.PhysicalAddr_Attr
			}
			proc, w := metadataProtocolWriter(t, contexts)
			source := batch.NewOffHeapWithSize(2)
			t.Cleanup(func() { source.Clean(proc.Mp()) })
			if rawDelete {
				source.Attrs = DeleteBatchAttrs
				source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_Rowid.ToType())
				block := types.BuildTestBlockid(1, 1)
				require.NoError(t, vector.AppendFixed(source.Vecs[0], types.NewRowid(&block, 0), false, proc.Mp()))
				source.Vecs[1] = testutil.MakeVarcharVector([]string{strings.Repeat("k", 1<<20)}, nil, proc.Mp())
			} else {
				source.Attrs = []string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
				source.Vecs[0] = testutil.MakeTextVector([]string{string(objectio.EncodeBlockInfo(&objectio.BlockInfo{}))}, nil, proc.Mp())
				source.Vecs[1] = vector.NewOffHeapVecWithType(types.T_binary.ToType())
				require.NoError(t, vector.AppendBytes(source.Vecs[1], objectio.ZeroObjectStats[:], false, proc.Mp()))
			}
			source.SetRowCount(1)
			require.NoError(t, w.spoolFrame(proc, action, idx, 1, name, source))
			require.NoError(t, w.readMetadataFrame(proc))
			out := w.outputBat
			require.NotNil(t, out)
			require.Equal(t, uint8(action), vector.MustFixedColNoTypeCheck[uint8](out.Vecs[0])[0])
			require.Equal(t, contexts[idx].TableDef.TblId, vector.MustFixedColNoTypeCheck[uint64](out.Vecs[1])[0])
			fixed, err := colexec.InsertMetadataFrameLimit(1, len(contexts))
			require.NoError(t, err)
			if rawDelete {
				require.Greater(t, out.MarshalBinarySize(), fixed, "a real wide secondary delete cannot use the main table's fixed INSERT cap")
				require.Equal(t, w.outputLease.Capacity()/4, w.outputFrameLimit)
			} else {
				require.Equal(t, fixed, w.outputFrameLimit)
			}
			decoded := batch.NewOffHeapEmpty()
			t.Cleanup(func() { decoded.Clean(proc.Mp()) })
			require.NoError(t, decoded.UnmarshalBinary(out.Vecs[4].GetBytesAt(0)))
			require.Equal(t, source.MarshalBinarySize(), decoded.MarshalBinarySize())
			for col := range source.Vecs {
				before, encodeErr := source.Vecs[col].MarshalBinary()
				require.NoError(t, encodeErr)
				after, encodeErr := decoded.Vecs[col].MarshalBinary()
				require.NoError(t, encodeErr)
				require.Equal(t, before, after)
			}
		})
	}
}
