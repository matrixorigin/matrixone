// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package external

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMemoryInputSchemaMatchesPhysicalPrepare(t *testing.T) {
	for _, format := range []string{tree.CSV} {
		for _, parallel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s-parallel-%t", format, parallel), func(t *testing.T) {
				proc := testutil.NewProc(t)
				t.Cleanup(proc.Free)
				param := &ExternalParam{ExParamConst: ExParamConst{ParallelLoad: parallel, Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: format, ScanType: tree.INLINE, Tail: &tree.TailParameter{}}, ExParam: tree.ExParam{JsonData: tree.ARRAY}}}, ExParam: ExParam{Fileparam: &ExFileparam{}}}
				for i, oid := range []types.T{types.T_int64, types.T_json, types.T_datetime, types.T_array_float64} {
					name := fmt.Sprintf("c%d", i)
					param.Cols = append(param.Cols, &plan.ColDef{Name: name, Typ: plan.Type{Id: int32(oid)}})
					param.Attrs = append(param.Attrs, plan.ExternAttr{ColName: name, ColIndex: int32(i), ColFieldIndex: int32(i)})
				}
				op := &External{Es: param}
				t.Cleanup(func() { op.Free(proc, false, nil) })
				schema, ok := op.MemoryInputSchema()
				require.True(t, ok)
				require.True(t, op.UsesMemoryInput())
				require.NoError(t, op.Prepare(proc))
				require.Len(t, schema, len(op.ctr.buf.Vecs))
				for i, vec := range op.ctr.buf.Vecs {
					require.Equal(t, *vec.GetType(), schema[i])
					if parallel {
						require.Equal(t, types.T_varchar, schema[i].Oid)
					} else {
						require.Equal(t, types.T(param.Cols[i].Typ.Id), schema[i].Oid)
					}
				}
			})
		}
	}
}

func TestMemoryInputSchemaRejectsUnprovenSyntheticPath(t *testing.T) {
	op := &External{Es: &ExternalParam{ExParamConst: ExParamConst{Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.CSV}, ExParam: tree.ExParam{ExternType: int32(plan.ExternType_EXTERNAL_TB)}}, Cols: []*plan.ColDef{{Name: catalog.ExternalFilePath, Typ: plan.Type{Id: int32(types.T_varchar)}}}, Attrs: []plan.ExternAttr{{ColName: catalog.ExternalFilePath}}}}}
	require.False(t, op.UsesMemoryInput())
	_, ok := op.MemoryInputSchema()
	require.False(t, ok)
	op.Es.Attrs[0].ColName = "value"
	require.True(t, op.UsesMemoryInput(), "an ordinary raw-record column retains its existing memory path")
}

func TestJSONLineRetainsLegacyReaderOutsideMemoryProof(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	param := &ExternalParam{ExParamConst: ExParamConst{Extern: &tree.ExternParam{ExParamConst: tree.ExParamConst{Format: tree.JSONLINE, Tail: &tree.TailParameter{}}, ExParam: tree.ExParam{JsonData: tree.ARRAY}}, Cols: []*plan.ColDef{{Typ: plan.Type{Id: int32(types.T_int64)}}}, Attrs: []plan.ExternAttr{{ColIndex: 0, ColFieldIndex: 0}}}}
	op := &External{Es: param}
	require.False(t, op.UsesMemoryInput())
	_, ok := op.MemoryInputSchema()
	require.False(t, ok, "JSON construction/normalization is not implied by physical vector types")
	reader, err := NewCsvReader(param, proc)
	require.NoError(t, err, "the legal JSONLINE reader remains available on its existing execution path")
	require.NoError(t, reader.Close())
}
