// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package frontend

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParquetVariantIndependentInstanceExport(t *testing.T) {
	dir := os.Getenv("MO_VARIANT_INTEROP_DIR")
	if dir == "" {
		t.Skip("set MO_VARIANT_INTEROP_DIR for independent-engine verification")
	}
	proc := testutil.NewProc(t)
	mrs := &MysqlResultSet{}
	id := new(MysqlColumn)
	id.SetName("a_unused_000")
	id.SetColumnType(defines.MYSQL_TYPE_LONG)
	mrs.AddColumn(id)
	doc := new(MysqlColumn)
	doc.SetName("z_nested")
	doc.SetColumnType(defines.MYSQL_TYPE_JSON)
	mrs.AddColumn(doc)
	w, e := NewParquetWriterWithOptions(context.Background(), mrs, ParquetWriterOptions{JSONAsVariant: true})
	require.NoError(t, e)
	for start := 0; start < 10000; start += 1000 {
		b := batch.New([]string{"a_unused_000", "z_nested"})
		b.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		b.Vecs[1] = vector.NewVec(types.T_json.ToType())
		for i := start; i < start+1000; i++ {
			drift := fmt.Sprint(i)
			if i%11 == 0 {
				drift = "null"
			}
			bj, e := bytejson.ParseFromStringExact(fmt.Sprintf(`{"trace_id":"trace-%d","cost":9007199254740993.%03d,"latency":%d,"drift":%s,"nested":{"ok":%t,"tags":["agent","tool"]}}`, i, i%1000, i%997, drift, i%2 == 0))
			require.NoError(t, e)
			require.NoError(t, vector.AppendFixed(b.Vecs[0], int32(i), false, proc.Mp()))
			require.NoError(t, vector.AppendByteJson(b.Vecs[1], bj, false, proc.Mp()))
		}
		b.SetRowCount(1000)
		require.NoError(t, w.WriteBatch(b, proc.Mp(), time.UTC))
		b.Clean(proc.Mp())
	}
	data, e := w.Close()
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mo-variant.parquet"), data, 0600))
}
