package external

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
)

func benchmarkParquetBoolPage(b *testing.B, dictionary, nullable bool) (parquet.Page, *columnMapper, *process.Process) {
	b.Helper()
	const rowsCount = 128 * 1024
	var buf bytes.Buffer
	var node parquet.Node = parquet.Leaf(parquet.BooleanType)
	if dictionary {
		node = parquet.Encoded(node, &parquet.RLEDictionary)
	}
	if nullable {
		node = parquet.Optional(node)
	}
	schema := parquet.NewSchema("x", parquet.Group{"c": node})
	w := parquet.NewWriter(&buf, schema)
	rows := make([]parquet.Row, rowsCount)
	for i := range rows {
		v := parquet.BooleanValue(i&1 == 0)
		if nullable && i%17 == 0 {
			v = parquet.NullValue()
		}
		def := 0
		if nullable && !v.IsNull() {
			def = 1
		}
		rows[i] = parquet.Row{v.Level(0, def, 0)}
	}
	_, err := w.WriteRows(rows)
	if err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		b.Fatal(err)
	}
	col := f.Root().Column("c")
	page, err := col.Pages().ReadPage()
	if err != nil {
		b.Fatal(err)
	}
	proc := testutil.NewProc(b)
	var h ParquetHandler
	mp := h.getMapper(col, plan.Type{Id: int32(types.T_bool), NotNullable: !nullable})
	if mp == nil {
		b.Fatal("missing bool mapper")
	}
	return page, mp, proc
}

func BenchmarkParquetDictionaryBoolMapping(b *testing.B) {
	for _, tc := range []struct {
		name       string
		dictionary bool
		nullable   bool
	}{
		{name: "plain_required"},
		{name: "dictionary_required", dictionary: true},
		{name: "plain_nullable", nullable: true},
		{name: "dictionary_nullable", dictionary: true, nullable: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			page, mp, proc := benchmarkParquetBoolPage(b, tc.dictionary, tc.nullable)
			defer proc.Free()
			vec := vector.NewVec(types.New(types.T_bool, 0, 0))
			defer vec.Free(proc.Mp())
			b.SetBytes(page.NumRows())
			b.ReportMetric(float64(page.NumRows()), "rows/op")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vec.ResetWithSameType()
				if err := mp.mapping(page, proc, vec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
