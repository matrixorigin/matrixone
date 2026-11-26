// Copyright 2026 Matrix Origin
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

package catalog

import (
	"context"
	"errors"
	"sync"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

type deferredCatalogReplay struct {
	jobs map[uint64][]func()
}

func (r *deferredCatalogReplay) Submit(_ context.Context, db uint64, fn func()) error {
	r.jobs[db] = append(r.jobs[db], fn)
	return nil
}

func TestReplaySystemTablesOwnsRowsAcrossBlocks(t *testing.T) {
	mp := mpool.MustNew("catalog-replay-stream")
	defer mpool.DeleteMPool(mp)
	packer := types.NewPacker()
	defer packer.Close()
	cat := MockCatalog(&nilReplayDataFactory{})
	defer cat.Close()

	integer := types.T_int64.ToType()
	integerBytes, err := types.Encode(&integer)
	require.NoError(t, err)
	textType := types.T_varchar.ToType()
	textBytes, err := types.Encode(&textType)
	require.NoError(t, err)
	rowIDType := types.T_Rowid.ToType()
	rowIDBytes, err := types.Encode(&rowIDType)
	require.NoError(t, err)
	generated, err := types.Encode(&plan.GeneratedCol{OriginString: "id + 1", IsStored: true})
	require.NoError(t, err)
	extra, err := (&apipb.SchemaExtra{BlockMaxRows: 128, ObjectMaxBlocks: 8, NextColSeqnum: 4}).Marshal()
	require.NoError(t, err)

	makeDB := func(id uint64, name string) *batch.Batch {
		b, e := pkgcatalog.GenCreateDatabaseTuple("", 0, 1, 2, name, id, "", mp, packer)
		require.NoError(t, e)
		return b
	}
	makeTable := func(id, dbID uint64, name, dbName string) *batch.Batch {
		table := pkgcatalog.Table{
			TableId: id, DatabaseId: dbID, TableName: name, DatabaseName: dbName,
			Comment: "owned comment " + name, CreateSql: "create " + name,
		}
		if name == "alpha" {
			table.ExtraInfo = extra
		} else {
			// Released schemas may have a nonzero version but no high water mark.
			table.Version = 1
		}
		b, e := pkgcatalog.GenCreateTableTuple(table, mp, packer)
		require.NoError(t, e)
		return b
	}
	makeColumns := func(cols ...pkgcatalog.Column) *batch.Batch {
		b, e := pkgcatalog.GenCreateColumnTuples(cols, mp, packer)
		require.NoError(t, e)
		return b
	}
	col := func(id, dbID uint64, name, dbName, colName string, number int32, typ []byte) pkgcatalog.Column {
		return pkgcatalog.Column{TableId: id, DatabaseId: dbID, TableName: name, DatabaseName: dbName,
			Name: colName, Num: number, Seqnum: uint16(number - 1), Typ: typ}
	}

	// Deliberately interleave tables and split each table's columns across
	// blocks. Source bytes are overwritten and freed before queued replay runs.
	alphaLabel := col(101, 11, "alpha", "db1", "label", 2, textBytes)
	alphaLabel.Seqnum = 3
	alphaLabel.HasGenerated = 1
	alphaLabel.GeneratedExpr = generated
	legacyBlock := makeColumns(col(101, 11, "alpha", "db1", "id", 1, integerBytes), col(102, 12, "beta", "db2", "id", 1, integerBytes))
	generatedIdx := pkgcatalog.MO_COLUMNS_ATT_GENERATED_IDX
	generatedType := *legacyBlock.Vecs[generatedIdx].GetType()
	legacyBlock.Vecs[generatedIdx].Free(mp)
	legacyBlock.Vecs[generatedIdx] = vector.NewConstNull(generatedType, legacyBlock.RowCount(), mp)
	// Exercise both a NULL sequence and an absent sequence vector on different
	// blocks of the same evolved legacy table. Finalize must recover ordinals.
	legacyBlock.Vecs[pkgcatalog.MO_COLUMNS_ATT_SEQNUM_IDX].GetNulls().Add(1)
	legacyLabel := makeColumns(col(102, 12, "beta", "db2", "label", 2, textBytes))
	blocks := map[uint64][]*batch.Batch{
		pkgcatalog.MO_DATABASE_ID: {
			makeDB(11, "db1"), makeDB(12, "db2"),
		},
		pkgcatalog.MO_TABLES_ID: {
			makeTable(102, 12, "beta", "db2"), makeTable(101, 11, "alpha", "db1"),
		},
		pkgcatalog.MO_COLUMNS_ID: {
			legacyLabel,
			makeColumns(alphaLabel),
			legacyBlock,
			makeColumns(col(101, 11, "alpha", "db1", PhyAddrColumnName, 3, rowIDBytes), col(102, 12, "beta", "db2", PhyAddrColumnName, 3, rowIDBytes)),
		},
	}
	scan := func(ctx context.Context, entry *TableEntry, _ txnif.AsyncTxn, visit func(*containers.Batch, int) error) error {
		for _, source := range blocks[entry.ID] {
			b := containers.ToTNBatch(source, catalogTupleAttrsForTest(entry.ID), mp)
			if source == legacyLabel {
				delete(b.Nameidx, pkgcatalog.SystemColAttr_Seqnum)
			}
			for row := 0; row < b.Length(); row++ {
				if err := visit(b, row); err != nil {
					b.Close()
					return err
				}
			}
			for _, v := range source.Vecs {
				if !v.GetType().IsVarlen() || v.IsConstNull() {
					continue
				}
				for row := 0; row < v.Length(); row++ {
					for i := range v.GetBytesAt(row) {
						v.GetBytesAt(row)[i] = 'x'
					}
				}
			}
			b.Close()
		}
		return ctx.Err()
	}
	replayer := &deferredCatalogReplay{jobs: make(map[uint64][]func())}
	require.NoError(t, cat.ReplayFromSysTableObjects(context.Background(), nil, scan, replayer))
	require.Len(t, replayer.jobs, 2)

	var wg sync.WaitGroup
	for _, jobs := range replayer.jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, job := range jobs {
				job()
			}
		}()
	}
	wg.Wait()
	for _, tc := range []struct {
		dbID, tableID uint64
		name          string
	}{{11, 101, "alpha"}, {12, 102, "beta"}} {
		db, e := cat.GetDatabaseByID(tc.dbID)
		require.NoError(t, e)
		table, e := db.GetTableEntryByID(tc.tableID)
		require.NoError(t, e)
		schema := table.GetLastestSchema(false)
		require.Equal(t, tc.name, schema.Name)
		require.Equal(t, "owned comment "+tc.name, schema.Comment)
		require.Equal(t, "create "+tc.name, schema.Createsql)
		require.Len(t, schema.ColDefs, 3)
		require.Equal(t, "id", schema.ColDefs[0].Name)
		require.Equal(t, "label", schema.ColDefs[1].Name)
		require.Equal(t, PhyAddrColumnName, schema.PhyAddrKey.Name)
		require.Equal(t, integer, schema.ColDefs[0].Type)
		require.Equal(t, textType, schema.ColDefs[1].Type)
		if tc.name == "alpha" {
			require.Equal(t, uint32(128), schema.Extra.BlockMaxRows)
			require.Equal(t, uint32(8), schema.Extra.ObjectMaxBlocks)
			require.Equal(t, uint16(3), schema.ColDefs[1].SeqNum)
			require.Equal(t, uint32(4), schema.Extra.NextColSeqnum)
			var got plan.GeneratedCol
			require.NoError(t, types.Decode(schema.ColDefs[1].GeneratedCol, &got))
			require.Equal(t, "id + 1", got.OriginString)
			require.True(t, got.IsStored)
		} else {
			require.Equal(t, uint32(objectio.BlockMaxRows), schema.Extra.BlockMaxRows)
			require.Equal(t, uint32(options.DefaultBlocksPerObject), schema.Extra.ObjectMaxBlocks)
			require.Equal(t, uint32(1), schema.Version)
			require.Equal(t, uint16(0), schema.GetSeqnum("id"))
			require.Equal(t, uint16(1), schema.GetSeqnum("label"))
			require.Equal(t, uint32(2), schema.Extra.NextColSeqnum)
		}
	}
}

func TestReplaySystemTablesStopsOnScannerError(t *testing.T) {
	cat := MockCatalog(&nilReplayDataFactory{})
	defer cat.Close()
	mp := mpool.MustNew("catalog-replay-error")
	defer mpool.DeleteMPool(mp)
	packer := types.NewPacker()
	defer packer.Close()
	want := errors.New("read system table failed")
	replayer := &deferredCatalogReplay{jobs: make(map[uint64][]func())}
	scan := func(_ context.Context, entry *TableEntry, _ txnif.AsyncTxn, visit func(*containers.Batch, int) error) error {
		var source *batch.Batch
		var err error
		switch entry.ID {
		case pkgcatalog.MO_DATABASE_ID:
			source, err = pkgcatalog.GenCreateDatabaseTuple("", 0, 0, 0, "db", 11, "", mp, packer)
		case pkgcatalog.MO_TABLES_ID:
			source, err = pkgcatalog.GenCreateTableTuple(pkgcatalog.Table{DatabaseId: 11, DatabaseName: "db", TableId: 101, TableName: "t"}, mp, packer)
		default:
			return want
		}
		require.NoError(t, err)
		b := containers.ToTNBatch(source, catalogTupleAttrsForTest(entry.ID), mp)
		defer b.Close()
		return visit(b, 0)
	}
	require.ErrorIs(t, cat.ReplayFromSysTableObjects(context.Background(), nil, scan, replayer), want)
	require.Empty(t, replayer.jobs)
	db, err := cat.GetDatabaseByID(11)
	require.NoError(t, err)
	_, err = db.GetTableEntryByID(101)
	require.Error(t, err)
}

func TestReplaySystemTablesRejectsIncompleteCatalog(t *testing.T) {
	for _, tc := range []struct {
		name                                                                                    string
		missingDB, missingRowID, wrongRowID, corruptType, missingSequence                       bool
		nullSequence, duplicateSequence, outOfRangeSequence, evolvedWithoutHighWater, rowIDOnly bool
		invalidHighWater, exhaustedHighWater                                                    bool
	}{
		{name: "missing parent database", missingDB: true},
		{name: "missing physical row ID", missingRowID: true},
		{name: "wrong physical row ID type", wrongRowID: true},
		{name: "corrupt column type", corruptType: true},
		{name: "missing evolved sequence metadata", missingSequence: true},
		{name: "null sequence with high water", nullSequence: true},
		{name: "duplicate sequence", duplicateSequence: true},
		{name: "sequence beyond high water", outOfRangeSequence: true},
		{name: "legacy evolved table with ordinal sequences", evolvedWithoutHighWater: true},
		{name: "row ID only", rowIDOnly: true},
		{name: "reserved high water", invalidHighWater: true},
		{name: "exhausted high water", exhaustedHighWater: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat := MockCatalog(&nilReplayDataFactory{})
			defer cat.Close()
			mp := mpool.MustNew("catalog-replay-invalid")
			defer mpool.DeleteMPool(mp)
			packer := types.NewPacker()
			defer packer.Close()
			integer := types.T_int64.ToType()
			integerBytes, err := types.Encode(&integer)
			require.NoError(t, err)
			rowIDType := types.T_Rowid.ToType()
			rowIDBytes, err := types.Encode(&rowIDType)
			require.NoError(t, err)
			if tc.wrongRowID {
				rowIDBytes = integerBytes
			}
			if tc.corruptType {
				integerBytes = []byte{1}
			}
			seqnum := uint16(0)
			if tc.missingSequence || tc.outOfRangeSequence {
				seqnum = 3
			}
			cols := []pkgcatalog.Column{{DatabaseId: 11, DatabaseName: "db", TableId: 101, TableName: "t", Name: "c", Num: 1, Seqnum: seqnum, Typ: integerBytes}}
			if tc.duplicateSequence {
				cols = append(cols, pkgcatalog.Column{DatabaseId: 11, DatabaseName: "db", TableId: 101, TableName: "t", Name: "c2", Num: 2, Seqnum: 0, Typ: integerBytes})
			}
			if tc.rowIDOnly {
				cols = nil
			}
			if !tc.missingRowID {
				cols = append(cols, pkgcatalog.Column{DatabaseId: 11, DatabaseName: "db", TableId: 101, TableName: "t", Name: PhyAddrColumnName, Num: int32(len(cols) + 1), Typ: rowIDBytes})
			}
			replayer := &deferredCatalogReplay{jobs: make(map[uint64][]func())}
			scan := func(_ context.Context, entry *TableEntry, _ txnif.AsyncTxn, visit func(*containers.Batch, int) error) error {
				var source *batch.Batch
				var e error
				switch entry.ID {
				case pkgcatalog.MO_DATABASE_ID:
					if tc.missingDB {
						return nil
					}
					source, e = pkgcatalog.GenCreateDatabaseTuple("", 0, 0, 0, "db", 11, "", mp, packer)
				case pkgcatalog.MO_TABLES_ID:
					table := pkgcatalog.Table{DatabaseId: 11, DatabaseName: "db", TableId: 101, TableName: "t"}
					if tc.nullSequence || tc.duplicateSequence || tc.outOfRangeSequence || tc.invalidHighWater || tc.exhaustedHighWater {
						nextSeq := uint32(3)
						if tc.invalidHighWater {
							nextSeq = uint32(objectio.SEQNUM_UPPER) + 1
						} else if tc.exhaustedHighWater {
							nextSeq = uint32(objectio.SEQNUM_UPPER)
						}
						table.ExtraInfo, e = (&apipb.SchemaExtra{NextColSeqnum: nextSeq}).Marshal()
						require.NoError(t, e)
					}
					if tc.evolvedWithoutHighWater || tc.rowIDOnly {
						table.Version = 1
					}
					source, e = pkgcatalog.GenCreateTableTuple(table, mp, packer)
				case pkgcatalog.MO_COLUMNS_ID:
					source, e = pkgcatalog.GenCreateColumnTuples(cols, mp, packer)
					if e == nil && tc.nullSequence {
						idx := pkgcatalog.MO_COLUMNS_ATT_SEQNUM_IDX
						typ := *source.Vecs[idx].GetType()
						source.Vecs[idx].Free(mp)
						source.Vecs[idx] = vector.NewConstNull(typ, source.RowCount(), mp)
					}
				}
				require.NoError(t, e)
				b := containers.ToTNBatch(source, catalogTupleAttrsForTest(entry.ID), mp)
				defer b.Close()
				for row := 0; row < b.Length(); row++ {
					if err := visit(b, row); err != nil {
						return err
					}
				}
				return nil
			}
			err = cat.ReplayFromSysTableObjects(context.Background(), nil, scan, replayer)
			if tc.rowIDOnly || tc.exhaustedHighWater || tc.evolvedWithoutHighWater {
				require.NoError(t, err)
				require.Len(t, replayer.jobs[11], 1)
				replayer.jobs[11][0]()
			} else {
				require.Error(t, err)
				require.Empty(t, replayer.jobs)
			}
		})
	}
}

// catalogTupleAttrsForTest names the columns of a catalog tuple batch built
// for the catalog table id.
func catalogTupleAttrsForTest(id uint64) []string {
	switch id {
	case pkgcatalog.MO_DATABASE_ID:
		return pkgcatalog.MoDatabaseSchema
	case pkgcatalog.MO_TABLES_ID:
		return pkgcatalog.MoTablesSchema
	default:
		return pkgcatalog.MoColumnsSchema
	}
}
