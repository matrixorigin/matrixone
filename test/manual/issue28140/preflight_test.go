package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func preflightMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectQuery("select current_account_id").WillReturnRows(
		sqlmock.NewRows([]string{"current_account_id"}).AddRow(0))
	return db, mock
}

func expectPreflightAccounts(mock sqlmock.Sqlmock, ids ...int) {
	rows := sqlmock.NewRows([]string{"account_id"})
	for _, id := range ids {
		rows.AddRow(id)
	}
	mock.ExpectQuery("select account_id from mo_catalog.mo_account").WillReturnRows(rows).RowsWillBeClosed()
}

func expectPreflightTable(mock sqlmock.Sqlmock, account, id int) {
	mock.ExpectQuery("select account_id, rel_id, reldatabase, relname from mo_catalog.mo_tables").
		WillReturnRows(sqlmock.NewRows([]string{"account_id", "rel_id", "reldatabase", "relname"}).
			AddRow(account, id, "db", "t")).RowsWillBeClosed()
}

func expectPreflightColumn(mock sqlmock.Sqlmock, typeBytes []byte, generated int, generatedBytes []byte) {
	mock.ExpectQuery("from mo_catalog.mo_columns").WillReturnRows(sqlmock.NewRows([]string{
		"account_id", "att_relname_id", "att_database", "att_relname", "attname",
		"attnum", "atttyp", "attr_has_generated", "attr_generated", "attisdropped",
	}).AddRow(0, 42, "db", "t", "c", 1, typeBytes, generated, generatedBytes, 0)).RowsWillBeClosed()
}

func TestPreflightCatalogSnapshot(t *testing.T) {
	typ := types.T_int32.ToType()
	typeBytes, err := typ.Marshal()
	require.NoError(t, err)

	for _, tc := range []struct {
		name          string
		accounts      []int
		tableAccount  int
		typeBytes     []byte
		generated     int
		generatedData []byte
		wantError     string
	}{
		{name: "complete catalog", accounts: []int{0}, tableAccount: 0, typeBytes: typeBytes},
		{name: "duplicate account", accounts: []int{0, 0}, wantError: "duplicate account"},
		{name: "unlisted table account", accounts: []int{0}, tableAccount: 1, wantError: "unlisted account"},
		{name: "invalid column type", accounts: []int{0}, typeBytes: []byte{1}, wantError: "decode type"},
		{name: "missing generated expression", accounts: []int{0}, typeBytes: typeBytes, generated: 1, wantError: "missing generated expression"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := preflightMockDB(t)
			defer func() {
				require.NoError(t, db.Close())
				require.NoError(t, mock.ExpectationsWereMet())
			}()
			expectPreflightAccounts(mock, tc.accounts...)
			if tc.name != "duplicate account" {
				expectPreflightTable(mock, tc.tableAccount, 42)
				if tc.name != "unlisted table account" {
					expectPreflightColumn(mock, tc.typeBytes, tc.generated, tc.generatedData)
				}
			}
			if tc.wantError == "" {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			mock.ExpectClose()
			err := run(context.Background(), db)
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError)
			}
		})
	}
}

func TestPreflightRejectsIncompleteCatalogSnapshots(t *testing.T) {
	typ := types.T_int32.ToType()
	typeBytes, err := typ.Marshal()
	require.NoError(t, err)
	emptyGenerated, err := types.Encode(&pb.GeneratedCol{OriginString: "c"})
	require.NoError(t, err)
	columnRows := func(tableID, ordinal int, generated int, data []byte) *sqlmock.Rows {
		return sqlmock.NewRows([]string{
			"account_id", "att_relname_id", "att_database", "att_relname", "attname",
			"attnum", "atttyp", "attr_has_generated", "attr_generated", "attisdropped",
		}).AddRow(0, tableID, "db", "t", "c", ordinal, typeBytes, generated, data, 0)
	}
	for _, tc := range []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  string
	}{
		{name: "empty account list", setup: func(mock sqlmock.Sqlmock) {
			expectPreflightAccounts(mock)
		}, want: "account inventory is empty"},
		{name: "table query error", setup: func(mock sqlmock.Sqlmock) {
			expectPreflightAccounts(mock, 0)
			mock.ExpectQuery("from mo_catalog.mo_tables").WillReturnError(errors.New("catalog unavailable"))
		}, want: "table inventory: catalog unavailable"},
		{name: "empty table list", setup: func(mock sqlmock.Sqlmock) {
			expectPreflightAccounts(mock, 0)
			mock.ExpectQuery("from mo_catalog.mo_tables").WillReturnRows(
				sqlmock.NewRows([]string{"account_id", "rel_id", "reldatabase", "relname"})).RowsWillBeClosed()
		}, want: "table inventory is empty"},
		{name: "duplicate table", setup: func(mock sqlmock.Sqlmock) {
			expectPreflightAccounts(mock, 0)
			mock.ExpectQuery("from mo_catalog.mo_tables").WillReturnRows(
				sqlmock.NewRows([]string{"account_id", "rel_id", "reldatabase", "relname"}).
					AddRow(0, 42, "db", "t").AddRow(0, 42, "db", "t")).RowsWillBeClosed()
		}, want: "duplicate table id"},
		{name: "column query error", setup: func(mock sqlmock.Sqlmock) {
			expectPreflightAccounts(mock, 0)
			expectPreflightTable(mock, 0, 42)
			mock.ExpectQuery("from mo_catalog.mo_columns").WillReturnError(errors.New("catalog unavailable"))
		}, want: "column inventory: catalog unavailable"},
		{name: "column without table", setup: func(mock sqlmock.Sqlmock) {
			expectPreflightAccounts(mock, 0)
			expectPreflightTable(mock, 0, 42)
			mock.ExpectQuery("from mo_catalog.mo_columns").WillReturnRows(columnRows(43, 1, 0, nil)).RowsWillBeClosed()
		}, want: "no matching table inventory row"},
		{name: "invalid column ordinal", setup: func(mock sqlmock.Sqlmock) {
			expectPreflightAccounts(mock, 0)
			expectPreflightTable(mock, 0, 42)
			mock.ExpectQuery("from mo_catalog.mo_columns").WillReturnRows(columnRows(42, 0, 0, nil)).RowsWillBeClosed()
		}, want: "invalid, dropped, or duplicate column ordinal"},
		{name: "invalid generated encoding", setup: func(mock sqlmock.Sqlmock) {
			expectPreflightAccounts(mock, 0)
			expectPreflightTable(mock, 0, 42)
			mock.ExpectQuery("from mo_catalog.mo_columns").WillReturnRows(columnRows(42, 1, 1, []byte{0x80})).RowsWillBeClosed()
		}, want: "decode generated expression"},
		{name: "generated expression omitted", setup: func(mock sqlmock.Sqlmock) {
			expectPreflightAccounts(mock, 0)
			expectPreflightTable(mock, 0, 42)
			mock.ExpectQuery("from mo_catalog.mo_columns").WillReturnRows(columnRows(42, 1, 1, emptyGenerated)).RowsWillBeClosed()
		}, want: "empty generated expression"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := preflightMockDB(t)
			defer func() {
				require.NoError(t, db.Close())
				require.NoError(t, mock.ExpectationsWereMet())
			}()
			tc.setup(mock)
			mock.ExpectRollback()
			mock.ExpectClose()
			require.ErrorContains(t, run(context.Background(), db), tc.want)
		})
	}
}

func TestAuditCatalogTablesRequiresCompleteInventory(t *testing.T) {
	key := tableKey{account: 0, id: 42}
	table := &catalogTable{key: key, database: "db", name: "t", columns: map[int]*pb.ColDef{}}
	accounts := map[uint32]bool{0: true}
	tables := map[tableKey]*catalogTable{key: table}
	_, err := auditCatalogTables(accounts, tables)
	require.ErrorContains(t, err, "no column inventory")
	table.columns[2] = &pb.ColDef{Name: "x", Typ: pb.Type{Id: int32(types.T_int32)}}
	_, err = auditCatalogTables(accounts, tables)
	require.ErrorContains(t, err, "column-ordinal gap")
	delete(table.columns, 2)
	table.columns[1] = &pb.ColDef{Name: "x", Typ: pb.Type{Id: int32(types.T_int32)}}
	findings, err := auditCatalogTables(accounts, tables)
	require.NoError(t, err)
	require.Empty(t, findings)
	tables[key] = nil
	_, err = auditCatalogTables(accounts, tables)
	require.ErrorContains(t, err, "missing account")
	delete(accounts, 0)
	_, err = auditCatalogTables(accounts, tables)
	require.ErrorContains(t, err, "incomplete account")
}
