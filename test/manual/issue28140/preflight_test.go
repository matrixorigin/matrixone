package main

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

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
	delete(accounts, 0)
	_, err = auditCatalogTables(accounts, tables)
	require.ErrorContains(t, err, "incomplete account")
}
