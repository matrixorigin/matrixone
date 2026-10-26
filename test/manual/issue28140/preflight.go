// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type tableKey struct {
	account uint32
	id      uint64
}

type catalogTable struct {
	key      tableKey
	database string
	name     string
	columns  map[int]*pb.ColDef
}

type oversizedCatalogColumn struct {
	table  *catalogTable
	column string
}

func main() {
	timeout := flag.Duration("timeout", 5*time.Minute, "catalog snapshot timeout")
	flag.Parse()
	dsn := os.Getenv("MO_ISSUE28140_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "MO_ISSUE28140_DSN is required (system-account connection)")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err == nil {
		defer db.Close()
		err = run(ctx, db)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "BLOCK:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var currentAccount uint32
	if err := tx.QueryRowContext(ctx, "select current_account_id()").Scan(&currentAccount); err != nil {
		return fmt.Errorf("read current account: %w", err)
	}
	if currentAccount != 0 {
		return errors.New("preflight requires a system-account connection")
	}

	accounts := make(map[uint32]bool)
	accountRows, err := tx.QueryContext(ctx, "select account_id from mo_catalog.mo_account")
	if err != nil {
		return fmt.Errorf("account inventory: %w", err)
	}
	for accountRows.Next() {
		var id uint32
		if err := accountRows.Scan(&id); err != nil {
			accountRows.Close()
			return err
		}
		if accounts[id] {
			accountRows.Close()
			return fmt.Errorf("duplicate account %d", id)
		}
		accounts[id] = true
	}
	if err := accountRows.Err(); err != nil {
		accountRows.Close()
		return err
	}
	accountRows.Close()
	if len(accounts) == 0 || !accounts[0] {
		return errors.New("account inventory is empty or omits the system account")
	}

	tables := make(map[tableKey]*catalogTable)
	tableRows, err := tx.QueryContext(ctx, "select account_id, rel_id, reldatabase, relname from mo_catalog.mo_tables")
	if err != nil {
		return fmt.Errorf("table inventory: %w", err)
	}
	for tableRows.Next() {
		var key tableKey
		var database, name string
		if err := tableRows.Scan(&key.account, &key.id, &database, &name); err != nil {
			tableRows.Close()
			return err
		}
		if !accounts[key.account] {
			tableRows.Close()
			return fmt.Errorf("table %s.%s belongs to unlisted account %d", database, name, key.account)
		}
		if tables[key] != nil {
			tableRows.Close()
			return fmt.Errorf("duplicate table id %d for account %d", key.id, key.account)
		}
		tables[key] = &catalogTable{key: key, database: database, name: name, columns: make(map[int]*pb.ColDef)}
	}
	if err := tableRows.Err(); err != nil {
		tableRows.Close()
		return err
	}
	tableRows.Close()
	if len(tables) == 0 {
		return errors.New("table inventory is empty")
	}

	columnRows, err := tx.QueryContext(ctx, `select account_id, att_relname_id, att_database,
		att_relname, attname, attnum, atttyp, attr_has_generated, attr_generated, attisdropped
		from mo_catalog.mo_columns`)
	if err != nil {
		return fmt.Errorf("column inventory: %w", err)
	}
	columnCount := 0
	for columnRows.Next() {
		var key tableKey
		var database, tableName, name string
		var ordinal, hasGenerated, dropped int
		var typeBytes, generatedBytes []byte
		if err := columnRows.Scan(&key.account, &key.id, &database, &tableName, &name,
			&ordinal, &typeBytes, &hasGenerated, &generatedBytes, &dropped); err != nil {
			columnRows.Close()
			return err
		}
		table := tables[key]
		if table == nil || table.database != database || table.name != tableName {
			columnRows.Close()
			return fmt.Errorf("column %s.%s.%s has no matching table inventory row (account %d)", database, tableName, name, key.account)
		}
		if ordinal <= 0 || ordinal > 65536 || dropped != 0 || table.columns[ordinal] != nil {
			columnRows.Close()
			return fmt.Errorf("invalid, dropped, or duplicate column ordinal %d for %s.%s.%s", ordinal, database, tableName, name)
		}
		var typ types.Type
		if err := typ.Unmarshal(typeBytes); err != nil {
			columnRows.Close()
			return fmt.Errorf("decode type for %s.%s.%s: %w", database, tableName, name, err)
		}
		col := &pb.ColDef{Name: name, Typ: pb.Type{Id: int32(typ.Oid)}}
		if hasGenerated != 0 {
			col.GeneratedCol = new(pb.GeneratedCol)
			if len(generatedBytes) == 0 {
				columnRows.Close()
				return fmt.Errorf("missing generated expression for %s.%s.%s", database, tableName, name)
			}
			if err := types.Decode(generatedBytes, col.GeneratedCol); err != nil {
				columnRows.Close()
				return fmt.Errorf("decode generated expression for %s.%s.%s: %w", database, tableName, name, err)
			}
			if col.GeneratedCol.Expr == nil {
				columnRows.Close()
				return fmt.Errorf("empty generated expression for %s.%s.%s", database, tableName, name)
			}
		}
		table.columns[ordinal] = col
		columnCount++
	}
	if err := columnRows.Err(); err != nil {
		columnRows.Close()
		return err
	}
	columnRows.Close()

	oversized, err := auditCatalogTables(accounts, tables)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit catalog snapshot: %w", err)
	}
	for _, finding := range oversized {
		fmt.Printf("OVERSIZE account=%d database=%s table=%s column=%s cost>65536\n",
			finding.table.key.account, finding.table.database, finding.table.name, finding.column)
	}
	if len(oversized) > 0 {
		return fmt.Errorf("%d oversized generated columns; defer upgrade until table owners approve a semantics-preserving rewrite", len(oversized))
	}
	fmt.Printf("PASS accounts=%d tables=%d columns=%d oversized=0\n", len(accounts), len(tables), columnCount)
	return nil
}

func auditCatalogTables(accounts map[uint32]bool, tables map[tableKey]*catalogTable) ([]oversizedCatalogColumn, error) {
	if len(accounts) == 0 || !accounts[0] || len(tables) == 0 {
		return nil, errors.New("incomplete account or table inventory")
	}
	keys := make([]tableKey, 0, len(tables))
	for key := range tables {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].account != keys[j].account {
			return keys[i].account < keys[j].account
		}
		return keys[i].id < keys[j].id
	})
	var oversized []oversizedCatalogColumn
	for _, key := range keys {
		table := tables[key]
		if table == nil || !accounts[key.account] {
			return nil, fmt.Errorf("table id %d belongs to missing account %d", key.id, key.account)
		}
		if len(table.columns) == 0 {
			return nil, fmt.Errorf("table %s.%s (account %d) has no column inventory", table.database, table.name, key.account)
		}
		def := &pb.TableDef{Name: table.name, Cols: make([]*pb.ColDef, len(table.columns))}
		for ordinal := 1; ordinal <= len(table.columns); ordinal++ {
			col := table.columns[ordinal]
			if col == nil {
				return nil, fmt.Errorf("table %s.%s (account %d) has a column-ordinal gap at %d", table.database, table.name, key.account, ordinal)
			}
			def.Cols[ordinal-1] = col
		}
		for _, finding := range plan.VirtualTimestampExpansionFindings(def) {
			oversized = append(oversized, oversizedCatalogColumn{table: table, column: finding.Column})
		}
	}
	return oversized, nil
}
