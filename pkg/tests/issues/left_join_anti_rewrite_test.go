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

package issues

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestOuterAndAntiJoinRewritesPreserveSQLResults(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const database = "left_join_anti_marker"
		execSQLMaybe(t, ctx, db, "drop database if exists "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "create database "+database)
		execSQLRequire(t, ctx, db, "use "+database)
		execSQLRequire(t, ctx, db, "create table left_rows (id int primary key)")
		execSQLRequire(t, ctx, db, "create table right_rows (id int primary key)")
		execSQLRequire(t, ctx, db, "insert into left_rows values (1)")

		var count int
		err = db.QueryRowContext(ctx, `select count(*)
			from left_rows l left join right_rows r on l.id = r.id
			where coalesce(r.id, 0) is null`).Scan(&count)
		require.NoError(t, err)
		require.Zero(t, count,
			"a non-NULL fallback must continue to reject the NULL-extended row")

		err = db.QueryRowContext(ctx, `select count(*)
			from left_rows l left join right_rows r on l.id = r.id
			where json_object(r.id, 1) is null`).Scan(&count)
		require.Error(t, err)
		var mysqlErr *mysqlDriver.MySQLError
		require.True(t, errors.As(err, &mysqlErr), "expected MySQL error, got %T: %v", err, err)
		require.Equal(t, moerr.ErrInvalidInput, mysqlErr.Number)

		// Positive LEFT-to-ANTI path.  The right marker is declared NOT NULL,
		// while duplicate right matches and a NULL left key exercise bag and
		// three-valued semantics.
		execSQLRequire(t, ctx, db, "create table anti_left (id int)")
		execSQLRequire(t, ctx, db, "create table anti_right (id int not null, payload varchar(10))")
		execSQLRequire(t, ctx, db, "insert into anti_left values (null), (1), (2), (3)")
		execSQLRequire(t, ctx, db, "insert into anti_right values (1, 'x'), (1, null), (4, 'unused')")
		antiRows := querySingleStringColumn(t, ctx, db, `
			select if(l.id is null, 'NULL', cast(l.id as char))
			from anti_left l left join anti_right r on l.id = r.id
			where r.id is null
			order by l.id is not null, l.id`)
		require.Equal(t, []string{"NULL", "2", "3"}, antiRows)

		// Preserved-side association: the unique inner input filters the
		// preserved side before the LEFT join.  The explicitly associated query
		// is an independent relational oracle for the optimized spelling.
		execSQLRequire(t, ctx, db, "create table preserved_left (id int, note varchar(10))")
		execSQLRequire(t, ctx, db, "create table preserved_many (aid int, payload varchar(10))")
		execSQLRequire(t, ctx, db, "create table preserved_unique (id int primary key)")
		execSQLRequire(t, ctx, db, "insert into preserved_left values (null, 'null-key'), (1, 'one'), (2, 'two'), (3, 'three')")
		execSQLRequire(t, ctx, db, "insert into preserved_many values (1, 'b1'), (1, null), (4, 'unused')")
		execSQLRequire(t, ctx, db, "insert into preserved_unique values (1), (2), (4)")
		const preservedSelect = `select concat(cast(l.id as char), ':', ifnull(b.payload, 'NULL'))
			from preserved_left l
			left join preserved_many b on l.id = b.aid
			join preserved_unique u on l.id = u.id
			order by l.id, b.payload is not null, b.payload`
		const preservedReference = `select concat(cast(l.id as char), ':', ifnull(b.payload, 'NULL'))
			from (preserved_left l join preserved_unique u on l.id = u.id)
			left join preserved_many b on l.id = b.aid
			order by l.id, b.payload is not null, b.payload`
		preservedRows := querySingleStringColumn(t, ctx, db, preservedSelect)
		require.Equal(t, querySingleStringColumn(t, ctx, db, preservedReference), preservedRows)
		require.Equal(t, []string{"1:NULL", "1:b1", "2:NULL"}, preservedRows)

		// Nullable-side association: the upper equality rejects NULL-extended
		// rows, so joining the nullable and third inputs first is equivalent.
		execSQLRequire(t, ctx, db, "create table nullable_left (id int)")
		execSQLRequire(t, ctx, db, "create table nullable_many (aid int, ck int, payload varchar(10))")
		execSQLRequire(t, ctx, db, "create table nullable_other (ck int, payload varchar(10))")
		execSQLRequire(t, ctx, db, "insert into nullable_left values (null), (1), (2), (3)")
		execSQLRequire(t, ctx, db, "insert into nullable_many values (1, 10, 'b1'), (1, 10, null), (2, null, 'b2'), (4, 10, 'unused')")
		execSQLRequire(t, ctx, db, "insert into nullable_other values (10, 'c1'), (10, null), (null, 'cn')")
		const nullableSelect = `select concat(cast(l.id as char), ':', ifnull(b.payload, 'NULL'), ':', ifnull(c.payload, 'NULL'))
			from nullable_left l
			left join nullable_many b on l.id = b.aid
			join nullable_other c on b.ck = c.ck
			order by l.id, b.payload is not null, b.payload, c.payload is not null, c.payload`
		const nullableReference = `select concat(cast(l.id as char), ':', ifnull(b.payload, 'NULL'), ':', ifnull(c.payload, 'NULL'))
			from nullable_left l
			join (nullable_many b join nullable_other c on b.ck = c.ck) on l.id = b.aid
			order by l.id, b.payload is not null, b.payload, c.payload is not null, c.payload`
		nullableRows := querySingleStringColumn(t, ctx, db, nullableSelect)
		require.Equal(t, querySingleStringColumn(t, ctx, db, nullableReference), nullableRows)
		require.Equal(t, []string{
			"1:NULL:NULL", "1:NULL:c1", "1:b1:NULL", "1:b1:c1",
		}, nullableRows)
	})
}

func querySingleStringColumn(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	query string,
) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var value string
		require.NoError(t, rows.Scan(&value))
		result = append(result, value)
	}
	require.NoError(t, rows.Err())
	return result
}
