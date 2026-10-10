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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestDataBranchFractionalTemporalPrimaryKey(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		dbName := testutils.GetDatabaseName(t)
		defer cleanupTestDatabases(t, db, dbName)
		execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", dbName))
		execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", dbName))

		temporalCases := []struct {
			name            string
			sqlType         string
			scale           int
			fractional      bool
			sessionTimeZone string
		}{
			{name: "datetime_fractional_3", sqlType: "datetime(3)", scale: 3, fractional: true, sessionTimeZone: "+00:00"},
			{name: "datetime_fractional_6", sqlType: "datetime(6)", scale: 6, fractional: true, sessionTimeZone: "+00:00"},
			{name: "timestamp_fractional_3_utc", sqlType: "timestamp(3)", scale: 3, fractional: true, sessionTimeZone: "+00:00"},
			{name: "timestamp_fractional_3_fixed_plus08", sqlType: "timestamp(3)", scale: 3, fractional: true, sessionTimeZone: "+08:00"},
			{name: "timestamp_fractional_6_utc", sqlType: "timestamp(6)", scale: 6, fractional: true, sessionTimeZone: "+00:00"},
			{name: "timestamp_fractional_6_fixed_plus08", sqlType: "timestamp(6)", scale: 6, fractional: true, sessionTimeZone: "+08:00"},
			{name: "time_fractional_3", sqlType: "time(3)", scale: 3, fractional: true, sessionTimeZone: "+00:00"},
			{name: "time_fractional_6", sqlType: "time(6)", scale: 6, fractional: true, sessionTimeZone: "+00:00"},
			{name: "datetime_scale_0", sqlType: "datetime(0)", scale: 0, sessionTimeZone: "+00:00"},
			{name: "timestamp_scale_0", sqlType: "timestamp(0)", scale: 0, sessionTimeZone: "+00:00"},
			{name: "time_scale_0", sqlType: "time(0)", scale: 0, sessionTimeZone: "+00:00"},
			{name: "datetime_zero_fraction", sqlType: "datetime(6)", scale: 6, sessionTimeZone: "+00:00"},
			{name: "timestamp_zero_fraction", sqlType: "timestamp(6)", scale: 6, sessionTimeZone: "+00:00"},
			{name: "time_zero_fraction", sqlType: "time(6)", scale: 6, sessionTimeZone: "+00:00"},
		}
		for _, tc := range temporalCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				execSQLDB(t, ctx, db, fmt.Sprintf("set time_zone = '%s'", tc.sessionTimeZone))
				runTemporalPrimaryKeyBranchCase(t, ctx, db, tc.name, tc.sqlType, tc.scale, tc.fractional)
			})
		}

		t.Run("fractional_rounding_boundary_neighbors", func(t *testing.T) {
			boundaryCases := []struct {
				name            string
				sqlType         string
				sessionTimeZone string
				before          string
				nextSecond      string
				inserted        string
			}{
				{
					name:            "datetime",
					sqlType:         "datetime(6)",
					sessionTimeZone: "+00:00",
					before:          "2024-01-01 00:00:00.999999",
					nextSecond:      "2024-01-01 00:00:01.000000",
					inserted:        "2024-01-01 00:00:01.000001",
				},
				{
					name:            "timestamp_fixed_plus08",
					sqlType:         "timestamp(6)",
					sessionTimeZone: "+08:00",
					before:          "2024-01-01 00:00:00.999999",
					nextSecond:      "2024-01-01 00:00:01.000000",
					inserted:        "2024-01-01 00:00:01.000001",
				},
				{
					name:            "time",
					sqlType:         "time(6)",
					sessionTimeZone: "+00:00",
					before:          "00:00:00.999999",
					nextSecond:      "00:00:01.000000",
					inserted:        "00:00:01.000001",
				},
			}
			for _, bc := range boundaryCases {
				bc := bc
				t.Run(bc.name, func(t *testing.T) {
					execSQLDB(t, ctx, db, fmt.Sprintf("set time_zone = '%s'", bc.sessionTimeZone))
					runTemporalPrimaryKeyValuesCase(t, ctx, db,
						"temporal_boundary_"+bc.name,
						bc.sqlType,
						[]temporalBranchKeyValue{
							{key: bc.before, value: "before-boundary"},
							{key: bc.nextSecond, value: "next-second"},
						},
						bc.before, "changed",
						bc.nextSecond,
						bc.inserted, "inserted")
				})
			}
		})

		t.Run("negative_fractional_time", func(t *testing.T) {
			execSQLDB(t, ctx, db, "set time_zone = '+00:00'")
			runTemporalPrimaryKeyValuesCase(t, ctx, db,
				"temporal_negative_time",
				"time(6)",
				[]temporalBranchKeyValue{
					{key: "-01:00:00.000000", value: "negative-whole"},
					{key: "-01:00:00.000001", value: "negative-old"},
					{key: "-01:00:00.000002", value: "negative-delete"},
				},
				"-01:00:00.000001", "negative-changed",
				"-01:00:00.000002",
				"-01:00:00.000003", "negative-inserted")
		})

		t.Run("composite_datetime_first", func(t *testing.T) {
			runCompositeTemporalPrimaryKeyBranchCase(t, ctx, db, true)
		})
		t.Run("composite_datetime_second", func(t *testing.T) {
			runCompositeTemporalPrimaryKeyBranchCase(t, ctx, db, false)
		})
		t.Run("fractional_temporal_payload_with_integer_key", func(t *testing.T) {
			runFractionalTemporalPayloadCase(t, ctx, db)
		})
		t.Run("fractional_temporal_key_conflict_is_atomic", func(t *testing.T) {
			execSQLDB(t, ctx, db, "set time_zone = '+00:00'")
			runFractionalTemporalConflictCase(t, ctx, db)
		})
		t.Run("composite_temporal_key_conflict_is_atomic", func(t *testing.T) {
			execSQLDB(t, ctx, db, "set time_zone = '+00:00'")
			runCompositeTemporalConflictCase(t, ctx, db)
		})
		t.Run("no_primary_key_vector_delete_conflict_is_atomic", func(t *testing.T) {
			runNoPrimaryKeyVectorDeleteConflictCase(t, ctx, db, "vecf32(2)", "[1,2]", "f32")
			runNoPrimaryKeyVectorDeleteConflictCase(t, ctx, db, "vecf64(2)", "[3,4]", "f64")
		})
		t.Run("no_primary_key_json_delete_conflict_is_atomic", func(t *testing.T) {
			runNoPrimaryKeyJSONDeleteConflictCase(t, ctx, db)
		})
	})
}

type temporalBranchKeyValue struct {
	key   string
	value string
}

func runTemporalPrimaryKeyValuesCase(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	baseName string,
	sqlType string,
	rows []temporalBranchKeyValue,
	updateKey string,
	updateValue string,
	deleteKey string,
	insertKey string,
	insertValue string,
) {
	t.Helper()
	branchName := baseName + "_branch"
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		values = append(values, fmt.Sprintf("('%s', '%s')", row.key, row.value))
	}

	execSQLDB(t, ctx, db, fmt.Sprintf(
		"create table `%s` (k %s primary key, v varchar(32))", baseName, sqlType))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"insert into `%s` values %s", baseName, strings.Join(values, ", ")))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"data branch create table `%s` from `%s`", branchName, baseName))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"update `%s` set v = '%s' where k = '%s'", branchName, updateValue, updateKey))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"delete from `%s` where k = '%s'", branchName, deleteKey))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"insert into `%s` values ('%s', '%s')", branchName, insertKey, insertValue))

	diffRows := fetchDiffRowsAsStrings(t, ctx, db,
		fmt.Sprintf("data branch diff `%s` against `%s`", branchName, baseName))
	require.ElementsMatch(t, [][]string{
		{branchName, "UPDATE", updateKey, updateValue},
		{branchName, "DELETE", deleteKey, valueForTemporalKey(rows, deleteKey)},
		{branchName, "INSERT", insertKey, insertValue},
	}, diffRows)

	execSQLDB(t, ctx, db, fmt.Sprintf(
		"data branch merge `%s` into `%s`", branchName, baseName))
	expected := make([][]string, 0, len(rows))
	for _, row := range rows {
		switch row.key {
		case updateKey:
			expected = append(expected, []string{row.key, updateValue})
		case deleteKey:
		default:
			expected = append(expected, []string{row.key, row.value})
		}
	}
	expected = append(expected, []string{insertKey, insertValue})
	require.ElementsMatch(t, expected,
		queryStringRows(t, ctx, db, fmt.Sprintf("select k, v from `%s` order by k", baseName)))
	require.ElementsMatch(t, expected,
		queryStringRows(t, ctx, db, fmt.Sprintf("select k, v from `%s` order by k", branchName)))
}

func valueForTemporalKey(rows []temporalBranchKeyValue, key string) string {
	for _, row := range rows {
		if row.key == key {
			return row.value
		}
	}
	return ""
}

func runFractionalTemporalConflictCase(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	const (
		base   = "temporal_conflict_base"
		branch = "temporal_conflict_branch"
		key1   = "2024-01-01 00:00:00.000001"
		key2   = "2024-01-01 00:00:00.000002"
	)

	execSQLDB(t, ctx, db,
		"create table `"+base+"` (k datetime(6) primary key, v varchar(32))")
	execSQLDB(t, ctx, db,
		"insert into `"+base+"` values ('"+key1+"', 'old-conflict'), ('"+key2+"', 'old-safe')")
	execSQLDB(t, ctx, db,
		"data branch create table `"+branch+"` from `"+base+"`")
	execSQLDB(t, ctx, db,
		"update `"+branch+"` set v = 'branch-conflict' where k = '"+key1+"'")
	execSQLDB(t, ctx, db,
		"update `"+branch+"` set v = 'branch-safe' where k = '"+key2+"'")
	execSQLDB(t, ctx, db,
		"update `"+base+"` set v = 'base-conflict' where k = '"+key1+"'")

	_, err := db.ExecContext(ctx, "data branch merge `"+branch+"` into `"+base+"`")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "conflict")
	require.Equal(t,
		[][]string{{key1, "base-conflict"}, {key2, "old-safe"}},
		queryStringRows(t, ctx, db, "select k, v from `"+base+"` order by k"))
	require.Equal(t,
		[][]string{{key1, "branch-conflict"}, {key2, "branch-safe"}},
		queryStringRows(t, ctx, db, "select k, v from `"+branch+"` order by k"))
}

func runCompositeTemporalConflictCase(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	const (
		base   = "temporal_composite_conflict_base"
		branch = "temporal_composite_conflict_branch"
		key1   = "2024-01-01 00:00:00.000001"
		key2   = "2024-01-01 00:00:00.000002"
	)

	execSQLDB(t, ctx, db,
		"create table `"+base+"` (id int, k datetime(6), v varchar(32), primary key (id, k))")
	execSQLDB(t, ctx, db,
		"insert into `"+base+"` values (1, '"+key1+"', 'old-conflict'), (2, '"+key2+"', 'old-safe')")
	execSQLDB(t, ctx, db,
		"data branch create table `"+branch+"` from `"+base+"`")
	execSQLDB(t, ctx, db,
		"update `"+branch+"` set v = 'branch-conflict' where id = 1 and k = '"+key1+"'")
	execSQLDB(t, ctx, db,
		"update `"+branch+"` set v = 'branch-safe' where id = 2 and k = '"+key2+"'")
	execSQLDB(t, ctx, db,
		"update `"+base+"` set v = 'base-conflict' where id = 1 and k = '"+key1+"'")

	_, err := db.ExecContext(ctx, "data branch merge `"+branch+"` into `"+base+"` when conflict fail")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "conflict")
	require.Contains(t, err.Error(), "1,'"+key1+"'")
	require.Equal(t,
		[][]string{{"1", key1, "base-conflict"}, {"2", key2, "old-safe"}},
		queryStringRows(t, ctx, db, "select id, k, v from `"+base+"` order by id"))
	require.Equal(t,
		[][]string{{"1", key1, "branch-conflict"}, {"2", key2, "branch-safe"}},
		queryStringRows(t, ctx, db, "select id, k, v from `"+branch+"` order by id"))
}

func runNoPrimaryKeyVectorDeleteConflictCase(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	vectorType string,
	vectorValue string,
	suffix string,
) {
	t.Helper()
	base := "vector_fake_pk_conflict_" + suffix
	branch := base + "_branch"

	execSQLDB(t, ctx, db, fmt.Sprintf(
		"create table `%s` (id int, embedding %s)", base, vectorType))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"insert into `%s` values (1, '%s'), (2, '%s')",
		base, vectorValue, vectorValue))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"data branch create table `%s` from `%s`", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"delete from `%s` where id = 1", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"delete from `%s` where id = 1", base))

	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"data branch merge `%s` into `%s` when conflict fail", branch, base))
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "conflict")
	formattedVectorValue := strings.ReplaceAll(vectorValue, ",", ", ")
	require.Equal(t, [][]string{{"2", formattedVectorValue}},
		queryStringRows(t, ctx, db, fmt.Sprintf("select id, embedding from `%s` order by id", base)))
	require.Equal(t, [][]string{{"2", formattedVectorValue}},
		queryStringRows(t, ctx, db, fmt.Sprintf("select id, embedding from `%s` order by id", branch)))
}

func runNoPrimaryKeyJSONDeleteConflictCase(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	const (
		base              = "json_fake_pk_conflict"
		branch            = base + "_branch"
		payload1          = `{"name":"same","n":1}`
		payload2          = `{"name":"same","n":2}`
		formattedPayload1 = `{"n": 1, "name": "same"}`
		formattedPayload2 = `{"n": 2, "name": "same"}`
	)

	execSQLDB(t, ctx, db,
		"create table `"+base+"` (id int, payload json)")
	execSQLDB(t, ctx, db,
		"insert into `"+base+"` values (1, '"+payload1+"'), (2, '"+payload2+"')")
	execSQLDB(t, ctx, db,
		"data branch create table `"+branch+"` from `"+base+"`")
	execSQLDB(t, ctx, db,
		"delete from `"+branch+"` where id = 1")
	execSQLDB(t, ctx, db,
		"delete from `"+base+"` where id = 1")

	_, err := db.ExecContext(ctx, "data branch merge `"+branch+"` into `"+base+"` when conflict fail")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "conflict")
	require.Contains(t, err.Error(), "1,'"+formattedPayload1+"'")
	require.Equal(t, [][]string{{"2", formattedPayload2}},
		queryStringRows(t, ctx, db, "select id, payload from `"+base+"` order by id"))
	require.Equal(t, [][]string{{"2", formattedPayload2}},
		queryStringRows(t, ctx, db, "select id, payload from `"+branch+"` order by id"))
}

func temporalBranchKey(kind string, scale, second int, fraction string) string {
	prefix := "2024-01-01 00:00"
	if kind == "time" {
		prefix = "00:00"
	}
	key := fmt.Sprintf("%s:%02d", prefix, second)
	if scale > 0 {
		key += "." + fraction
	}
	return key
}

func temporalBranchFractions(scale int, fractional bool) (string, string, string, string) {
	if scale == 0 {
		return "", "", "", ""
	}
	if !fractional {
		zero := fmt.Sprintf("%0*d", scale, 0)
		return zero, zero, zero, zero
	}
	if scale == 3 {
		return "000", "001", "002", "003"
	}
	return "000000", "000001", "000002", "000003"
}

func runTemporalPrimaryKeyBranchCase(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	caseName string,
	sqlType string,
	scale int,
	fractional bool,
) {
	t.Helper()
	kind := "datetime"
	if len(sqlType) >= len("timestamp") && sqlType[:len("timestamp")] == "timestamp" {
		kind = "timestamp"
	} else if len(sqlType) >= len("time") && sqlType[:len("time")] == "time" {
		kind = "time"
	}
	baseName := "temporal_pk_" + caseName
	branchName := baseName + "_branch"

	wholeFraction, firstFraction, secondFraction, thirdFraction := temporalBranchFractions(scale, fractional)
	whole := temporalBranchKey(kind, scale, 0, wholeFraction)
	firstSecond := 0
	secondSecond := 0
	thirdSecond := 0
	if !fractional {
		firstSecond = 1
		secondSecond = 2
		thirdSecond = 3
	}
	first := temporalBranchKey(kind, scale, firstSecond, firstFraction)
	second := temporalBranchKey(kind, scale, secondSecond, secondFraction)
	third := temporalBranchKey(kind, scale, thirdSecond, thirdFraction)

	execSQLDB(t, ctx, db, fmt.Sprintf(
		"create table `%s` (k %s primary key, v varchar(32))", baseName, sqlType))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"insert into `%s` values ('%s', 'whole'), ('%s', 'old'), ('%s', 'delete-me')",
		baseName, whole, first, second))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"data branch create table `%s` from `%s`", branchName, baseName))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"update `%s` set v = 'changed' where k = '%s'", branchName, first))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"delete from `%s` where k = '%s'", branchName, second))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"insert into `%s` values ('%s', 'inserted')", branchName, third))

	diffRows := fetchDiffRowsAsStrings(t, ctx, db,
		fmt.Sprintf("data branch diff `%s` against `%s`", branchName, baseName))
	require.ElementsMatch(t, [][]string{
		{branchName, "UPDATE", first, "changed"},
		{branchName, "DELETE", second, "delete-me"},
		{branchName, "INSERT", third, "inserted"},
	}, diffRows)

	execSQLDB(t, ctx, db, fmt.Sprintf(
		"data branch merge `%s` into `%s`", branchName, baseName))
	expected := [][]string{{whole, "whole"}, {first, "changed"}, {third, "inserted"}}
	require.Equal(t, expected,
		queryStringRows(t, ctx, db, fmt.Sprintf("select k, v from `%s` order by k", baseName)))
	require.Equal(t, expected,
		queryStringRows(t, ctx, db, fmt.Sprintf("select k, v from `%s` order by k", branchName)))
}

func compositeTemporalRow(temporalFirst bool, key string, id int, value string) string {
	if temporalFirst {
		return fmt.Sprintf("('%s', %d, '%s')", key, id, value)
	}
	return fmt.Sprintf("(%d, '%s', '%s')", id, key, value)
}

func runCompositeTemporalPrimaryKeyBranchCase(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	temporalFirst bool,
) {
	t.Helper()
	const sqlType = "datetime(6)"
	whole := temporalBranchKey("datetime", 6, 0, "000000")
	first := temporalBranchKey("datetime", 6, 0, "000001")
	second := temporalBranchKey("datetime", 6, 0, "000002")
	third := temporalBranchKey("datetime", 6, 0, "000003")
	suffix := "second"
	if temporalFirst {
		suffix = "first"
	}
	base := "temporal_composite_" + suffix
	branch := base + "_branch"
	columns := "(k datetime(6), id int, v varchar(32), primary key (k, id))"
	orderBy := "k, id"
	if !temporalFirst {
		columns = "(id int, k datetime(6), v varchar(32), primary key (id, k))"
		orderBy = "id, k"
	}

	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` %s", base, columns))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"insert into `%s` values %s, %s, %s", base,
		compositeTemporalRow(temporalFirst, whole, 0, "whole"),
		compositeTemporalRow(temporalFirst, first, 1, "old"),
		compositeTemporalRow(temporalFirst, second, 2, "delete-me")))
	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s` from `%s`", branch, base))
	updateWhere := fmt.Sprintf("k = '%s' and id = 1", first)
	deleteWhere := fmt.Sprintf("k = '%s' and id = 2", second)
	if !temporalFirst {
		updateWhere = fmt.Sprintf("id = 1 and k = '%s'", first)
		deleteWhere = fmt.Sprintf("id = 2 and k = '%s'", second)
	}
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set v = 'changed' where %s", branch, updateWhere))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from `%s` where %s", branch, deleteWhere))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"insert into `%s` values %s", branch,
		compositeTemporalRow(temporalFirst, third, 3, "inserted")))

	diffRows := fetchDiffRowsAsStrings(t, ctx, db,
		fmt.Sprintf("data branch diff `%s` against `%s`", branch, base))
	updateRow := []string{branch, "UPDATE", first, "1", "changed"}
	deleteRow := []string{branch, "DELETE", second, "2", "delete-me"}
	insertRow := []string{branch, "INSERT", third, "3", "inserted"}
	if !temporalFirst {
		updateRow = []string{branch, "UPDATE", "1", first, "changed"}
		deleteRow = []string{branch, "DELETE", "2", second, "delete-me"}
		insertRow = []string{branch, "INSERT", "3", third, "inserted"}
	}
	require.ElementsMatch(t, [][]string{updateRow, deleteRow, insertRow}, diffRows)

	execSQLDB(t, ctx, db, fmt.Sprintf(
		"data branch merge `%s` into `%s`", branch, base))
	require.Equal(t,
		queryStringRows(t, ctx, db, fmt.Sprintf("select * from `%s` order by %s", branch, orderBy)),
		queryStringRows(t, ctx, db, fmt.Sprintf("select * from `%s` order by %s", base, orderBy)))

	pickBase := base + "_pick"
	pickSource := pickBase + "_source"
	execSQLDB(t, ctx, db, fmt.Sprintf("create table `%s` %s", pickBase, columns))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"insert into `%s` values %s, %s, %s", pickBase,
		compositeTemporalRow(temporalFirst, whole, 0, "whole"),
		compositeTemporalRow(temporalFirst, first, 1, "old"),
		compositeTemporalRow(temporalFirst, second, 2, "delete-me")))
	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s` from `%s`", pickSource, pickBase))
	execSQLDB(t, ctx, db, fmt.Sprintf("update `%s` set v = 'picked' where %s", pickSource, updateWhere))
	keyTuple := fmt.Sprintf("('%s', 1)", first)
	if !temporalFirst {
		keyTuple = fmt.Sprintf("(1, '%s')", first)
	}
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"data branch pick `%s` into `%s` keys(%s) when conflict accept", pickSource, pickBase, keyTuple))
	picked := [][]string{
		compositeTemporalRowAsStrings(temporalFirst, whole, 0, "whole"),
		compositeTemporalRowAsStrings(temporalFirst, first, 1, "picked"),
		compositeTemporalRowAsStrings(temporalFirst, second, 2, "delete-me"),
	}
	require.Equal(t, picked,
		queryStringRows(t, ctx, db, fmt.Sprintf("select * from `%s` order by %s", pickBase, orderBy)))
}

func compositeTemporalRowAsStrings(temporalFirst bool, key string, id int, value string) []string {
	if temporalFirst {
		return []string{key, fmt.Sprintf("%d", id), value}
	}
	return []string{fmt.Sprintf("%d", id), key, value}
}

func runFractionalTemporalPayloadCase(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	base := "temporal_payload_base"
	branch := "temporal_payload_branch"
	oldValue := "2024-01-01 00:00:00.000001"
	newValue := "2024-01-01 00:00:00.000002"
	deletedValue := "2024-01-01 00:00:00.000003"
	insertedValue := "2024-01-01 00:00:00.000004"
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"create table `%s` (id int primary key, payload datetime(6))", base))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"insert into `%s` values (1, '%s'), (2, '%s')", base, oldValue, deletedValue))
	execSQLDB(t, ctx, db, fmt.Sprintf("data branch create table `%s` from `%s`", branch, base))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"update `%s` set payload = '%s' where id = 1", branch, newValue))
	execSQLDB(t, ctx, db, fmt.Sprintf("delete from `%s` where id = 2", branch))
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"insert into `%s` values (3, '%s')", branch, insertedValue))

	diffRows := fetchDiffRowsAsStrings(t, ctx, db,
		fmt.Sprintf("data branch diff `%s` against `%s`", branch, base))
	require.ElementsMatch(t, [][]string{
		{branch, "UPDATE", "1", newValue},
		{branch, "DELETE", "2", deletedValue},
		{branch, "INSERT", "3", insertedValue},
	}, diffRows)
	execSQLDB(t, ctx, db, fmt.Sprintf(
		"data branch merge `%s` into `%s`", branch, base))
	require.Equal(t,
		[][]string{{"1", newValue}, {"3", insertedValue}},
		queryStringRows(t, ctx, db, fmt.Sprintf("select id, payload from `%s` order by id", base)))
}
