// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type rebaseConflictPolicy uint8

const (
	rebaseFail rebaseConflictPolicy = iota
	rebaseKeepUpstream
	rebaseKeepBranch
)

// Reads have already been authorized, resolved and protected by the coordinator.
// A timestamp alone is not authority to read an unretained historical version.
type rebaseSQLRead struct {
	database  string
	timestamp int64
}

type rebaseSQLColumn struct {
	name    string
	typ     types.Type
	primary bool
}

// The SQL engine owns all row batches, joins and writes. This value holds only
// schema-sized SQL, never row data. Every statement must use the same admitted
// transaction; these strings are not a sequence of independently committed SQL.
type rebaseTableSQL struct {
	baseMismatch string
	changes      string
	deleteRows   string
	insertRows   string
}

// SQL equality can collapse distinct stored values, such as signed zeros or
// text compared using a case-insensitive collation. Rebase preserves the
// stored value; key joins still use the table's logical key equality.
func rebaseComparableValue(value string, typ types.Type) string {
	// typeswitch:partial schema admission rejects low-precision floats before SQL lowering.
	switch typ.Oid {
	case types.T_float32, types.T_float64, types.T_char, types.T_varchar,
		types.T_text, types.T_binary, types.T_varbinary, types.T_blob:
		return "serial(" + value + ")"
	}
	return value
}

func rebaseValueEqual(left, right string, typ types.Type) string {
	return rebaseComparableValue(left, typ) + " <=> " + rebaseComparableValue(right, typ)
}

// buildRebaseTableSQL lowers four fixed endpoints (Bf, F, Bu, U). The caller
// must check baseMismatch == 0 and, under FAIL, conflict_keys == 0 before DML.
// Deletes include replaced rows; inserted/deleted counts in changes are logical
// net-state counts, not the affected-row counts of the two physical statements.
func buildRebaseTableSQL(
	ctx context.Context,
	table, destination string,
	reads [4]rebaseSQLRead,
	columns []rebaseSQLColumn,
	policy rebaseConflictPolicy,
) (rebaseTableSQL, error) {
	var out rebaseTableSQL
	if policy > rebaseKeepBranch {
		return out, moerr.NewInvalidInput(ctx, "invalid Rebase conflict policy")
	}
	validName := func(name string) bool { return name != "" && !strings.ContainsRune(name, 0) }
	if !validName(table) || !validName(destination) || len(columns) == 0 {
		return out, moerr.NewInvalidInput(ctx, "invalid Rebase table layout")
	}
	for _, read := range reads {
		if !validName(read.database) || read.timestamp <= 0 || strings.EqualFold(destination, read.database) {
			return out, moerr.NewInvalidInput(ctx, "Rebase requires fixed sources and a distinct destination")
		}
	}
	var keys []rebaseSQLColumn
	names := make(map[string]bool, len(columns))
	for _, column := range columns {
		name := strings.ToLower(column.name)
		if !validName(column.name) || names[name] {
			return out, moerr.NewInvalidInput(ctx, "invalid or duplicate Rebase column")
		}
		names[name] = true
		if column.primary {
			keys = append(keys, column)
		}
	}
	if len(keys) == 0 {
		return out, moerr.NewNotSupported(ctx, "Rebase row replay requires an explicit primary key")
	}
	col := func(alias, name string) string { return alias + "." + quoteIdentifierForSQL(name) }
	present := func(alias string) string { return col(alias, keys[0].name) + " is not null" }
	absent := func(alias string) string { return col(alias, keys[0].name) + " is null" }
	joinKeys := func(left, right string) string {
		terms := make([]string, len(keys))
		for i, key := range keys {
			terms[i] = dataBranchSQLKeyEqual(col(left, key.name), col(right, key.name), key.typ)
		}
		return "(" + strings.Join(terms, " and ") + ")"
	}
	equalRows := func(left, right string) string {
		terms := make([]string, len(columns))
		for i, column := range columns {
			terms[i] = rebaseValueEqual(col(left, column.name), col(right, column.name), column.typ)
		}
		return "((" + absent(left) + " and " + absent(right) + ") or (" + present(left) + " and " + present(right) + " and " + strings.Join(terms, " and ") + "))"
	}
	relation := func(read rebaseSQLRead, alias string) string {
		return fmt.Sprintf("%s.%s{MO_TS=%d} %s", quoteIdentifierForSQL(read.database), quoteIdentifierForSQL(table), read.timestamp, alias)
	}
	baseJoin := relation(reads[0], "b") + " full outer join " + relation(reads[2], "x") + " on " + joinKeys("b", "x")
	out.baseMismatch = "select count(*) from " + baseJoin + " where not " + equalRows("b", "x")

	third := make([]string, len(keys))
	for i, key := range keys {
		left := "coalesce(" + col("b", key.name) + ", " + col("f", key.name) + ")"
		third[i] = dataBranchSQLKeyEqual(left, col("u", key.name), key.typ)
	}
	state := relation(reads[0], "b") + " full outer join " + relation(reads[1], "f") + " on " + joinKeys("b", "f") +
		" full outer join " + relation(reads[3], "u") + " on (" + strings.Join(third, " and ") + ")"
	featureChanged := "not " + equalRows("b", "f")
	upstreamChanged := "not " + equalRows("b", "u")
	differentHeads := "not " + equalRows("f", "u")
	conflict := "(" + featureChanged + " and " + upstreamChanged + " and " + differentHeads + ")"
	apply := "(" + featureChanged + " and " + differentHeads
	if policy == rebaseKeepUpstream {
		apply += " and not " + conflict
	}
	apply += ")"
	countIf := func(condition, alias string) string {
		return "coalesce(sum(case when " + condition + " then 1 else 0 end), 0) as " + alias
	}
	counts := []string{
		countIf(featureChanged, "feature_changed_keys"),
		countIf(upstreamChanged, "upstream_changed_keys"),
		countIf(conflict, "conflict_keys"),
		countIf(apply+" and "+present("f")+" and "+absent("u"), "applied_insert_rows"),
		countIf(apply+" and "+present("f")+" and "+present("u"), "applied_update_rows"),
		countIf(apply+" and "+absent("f")+" and "+present("u"), "applied_delete_rows"),
	}
	out.changes = "select " + strings.Join(counts, ", ") + " from " + state

	deleteKeys := make([]string, len(keys))
	deleteMatch := make([]string, len(keys))
	for i, key := range keys {
		alias := fmt.Sprintf("k%d", i)
		deleteKeys[i] = col("u", key.name) + " as " + alias
		deleteMatch[i] = dataBranchSQLKeyEqual(col("d", key.name), "r."+alias, key.typ)
	}
	target := quoteIdentifierForSQL(destination) + "." + quoteIdentifierForSQL(table)
	out.deleteRows = "delete d from " + target + " d inner join (select " + strings.Join(deleteKeys, ", ") +
		" from " + state + " where " + apply + " and " + present("u") + ") r on (" + strings.Join(deleteMatch, " and ") + ")"
	insertNames := make([]string, len(columns))
	insertValues := make([]string, len(columns))
	for i, column := range columns {
		insertNames[i] = quoteIdentifierForSQL(column.name)
		insertValues[i] = col("f", column.name)
	}
	out.insertRows = "insert into " + target + " (" + strings.Join(insertNames, ", ") + ") select " +
		strings.Join(insertValues, ", ") + " from " + state + " where " + apply + " and " + present("f")
	return out, nil
}

// No-PK tables are not replayed by R1. This exact visible-bag comparison is a
// pre-write proof obligation for retaining an unchanged table, including NULLs
// and duplicate multiplicities. Grouping/joins stay in the engine, not Go maps.
func buildRebaseBagMismatchSQL(ctx context.Context, table string, left, right rebaseSQLRead, columns []rebaseSQLColumn) (string, error) {
	if table == "" || strings.ContainsRune(table, 0) || len(columns) == 0 {
		return "", moerr.NewInvalidInput(ctx, "invalid Rebase bag layout")
	}
	var grouped []string
	var projected []string
	var aliases []string
	seen := make(map[string]bool, len(columns))
	for i, column := range columns {
		name := strings.ToLower(column.name)
		if column.name == "" || strings.ContainsRune(column.name, 0) || seen[name] {
			return "", moerr.NewInvalidInput(ctx, "invalid Rebase bag column")
		}
		seen[name] = true
		alias := fmt.Sprintf("c%d", i)
		value := rebaseComparableValue(quoteIdentifierForSQL(column.name), column.typ)
		grouped = append(grouped, value)
		projected = append(projected, value+" as "+alias)
		aliases = append(aliases, alias)
	}
	for _, read := range []rebaseSQLRead{left, right} {
		if read.database == "" || strings.ContainsRune(read.database, 0) || read.timestamp <= 0 {
			return "", moerr.NewInvalidInput(ctx, "Rebase bag comparison requires fixed sources")
		}
	}
	bag := func(read rebaseSQLRead, sign string) string {
		return fmt.Sprintf("select %s, %scast(count(*) as decimal(38,0)) as n from %s.%s{MO_TS=%d} group by %s",
			strings.Join(projected, ", "), sign, quoteIdentifierForSQL(read.database), quoteIdentifierForSQL(table),
			read.timestamp, strings.Join(grouped, ", "))
	}
	// Null-safe join predicates are not hash keys, so a full join here compares
	// every distinct row pair. Group signed multiplicities instead; GROUP BY
	// retains NULL equality, and the wide signed counter cannot wrap COUNT.
	keys := strings.Join(aliases, ", ")
	return "select count(*) from (select " + keys + " from (" + bag(left, "") + " union all " + bag(right, "-") +
		") rebase_bags group by " + keys + " having sum(n) <> 0) rebase_differences", nil
}
