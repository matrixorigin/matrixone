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

package frontend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var errRoutineNamespaceBudget = errors.New("routine namespace cache budget exceeded")

const maxRoutineNamespaceRows = 65536
const maxRoutineNamespaceBytes = 64 << 20

// readRoutineNamespaces reads the complete candidate set, not just matching
// signatures. The selected identity locates its database/name under the tenant
// context; all languages participate. Revision-local namespace_version remains
// immutable. The separate digest is state-based and naturally follows rollback,
// snapshot/PITR and clone publication without a second mutable catalog index.
func readRoutineNamespaces(ctx context.Context, bh BackgroundExec, ids []uint64, snapshot *planpb.Snapshot) (map[uint64]string, error) {
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, routineNamespacesSQL(ids, snapshot)); err != nil {
		return nil, err
	}
	rows, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	row, err := candidateCatalogResultSet(rows, "routine namespaces")
	if err != nil {
		return nil, err
	}
	if row == nil {
		return map[uint64]string{}, nil
	}
	return decodeRoutineNamespaces(ctx, row)
}

func routineNamespacesSQL(ids []uint64, snapshot *planpb.Snapshot) string {
	values := make([]string, len(ids))
	for i, id := range ids {
		values[i] = strconv.FormatUint(id, 10)
	}
	suffix := ""
	if snapshot != nil && snapshot.TS != nil {
		suffix = fmt.Sprintf(" {MO_TS = %d}", snapshot.TS.PhysicalTime)
	}
	// Read both base and revision metadata: legacy SQL resolves the base, typed
	// SQL/Python resolves the immutable revision. Never drop an unmatching sibling.

	return fmt.Sprintf(`select selected.function_id, f.function_id, f.active_revision, f.namespace_version,
 sha2(cast(f.args as varchar),256), sha2(f.body,256), sha2(f.language,256), sha2(f.rettype,256), sha2(f.db,256), sha2(f.sql_mode,256), sha2(f.security_type,256),
 sha2(cast(coalesce(r.args,'[]') as varchar),256), sha2(coalesce(r.body,''),256), sha2(coalesce(r.language,''),256), sha2(coalesce(r.rettype,''),256), sha2(coalesce(r.security_type,''),256)
 from (select db,name,min(function_id) as function_id from mo_catalog.mo_user_defined_function%s where function_id in (%s) group by db,name) selected
 join mo_catalog.mo_user_defined_function%s f on f.db=selected.db and f.name=selected.name
 left join mo_catalog.mo_function_revisions%s r on r.function_id=f.function_id and r.revision=f.active_revision and r.namespace_version=f.namespace_version
 limit %d;`, suffix, strings.Join(values, ","), suffix, suffix, maxRoutineNamespaceRows+1)
}

func decodeRoutineNamespaces(ctx context.Context, rows ExecResult) (map[uint64]string, error) {
	if rows.GetRowCount() > maxRoutineNamespaceRows {
		return nil, fmt.Errorf("%w: namespace rows exceed %d", errRoutineNamespaceBudget, maxRoutineNamespaceRows)
	}
	groups := make(map[uint64]map[uint64]string)
	remaining := maxRoutineNamespaceBytes
	for row := uint64(0); row < rows.GetRowCount(); row++ {
		var numbers [4]uint64
		for col := range numbers {
			value, err := rows.GetUint64(ctx, row, uint64(col))
			if err != nil {
				return nil, err
			}
			numbers[col] = value
		}
		var fields [12]string
		for col := range fields {
			value, err := rows.GetString(ctx, row, uint64(col+4))
			if err != nil {
				return nil, err
			}
			if len(value) > remaining {
				return nil, fmt.Errorf("%w: namespace bytes exceed %d", errRoutineNamespaceBudget, maxRoutineNamespaceBytes)
			}
			remaining -= len(value)
			fields[col] = value
		}
		// JSON arrays give unambiguous length/escaping, with fixed version/domain.
		// Exclude selected id so different overloads in one namespace share a token.
		encoded, err := json.Marshal(struct {
			Version  int
			Identity [3]uint64
			Fields   [12]string
		}{1, [3]uint64{numbers[1], numbers[2], numbers[3]}, fields})
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(encoded)
		if groups[numbers[0]] == nil {
			groups[numbers[0]] = make(map[uint64]string)
		}
		if _, duplicate := groups[numbers[0]][numbers[1]]; duplicate {
			return nil, fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: duplicate routine namespace member")
		}
		groups[numbers[0]][numbers[1]] = hex.EncodeToString(digest[:])
	}
	result := make(map[uint64]string, len(groups))
	for _, members := range groups {
		fingerprints := make([]string, 0, len(members))
		for _, fp := range members {
			fingerprints = append(fingerprints, fp)
		}
		sort.Strings(fingerprints)
		digest := sha256.Sum256([]byte("mo-routine-namespace-state/1\x00" + strings.Join(fingerprints, "")))
		for id := range members {
			result[id] = hex.EncodeToString(digest[:])
		}
	}
	return result, nil
}

// begin creates the catalog read transaction. Mark its internal SELECTs derived
// until finishTxn so READ COMMITTED cannot advance between enumeration, revision
// reads and the namespace fence. Restore before COMMIT/ROLLBACK and Close.
func pinRoutineCatalogReads(bh BackgroundExec) func() {
	if back, ok := bh.(*backExec); ok {
		previous := back.backSes.ReplaceDerivedStmt(true)
		return func() { back.backSes.ReplaceDerivedStmt(previous) }
	}
	// Non-engine executors are deterministic unit-test readers.
	return func() {}
}

func routineCatalogDatabaseID(ctx context.Context, bh BackgroundExec, database string, accountID uint32, snapshot *planpb.Snapshot) (uint64, error) {
	table := "mo_catalog.mo_database"
	if snapshot != nil && snapshot.TS != nil {
		table += fmt.Sprintf(" {MO_TS = %d}", snapshot.TS.PhysicalTime)
	}
	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, fmt.Sprintf("select dat_id from %s where datname = '%s' and account_id = %d;", table, strings.ReplaceAll(database, "'", "''"), accountID)); err != nil {
		return 0, err
	}
	rows, err := getResultSet(ctx, bh)
	if err != nil {
		return 0, err
	}
	row, err := exactlyOneCatalogRow(ctx, rows, "routine database identity")
	if err != nil {
		return 0, err
	}
	return row.GetUint64(ctx, 0, 0)
}

func routineCatalogAccountID(sessionAccount uint32, snapshot *planpb.Snapshot) uint32 {
	if snapshot != nil && snapshot.Tenant != nil {
		return snapshot.Tenant.TenantID
	}
	return sessionAccount
}
