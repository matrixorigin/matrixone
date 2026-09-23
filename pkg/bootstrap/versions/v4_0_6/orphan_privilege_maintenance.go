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

package v4_0_6

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const orphanPrivilegePageSize = 1000

const (
	orphanPrivilegeKeyColumns        = "role_id,obj_type,obj_id,privilege_id,privilege_level"
	orphanPrivilegePhysicalKeyColumn = "__mo_cpkey_col"
	orphanPrivilegeCandidateColumns  = orphanPrivilegeKeyColumns + ",hex(" + orphanPrivilegePhysicalKeyColumn + ")"

	// OrphanPrivilegePhysicalKeyMaxSize is the maximum serial() encoding size
	// of the five mo_role_privs primary-key columns.
	OrphanPrivilegePhysicalKeyMaxSize = 956
)

// OrphanPrivilegeKey is the stable mo_role_privs primary key used by the
// maintenance keyset scan.
type OrphanPrivilegeKey struct {
	RoleID         int32
	ObjectType     string
	ObjectID       uint64
	PrivilegeID    int32
	PrivilegeLevel string
}

// OrphanPrivilegeScan is process-local progress for one high-water-bounded
// physical-key ring. HighWater fixes the upper key boundary, not a snapshot:
// each page transaction can observe new rows inserted ahead of Cursor. Start
// chooses the first ring segment after a process restart. Cursor identifies the
// last physical key examined, whether that row was deleted or preserved.
type OrphanPrivilegeScan struct {
	Initialized bool
	Wrapped     bool
	CursorValid bool
	Start       string
	Cursor      string
	HighWater   string
}

type orphanPrivilegeCandidate struct {
	OrphanPrivilegeKey
	physicalKey string
}

type orphanPrivilegeKind uint8

const (
	orphanPrivilegePreserve orphanPrivilegeKind = iota
	orphanPrivilegeDatabase
	orphanPrivilegeRelation
)

// MaintainOrphanObjectPrivilegesPage examines at most orphanPrivilegePageSize
// rows from a stable physical-primary-key range and deletes only confirmed
// orphans from that candidate set. It has no durable completion marker: callers
// start a new high-water-bounded ring after completion so mixed-version writers
// remain repairable. Start must be a hex-encoded physical-key threshold; an
// empty Start scans from the physical minimum without a wrap segment.
func MaintainOrphanObjectPrivilegesPage(
	txn executor.TxnExecutor,
	accountID uint32,
	scan OrphanPrivilegeScan,
) (next OrphanPrivilegeScan, scanComplete bool, err error) {
	option := executor.StatementOption{}.WithAccountID(accountID)
	next = scan
	if err := validateOrphanPrivilegePhysicalKey(next.Start); err != nil {
		return scan, false, err
	}
	if !next.Initialized {
		highWater, found, err := loadOrphanPrivilegeHighWater(txn, option)
		if err != nil {
			return scan, false, err
		}
		if !found {
			return OrphanPrivilegeScan{}, true, nil
		}
		next.Initialized = true
		next.HighWater = highWater.physicalKey
	}

	candidates, err := loadOrphanPrivilegeCandidates(txn, option, next)
	if err != nil {
		return scan, false, err
	}
	if len(candidates) == 0 && !next.Wrapped && next.Start != "" {
		// The randomized threshold can sort above HighWater. Wrap immediately;
		// the empty suffix did not inspect a privilege candidate.
		next.Wrapped = true
		next.CursorValid = false
		next.Cursor = ""
		candidates, err = loadOrphanPrivilegeCandidates(txn, option, next)
		if err != nil {
			return scan, false, err
		}
	}
	if len(candidates) == 0 {
		return OrphanPrivilegeScan{}, true, nil
	}

	liveDatabaseIDs, err := loadLiveOrphanPrivilegeObjectIDs(
		txn, option, "mo_database", "dat_id", candidateObjectIDs(candidates, orphanPrivilegeDatabase))
	if err != nil {
		return scan, false, err
	}
	liveRelationIDs, err := loadLiveOrphanPrivilegeObjectIDs(
		txn, option, "mo_tables", "rel_logical_id", candidateObjectIDs(candidates, orphanPrivilegeRelation))
	if err != nil {
		return scan, false, err
	}

	orphans := make([]OrphanPrivilegeKey, 0, len(candidates))
	for _, candidate := range candidates {
		switch classifyOrphanPrivilege(candidate.OrphanPrivilegeKey) {
		case orphanPrivilegeDatabase:
			if _, live := liveDatabaseIDs[candidate.ObjectID]; !live {
				orphans = append(orphans, candidate.OrphanPrivilegeKey)
			}
		case orphanPrivilegeRelation:
			if _, live := liveRelationIDs[candidate.ObjectID]; !live {
				orphans = append(orphans, candidate.OrphanPrivilegeKey)
			}
		}
	}
	if len(orphans) > 0 {
		res, err := txn.Exec(deleteOrphanPrivilegeCandidatesSQL(orphans), option)
		if err != nil {
			return scan, false, err
		}
		affected := res.AffectedRows
		res.Close()
		if affected > uint64(len(orphans)) {
			return scan, false, moerr.NewInternalErrorNoCtxf(
				"orphan privilege delete affected %d rows from %d candidates", affected, len(orphans))
		}
	}

	lastPhysicalKey := candidates[len(candidates)-1].physicalKey
	segmentComplete := len(candidates) < orphanPrivilegePageSize || lastPhysicalKey == next.HighWater
	if segmentComplete {
		if !next.Wrapped && next.Start != "" {
			next.Wrapped = true
			next.CursorValid = false
			next.Cursor = ""
			return next, false, nil
		}
		return OrphanPrivilegeScan{}, true, nil
	}
	next.Cursor = lastPhysicalKey
	next.CursorValid = true
	return next, false, nil
}

func loadOrphanPrivilegeHighWater(
	txn executor.TxnExecutor,
	option executor.StatementOption,
) (orphanPrivilegeCandidate, bool, error) {
	res, err := txn.Exec(
		"select "+orphanPrivilegeCandidateColumns+" from mo_catalog.mo_role_privs order by "+
			orphanPrivilegePhysicalKeyColumn+" desc limit 1",
		option,
	)
	if err != nil {
		return orphanPrivilegeCandidate{}, false, err
	}
	defer res.Close()
	rows, err := decodeOrphanPrivilegeCandidates(res, 1)
	if err != nil {
		return orphanPrivilegeCandidate{}, false, err
	}
	if len(rows) == 0 {
		return orphanPrivilegeCandidate{}, false, nil
	}
	return rows[0], true, nil
}

func loadOrphanPrivilegeCandidates(
	txn executor.TxnExecutor,
	option executor.StatementOption,
	scan OrphanPrivilegeScan,
) ([]orphanPrivilegeCandidate, error) {
	highWater, err := orphanPrivilegePhysicalKeySQL(scan.HighWater)
	if err != nil {
		return nil, err
	}
	where := orphanPrivilegePhysicalKeyColumn + " <= " + highWater
	if scan.Wrapped {
		start, err := orphanPrivilegePhysicalKeySQL(scan.Start)
		if err != nil {
			return nil, err
		}
		where += " and " + orphanPrivilegePhysicalKeyColumn + " < " + start
	} else if scan.Start != "" {
		start, err := orphanPrivilegePhysicalKeySQL(scan.Start)
		if err != nil {
			return nil, err
		}
		where += " and " + orphanPrivilegePhysicalKeyColumn + " >= " + start
	}
	if scan.CursorValid {
		cursor, err := orphanPrivilegePhysicalKeySQL(scan.Cursor)
		if err != nil {
			return nil, err
		}
		where += " and " + orphanPrivilegePhysicalKeyColumn + " > " + cursor
	}
	res, err := txn.Exec(fmt.Sprintf(
		"select %s from mo_catalog.mo_role_privs where %s order by %s limit %d",
		orphanPrivilegeCandidateColumns, where, orphanPrivilegePhysicalKeyColumn, orphanPrivilegePageSize,
	), option)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	return decodeOrphanPrivilegeCandidates(res, orphanPrivilegePageSize)
}

func orphanPrivilegePhysicalKeySQL(value string) (string, error) {
	if err := validateOrphanPrivilegePhysicalKey(value); err != nil {
		return "", err
	}
	return "unhex('" + value + "')", nil
}

func validateOrphanPrivilegePhysicalKey(value string) error {
	if len(value) > OrphanPrivilegePhysicalKeyMaxSize*2 {
		return moerr.NewInternalErrorNoCtxf(
			"orphan privilege physical key is %d bytes, maximum is %d",
			len(value)/2, OrphanPrivilegePhysicalKeyMaxSize)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return moerr.NewInternalErrorNoCtxf("invalid orphan privilege physical key: %v", err)
	}
	return nil
}

func decodeOrphanPrivilegeCandidates(res executor.Result, limit int) ([]orphanPrivilegeCandidate, error) {
	keys := make([]orphanPrivilegeCandidate, 0, limit)
	var decodeErr error
	res.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if rows == 0 {
			return true
		}
		if rows < 0 || len(keys) > limit || rows > limit-len(keys) {
			decodeErr = moerr.NewInternalErrorNoCtxf(
				"orphan privilege candidate page has too many rows: batch=%d, read=%d, maximum=%d",
				rows, len(keys), limit)
			return false
		}
		if len(columns) != 6 {
			decodeErr = moerr.NewInternalErrorNoCtxf(
				"orphan privilege candidate page returned %d columns, expected 6", len(columns))
			return false
		}
		for row := 0; row < rows; row++ {
			for column := range columns {
				if columns[column].IsNull(uint64(row)) {
					decodeErr = moerr.NewInternalErrorNoCtxf(
						"orphan privilege primary key contains NULL at row %d column %d", len(keys)+row, column)
					return false
				}
			}
			physicalKey := columns[5].GetStringAt(row)
			if err := validateOrphanPrivilegePhysicalKey(physicalKey); err != nil {
				decodeErr = err
				return false
			}
			keys = append(keys, orphanPrivilegeCandidate{
				OrphanPrivilegeKey: OrphanPrivilegeKey{
					RoleID:         vector.GetFixedAtWithTypeCheck[int32](columns[0], row),
					ObjectType:     columns[1].GetStringAt(row),
					ObjectID:       vector.GetFixedAtWithTypeCheck[uint64](columns[2], row),
					PrivilegeID:    vector.GetFixedAtWithTypeCheck[int32](columns[3], row),
					PrivilegeLevel: columns[4].GetStringAt(row),
				},
				physicalKey: physicalKey,
			})
		}
		return true
	})
	return keys, decodeErr
}

func candidateObjectIDs(candidates []orphanPrivilegeCandidate, kind orphanPrivilegeKind) []uint64 {
	seen := make(map[uint64]struct{}, len(candidates))
	ids := make([]uint64, 0, len(candidates))
	for _, candidate := range candidates {
		if classifyOrphanPrivilege(candidate.OrphanPrivilegeKey) != kind {
			continue
		}
		if _, ok := seen[candidate.ObjectID]; ok {
			continue
		}
		seen[candidate.ObjectID] = struct{}{}
		ids = append(ids, candidate.ObjectID)
	}
	return ids
}

func loadLiveOrphanPrivilegeObjectIDs(
	txn executor.TxnExecutor,
	option executor.StatementOption,
	table string,
	column string,
	ids []uint64,
) (map[uint64]struct{}, error) {
	live := make(map[uint64]struct{}, len(ids))
	if len(ids) == 0 {
		return live, nil
	}
	values := make([]string, len(ids))
	for i, id := range ids {
		values[i] = fmt.Sprintf("%d", id)
	}
	res, err := txn.Exec(fmt.Sprintf(
		"select %s from mo_catalog.%s where account_id = current_account_id() and %s in (%s)",
		column, table, column, strings.Join(values, ","),
	), option)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	var decodeErr error
	res.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if rows == 0 {
			return true
		}
		if len(columns) != 1 {
			decodeErr = moerr.NewInternalErrorNoCtxf(
				"orphan privilege live-object lookup returned %d columns, expected 1", len(columns))
			return false
		}
		for row := 0; row < rows; row++ {
			if columns[0].IsNull(uint64(row)) {
				decodeErr = moerr.NewInternalErrorNoCtx("orphan privilege live-object lookup returned NULL")
				return false
			}
			live[vector.GetFixedAtWithTypeCheck[uint64](columns[0], row)] = struct{}{}
		}
		return true
	})
	return live, decodeErr
}

func classifyOrphanPrivilege(key OrphanPrivilegeKey) orphanPrivilegeKind {
	if key.ObjectID == 0 {
		return orphanPrivilegePreserve
	}
	if (key.ObjectType == "database" && key.PrivilegeLevel == "d") ||
		((key.ObjectType == "table" || key.ObjectType == "view") &&
			(key.PrivilegeLevel == "d.*" || key.PrivilegeLevel == "*")) {
		return orphanPrivilegeDatabase
	}
	if (key.ObjectType == "table" || key.ObjectType == "view") &&
		(key.PrivilegeLevel == "d.t" || key.PrivilegeLevel == "t") {
		return orphanPrivilegeRelation
	}
	return orphanPrivilegePreserve
}

func deleteOrphanPrivilegeCandidatesSQL(keys []OrphanPrivilegeKey) string {
	tuples := make([]string, len(keys))
	for i, key := range keys {
		tuples[i] = orphanPrivilegeKeyTuple(key)
	}
	return fmt.Sprintf(
		"delete from mo_catalog.mo_role_privs where (%s) in (%s) limit %d",
		orphanPrivilegeKeyColumns, strings.Join(tuples, ","), orphanPrivilegePageSize)
}

func orphanPrivilegeKeyTuple(key OrphanPrivilegeKey) string {
	return fmt.Sprintf("(%d,%s,%d,%d,%s)",
		key.RoleID,
		sqlquote.String(key.ObjectType),
		key.ObjectID,
		key.PrivilegeID,
		sqlquote.String(key.PrivilegeLevel),
	)
}
