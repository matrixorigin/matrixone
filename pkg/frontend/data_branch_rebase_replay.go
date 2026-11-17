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
	"bytes"
	"context"
	"errors"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const rebaseDeltaReplayBufferBytes = 4 << 20

// Conservative capability admission bounds typed probe/action batches and SQL
// formatting. All other layouts keep the exact SQL implementation; this is
// not a restriction on the public Rebase schema contract.
func rebaseDeltaLayout(columns []rebaseSQLColumn) ([]types.Type, bool) {
	if len(columns) == 0 || len(columns) > 128 {
		return nil, false
	}
	var keys []types.Type
	width := int64(0)
	for _, column := range columns {
		// Replay formats values as SQL literals. A session-local TIMESTAMP
		// literal loses its UTC offset during a DST fold; keep typed INSERT
		// SELECT replay for this layout instead.
		if column.typ.Oid == types.T_timestamp {
			return nil, false
		}
		if column.primary {
			if !rebaseIntegerKeyType(column.typ.Oid) {
				return nil, false
			}
			keys = append(keys, column.typ)
		}
		if column.typ.IsVarlen() {
			if column.typ.Width <= 0 || column.typ.Width > 4096 {
				return nil, false
			}
			width += int64(column.typ.Width)*8 + 64 // UTF-8 plus hex SQL representation.
		} else {
			width += 128 // Includes decimal256 text and type/cast overhead.
		}
		if width > 8192 {
			return nil, false
		}
	}
	return keys, len(keys) > 0 && len(keys) <= 16
}

type rebaseDeltaReplay struct {
	counts    [6]uint64
	deletes   *dataBranchOutputSpool
	inserts   *dataBranchOutputSpool
	resources dataBranchVisibleStateRecoveryResources
	reserved  bool
}

func (replay *rebaseDeltaReplay) close() error {
	err := errors.Join(replay.deletes.close(), replay.inserts.close())
	replay.deletes, replay.inserts = nil, nil
	if replay.reserved {
		replay.resources.ReleaseBuffer(rebaseDeltaReplayBufferBytes)
		replay.reserved = false
	}
	return err
}

func prepareRebaseDeltaReplay(ctx context.Context, ses *Session, bh BackgroundExec,
	table rebasePlannedTable, reads [4]rebaseSQLRead, policy rebaseConflictPolicy,
	allocator *branchHashmapAllocator, keyTypes []types.Type) (replay *rebaseDeltaReplay, err error) {
	replay = &rebaseDeltaReplay{resources: dataBranchVisibleStateRecoveryResources{allocator: allocator}}
	if err = replay.resources.ReserveBuffer(rebaseDeltaReplayBufferBytes); err != nil {
		return nil, err
	}
	replay.reserved = true
	owner := replay
	defer func() {
		if err != nil {
			err = errors.Join(err, owner.close())
		}
	}()
	keys, err := newRebaseDeltaKeys(allocator, ses.proc.Mp(), keyTypes)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, keys.close()) }()
	finish := beginRebasePhase("delta_keys", table.name, "")
	for _, pair := range [][2]int{{0, 1}, {2, 3}} {
		err = collectRebaseChangedKeyBatches(ctx, ses, bh, table.name, reads[pair[0]], reads[pair[1]], allocator,
			func(vecs []*vector.Vector) error { return keys.add(ctx, vecs) })
		if err != nil {
			finish(err)
			return nil, err
		}
	}
	finish(nil)
	err = keys.drain(ctx, ses, func(rows [][]string) error {
		query, queryErr := buildRebaseDeltaProbeSQL(ctx, table.name, reads, table.columns, rows)
		if queryErr != nil {
			return queryErr
		}
		finishProbe := beginRebaseStatementPhase(bh, "delta_probe", table.name, query)
		result, queryErr := runSqlWithBackExec(ctx, ses, bh, query)
		finishProbe(queryErr)
		if queryErr != nil {
			return queryErr
		}
		defer result.Close()
		for _, data := range result.Batches {
			if queryErr = replay.appendProbe(ctx, ses, table, data, policy); queryErr != nil {
				return queryErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if policy == rebaseFail && replay.counts[2] != 0 {
		return nil, moerr.NewInvalidInputf(ctx, "Rebase conflict in table %s", table.name)
	}
	if replay.deletes == nil && replay.inserts == nil {
		err = replay.close()
	}
	return replay, err
}

func (replay *rebaseDeltaReplay) appendProbe(ctx context.Context, ses *Session,
	table rebasePlannedTable, data *batch.Batch, policy rebaseConflictPolicy) error {
	var keyColumns []rebaseSQLColumn
	for _, column := range table.columns {
		if column.primary {
			keyColumns = append(keyColumns, column)
		}
	}
	if data == nil || len(data.Vecs) != 5+len(table.columns)+len(keyColumns) {
		return moerr.NewInternalError(ctx, "invalid Rebase delta probe result")
	}
	for _, vec := range data.Vecs[:5] {
		if vec == nil || vec.GetType().Oid != types.T_bool {
			return moerr.NewInternalError(ctx, "invalid Rebase delta state predicate")
		}
	}
	makeBatch := func(columns []rebaseSQLColumn) *batch.Batch {
		out := batch.NewWithSize(len(columns))
		for i, column := range columns {
			out.Vecs[i] = vector.NewVec(column.typ)
		}
		return out
	}
	deletes, inserts := makeBatch(keyColumns), makeBatch(table.columns)
	defer deletes.Clean(ses.proc.Mp())
	defer inserts.Clean(ses.proc.Mp())
	for row := 0; row < data.RowCount(); row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var flags [5]bool
		for i, vec := range data.Vecs[:5] {
			if vec.IsNull(uint64(row)) {
				return moerr.NewInternalError(ctx, "NULL Rebase delta state predicate")
			}
			flags[i] = vector.GetFixedAtWithTypeCheck[bool](vec, row)
		}
		decision := decideRebaseDeltaKey(rebaseDeltaKeyState{flags[0], flags[1], flags[2], flags[3], flags[4]}, policy)
		for i, count := range decision.counts {
			if replay.counts[i] > math.MaxUint64-count {
				return moerr.NewInternalError(ctx, "Rebase summary overflow")
			}
			replay.counts[i] += count
		}
		if decision.remove {
			for i, vec := range deletes.Vecs {
				if err := vec.UnionOne(data.Vecs[5+len(table.columns)+i], int64(row), ses.proc.Mp()); err != nil {
					return err
				}
			}
			deletes.SetRowCount(deletes.Vecs[0].Length())
		}
		if decision.insert {
			for i, vec := range inserts.Vecs {
				if err := vec.UnionOne(data.Vecs[5+i], int64(row), ses.proc.Mp()); err != nil {
					return err
				}
			}
			inserts.SetRowCount(inserts.Vecs[0].Length())
		}
	}
	for _, action := range []struct {
		data  *batch.Batch
		kind  string
		spool **dataBranchOutputSpool
	}{{deletes, diffDelete, &replay.deletes}, {inserts, diffInsert, &replay.inserts}} {
		if action.data.RowCount() == 0 {
			continue
		}
		if *action.spool == nil {
			spool, err := newDataBranchOutputSpool(ctx, ses.proc)
			if err != nil {
				return err
			}
			*action.spool = spool
		}
		if err := (*action.spool).append(batchWithKind{name: table.name, kind: action.kind, batch: action.data}); err != nil {
			return err
		}
	}
	return nil
}

func (replay *rebaseDeltaReplay) apply(ctx context.Context, ses *Session, bh BackgroundExec,
	destination string, table rebasePlannedTable) error {
	var keys []rebaseSQLColumn
	for _, column := range table.columns {
		if column.primary {
			keys = append(keys, column)
		}
	}
	for _, action := range []struct {
		spool   *dataBranchOutputSpool
		columns []rebaseSQLColumn
		phase   string
	}{{replay.deletes, keys, "delete"}, {replay.inserts, table.columns, "insert"}} {
		if action.spool == nil {
			continue
		}
		if err := action.spool.rewind(); err != nil {
			return err
		}
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			wrapped, ok, err := action.spool.next()
			if err != nil || !ok {
				if err != nil {
					return err
				}
				break
			}
			var buffer bytes.Buffer
			target := quoteIdentifierForSQL(destination) + "." + quoteIdentifierForSQL(table.name)
			if action.phase == "delete" {
				buffer.WriteString("delete from " + target + " where ")
			} else {
				names := make([]string, len(action.columns))
				for i, column := range action.columns {
					names[i] = quoteIdentifierForSQL(column.name)
				}
				buffer.WriteString("insert into " + target + " (" + strings.Join(names, ",") + ") values ")
			}
			values := make([]any, len(action.columns))
			for row := 0; row < wrapped.batch.RowCount(); row++ {
				if row > 0 {
					if action.phase == "delete" {
						buffer.WriteString(" or ")
					} else {
						buffer.WriteByte(',')
					}
				}
				buffer.WriteByte('(')
				for i, column := range action.columns {
					if i > 0 {
						if action.phase == "delete" {
							buffer.WriteString(" and ")
						} else {
							buffer.WriteByte(',')
						}
					}
					if action.phase == "delete" {
						buffer.WriteString(quoteIdentifierForSQL(column.name) + " = ")
					}
					if err = extractDataBranchSQLRowValue(ctx, ses, wrapped.batch.Vecs[i], i, values, row); err != nil {
						return err
					}
					if err = formatValIntoString(ses, values[i], column.typ, &buffer); err != nil {
						return err
					}
				}
				buffer.WriteByte(')')
			}
			query := buffer.String()
			finish := beginRebaseStatementPhase(bh, action.phase, table.name, query)
			err = bh.Exec(ctx, query)
			finish(err)
			if err != nil {
				return err
			}
			clearRebaseResults(bh)
		}
	}
	return nil
}
