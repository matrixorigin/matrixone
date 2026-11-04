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

package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const snapshotDecisionPageSize = 128

type catalogSnapshot struct {
	preimage snapshotDecisionPreimage
	kind     string
}

func (s *service) openSnapshotDecisions(ctx context.Context) (*snapshotDecisionReader, io.Closer, error) {
	if s.upgrade.decisionFileService == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("shared file service is unavailable for catalog upgrade")
	}
	var input io.ReadCloser
	v := fileservice.IOVector{
		FilePath: SnapshotUpgradeDecisionPath,
		Policy:   fileservice.SkipAllCache,
		Entries:  []fileservice.IOEntry{{Size: -1, ReadCloserForRead: &input}},
	}
	if err := s.upgrade.decisionFileService.Read(ctx, &v); err != nil {
		v.Release()
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	if input == nil {
		v.Release()
		return nil, nil, moerr.NewInternalErrorNoCtx("shared decision file did not provide a reader")
	}
	reader, err := newSnapshotDecisionReader(input)
	if err != nil {
		input.Close()
		v.Release()
		return nil, nil, &terminalUpgradeError{wrapUpgradeError(err, "invalid %s", SnapshotUpgradeDecisionPath)}
	}
	return reader, &snapshotInputCloser{reader: input, vector: &v}, nil
}

type snapshotInputCloser struct {
	reader io.Closer
	vector *fileservice.IOVector
}

func (c *snapshotInputCloser) Close() error { err := c.reader.Close(); c.vector.Release(); return err }

func (s *service) snapshotPage(ctx context.Context, after string) ([]catalogSnapshot, error) {
	var page []catalogSnapshot
	ctx, cancel := context.WithTimeoutCause(ctx, upgradeTransactionTimeout, moerr.CauseAsyncUpgradeTask)
	defer cancel()
	opts := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithTxnMode(txn.TxnMode_Pessimistic).
		WithTxnIsolation(txn.TxnIsolation_RC).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied()
	err := s.exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		var err error
		page, err = readCatalogSnapshotPage(tx, after)
		return err
	}, opts)
	return page, err
}

// Snapshot name is the existing primary key. Ordered keyset pages cap the
// retained SQL result; the file remains ordered by that same canonical name.
func readCatalogSnapshotPage(tx executor.TxnExecutor, after string) ([]catalogSnapshot, error) {
	return readCatalogSnapshots(tx, fmt.Sprintf("sname > ? order by sname limit %d", snapshotDecisionPageSize), after)
}

func readCatalogSnapshots(tx executor.TxnExecutor, predicate, parameter string) ([]catalogSnapshot, error) {
	query := "select cast(snapshot_id as varchar(36)), sname, cast(ts as varchar(32)), " +
		"cast(level as varchar(32)), account_name, database_name, table_name, cast(obj_id as varchar(32)), kind " +
		"from mo_catalog.mo_snapshots where " + predicate
	res, err := tx.Exec(query, executor.StatementOption{}.WithAccountID(catalog.System_Account).WithParams([]string{parameter}).WithMaxResultBytes(CatalogUpgradePageBytes))
	if err != nil {
		return nil, snapshotDecisionLimitError(err)
	}
	defer res.Close()
	page := make([]catalogSnapshot, 0, snapshotDecisionPageSize)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			var id, name, kind string
			if !cols[0].IsNull(uint64(i)) {
				id = cols[0].GetStringAt(i)
			}
			if !cols[1].IsNull(uint64(i)) {
				name = cols[1].GetStringAt(i)
			}
			if !cols[8].IsNull(uint64(i)) {
				kind = cols[8].GetStringAt(i)
			}
			page = append(page, catalogSnapshot{preimage: snapshotDecisionPreimage{
				CatalogAccountID: "0", SnapshotID: id, Name: name, Timestamp: nullableString(cols[2], i), Level: nullableString(cols[3], i),
				AccountName: nullableString(cols[4], i), DatabaseName: nullableString(cols[5], i), TableName: nullableString(cols[6], i),
				ObjectID: nullableString(cols[7], i), PriorKind: "user",
			}, kind: kind})
		}
		return true
	})
	return page, nil
}

func nullableString(v *vector.Vector, row int) *string {
	if v.IsNull(uint64(row)) {
		return nil
	}
	s := v.GetStringAt(row)
	return &s
}

func readSnapshotEdge(tx executor.TxnExecutor, child uint64) (*snapshotDecisionEdge, error) {
	query := "select table_id,p_table_id,clone_ts,creator,level,table_deleted from mo_catalog.mo_branch_metadata where table_id = ? limit 2"
	res, err := tx.Exec(query, executor.StatementOption{}.WithAccountID(catalog.System_Account).WithParams([]string{strconv.FormatUint(child, 10)}).WithMaxResultBytes(CatalogUpgradePageBytes))
	if err != nil {
		return nil, snapshotDecisionLimitError(err)
	}
	defer res.Close()
	var edge *snapshotDecisionEdge
	var count int
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			count++
			if count > 1 {
				continue
			}
			id := strconv.FormatUint(vector.GetFixedAtWithTypeCheck[uint64](cols[0], i), 10)
			parent := strconv.FormatUint(vector.GetFixedAtWithTypeCheck[uint64](cols[1], i), 10)
			ts := strconv.FormatInt(vector.GetFixedAtWithTypeCheck[int64](cols[2], i), 10)
			creator := strconv.FormatUint(vector.GetFixedAtWithTypeCheck[uint64](cols[3], i), 10)
			level := cols[4].GetStringAt(i)
			deleted := vector.GetFixedAtWithTypeCheck[bool](cols[5], i)
			edge = &snapshotDecisionEdge{TableID: &id, ParentID: &parent, CloneTS: &ts, Creator: &creator, Level: &level, Deleted: &deleted}
		}
		return true
	})
	if count > 1 {
		return nil, moerr.NewInvalidStateNoCtx("duplicate branch metadata for snapshot")
	}
	return edge, nil
}

func (s *service) snapshotEdge(ctx context.Context, child uint64) (*snapshotDecisionEdge, error) {
	var edge *snapshotDecisionEdge
	ctx, cancel := context.WithTimeoutCause(ctx, upgradeTransactionTimeout, moerr.CauseAsyncUpgradeTask)
	defer cancel()
	err := s.exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		var err error
		edge, err = readSnapshotEdge(tx, child)
		return err
	}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithTxnMode(txn.TxnMode_Pessimistic).
		WithTxnIsolation(txn.TxnIsolation_RC).WithMinCommittedTS(s.now()).WithWaitCommittedLogApplied())
	return edge, err
}

// scanSnapshotDecisions validates the complete file against a frozen catalog
// before its caller writes any row. The visitor is invoked only for matched
// decisions and can be nil during preflight.
func (s *service) scanSnapshotDecisions(ctx context.Context, visit func(snapshotDecisionRecord) error) error {
	reader, closer, err := s.openSnapshotDecisions(ctx)
	if err != nil {
		return err
	}
	if closer != nil {
		defer closer.Close()
	}
	var decision *snapshotDecisionRecord
	if reader != nil {
		decision, err = reader.next()
		if err != nil {
			return &terminalUpgradeError{err}
		}
	}
	var missing []snapshotDecisionPreimage
	missingError := func() error {
		detail, _ := json.Marshal(missing)
		return &terminalUpgradeError{moerr.NewInternalErrorNoCtxf("snapshot ownership requires administrator decisions by UUID; stop all CNs, prepare a decision file and publish it with mo-service -cfg <cn.toml> -catalog-upgrade-decisions <file>; see docs/guides/upgrade-4.2-to-main.md; undecided snapshots: %s", detail)}
	}
	after := ""
	for {
		page, err := s.snapshotPage(ctx, after)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			break
		}
		for _, snapshot := range page {
			after = snapshot.preimage.Name
			child, canonical := databranchutils.ParseBranchSnapshotName(after)
			if !canonical || databranchutils.BranchSnapshotName(child) != after || snapshot.preimage.Level == nil || *snapshot.preimage.Level != "table" {
				continue
			}
			if snapshot.kind != "user" && snapshot.kind != "branch" {
				return &terminalUpgradeError{moerr.NewInternalErrorNoCtxf("snapshot %s has unexpected kind %q", after, snapshot.kind)}
			}
			if decision != nil && decision.Name < after {
				return &terminalUpgradeError{moerr.NewInternalErrorNoCtxf("unknown or stale decision for snapshot %s", decision.Name)}
			}
			if decision == nil || decision.Name != after {
				if snapshot.kind == "branch" {
					continue
				}
				edge, err := s.snapshotEdge(ctx, child)
				if err != nil {
					return err
				}
				snapshot.preimage.Edge = edge
				missing = append(missing, snapshot.preimage)
				if len(missing) == snapshotDecisionPageSize {
					return missingError()
				}
				continue
			}
			edge, err := s.snapshotEdge(ctx, child)
			if err != nil {
				return err
			}
			snapshot.preimage.Edge = edge
			if err := validateSnapshotDecision(*decision, snapshot.preimage, snapshot.kind); err != nil {
				return &terminalUpgradeError{wrapUpgradeError(err, "snapshot %s (%s)", after, snapshot.preimage.SnapshotID)}
			}
			if visit != nil {
				if err := visit(*decision); err != nil {
					return err
				}
			}
			decision, err = reader.next()
			if err != nil {
				return &terminalUpgradeError{err}
			}
		}
		if len(page) < snapshotDecisionPageSize {
			break
		}
	}
	if decision != nil {
		return &terminalUpgradeError{moerr.NewInternalErrorNoCtxf("unknown or stale decision for snapshot %s", decision.Name)}
	}
	if len(missing) != 0 {
		return missingError()
	}
	return nil
}

// reconcileSnapshotDecisions performs a complete preflight before applying any
// decision, then rechecks each bounded write page under the existing owners.
func (s *service) reconcileSnapshotDecisions(ctx context.Context, upgrade versions.VersionUpgrade) error {
	if err := s.scanSnapshotDecisions(ctx, nil); err != nil {
		return err
	}
	page := make([]snapshotDecisionRecord, 0, snapshotDecisionPageSize)
	var pageBytes uint64
	flush := func() error {
		if len(page) == 0 {
			return nil
		}
		if err := s.applySnapshotDecisionPage(ctx, upgrade, page); err != nil {
			return err
		}
		page = page[:0]
		pageBytes = 0
		return nil
	}
	if err := s.scanSnapshotDecisions(ctx, func(d snapshotDecisionRecord) error {
		encoded, err := json.Marshal(d)
		if err != nil {
			return err
		}
		size := uint64(len(encoded))
		if len(page) == snapshotDecisionPageSize || pageBytes+size > CatalogUpgradePageBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		page = append(page, d)
		pageBytes += size
		return nil
	}); err != nil {
		return err
	}
	return flush()
}

func (s *service) runV407ClusterPhase(ctx context.Context, upgrade versions.VersionUpgrade) error {
	if err := s.reconcileSnapshotDecisions(ctx, upgrade); err != nil {
		return catalogOperationError(err, "cluster", "checking snapshot decisions", CatalogUpgradePageBytes, false)
	}
	for i := 0; i < v4_0_7.Handler.ClusterUpgradeEntryCount(); i++ {
		for {
			var more bool
			active, err := s.withV407Step(ctx, upgrade, func(tx executor.TxnExecutor) error {
				var err error
				more, err = v4_0_7.Handler.UpgradeClusterEntryPage(ctx, i, tx, s.upgrade.catalogObjectLimit)
				return err
			})
			if err != nil {
				return catalogOperationError(err, "cluster", fmt.Sprintf("entry %d", i), s.upgrade.catalogObjectLimit, true)
			}
			if !active {
				return nil
			}
			if !more {
				break
			}
		}
	}
	return nil
}

func (s *service) applySnapshotDecisionPage(ctx context.Context, upgrade versions.VersionUpgrade, page []snapshotDecisionRecord) error {
	_, err := s.withV407Step(ctx, upgrade, func(tx executor.TxnExecutor) error {
		lock, err := tx.Exec(databranchutils.LineageOwnerLifecyclePessimisticLockSQL(), executor.StatementOption{}.WithAccountID(catalog.System_Account))
		if err != nil {
			return err
		}
		gateRows := 0
		lock.ReadRows(func(rows int, _ []*vector.Vector) bool { gateRows += rows; return true })
		lock.Close()
		if gateRows != 1 {
			return &terminalUpgradeError{moerr.NewInvalidStateNoCtxf("snapshot lifecycle gate has %d rows", gateRows)}
		}
		for _, d := range page {
			rows, err := readCatalogSnapshots(tx, "sname = ? limit 2", d.Name)
			if err != nil {
				return err
			}
			if len(rows) != 1 {
				return &terminalUpgradeError{moerr.NewInternalErrorNoCtxf("snapshot %s (%s) changed or disappeared", d.Name, d.SnapshotID)}
			}
			child, _ := databranchutils.ParseBranchSnapshotName(d.Name)
			rows[0].preimage.Edge, err = readSnapshotEdge(tx, child)
			if err != nil {
				return err
			}
			if err := validateSnapshotDecision(d, rows[0].preimage, rows[0].kind); err != nil {
				return &terminalUpgradeError{wrapUpgradeError(err, "snapshot %s (%s)", d.Name, d.SnapshotID)}
			}
			if d.Decision != "branch" || rows[0].kind == "branch" {
				continue
			}
			res, err := tx.Exec("update mo_catalog.mo_snapshots set kind = 'branch' where snapshot_id = cast(? as uuid) and kind = 'user'",
				executor.StatementOption{}.WithAccountID(catalog.System_Account).WithParams([]string{d.SnapshotID}))
			if err != nil {
				return err
			}
			affected := res.AffectedRows
			res.Close()
			if affected != 1 {
				return &terminalUpgradeError{moerr.NewInternalErrorNoCtxf("snapshot %s (%s) changed while applying decision", d.Name, d.SnapshotID)}
			}
		}
		return nil
	})
	return err
}

// withV407Step locks the persisted target and step in the same order as the
// cluster owner. No work may be published after another owner advanced it.
func (s *service) withV407Step(ctx context.Context, upgrade versions.VersionUpgrade, work func(executor.TxnExecutor) error) (bool, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, upgradeTransactionTimeout, moerr.CauseAsyncUpgradeTask)
	defer cancel()
	active := false
	err := s.exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		state, exists, err := versions.GetVersionState(upgrade.FinalVersion, upgrade.FinalVersionOffset, tx, true)
		if err != nil {
			return err
		}
		if !exists {
			return &terminalUpgradeError{moerr.NewInvalidStateNoCtx("catalog upgrade target is missing")}
		}
		if state == versions.StateReady {
			return nil
		}
		current, err := versions.GetUpgradeVersionForUpdateByID(upgrade.ID, tx)
		if err != nil {
			return err
		}
		if current.ToVersion != upgrade.ToVersion || current.FinalVersion != upgrade.FinalVersion ||
			current.FinalVersionOffset != upgrade.FinalVersionOffset {
			return &terminalUpgradeError{moerr.NewInvalidStateNoCtx("catalog upgrade step identity changed")}
		}
		if current.State != versions.StateCreated {
			return nil
		}
		active = true
		return work(tx)
	}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithTxnMode(txn.TxnMode_Pessimistic).
		WithTxnIsolation(txn.TxnIsolation_RC).WithMinCommittedTS(s.now()).WithWaitCommittedLogApplied())
	return active, err
}

func snapshotDecisionLimitError(err error) error {
	var limit *executor.ResultLimitError
	if errors.As(err, &limit) {
		return &terminalUpgradeError{wrapUpgradeError(err, "snapshot catalog page exceeds %d bytes", CatalogUpgradePageBytes)}
	}
	return err
}
