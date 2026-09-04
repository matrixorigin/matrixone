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

package bootstrap

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// orphanPrivilegeMaintenanceState is intentionally process-local. A round
// freezes account IDs at roundHighWater and walks the ring from roundStart back
// to (but not including) roundStart, so account creation cannot extend a live
// round. A fresh process chooses an independent start; even if every process is
// restarted before finishing a round, each existing account has a non-zero,
// uniformly bounded probability of being selected first and is therefore
// visited with probability one across repeated restarts.
type orphanPrivilegeMaintenanceState struct {
	restartSeed uint64
	round       uint64

	roundInitialized bool
	roundHighWater   int32
	roundStart       int32
	accountCursor    int32
	wrapped          bool

	tenantSelected bool
	tenantID       int32
	tenantScan     v4_0_6.OrphanPrivilegeScan
}

func newOrphanPrivilegeMaintenanceRestartSeed() uint64 {
	var value [8]byte
	if _, err := cryptorand.Read(value[:]); err == nil {
		return binary.LittleEndian.Uint64(value[:])
	}
	// Entropy failure must not restore the old deterministic "always start at
	// zero" behavior. Wall time is only a degraded seed, not correctness state.
	return uint64(time.Now().UnixNano())
}

func orphanPrivilegeMaintenanceRoundStart(seed, round uint64, highWater int32) int32 {
	if highWater <= 0 {
		return 0
	}
	// SplitMix64 gives every process restart and every completed round a
	// well-distributed ring start without introducing durable cursor state.
	value := seed + (round+1)*0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31
	return int32(value % (uint64(highWater) + 1))
}

func (s *service) maintainOrphanObjectPrivileges(ctx context.Context) error {
	if !s.upgrade.orphanPrivilegeMaintenanceRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer s.upgrade.orphanPrivilegeMaintenanceRunning.Store(false)

	if !s.upgrade.orphanPrivilegeMaintenanceState.tenantSelected {
		found, err := s.selectNextOrphanPrivilegeMaintenanceTenant(ctx)
		if err != nil || !found {
			return err
		}
	}

	state := &s.upgrade.orphanPrivilegeMaintenanceState
	tenantID := state.tenantID
	scan := state.tenantScan
	var (
		nextScan v4_0_6.OrphanPrivilegeScan
		complete bool
	)
	maintenanceOptions := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithAccountID(uint32(tenantID)).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied().
		WithTimeZone(time.Local)
	err := s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		var err error
		nextScan, complete, err = v4_0_6.MaintainOrphanObjectPrivilegesPage(
			txn, uint32(tenantID), scan)
		return err
	}, maintenanceOptions)
	if err != nil {
		// A dropped/broken tenant must not hold the finite account round. Its
		// rolled-back scan is retried when a later round visits the account.
		s.advanceOrphanPrivilegeMaintenanceAccount(tenantID)
		return err
	}
	if complete {
		s.advanceOrphanPrivilegeMaintenanceAccount(tenantID)
	} else {
		state.tenantScan = nextScan
	}
	return nil
}

func (s *service) selectNextOrphanPrivilegeMaintenanceTenant(ctx context.Context) (bool, error) {
	current := s.upgrade.orphanPrivilegeMaintenanceState
	next := current
	found := false
	lookupOptions := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithAccountID(catalog.System_Account).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied().
		WithTimeZone(time.Local)
	err := s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		// ExecTxn may retry this closure. Never carry lookup progress from a
		// rolled-back attempt into the next transaction snapshot.
		next = current
		found = false
		option := executor.StatementOption{}.WithAccountID(catalog.System_Account)
		if !next.roundInitialized {
			highWater, exists, err := loadOrphanPrivilegeMaintenanceAccount(
				txn,
				"select account_id from mo_catalog.mo_account order by account_id desc limit 1",
				option,
			)
			if err != nil {
				return err
			}
			if !exists {
				return nil
			}
			next.roundInitialized = true
			next.roundHighWater = highWater
			next.roundStart = orphanPrivilegeMaintenanceRoundStart(
				next.restartSeed, next.round, highWater)
			next.accountCursor = next.roundStart
			next.wrapped = false
		}

		var err error
		next.tenantID, found, err = loadOrphanPrivilegeMaintenanceAccount(
			txn, orphanPrivilegeMaintenanceAccountSQL(next), option)
		if err != nil {
			return err
		}
		if !found && !next.wrapped && next.roundStart > 0 {
			next.wrapped = true
			next.accountCursor = 0
			next.tenantID, found, err = loadOrphanPrivilegeMaintenanceAccount(
				txn, orphanPrivilegeMaintenanceAccountSQL(next), option)
			if err != nil {
				return err
			}
		}
		if found {
			next.tenantSelected = true
			next.tenantScan = v4_0_6.OrphanPrivilegeScan{}
		} else {
			finishOrphanPrivilegeMaintenanceRound(&next)
		}
		return nil
	}, lookupOptions)
	if err != nil {
		return false, err
	}
	// Publish lookup progress only after its transaction committed.
	s.upgrade.orphanPrivilegeMaintenanceState = next
	return found, nil
}

func orphanPrivilegeMaintenanceAccountSQL(state orphanPrivilegeMaintenanceState) string {
	upperBound := fmt.Sprintf("account_id <= %d", state.roundHighWater)
	if state.wrapped {
		upperBound = fmt.Sprintf("account_id < %d", state.roundStart)
	}
	return fmt.Sprintf(
		"select account_id from mo_catalog.mo_account where account_id >= %d and %s order by account_id limit 1",
		state.accountCursor, upperBound)
}

func loadOrphanPrivilegeMaintenanceAccount(
	txn executor.TxnExecutor,
	sql string,
	option executor.StatementOption,
) (int32, bool, error) {
	res, err := txn.Exec(sql, option)
	if err != nil {
		return 0, false, err
	}
	defer res.Close()
	var (
		accountID int32
		found     bool
		decodeErr error
	)
	res.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if rows == 0 {
			return true
		}
		if found || rows != 1 || len(columns) != 1 || columns[0].IsNull(0) {
			decodeErr = moerr.NewInternalErrorNoCtx(
				"orphan privilege account lookup must return at most one non-NULL account_id")
			return false
		}
		accountID = vector.GetFixedAtWithTypeCheck[int32](columns[0], 0)
		if accountID < 0 {
			decodeErr = moerr.NewInternalErrorNoCtxf(
				"orphan privilege account lookup returned negative account_id %d", accountID)
			return false
		}
		found = true
		return true
	})
	return accountID, found, decodeErr
}

func (s *service) advanceOrphanPrivilegeMaintenanceAccount(tenantID int32) {
	state := &s.upgrade.orphanPrivilegeMaintenanceState
	state.tenantSelected = false
	state.tenantScan = v4_0_6.OrphanPrivilegeScan{}

	if tenantID == math.MaxInt32 {
		if !state.wrapped && state.roundStart > 0 {
			state.wrapped = true
			state.accountCursor = 0
			return
		}
		finishOrphanPrivilegeMaintenanceRound(state)
		return
	}

	state.accountCursor = tenantID + 1
	if state.wrapped && state.accountCursor >= state.roundStart ||
		!state.wrapped && state.accountCursor > state.roundHighWater && state.roundStart == 0 {
		finishOrphanPrivilegeMaintenanceRound(state)
	}
}

func finishOrphanPrivilegeMaintenanceRound(state *orphanPrivilegeMaintenanceState) {
	restartSeed := state.restartSeed
	round := state.round + 1
	*state = orphanPrivilegeMaintenanceState{
		restartSeed: restartSeed,
		round:       round,
	}
}
