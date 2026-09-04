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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func (s *service) maintainOrphanObjectPrivileges(ctx context.Context) error {
	if !s.upgrade.orphanPrivilegeMaintenanceRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer s.upgrade.orphanPrivilegeMaintenanceRunning.Store(false)

	cursor := s.upgrade.orphanPrivilegeMaintenanceCursor.Load()
	tenantID := int32(0)
	found := false
	lookupOptions := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithAccountID(catalog.System_Account).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied().
		WithTimeZone(time.Local)
	err := s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		res, err := txn.Exec(
			fmt.Sprintf("select account_id from mo_catalog.mo_account where account_id >= %d order by account_id limit 1", cursor),
			executor.StatementOption{}.WithAccountID(catalog.System_Account),
		)
		if err != nil {
			return err
		}
		defer res.Close()
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			tenantID = vector.GetFixedAtWithTypeCheck[int32](cols[0], 0)
			found = true
			return false
		})
		return nil
	}, lookupOptions)
	if err != nil {
		return err
	}
	if !found {
		s.upgrade.orphanPrivilegeMaintenanceCursor.Store(0)
		return nil
	}

	maintenanceOptions := executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithAccountID(uint32(tenantID)).
		WithMinCommittedTS(s.now()).
		WithWaitCommittedLogApplied().
		WithTimeZone(time.Local)
	err = s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		_, err := v4_0_6.MaintainOrphanObjectPrivilegesPage(txn, uint32(tenantID))
		return err
	}, maintenanceOptions)
	// One tenant cannot monopolize maintenance. Advance after every selected
	// tenant attempt; failures and incomplete pages are revisited after wrap.
	s.advanceOrphanPrivilegeMaintenanceCursor(tenantID)
	return err
}

func (s *service) advanceOrphanPrivilegeMaintenanceCursor(tenantID int32) {
	const maxInt32 = int32(^uint32(0) >> 1)
	if tenantID == maxInt32 {
		s.upgrade.orphanPrivilegeMaintenanceCursor.Store(0)
	} else {
		s.upgrade.orphanPrivilegeMaintenanceCursor.Store(tenantID + 1)
	}
}
