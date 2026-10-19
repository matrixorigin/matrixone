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

package compile

import (
	"context"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// checkConstraintIdentities is shared by sibling and source admission. Older
// catalog rows expose CHECK names through retained CREATE SQL, not Checks.
// These name-only definitions are for ownership checks, never enforcement.
func checkConstraintIdentities(ctx context.Context, def *plan.TableDef) ([]*plan.CheckDef, error) {
	if def == nil {
		return nil, nil
	}
	for _, check := range def.Checks {
		if check != nil {
			return def.Checks, nil
		}
	}
	if def.TableType != "" && def.TableType != catalog.SystemOrdinaryRel {
		return nil, nil
	}
	legacy, err := plan2.RecoverLegacyCheckConstraints(ctx, def.Createsql)
	if err != nil {
		return nil, err
	}
	checks := make([]*plan.CheckDef, 0, len(legacy))
	for _, check := range legacy {
		checks = append(checks, &plan.CheckDef{Name: check.Name})
	}
	return checks, nil
}

// introducedCheckConstraints excludes identities already owned by the source.
// COPY and RENAME may retain historical names shared by legacy logical tables;
// only a newly authored claim needs to establish schema-wide availability.
func introducedCheckConstraints(previous, next []*plan.CheckDef) []*plan.CheckDef {
	owned := make(map[string]struct{}, len(previous))
	for _, check := range previous {
		if check != nil {
			owned[strings.ToLower(check.Name)] = struct{}{}
		}
	}
	var added []*plan.CheckDef
	for _, check := range next {
		if check != nil {
			if _, exists := owned[strings.ToLower(check.Name)]; !exists {
				added = append(added, check)
			}
		}
	}
	return added
}

// claimCheckConstraintNames retains a common transactional name reservation
// until commit/rollback. Distinct destination table locks cannot protect this
// namespace. The extra tuple component keeps these keys disjoint from real
// mo_tables primary keys, even if a user chooses a similar table name.
func (c *Compile) claimCheckConstraintNames(database string, checks []*plan.CheckDef) error {
	if c.disableLock || len(checks) == 0 {
		return nil
	}
	// Optimistic commit has no MVCC row to validate for a synthetic name key.
	// Pessimistic SI keeps its snapshot and checks the grant watermark instead
	// of using RC's applied-frontier refresh below.
	if !c.proc.GetTxnOperator().Txn().IsPessimistic() {
		return moerr.NewNotSupported(c.proc.Ctx, "new schema-wide CHECK constraint names require pessimistic transactions")
	}
	actor, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	rel, err := getRelFromMoCatalog(c, catalog.MO_TABLES)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(checks))
	for _, check := range checks {
		if check != nil {
			names = append(names, strings.ToLower(check.Name))
		}
	}
	sort.Strings(names)
	for i, name := range names {
		if i > 0 && name == names[i-1] {
			continue
		}
		if err = c.lockCheckConstraintName(rel, actor, database, name); err != nil {
			return err
		}
	}
	if !c.proc.GetTxnOperator().Txn().IsRCIsolation() {
		return nil
	}
	// The reservation is not a catalog row, so row-version probing cannot
	// refresh it. Read an applied frontier after acquiring every name,
	// including when the preceding owner committed before we began waiting.
	return c.advanceLifecycleAdmissionSnapshot()
}

func (c *Compile) lockCheckConstraintName(rel engine.Relation, actor uint32, database, name string) error {
	keys, err := getLockBatch(c.proc, actor, []string{database, "CHECK_CONSTRAINT", name})
	if err != nil {
		return err
	}
	defer keys.GetVector(0).Free(c.proc.Mp())
	admit := lockop.LockRowsForAdmissionResult
	if !c.proc.GetTxnOperator().Txn().IsRCIsolation() {
		admit = lockop.LockRowsForFixedSnapshotAdmissionResult
	}
	_, err = admit(c.proc.Ctx, c.e, c.proc, rel.GetTableID(c.proc.Ctx),
		keys, 0, *keys.GetVector(0).GetType(), lock.LockMode_Exclusive, actor, lock.WaitPolicy_Wait)
	return err
}

func (c *Compile) validateIntroducedCheckConstraintNames(database, table string, previous, next []*plan.CheckDef) error {
	if trusted, _ := c.proc.Ctx.Value(defines.TrustedCheckConstraintNames{}).(bool); trusted {
		return nil
	}
	if err := plan2.ValidateCheckConstraintNames(c.proc.Ctx, next); err != nil {
		return err
	}
	added := introducedCheckConstraints(previous, next)
	if len(added) == 0 {
		return nil
	}
	if err := c.claimCheckConstraintNames(database, added); err != nil {
		return err
	}
	db, err := c.e.Database(c.proc.Ctx, database, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	return checkConstraintNamesAvailable(c.proc.Ctx, db, table, added)
}
