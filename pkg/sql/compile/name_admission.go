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

package compile

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/identifier"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// A namespace is not a physical catalog row. Both standalone creators and
// the lifecycle owners use this exact-row grant path; only the owning RC
// admission installs a TN frontier. SI admission never advances its snapshot.
func (c *Compile) lockNameNamespace(rel engine.Relation, actor uint32, names []string,
	mode lock.LockMode, policy lock.WaitPolicy) (lock.Result, error) {
	if !c.proc.GetTxnOperator().Txn().IsPessimistic() {
		return lock.Result{}, nil
	}
	keys, err := getLockBatch(c.proc, actor, names)
	if err != nil {
		return lock.Result{}, err
	}
	defer keys.GetVector(0).Free(c.proc.Mp())
	return lockop.LockRowsForNameAdmissionResult(c.proc.Ctx, c.e, c.proc,
		rel.GetTableID(c.proc.Ctx), keys, 0, *keys.GetVector(0).GetType(), mode, actor, policy)
}

func (c *Compile) namespaceWaitPolicy() (lock.WaitPolicy, error) {
	snapshot, err := c.proc.GetTxnOperator().Snapshot()
	if err != nil {
		return lock.WaitPolicy_Wait, err
	}
	if len(snapshot.LockTables) > 0 {
		return lock.WaitPolicy_FastFail, nil
	}
	return lock.WaitPolicy_Wait, nil
}

// The caller has already acquired sorted database pins. Prefix gates precede
// every folded name; target exact-name pins follow this stage. Missing FK
// databases retain a negative dependency on the database creation namespace.
func (c *Compile) pinMode2NameDomain(domain map[plan2.FkReferKey]struct{}, policy lock.WaitPolicy) ([]lock.Result, error) {
	if !defines.Mode2NameResolutionEnabled(c.proc.Ctx) || c.disableLock {
		return nil, nil
	}
	actor, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return nil, err
	}
	dbs := make(map[string][]string)
	for key := range domain {
		dbs[key.Db] = append(dbs[key.Db], key.Tbl)
	}
	dbRel, err := getRelFromMoCatalog(c, catalog.MO_DATABASE)
	if err != nil {
		return nil, err
	}
	tableRel, err := getRelFromMoCatalog(c, catalog.MO_TABLES)
	if err != nil {
		return nil, err
	}
	groups := make(map[uint64][]string)
	var results []lock.Result
	for _, name := range slices.Sorted(maps.Keys(dbs)) {
		db, err := c.e.Database(c.proc.Ctx, name, c.proc.GetTxnOperator())
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
			result, err := c.lockNameNamespace(dbRel, actor,
				[]string{foldedNamespaceLockTag, identifier.Fold(name)}, lock.LockMode_Exclusive, policy)
			if err != nil {
				return nil, err
			}
			results = append(results, result)
			continue
		}
		if err != nil {
			return nil, err
		}
		id, err := strconv.ParseUint(db.GetDatabaseId(c.proc.Ctx), 10, 64)
		if err != nil {
			return nil, moerr.NewInternalError(c.proc.Ctx, "invalid database ID for name admission")
		}
		for _, table := range dbs[name] {
			groups[id] = append(groups[id], identifier.Fold(table))
		}
	}
	for id, names := range groups {
		slices.Sort(names)
		groups[id] = slices.Compact(names)
	}
	// All prefix grants precede all per-name grants, across databases too.
	for _, id := range slices.Sorted(maps.Keys(groups)) {
		for _, name := range groups[id] {
			if strings.HasPrefix(name, catalog.PrefixIndexTableName) {
				result, err := c.lockNameNamespace(tableRel, actor,
					[]string{foldedNamespaceLockTag, strconv.FormatUint(id, 10), catalog.PrefixIndexTableName, "all"}, lock.LockMode_Shared, policy)
				if err != nil {
					return nil, err
				}
				results = append(results, result)
				break
			}
		}
	}
	for _, id := range slices.Sorted(maps.Keys(groups)) {
		for _, name := range groups[id] {
			result, err := c.lockNameNamespace(tableRel, actor,
				[]string{foldedNamespaceLockTag, strconv.FormatUint(id, 10), name}, lock.LockMode_Exclusive, policy)
			if err != nil {
				return nil, err
			}
			results = append(results, result)
		}
	}
	return results, nil
}
