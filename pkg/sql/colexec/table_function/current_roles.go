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

package table_function

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	currentRoleGrantQueryPrefix       = "SELECT DISTINCT cast(granted_id AS bigint) FROM mo_catalog.mo_role_grant WHERE grantee_id IN ("
	currentRoleGrantFrontierBatchSize = 256
	currentRoleClosureMaxRoles        = 4096
)

type currentRoleSQLRunner func(*process.Process, string) (executor.Result, error)

type currentRoleFrontierExpander func(*process.Process, []int64, func(int64) error) error

// validateCurrentRoleContext rejects absent or cross-account statement identities.
// Catalog identity and role membership must still be checked in the query transaction.
func validateCurrentRoleContext(ctx context.Context, identity process.CurrentRoleContext) error {
	if ctx == nil {
		return moerr.NewInternalErrorNoCtx("current role context has no execution context")
	}
	if identity.Mode != 1 && identity.Mode != 2 {
		return moerr.NewInternalError(ctx, "current role context is unset or invalid")
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	if accountID != identity.AccountID {
		return moerr.NewInternalError(ctx, "current role context account does not match execution account")
	}
	return nil
}

func buildCurrentRoleGrantQuery(frontier []int64) string {
	var sql strings.Builder
	sql.Grow(len(currentRoleGrantQueryPrefix) + len(frontier)*12 + 1)
	sql.WriteString(currentRoleGrantQueryPrefix)
	for i, roleID := range frontier {
		if i > 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(strconv.FormatInt(roleID, 10))
	}
	sql.WriteByte(')')
	// One more distinct role than the closure limit is enough to prove that
	// the query must fail, even when all previously visited roles overlap.
	// Bound the result before the SQL executor materializes it.
	sql.WriteString(" LIMIT ")
	sql.WriteString(strconv.Itoa(currentRoleClosureMaxRoles + 1))
	return sql.String()
}

func runCurrentRolesSQL(proc *process.Process, sql string) (executor.Result, error) {
	return runCurrentRolesSQLAsAccount(proc, proc.GetSessionInfo().CurrentRoleContext.AccountID, sql)
}

func visitCurrentRoleGrants(result executor.Result, visit func(int64) error) error {
	var visitErr error
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		grantedIDs := vector.MustFixedColWithTypeCheck[int64](cols[0])
		for i := 0; i < rows; i++ {
			if visitErr = visit(grantedIDs[i]); visitErr != nil {
				return false
			}
		}
		return true
	})
	return visitErr
}

func checkCurrentRoleClosureCanceled(proc *process.Process) error {
	if proc == nil || proc.Ctx == nil {
		return nil
	}
	return proc.Ctx.Err()
}

func expandCurrentRoleFrontierWithRunner(
	proc *process.Process,
	frontier []int64,
	visit func(int64) error,
	run currentRoleSQLRunner,
) error {
	for start := 0; start < len(frontier); start += currentRoleGrantFrontierBatchSize {
		if err := checkCurrentRoleClosureCanceled(proc); err != nil {
			return err
		}
		end := min(start+currentRoleGrantFrontierBatchSize, len(frontier))
		result, err := run(proc, buildCurrentRoleGrantQuery(frontier[start:end]))
		if err != nil {
			result.Close()
			return err
		}
		visitErr := func() error {
			defer result.Close()
			return visitCurrentRoleGrants(result, visit)
		}()
		if visitErr != nil {
			return visitErr
		}
	}
	return nil
}

func expandCurrentRoleFrontier(proc *process.Process, frontier []int64, visit func(int64) error) error {
	return expandCurrentRoleFrontierWithRunner(proc, frontier, visit, runCurrentRolesSQL)
}

func currentRoleClosure(
	proc *process.Process,
	roots []int64,
	expand currentRoleFrontierExpander,
) ([]int64, error) {
	visited := make(map[int64]struct{})
	frontier := make([]int64, 0, len(roots))
	for _, root := range roots {
		if _, ok := visited[root]; ok {
			continue
		}
		if len(visited) >= currentRoleClosureMaxRoles {
			return nil, moerr.NewInvalidInputNoCtxf("current role closure exceeds the %d-role query limit", currentRoleClosureMaxRoles)
		}
		visited[root] = struct{}{}
		frontier = append(frontier, root)
	}
	for len(frontier) > 0 {
		if err := checkCurrentRoleClosureCanceled(proc); err != nil {
			return nil, err
		}
		next := make([]int64, 0)
		if err := expand(proc, frontier, func(grantedID int64) error {
			if err := checkCurrentRoleClosureCanceled(proc); err != nil {
				return err
			}
			if _, ok := visited[grantedID]; ok {
				return nil
			}
			if len(visited) >= currentRoleClosureMaxRoles {
				return moerr.NewInvalidInputNoCtxf(
					"current role closure exceeds the %d-role query limit",
					currentRoleClosureMaxRoles)
			}
			visited[grantedID] = struct{}{}
			next = append(next, grantedID)
			return nil
		}); err != nil {
			return nil, err
		}
		sort.Slice(next, func(i, j int) bool { return next[i] < next[j] })
		frontier = next
	}

	roles := make([]int64, 0, len(visited))
	for roleID := range visited {
		roles = append(roles, roleID)
	}
	sort.Slice(roles, func(i, j int) bool { return roles[i] < roles[j] })
	return roles, nil
}

func runCurrentRolesSQLAsAccount(proc *process.Process, accountID uint32, sql string) (executor.Result, error) {
	if proc == nil || proc.Ctx == nil || proc.Base == nil || proc.GetTxnOperator() == nil {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("current roles requires an execution transaction")
	}
	if err := proc.Ctx.Err(); err != nil {
		return executor.Result{}, err
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return executor.Result{}, moerr.NewInternalError(proc.Ctx, "current roles runtime unavailable")
	}
	v, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	e, valid := v.(executor.SQLExecutor)
	if !ok || !valid || e == nil {
		return executor.Result{}, moerr.NewInternalError(proc.Ctx, "current roles executor unavailable")
	}
	info := proc.GetSessionInfo()
	opts := executor.Options{}.WithTxn(proc.GetTxnOperator()).WithKeepTxnAlive().WithDisableIncrStatement().WithAccountID(accountID).WithDatabase("mo_catalog").WithTimeZone(info.TimeZone).WithCNLabels(info.CNLabels).WithQuerySchedulingIntent(info.QuerySchedulingIntent).WithResolveVariableFunc(proc.GetResolveVariableFunc()).WithFrontend(proc.Base.IsFrontend)
	return e.Exec(proc.Ctx, sql, opts)
}

func currentRoleLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func loadCurrentRoleRoots(proc *process.Process) ([]int64, error) {
	if proc == nil || proc.Base == nil {
		return nil, moerr.NewInternalErrorNoCtx("current roles process unavailable")
	}
	id := proc.GetSessionInfo().CurrentRoleContext
	if err := validateCurrentRoleContext(proc.Ctx, id); err != nil {
		return nil, err
	}
	query := func(account uint32, sql string) ([]int64, error) {
		result, err := runCurrentRolesSQLAsAccount(proc, account, sql)
		defer result.Close()
		if err != nil {
			return nil, err
		}
		var ids []int64
		err = visitCurrentRoleGrants(result, func(v int64) error {
			if err := proc.Ctx.Err(); err != nil {
				return err
			}
			ids = append(ids, v)
			return nil
		})
		return ids, err
	}
	require := func(account uint32, sql string, want int64) error {
		ids, err := query(account, sql)
		if err != nil {
			return err
		}
		if len(ids) != 1 || ids[0] != want {
			return moerr.NewInternalError(proc.Ctx, "current roles identity validation failed")
		}
		return nil
	}
	isSys := id.AccountID == 0
	if isSys {
		if !strings.EqualFold(id.AccountName, "sys") {
			return nil, moerr.NewInternalError(proc.Ctx, "invalid system account")
		}
	} else {
		if err := require(0, fmt.Sprintf("SELECT cast(account_id AS bigint) FROM mo_catalog.mo_account WHERE account_id = %d AND account_name = %s LIMIT 1", id.AccountID, currentRoleLiteral(id.AccountName)), int64(id.AccountID)); err != nil {
			return nil, err
		}
	}
	if err := require(id.AccountID, fmt.Sprintf("SELECT cast(user_id AS bigint) FROM mo_catalog.mo_user WHERE user_id = %d AND user_name = %s LIMIT 1", id.UserID, currentRoleLiteral(id.UserName)), int64(id.UserID)); err != nil {
		return nil, err
	}
	trusted := id.PrimaryRoleID == 1 || (isSys && id.PrimaryRoleID == 0) || (!isSys && id.PrimaryRoleID == 2)
	if !trusted {
		if err := require(id.AccountID, fmt.Sprintf("SELECT cast(role_id AS bigint) FROM mo_catalog.mo_user_grant WHERE user_id = %d AND role_id = %d LIMIT 1", id.UserID, id.PrimaryRoleID), int64(id.PrimaryRoleID)); err != nil {
			return nil, err
		}
	}
	roots := []int64{int64(id.PrimaryRoleID)}
	if id.Mode == 2 {
		ids, err := query(id.AccountID, fmt.Sprintf("SELECT DISTINCT cast(role_id AS bigint) FROM mo_catalog.mo_user_grant WHERE user_id = %d LIMIT 4097", id.UserID))
		if err != nil {
			return nil, err
		}
		roots = append(roots, ids...)
	}
	return roots, nil
}

type currentRolesState struct{ simpleOneBatchState }

func currentRolesPrepare(_ *process.Process, _ *TableFunction) (tvfState, error) {
	return &currentRolesState{}, nil
}
func checkCurrentRolesProtocol(proc *process.Process) error {
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return moerr.NewNotSupported(proc.Ctx, "mo_current_roles requires protocol version 12")
	}
	v, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := v.(int64)
	if !ok || !valid || version < defines.MORPCVersion12 {
		return moerr.NewNotSupported(proc.Ctx, "mo_current_roles requires protocol version 12")
	}
	return nil
}

func (s *currentRolesState) start(tf *TableFunction, proc *process.Process, nthRow int, _ process.Analyzer) error {
	s.startPreamble(tf, proc, nthRow)
	if nthRow != 0 {
		s.batch.SetRowCount(0)
		return nil
	}
	if err := checkCurrentRolesProtocol(proc); err != nil {
		return err
	}
	roles, err := getCurrentRoles(proc)
	if err != nil {
		return err
	}
	if err := checkCurrentRolesProtocol(proc); err != nil {
		return err
	}
	for _, role := range roles {
		if err := vector.AppendFixed(s.batch.Vecs[0], role, false, proc.Mp()); err != nil {
			return err
		}
	}
	s.batch.SetRowCount(len(roles))
	return nil
}
