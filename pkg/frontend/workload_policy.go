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
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

const (
	queryWorkloadPolicy                = "query_workload_policy"
	defaultWorkloadPolicyRefreshPeriod = 30 * time.Second
	minWorkloadPolicyRefreshBackoff    = time.Second
	maxWorkloadPolicyRefreshBackoff    = 30 * time.Second
	workloadPolicyPublishTimeout       = 3 * time.Second
	workloadPolicyRefreshTimeout       = 5 * time.Second
)

type workloadPolicyBypassContextKey struct{}

// withWorkloadPolicyBypass marks control-plane SQL that reads or mutates the
// workload policy itself. Those statements must remain executable even when
// the currently cached internal-workload rule points at an unavailable pool.
func withWorkloadPolicyBypass(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, workloadPolicyBypassContextKey{}, true)
}

func workloadPolicyBypassed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	bypassed, _ := ctx.Value(workloadPolicyBypassContextKey{}).(bool)
	return bypassed
}

// workloadPolicySnapshot is immutable after publication. In particular, Set
// contains maps and must never be modified after it is stored in accountState.
type workloadPolicySnapshot struct {
	raw      string
	revision uint64
	set      schedule.WorkloadPolicySet
}

type workloadPolicyRefresh struct {
	done chan struct{}
	err  error
}

// accountWorkloadPolicy is the per-CN cache for one account. Statement reads
// are lock-free. mu only serializes refreshes and revision changes.
type accountWorkloadPolicy struct {
	mu           sync.Mutex
	snapshot     atomic.Pointer[workloadPolicySnapshot]
	refreshAfter atomic.Int64
	refreshing   *workloadPolicyRefresh
	accountID    uint32
	references   uint64
	failures     uint8
	removed      bool
}

type workloadPolicyLoader func(
	context.Context,
	FeSession,
	uint32,
) (string, uint64, error)

// WorkloadPolicyManager owns only account-level cache state. The catalog table
// is authoritative; RPC messages are an acceleration path and are ordered by
// the table's monotonically increasing revision.
type WorkloadPolicyManager struct {
	accounts      sync.Map
	refreshPeriod time.Duration
	load          workloadPolicyLoader
}

func NewWorkloadPolicyManager() *WorkloadPolicyManager {
	return newWorkloadPolicyManager(
		defaultWorkloadPolicyRefreshPeriod,
		loadWorkloadPolicyFromCatalog,
	)
}

func newWorkloadPolicyManager(
	refreshPeriod time.Duration,
	load workloadPolicyLoader,
) *WorkloadPolicyManager {
	if refreshPeriod <= 0 {
		refreshPeriod = defaultWorkloadPolicyRefreshPeriod
	}
	return &WorkloadPolicyManager{
		refreshPeriod: refreshPeriod,
		load:          load,
	}
}

var GWorkloadPolicyManager = NewWorkloadPolicyManager()

func (m *WorkloadPolicyManager) acquire(accountID uint32) *accountWorkloadPolicy {
	for {
		value, _ := m.accounts.LoadOrStore(
			accountID,
			&accountWorkloadPolicy{accountID: accountID},
		)
		state := value.(*accountWorkloadPolicy)
		state.mu.Lock()
		if state.removed {
			state.mu.Unlock()
			m.accounts.CompareAndDelete(accountID, state)
			continue
		}
		state.references++
		state.mu.Unlock()
		return state
	}
}

func (m *WorkloadPolicyManager) release(state *accountWorkloadPolicy) {
	if state == nil {
		return
	}
	state.mu.Lock()
	if state.references == 0 {
		state.mu.Unlock()
		return
	}
	state.references--
	remove := state.references == 0
	if remove {
		state.removed = true
	}
	state.mu.Unlock()
	if remove {
		m.accounts.CompareAndDelete(state.accountID, state)
	}
}

func (m *WorkloadPolicyManager) cached(
	state *accountWorkloadPolicy,
) schedule.WorkloadPolicySet {
	if state == nil {
		return schedule.WorkloadPolicySet{}
	}
	if snapshot := state.snapshot.Load(); snapshot != nil {
		return snapshot.set
	}
	return schedule.WorkloadPolicySet{}
}

// Apply accepts a notification only for an account already active on this CN.
// Ignoring inactive accounts keeps the cache bounded by locally served tenants.
func (m *WorkloadPolicyManager) Apply(
	accountID uint32,
	raw string,
	revision uint64,
) (applied bool, currentRevision uint64, err error) {
	if revision == 0 {
		return false, 0, moerr.NewInvalidInputNoCtx(
			"workload policy revision must be greater than zero",
		)
	}
	set, err := schedule.ParseWorkloadPolicyConfig(raw)
	if err != nil {
		return false, 0, err
	}
	value, ok := m.accounts.Load(accountID)
	if !ok {
		return false, 0, nil
	}
	state := value.(*accountWorkloadPolicy)

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.removed || state.references == 0 {
		return false, 0, nil
	}
	current := state.snapshot.Load()
	if current != nil {
		switch {
		case revision < current.revision:
			return false, current.revision, nil
		case revision == current.revision && raw == current.raw:
			return false, current.revision, nil
		case revision == current.revision:
			return false, current.revision, moerr.NewInternalErrorNoCtxf(
				"conflicting workload policies at revision %d for account %d",
				revision,
				accountID,
			)
		}
	}
	state.snapshot.Store(&workloadPolicySnapshot{
		raw:      raw,
		revision: revision,
		set:      set,
	})
	state.failures = 0
	state.refreshAfter.Store(time.Now().Add(m.refreshPeriod).UnixNano())
	return true, revision, nil
}

func (m *WorkloadPolicyManager) Remove(accountID uint32) {
	value, ok := m.accounts.LoadAndDelete(accountID)
	if !ok {
		return
	}
	state := value.(*accountWorkloadPolicy)
	state.mu.Lock()
	state.removed = true
	state.mu.Unlock()
}

// Refresh reconciles one account with the authoritative catalog. When a valid
// cached snapshot exists, callers never queue behind an in-flight refresh and
// continue with that immutable snapshot. The first load waits because there is
// no safe state to use yet.
func (m *WorkloadPolicyManager) Refresh(
	ctx context.Context,
	ses FeSession,
	accountID uint32,
	state *accountWorkloadPolicy,
) error {
	return m.refreshWith(
		ctx,
		accountID,
		state,
		func() (string, uint64, error) {
			return m.load(ctx, ses, accountID)
		},
	)
}

// RefreshAsync reconciles an established snapshot without putting catalog
// latency on the statement path. It uses the CN's independent internal SQL
// executor rather than borrowing the foreground session across goroutines.
func (m *WorkloadPolicyManager) RefreshAsync(
	ctx context.Context,
	service string,
	accountID uint32,
	state *accountWorkloadPolicy,
) error {
	// Keep the overwhelmingly common path ahead of closure construction. A
	// workloadPolicyLoader closure escapes into the detached refresh goroutine
	// on the cold path; constructing it before this check adds two allocations
	// to every statement even while the snapshot is fresh.
	if state != nil {
		current := state.snapshot.Load()
		if current != nil &&
			time.Now().UnixNano() < state.refreshAfter.Load() {
			return nil
		}
	}
	return m.refreshWithMode(
		ctx,
		accountID,
		state,
		func(loadCtx context.Context) (string, uint64, error) {
			return loadWorkloadPolicyFromCatalogByService(
				loadCtx,
				service,
				accountID,
			)
		},
		true,
	)
}

func (m *WorkloadPolicyManager) refreshWith(
	ctx context.Context,
	accountID uint32,
	state *accountWorkloadPolicy,
	load func() (string, uint64, error),
) error {
	return m.refreshWithMode(
		ctx,
		accountID,
		state,
		func(context.Context) (string, uint64, error) {
			return load()
		},
		false,
	)
}

func (m *WorkloadPolicyManager) refreshWithMode(
	ctx context.Context,
	accountID uint32,
	state *accountWorkloadPolicy,
	load func(context.Context) (string, uint64, error),
	asyncEstablished bool,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if state == nil {
		return moerr.NewInternalError(ctx, "workload policy state is not initialized")
	}
	if state.accountID != accountID {
		return moerr.NewInternalErrorf(
			ctx,
			"workload policy state belongs to account %d, not account %d",
			state.accountID,
			accountID,
		)
	}
	now := time.Now()
	current := state.snapshot.Load()
	if current != nil && now.UnixNano() < state.refreshAfter.Load() {
		return nil
	}

	state.mu.Lock()
	if state.removed || state.references == 0 {
		state.mu.Unlock()
		return nil
	}
	current = state.snapshot.Load()
	if current != nil && now.UnixNano() < state.refreshAfter.Load() {
		state.mu.Unlock()
		return nil
	}
	if refresh := state.refreshing; refresh != nil {
		if current != nil {
			state.mu.Unlock()
			return nil
		}
		done := refresh.done
		state.mu.Unlock()
		select {
		case <-done:
			return refresh.err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	refresh := &workloadPolicyRefresh{done: make(chan struct{})}
	state.refreshing = refresh
	refreshBase := current
	state.mu.Unlock()

	run := func(loadCtx context.Context) error {
		var raw string
		var revision uint64
		var loadErr error
		var failureCount uint8
		enteredInvalid := false
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					loadErr = moerr.NewInternalErrorf(
						loadCtx,
						"panic while loading workload policy for account %d: %v",
						accountID,
						recovered,
					)
				}
			}()
			raw, revision, loadErr = load(loadCtx)
		}()
		var parsed schedule.WorkloadPolicySet
		if loadErr == nil {
			var parseErr error
			if parsed, parseErr = schedule.ParseWorkloadPolicyConfig(raw); parseErr != nil {
				// A corrupt authoritative row must fail closed until an administrator
				// repairs it; it is not a transient catalog-read failure.
				parsed = schedule.WorkloadPolicySet{InvalidReason: parseErr.Error()}
			}
		}

		state.mu.Lock()
		current = state.snapshot.Load()
		hadSnapshot := current != nil
		resultErr := loadErr
		superseded := current != nil && current != refreshBase
		if state.removed || state.references == 0 {
			// The final foreground session disappeared while the catalog read was
			// running. Wake initial-load waiters, but do not resurrect this cache.
		} else if loadErr == nil {
			state.failures = 0
			if current == nil || revision >= current.revision {
				enteredInvalid = parsed.InvalidReason != "" &&
					(current == nil ||
						current.revision != revision ||
						current.raw != raw ||
						current.set.InvalidReason == "")
				state.snapshot.Store(&workloadPolicySnapshot{
					raw:      raw,
					revision: revision,
					set:      parsed,
				})
			}
			state.refreshAfter.Store(time.Now().Add(m.refreshPeriod).UnixNano())
		} else if superseded {
			// Apply installed a committed, newer RPC snapshot while this catalog
			// read was in flight. That snapshot satisfies this refresh generation;
			// do not shorten its healthy deadline or wake initial-load waiters with
			// the obsolete read error.
			resultErr = nil
		} else {
			if state.failures < ^uint8(0) {
				state.failures++
			}
			failureCount = state.failures
			if current == nil {
				state.snapshot.Store(&workloadPolicySnapshot{
					set: schedule.WorkloadPolicySet{
						InvalidReason: "query workload policy refresh failed: " +
							loadErr.Error(),
					},
				})
			}
			state.refreshAfter.Store(
				time.Now().Add(workloadPolicyRefreshBackoff(state.failures)).UnixNano(),
			)
		}
		refresh.err = resultErr
		state.refreshing = nil
		close(refresh.done)
		state.mu.Unlock()

		// The foreground statement has already continued with its pinned
		// snapshot, so report the first failure in a consecutive retry streak
		// here. Later failures are intentionally silent until a success resets
		// the counter, avoiding one warning per account on every backoff tick.
		if asyncEstablished && resultErr != nil && failureCount == 1 {
			logutil.Warn(
				"failed to asynchronously refresh query workload policy",
				zap.Uint32("account-id", accountID),
				zap.Error(resultErr),
			)
		}

		if enteredInvalid {
			logutil.Warn(
				"authoritative query workload policy is invalid",
				zap.Uint32("account-id", accountID),
				zap.Uint64("revision", revision),
				zap.String("reason", parsed.InvalidReason),
			)
		}

		// A transient read failure does not invalidate a previously established
		// snapshot. It remains usable while the bounded backoff drives a retry.
		if resultErr != nil && hadSnapshot {
			return nil
		}
		return resultErr
	}

	if asyncEstablished && refreshBase != nil {
		refreshCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			workloadPolicyRefreshTimeout,
		)
		go func() {
			defer cancel()
			_ = run(refreshCtx)
		}()
		return nil
	}
	return run(ctx)
}

func workloadPolicyRefreshBackoff(failures uint8) time.Duration {
	if failures == 0 {
		return minWorkloadPolicyRefreshBackoff
	}
	shift := failures - 1
	if shift > 5 {
		shift = 5
	}
	backoff := minWorkloadPolicyRefreshBackoff * time.Duration(uint64(1)<<shift)
	if backoff > maxWorkloadPolicyRefreshBackoff {
		return maxWorkloadPolicyRefreshBackoff
	}
	return backoff
}

func loadWorkloadPolicyFromCatalog(
	ctx context.Context,
	ses FeSession,
	accountID uint32,
) (string, uint64, error) {
	if ses == nil {
		return "", 0, moerr.NewInternalError(ctx, "session is nil")
	}
	tenantCtx := defines.AttachAccountId(ctx, accountID)
	bh := ses.GetBackgroundExec(tenantCtx)
	defer bh.Close()
	return loadWorkloadPolicyFromCatalogWithExec(tenantCtx, bh, accountID)
}

func loadWorkloadPolicyFromCatalogByService(
	ctx context.Context,
	service string,
	accountID uint32,
) (string, uint64, error) {
	ctx = withWorkloadPolicyBypass(ctx)
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return "", 0, moerr.NewInternalError(
			ctx,
			"workload policy runtime is not initialized",
		)
	}
	value, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return "", 0, moerr.NewInternalError(
			ctx,
			"workload policy internal SQL executor is not initialized",
		)
	}
	sqlExecutor, ok := value.(executor.SQLExecutor)
	if !ok || sqlExecutor == nil {
		return "", 0, moerr.NewInternalError(
			ctx,
			"workload policy internal SQL executor has invalid type",
		)
	}
	sql := fmt.Sprintf(
		"select policy, revision from %s.%s where account_id = %d",
		catalog.MO_CATALOG,
		catalog.MO_QUERY_WORKLOAD_POLICY,
		accountID,
	)
	result, err := sqlExecutor.Exec(
		ctx,
		sql,
		executor.Options{}.
			WithAccountID(accountID).
			WithDatabase(catalog.MO_CATALOG).
			WithStatementOption(
				executor.StatementOption{}.WithDisableLog(),
			),
	)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) ||
			moerr.IsMoErrCode(err, moerr.ErrBadDB) {
			return "", 0, nil
		}
		return "", 0, err
	}
	defer result.Close()

	var raw string
	var revision uint64
	found := false
	for _, rows := range result.Batches {
		if rows == nil || rows.RowCount() == 0 {
			continue
		}
		if found || rows.RowCount() != 1 || len(rows.Vecs) < 2 {
			return "", 0, moerr.NewInternalErrorf(
				ctx,
				"expected one workload policy row for account %d",
				accountID,
			)
		}
		raw = rows.Vecs[0].GetStringAt(0)
		revision = vector.GetFixedAtWithTypeCheck[uint64](rows.Vecs[1], 0)
		found = true
	}
	if !found {
		return "", 0, nil
	}
	if revision == 0 {
		return "", 0, moerr.NewInternalErrorf(
			ctx,
			"workload policy revision is zero for account %d",
			accountID,
		)
	}
	return raw, revision, nil
}

func loadWorkloadPolicyFromCatalogWithExec(
	ctx context.Context,
	bh BackgroundExec,
	accountID uint32,
) (string, uint64, error) {
	if bh == nil {
		return "", 0, moerr.NewInternalError(
			ctx,
			"workload policy background executor is nil",
		)
	}
	ctx = withWorkloadPolicyBypass(ctx)
	bh.ClearExecResultSet()
	sql := fmt.Sprintf(
		"select policy, revision from %s.%s where account_id = %d",
		catalog.MO_CATALOG,
		catalog.MO_QUERY_WORKLOAD_POLICY,
		accountID,
	)
	if err := bh.Exec(ctx, sql); err != nil {
		// The table is installed only after the cluster-version upgrade. Its
		// absence therefore means the feature is not active yet.
		if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) ||
			moerr.IsMoErrCode(err, moerr.ErrBadDB) {
			return "", 0, nil
		}
		return "", 0, err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil {
		return "", 0, err
	}
	if !execResultArrayHasData(results) {
		return "", 0, nil
	}
	if results[0].GetRowCount() != 1 {
		return "", 0, moerr.NewInternalErrorf(
			ctx,
			"expected one workload policy row for account %d, found %d",
			accountID,
			results[0].GetRowCount(),
		)
	}
	raw, err := results[0].GetString(ctx, 0, 0)
	if err != nil {
		return "", 0, err
	}
	revision, err := results[0].GetUint64(ctx, 0, 1)
	if err != nil {
		return "", 0, err
	}
	if revision == 0 {
		return "", 0, moerr.NewInternalErrorf(
			ctx,
			"workload policy revision is zero for account %d",
			accountID,
		)
	}
	return raw, revision, nil
}

func workloadPolicyState(ses FeSession) *accountWorkloadPolicy {
	if holder, ok := ses.(interface {
		getWorkloadPolicyState() *accountWorkloadPolicy
	}); ok {
		return holder.getWorkloadPolicyState()
	}
	return nil
}

func doAlterQueryWorkloadPolicy(
	ctx context.Context,
	ses *Session,
	stmt *tree.AlterAccountConfig,
) error {
	if !strings.EqualFold(stmt.ConfigName, queryWorkloadPolicy) {
		return moerr.NewInvalidInputf(
			ctx,
			"unsupported account configuration %q",
			stmt.ConfigName,
		)
	}
	if err := doCheckRole(ctx, ses); err != nil {
		return err
	}
	// Policy administration is its own control plane. Bypass the current
	// policy for account lookup, version gating, and the transactional upsert
	// so a broken internal-workload pool cannot prevent repair or RESET.
	controlCtx := withWorkloadPolicyBypass(ctx)

	raw := stmt.Value
	if stmt.Reset {
		raw = ""
	}
	if _, err := schedule.ParseWorkloadPolicyConfig(raw); err != nil {
		return err
	}

	targetID, err := resolveWorkloadPolicyAccount(controlCtx, ses, stmt.AccountName)
	if err != nil {
		return err
	}
	if err := ensureWorkloadPolicyFeatureReady(controlCtx, ses); err != nil {
		return err
	}
	tenant := ses.GetTenantInfo()
	operatorAccountID := tenant.GetTenantID()
	operatorUserID := tenant.GetUserID()
	targetCtx := defines.AttachAccountId(controlCtx, targetID)
	revision, err := upsertWorkloadPolicy(
		targetCtx,
		ses,
		targetID,
		raw,
		operatorAccountID,
		operatorUserID,
	)
	if err != nil {
		return err
	}

	if _, _, applyErr := GWorkloadPolicyManager.Apply(targetID, raw, revision); applyErr != nil {
		logutil.Warn(
			"failed to apply committed workload policy to the local cache",
			zap.Uint32("account-id", targetID),
			zap.Uint64("revision", revision),
			zap.Error(applyErr),
		)
	}

	publishCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		workloadPolicyPublishTimeout,
	)
	defer cancel()
	if err := publishWorkloadPolicy(publishCtx, ses, targetID, raw, revision); err != nil {
		// The transaction has committed. Notification is best-effort and the
		// periodic catalog reconciliation provides the durable repair path.
		logutil.Warn(
			"failed to publish committed workload policy",
			zap.Uint32("account-id", targetID),
			zap.Uint64("revision", revision),
			zap.Error(err),
		)
	}
	return nil
}

func ensureWorkloadPolicyFeatureReady(
	ctx context.Context,
	ses *Session,
) error {
	rt := moruntime.ServiceRuntime(ses.GetService())
	if rt == nil {
		return moerr.NewInternalError(
			ctx,
			"workload policy runtime is not initialized",
		)
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	protocolVersion, valid := value.(int64)
	if !ok || !valid {
		return moerr.NewInternalError(
			ctx,
			"workload policy protocol version is not initialized",
		)
	}
	// The table upgrade and RPC rollout are separate compatibility barriers.
	// Refuse the mutation before it commits when mixed-version CNs have kept
	// the negotiated protocol below the workload-policy RPC version.
	if protocolVersion < defines.MORPCVersion5 {
		return moerr.NewInternalErrorf(
			ctx,
			"workload policy requires protocol version %d or later; current protocol version is %d",
			defines.MORPCVersion5,
			protocolVersion,
		)
	}

	systemCtx := defines.AttachAccountId(ctx, sysAccountID)
	bh := ses.GetBackgroundExec(systemCtx)
	defer bh.Close()
	bh.ClearExecResultSet()
	if err := bh.Exec(
		systemCtx,
		"select version, version_offset from mo_catalog.mo_version "+
			"where state = 2 order by create_at desc limit 1",
	); err != nil {
		return err
	}
	results, err := getResultSet(systemCtx, bh)
	if err != nil {
		return err
	}
	if !execResultArrayHasData(results) {
		return moerr.NewInternalError(
			ctx,
			"workload policy requires a completed cluster version upgrade",
		)
	}
	readyVersion, err := results[0].GetString(systemCtx, 0, 0)
	if err != nil {
		return err
	}
	readyOffset, err := results[0].GetUint64(systemCtx, 0, 1)
	if err != nil {
		return err
	}
	versionComparison := versions.Compare(
		readyVersion,
		catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION,
	)
	// MatrixOne can add idempotent upgrade entries to the current release by
	// increasing version_offset. The version string alone would therefore
	// enable ALTER while the tenant-table upgrade is still pending.
	if versionComparison < 0 ||
		(versionComparison == 0 &&
			readyOffset < uint64(catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET)) {
		return moerr.NewInternalErrorf(
			ctx,
			"workload policy requires cluster version %s offset %d or later to be ready; latest ready version is %s offset %d",
			catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION,
			catalog.MO_QUERY_WORKLOAD_POLICY_MIN_VERSION_OFFSET,
			readyVersion,
			readyOffset,
		)
	}
	return nil
}

func resolveWorkloadPolicyAccount(
	ctx context.Context,
	ses *Session,
	accountName string,
) (uint32, error) {
	tenant := ses.GetTenantInfo()
	if accountName == "" || strings.EqualFold(accountName, tenant.GetTenant()) {
		return tenant.GetTenantID(), nil
	}
	if !tenant.IsMoAdminRole() {
		return 0, moerr.NewInternalError(
			ctx,
			"only the system administrator can configure another account",
		)
	}
	normalized, err := normalizeName(ctx, accountName)
	if err != nil {
		return 0, err
	}
	sql, err := getSqlForCheckTenant(ctx, normalized)
	if err != nil {
		return 0, err
	}
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return 0, err
	}
	results, err := getResultSet(ctx, bh)
	if err != nil {
		return 0, err
	}
	if !execResultArrayHasData(results) {
		return 0, moerr.NewInternalErrorf(
			ctx,
			"account %s does not exist",
			normalized,
		)
	}
	id, err := results[0].GetUint64(ctx, 0, 0)
	if err != nil {
		return 0, err
	}
	if id > uint64(^uint32(0)) {
		return 0, moerr.NewInternalErrorf(
			ctx,
			"account id %d is out of range",
			id,
		)
	}
	return uint32(id), nil
}

func upsertWorkloadPolicy(
	ctx context.Context,
	ses *Session,
	accountID uint32,
	raw string,
	operatorAccountID uint32,
	operatorUserID uint32,
) (revision uint64, err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return 0, err
	}
	finished := false
	defer func() {
		if !finished {
			err = finishTxn(ctx, bh, err)
		}
	}()

	encoded := hex.EncodeToString([]byte(raw))
	sql := fmt.Sprintf(
		"insert into %s.%s "+
			"(account_id, policy, revision, updated_by_account_id, updated_by_user_id) "+
			"values (%d, unhex('%s'), 1, %d, %d) "+
			"on duplicate key update "+
			"policy = values(policy), revision = revision + 1, "+
			"updated_by_account_id = values(updated_by_account_id), "+
			"updated_by_user_id = values(updated_by_user_id)",
		catalog.MO_CATALOG,
		catalog.MO_QUERY_WORKLOAD_POLICY,
		accountID,
		encoded,
		operatorAccountID,
		operatorUserID,
	)
	if err = bh.Exec(ctx, sql); err != nil {
		return 0, err
	}

	bh.ClearExecResultSet()
	sql = fmt.Sprintf(
		"select revision from %s.%s where account_id = %d",
		catalog.MO_CATALOG,
		catalog.MO_QUERY_WORKLOAD_POLICY,
		accountID,
	)
	if err = bh.Exec(ctx, sql); err != nil {
		return 0, err
	}
	results, resultErr := getResultSet(ctx, bh)
	if resultErr != nil {
		err = resultErr
		return 0, err
	}
	if !execResultArrayHasData(results) || results[0].GetRowCount() != 1 {
		err = moerr.NewInternalErrorf(
			ctx,
			"workload policy upsert did not return one row for account %d",
			accountID,
		)
		return 0, err
	}
	revision, err = results[0].GetUint64(ctx, 0, 0)
	if err != nil {
		return 0, err
	}
	if revision == 0 {
		err = moerr.NewInternalError(
			ctx,
			"workload policy upsert returned revision zero",
		)
		return 0, err
	}

	finished = true
	if err = finishTxn(ctx, bh, nil); err != nil {
		return 0, err
	}
	return revision, nil
}

func publishWorkloadPolicy(
	ctx context.Context,
	ses *Session,
	accountID uint32,
	raw string,
	revision uint64,
) error {
	qc := getPu(ses.GetService()).QueryClient
	if qc == nil {
		return moerr.NewInternalError(ctx, "query client is not initialized")
	}
	var nodes []string
	clusterservice.GetMOCluster(qc.ServiceID()).GetCNService(
		clusterservice.NewSelectAll(),
		func(service metadata.CNService) bool {
			nodes = append(nodes, service.QueryAddress)
			return true
		},
	)
	var responseErr error
	genRequest := func() *query.Request {
		request := qc.NewRequest(query.CmdMethod_WorkloadPolicyUpdate)
		request.WorkloadPolicyUpdateRequest = &query.WorkloadPolicyUpdateRequest{
			AccountID: accountID,
			Policy:    raw,
			Revision:  revision,
		}
		return request
	}
	handleValidResponse := func(node string, response *query.Response) {
		if err := validateWorkloadPolicyUpdateResponse(
			ctx,
			node,
			revision,
			response,
		); err != nil {
			responseErr = errors.Join(
				responseErr,
				err,
			)
		}
	}
	handleInvalidResponse := func(node string) {
		responseErr = errors.Join(
			responseErr,
			moerr.NewInternalErrorf(
				ctx,
				"failed to publish workload policy to CN %s",
				node,
			),
		)
	}
	requestErr := queryservice.RequestMultipleCn(
		ctx,
		nodes,
		qc,
		genRequest,
		handleValidResponse,
		handleInvalidResponse,
	)
	return errors.Join(requestErr, responseErr)
}

func validateWorkloadPolicyUpdateResponse(
	ctx context.Context,
	node string,
	requestedRevision uint64,
	response *query.Response,
) error {
	if response == nil || response.WorkloadPolicyUpdateResponse == nil {
		return moerr.NewInternalErrorf(
			ctx,
			"empty workload policy response from CN %s",
			node,
		)
	}
	update := response.WorkloadPolicyUpdateResponse
	if update.Applied && update.Revision != requestedRevision {
		return moerr.NewInternalErrorf(
			ctx,
			"CN %s acknowledged workload policy revision %d as %d",
			node,
			requestedRevision,
			update.Revision,
		)
	}
	if !update.Applied &&
		update.Revision != 0 &&
		update.Revision < requestedRevision {
		return moerr.NewInternalErrorf(
			ctx,
			"CN %s retained workload policy revision %d behind requested revision %d",
			node,
			update.Revision,
			requestedRevision,
		)
	}
	return nil
}
