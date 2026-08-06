// Copyright 2023 Matrix Origin
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

package compile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/idxcron"
)

var (
	iscpRegisterJobFunc     = iscp.RegisterJob
	iscpUnregisterJobFunc   = iscp.UnregisterJob
	iscpLookupJobLogFunc    = iscp.LookupJobLog
	iscpGetExecutorFunc     = iscp.GetExecutorRuntime
	iscpGetTaskRunnerFunc   = iscp.GetTaskRunner
	iscpGetCNQueryAddress   = getCNQueryAddress
	iscpDrainReadyTimeout   = 10 * time.Second
	iscpDrainRetryInterval  = 100 * time.Millisecond
	iscpFenceCleanupTimeout = 5 * time.Second
	isTableInCCPRFunc       = isTableInCCPRImpl
)

/* CDC APIs */
func RegisterJob(ctx context.Context, cnUUID string, txn client.TxnOperator, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
	//dummyurl := "mysql://root:111@127.0.0.1:6001"
	// sql = fmt.Sprintf("CREATE CDC `%s` '%s' 'indexsync' '%s' '%s.%s' {'Level'='table'};", cdcname, dummyurl, dummyurl, qryDatabase, srctbl)
	return iscpRegisterJobFunc(ctx, cnUUID, txn, spec, job, startFromNow)
}

func UnregisterJob(ctx context.Context, cnUUID string, txn client.TxnOperator, job *iscp.JobID) (bool, error) {
	return iscpUnregisterJobFunc(ctx, cnUUID, txn, job)
}

/* start here */
func CreateCdcTask(c *Compile, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
	logutil.Infof("Create Index Task %v", spec)

	return RegisterJob(c.proc.Ctx, c.proc.GetService(), c.proc.GetTxnOperator(), spec, job, startFromNow)
}

func DeleteCdcTask(c *Compile, job *iscp.JobID) (bool, error) {
	logutil.Infof("Delete Index Task %v", job)
	return UnregisterJob(c.proc.Ctx, c.proc.GetService(), c.proc.GetTxnOperator(), job)
}

func checkValidIndexCdcByIndexdef(idx *plan.IndexDef) (bool, error) {
	if !idx.TableExist {
		return false, nil
	}

	// Plugin-registered algorithms (vector + fulltext) describe their
	// CDC participation via SyncDescriptor().
	if p, ok := indexplugin.Get(idx.IndexAlgo); ok {
		if !p.Catalog().SyncDescriptor().UsesCDC {
			return false, nil
		}
		return indexplugin.IsAsync(idx.IndexAlgo, idx.IndexAlgoParams)
	}

	return false, nil
}

func checkValidIndexCdc(tableDef *plan.TableDef, indexname string) (bool, error) {
	for _, idx := range tableDef.Indexes {

		if idx.IndexName == indexname {
			valid, err := checkValidIndexCdcByIndexdef(idx)
			if err != nil {
				return false, err
			}
			if valid {
				return true, nil
			}
		}
	}
	return false, nil
}

// isTableInCCPR checks if a table is managed by CCPR (in mo_ccpr_tables)
// Returns true if the table is in CCPR system, false otherwise
func isTableInCCPR(c *Compile, tableid uint64) bool {
	return isTableInCCPRFunc(c, tableid)
}

func isTableInCCPRImpl(c *Compile, tableid uint64) bool {
	// Check mo_ccpr_tables by tableid
	querySql := fmt.Sprintf(
		"SELECT tableid FROM `%s`.`%s` WHERE tableid = %d",
		catalog.MO_CATALOG,
		catalog.MO_CCPR_TABLES,
		tableid,
	)

	res, err := c.runSqlWithResult(querySql, int32(catalog.System_Account))
	if err != nil {
		// If query fails, assume not in CCPR
		return false
	}
	defer res.Close()

	var found bool
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 {
			found = true
		}
		return false
	})

	return found
}

// NOTE: CreateIndexCdcTask will create CDC task without any checking.  Original TableDef may be empty
func CreateIndexCdcTask(c *Compile, dbname string, tablename string, tableid uint64, indexname string, sinker_type int8, startFromNow bool, sql string, tableDef *plan.TableDef) error {
	var err error

	// Skip ISCP task creation if table is from CCPR subscription (from_publication = true)
	if isTableFromPublication(tableDef) {
		logutil.Infof("skip creating index cdc task for CCPR subscribed table (%s, %s, %s)", dbname, tablename, indexname)
		return nil
	}

	// Skip ISCP task creation if table is managed by CCPR
	if isTableInCCPR(c, tableid) {
		logutil.Infof("skip creating index cdc task for CCPR table (%s, %s, %s)", dbname, tablename, indexname)
		return nil
	}

	spec := &iscp.JobSpec{
		ConsumerInfo: iscp.ConsumerInfo{ConsumerType: sinker_type,
			DBName:    dbname,
			TableName: tablename,
			IndexName: indexname,
			InitSQL:   sql},
	}
	job := &iscp.JobID{DBName: dbname, TableName: tablename, JobName: genCdcTaskJobID(indexname)}

	// create index cdc task
	ok, err := CreateCdcTask(c, spec, job, startFromNow)
	if err != nil {
		return err
	}

	if !ok {
		// cdc task already exist. ignore it.  IVFFLAT alter reindex will call CreateIndexCdcTask multiple times.
		logutil.Infof("index cdc task (%s, %s, %s) already exists", dbname, tablename, indexname)
		return nil
	}
	return nil
}

func genCdcTaskJobID(indexname string) string {
	return "index_" + indexname
}

func DropIndexCdcTask(c *Compile, tableDef *plan.TableDef, dbname string, tablename string, indexname string) error {
	var err error

	valid, err := checkValidIndexCdc(tableDef, indexname)
	if err != nil {
		return err
	}

	if !valid {
		// index name is not valid cdc task. ignore it
		return nil
	}

	// delete index cdc task
	_, err = DeleteCdcTask(c, &iscp.JobID{DBName: dbname, TableName: tablename, JobName: genCdcTaskJobID(indexname)})
	if err != nil {
		return err
	}

	return nil
}

func DrainIndexCdcTaskConsumer(c *Compile, tableDef *plan.TableDef, dbname string, tablename string, indexname string) error {
	return drainIndexCdcTaskConsumer(c, tableDef, dbname, tablename, indexname)
}

func drainIndexCdcTaskConsumer(
	c *Compile,
	tableDef *plan.TableDef,
	dbname string,
	tablename string,
	indexname string,
) error {
	valid, err := checkValidIndexCdc(tableDef, indexname)
	if err != nil {
		return err
	}
	if !valid {
		return nil
	}
	accountID, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return err
	}
	jobName := genCdcTaskJobID(indexname)
	_, tableID, jobID, exists, _, err := iscpLookupJobLogFunc(
		c.proc.Ctx,
		c.proc.GetService(),
		c.proc.GetTxnOperator(),
		&iscp.JobID{DBName: dbname, TableName: tablename, JobName: jobName},
	)
	if err != nil {
		return err
	}
	if !exists {
		logutil.Infof("skip draining index cdc task consumer, iscp job not found: tableID=%d index=%s", tableDef.TblId, indexname)
		return nil
	}
	if tableID == 0 {
		tableID = tableDef.TblId
	}
	key := iscp.NewJobRuntimeKey(accountID, tableID, jobName, jobID)
	logutil.Infof("drain index cdc task consumer: accountID=%d tableID=%d jobName=%s jobID=%d", accountID, tableID, jobName, jobID)
	readyCtx, cancel := context.WithTimeout(c.proc.Ctx, iscpDrainReadyTimeout)
	defer cancel()
	var runnerCN string
	fencedRunners := make(map[string]struct{})
	fencedTargets := newISCPDrainTargetSet()
	txnOp := c.proc.GetTxnOperator()
	var lease iscpJobFenceLease
	leaseStarted := false
	startLease := func() {
		if leaseStarted || txnOp == nil {
			return
		}
		leaseStarted = true
		lease = startISCPJobFenceLease(func(leaseCtx context.Context) error {
			ttl := iscp.RollbackFenceTTL()
			renewCtx, renewCancel := context.WithTimeoutCause(
				leaseCtx,
				iscpFenceRenewTimeout(ttl),
				moerr.NewInternalErrorNoCtx("iscp fence lease renew timeout"),
			)
			defer renewCancel()
			return renewISCPDrainTargetFences(
				renewCtx,
				fencedTargets.Snapshot(),
				accountID,
				tableID,
				jobName,
				jobID,
			)
		})
	}
	addFencedTarget := func(target iscpDrainTarget) {
		fencedTargets.Add(target)
		// Start renewal as soon as the first CN has installed a fence. Later
		// targets are incorporated through the synchronized snapshot.
		startLease()
	}
	notReadyErr := func() error {
		if c.proc.Ctx.Err() != nil {
			return c.proc.Ctx.Err()
		}
		target := runnerCN
		if target == "" {
			target = "<unknown>"
		}
		return moerr.NewInternalErrorf(
			c.proc.Ctx,
			"ISCP executor on task runner %s did not become ready within %s for tableID=%d jobName=%s jobID=%d",
			target,
			iscpDrainReadyTimeout,
			tableID,
			jobName,
			jobID,
		)
	}
	cleanupAfterFailure := func(cause error, uncertainTarget *iscpDrainTarget) error {
		lease.Stop()
		if uncertainTarget != nil {
			fencedTargets.Add(*uncertainTarget)
		}
		targets := fencedTargets.Snapshot()
		if len(targets) == 0 {
			return cause
		}
		cleanupCtx, cleanupCancel := context.WithTimeoutCause(
			context.Background(),
			iscpFenceCleanupTimeout,
			moerr.NewInternalErrorNoCtx("iscp failed-drain fence cleanup timeout"),
		)
		defer cleanupCancel()
		if cleanupErr := removeISCPDrainTargetFences(
			cleanupCtx,
			targets,
			accountID,
			tableID,
			jobName,
			jobID,
		); cleanupErr != nil {
			// The operation error is authoritative for frontend transaction
			// classification. Fence cleanup is best effort and the TTL remains
			// the fail-closed fallback.
			logutil.Warnf("failed to clean ISCP fences after drain failure: %v", cleanupErr)
		}
		return cause
	}
	for {
		if readyCtx.Err() != nil {
			return cleanupAfterFailure(notReadyErr(), nil)
		}
		// The daemon-task assignment is independent of the DDL transaction.
		// Use a fresh internal transaction on every attempt so a runner handoff
		// after the DDL snapshot can be observed.
		runnerCN, err = iscpGetTaskRunnerFunc(readyCtx, c.proc.GetService(), nil)
		if err != nil {
			if readyCtx.Err() != nil {
				return cleanupAfterFailure(notReadyErr(), nil)
			}
			return cleanupAfterFailure(err, nil)
		}
		if readyCtx.Err() != nil {
			return cleanupAfterFailure(notReadyErr(), nil)
		}
		if runnerCN == "" {
			runnerCN = c.proc.GetService()
		}
		// A successful drain is not the ownership linearization point: the daemon
		// task can move while the request is in flight. Converge only after a
		// fresh runner read observes an owner this DDL already fenced.
		if _, ok := fencedRunners[runnerCN]; ok {
			break
		}

		var target iscpDrainTarget
		_, _, forceRemote := fault.TriggerFault(objectio.FJ_ISCPCancelForceRemote)
		if localExec, ok := iscpGetExecutorFunc(runnerCN); !forceRemote && ok && localExec != nil {
			target = iscpDrainTarget{runnerCN: runnerCN, exec: localExec}
			err = localExec.CancelAndDrainJobConsumer(c.proc.Ctx, accountID, tableID, jobName, jobID)
			if err != nil {
				localExec.RemoveJobFence(key)
				return cleanupAfterFailure(err, nil)
			}
		} else {
			qc := c.proc.GetQueryClient()
			if qc == nil {
				return cleanupAfterFailure(
					moerr.NewInternalErrorf(
						c.proc.Ctx,
						"cannot confirm ISCP consumer quiescence on CN %s for tableID=%d jobName=%s jobID=%d",
						runnerCN,
						tableID,
						jobName,
						jobID,
					),
					nil,
				)
			}
			queryAddress, addressErr := iscpGetCNQueryAddress(readyCtx, c.proc.GetService(), runnerCN)
			if addressErr != nil {
				if readyCtx.Err() != nil {
					return cleanupAfterFailure(notReadyErr(), nil)
				}
				return cleanupAfterFailure(addressErr, nil)
			}
			if readyCtx.Err() != nil {
				return cleanupAfterFailure(notReadyErr(), nil)
			}
			target = iscpDrainTarget{
				runnerCN:     runnerCN,
				qc:           qc,
				queryAddress: queryAddress,
			}
			// Do not apply the readiness deadline to the actual drain. Once the
			// executor is ready, draining an active consumer is bounded by the
			// statement context and may legitimately take longer.
			err = sendISCPDrainConsumerRequest(c.proc.Ctx, qc, queryAddress, accountID, tableID, jobName, jobID, false, false)
			if err != nil {
				if !moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart) {
					// The handler may have fenced the consumer before its response
					// was lost. Treat this target as uncertain and remove its fence
					// while unwinding all earlier successful targets.
					return cleanupAfterFailure(err, &target)
				}
				// The handler installs the CN-scoped fence before returning the
				// retryable not-ready error. Track and renew that pending fence
				// while waiting for the executor generation to publish.
				addFencedTarget(target)
				if readyCtx.Err() != nil {
					return cleanupAfterFailure(notReadyErr(), nil)
				}
				timer := time.NewTimer(iscpDrainRetryInterval)
				select {
				case <-readyCtx.Done():
					timer.Stop()
					return cleanupAfterFailure(notReadyErr(), nil)
				case <-timer.C:
				}
				continue
			}
		}

		fencedRunners[runnerCN] = struct{}{}
		addFencedTarget(target)

		// Start a new bounded readiness generation after every successful drain.
		// The drain itself may legitimately outlive the previous readiness window.
		cancel()
		readyCtx, cancel = context.WithTimeout(c.proc.Ctx, iscpDrainReadyTimeout)
		defer cancel()
	}
	if txnOp != nil {
		cleanup := client.NewTxnEventCallback(func(_ context.Context, _ client.TxnOperator, event client.TxnEvent, _ any) error {
			lease.Stop()
			if !event.CostEvent {
				return nil
			}
			cleanupCtx, cancel := context.WithTimeoutCause(
				context.Background(),
				iscpFenceCleanupTimeout,
				moerr.NewInternalErrorNoCtx("iscp rollback fence cleanup timeout"),
			)
			defer cancel()
			if cleanupErr := removeISCPDrainTargetFences(
				cleanupCtx,
				fencedTargets.Snapshot(),
				accountID,
				tableID,
				jobName,
				jobID,
			); cleanupErr != nil {
				logutil.Warnf("failed to clean ISCP fences after rollback: %v", cleanupErr)
			}
			return nil
		})
		txnOp.AppendEventCallback(client.RollbackEvent, cleanup)
		txnOp.AppendEventCallback(client.CommitEvent, client.NewTxnEventCallback(func(_ context.Context, _ client.TxnOperator, event client.TxnEvent, _ any) error {
			if !event.CostEvent {
				lease.Stop()
			}
			return nil
		}))
	}
	return nil
}

type iscpDrainTarget struct {
	runnerCN     string
	exec         *iscp.ISCPTaskExecutor
	qc           qclient.QueryClient
	queryAddress string
}

type iscpDrainTargetSet struct {
	mu      sync.RWMutex
	order   []string
	targets map[string]iscpDrainTarget
}

func newISCPDrainTargetSet() *iscpDrainTargetSet {
	return &iscpDrainTargetSet{
		targets: make(map[string]iscpDrainTarget),
	}
}

func (s *iscpDrainTargetSet) Add(target iscpDrainTarget) {
	s.mu.Lock()
	if _, ok := s.targets[target.runnerCN]; !ok {
		s.order = append(s.order, target.runnerCN)
	}
	s.targets[target.runnerCN] = target
	s.mu.Unlock()
}

func (s *iscpDrainTargetSet) Snapshot() []iscpDrainTarget {
	s.mu.RLock()
	defer s.mu.RUnlock()
	targets := make([]iscpDrainTarget, 0, len(s.order))
	for _, runnerCN := range s.order {
		targets = append(targets, s.targets[runnerCN])
	}
	return targets
}

func renewISCPDrainTargetFences(
	ctx context.Context,
	targets []iscpDrainTarget,
	accountID uint32,
	tableID uint64,
	jobName string,
	jobID uint64,
) error {
	key := iscp.NewJobRuntimeKey(accountID, tableID, jobName, jobID)
	return runISCPDrainTargetOperations(targets, func(target iscpDrainTarget) error {
		var err error
		if target.exec != nil {
			if !target.exec.RenewJobFence(key, iscp.RollbackFenceTTL()) {
				err = moerr.NewInternalErrorf(
					ctx,
					"cannot renew ISCP consumer quiescence fence on CN %s for tableID=%d jobName=%s jobID=%d",
					target.runnerCN,
					tableID,
					jobName,
					jobID,
				)
			}
		} else {
			err = sendISCPDrainConsumerRequest(
				ctx,
				target.qc,
				target.queryAddress,
				accountID,
				tableID,
				jobName,
				jobID,
				false,
				true,
			)
		}
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "renew ISCP fence on runner %s: %v", target.runnerCN, err)
		}
		return nil
	})
}

func removeISCPDrainTargetFences(
	ctx context.Context,
	targets []iscpDrainTarget,
	accountID uint32,
	tableID uint64,
	jobName string,
	jobID uint64,
) error {
	key := iscp.NewJobRuntimeKey(accountID, tableID, jobName, jobID)
	return runISCPDrainTargetOperations(targets, func(target iscpDrainTarget) error {
		var err error
		if target.exec != nil {
			target.exec.RemoveJobFence(key)
		} else {
			err = sendISCPDrainConsumerRequest(
				ctx,
				target.qc,
				target.queryAddress,
				accountID,
				tableID,
				jobName,
				jobID,
				true,
				false,
			)
		}
		if err != nil {
			return moerr.NewInternalErrorf(ctx, "remove ISCP fence on runner %s: %v", target.runnerCN, err)
		}
		return nil
	})
}

func runISCPDrainTargetOperations(
	targets []iscpDrainTarget,
	operation func(iscpDrainTarget) error,
) error {
	// Every caller supplies one overall deadline. Start all targets before
	// waiting so an unreachable runner cannot consume another runner's entire
	// opportunity to renew or remove its fence.
	results := make([]error, len(targets))
	var wg sync.WaitGroup
	wg.Add(len(targets))
	for i := range targets {
		go func() {
			defer wg.Done()
			results[i] = operation(targets[i])
		}()
	}
	wg.Wait()
	return errors.Join(results...)
}

type iscpJobFenceLease struct {
	stop func()
}

func startISCPJobFenceLease(renew func(context.Context) error) iscpJobFenceLease {
	ttl := iscp.RollbackFenceTTL()
	if ttl <= 0 || renew == nil {
		return iscpJobFenceLease{stop: func() {}}
	}
	interval := ttl / 2
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	leaseCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if err := renew(leaseCtx); err != nil && leaseCtx.Err() == nil {
					logutil.Warnf("failed to renew ISCP job fence lease: %v", err)
				}
				timer.Reset(interval)
			case <-leaseCtx.Done():
				return
			}
		}
	}()
	return iscpJobFenceLease{stop: func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}}
}

func iscpFenceRenewTimeout(ttl time.Duration) time.Duration {
	timeout := ttl / 4
	if timeout < 10*time.Millisecond {
		timeout = 10 * time.Millisecond
	}
	return timeout
}

func (l iscpJobFenceLease) Stop() {
	if l.stop != nil {
		l.stop()
	}
}

func getCNQueryAddress(ctx context.Context, service string, cnUUID string) (string, error) {
	cluster, err := clusterservice.GetMOClusterWithContext(ctx, service)
	if err != nil {
		return "", err
	}
	var queryAddress string
	err = clusterservice.GetCNServiceWithoutWorkingStateWithContext(
		ctx,
		cluster,
		clusterservice.NewServiceIDSelector(cnUUID),
		func(cn metadata.CNService) bool {
			queryAddress = cn.QueryAddress
			return false
		},
	)
	if err != nil {
		return "", err
	}
	if queryAddress == "" {
		return "", moerr.NewInternalErrorf(ctx, "cannot find query address for CN %s", cnUUID)
	}
	return queryAddress, nil
}

func sendISCPDrainConsumerRequest(
	ctx context.Context,
	qc qclient.QueryClient,
	queryAddress string,
	accountID uint32,
	tableID uint64,
	jobName string,
	jobID uint64,
	removeFenceOnly bool,
	renewFenceOnly bool,
) error {
	req := qc.NewRequest(query.CmdMethod_ISCPDrainConsumer)
	req.ISCPDrainConsumerRequest = &query.ISCPDrainConsumerRequest{
		AccountID:       accountID,
		TableID:         tableID,
		JobName:         jobName,
		JobID:           jobID,
		RemoveFenceOnly: removeFenceOnly,
		RenewFenceOnly:  renewFenceOnly,
	}
	resp, err := qc.SendMessage(ctx, queryAddress, req)
	if err != nil {
		return err
	}
	if resp != nil {
		qc.Release(resp)
	}
	return nil
}

// drop all cdc tasks according to tableDef
func DropAllIndexCdcTasks(c *Compile, tabledef *plan.TableDef, dbname string, tablename string) error {
	idxmap := make(map[string]bool)
	for _, idx := range tabledef.Indexes {

		_, ok := idxmap[idx.IndexName]
		if ok {
			continue
		}

		valid, err := checkValidIndexCdcByIndexdef(idx)
		if err != nil {
			return err
		}

		if valid {
			idxmap[idx.IndexName] = true
			//hasindex = true
			_, e := DeleteCdcTask(c, &iscp.JobID{DBName: dbname, TableName: tablename, JobName: genCdcTaskJobID(idx.IndexName)})
			if e != nil {
				return e
			}
			if e = drainIndexCdcTaskConsumer(c, tabledef, dbname, tablename, idx.IndexName); e != nil {
				return e
			}
		}
	}
	return nil
}

func getSinkerTypeFromAlgo(algo string) int8 {
	if p, ok := indexplugin.Get(algo); ok {
		if d := p.Catalog().SyncDescriptor(); d.UsesCDC {
			return d.SinkerType
		}
	}
	panic("getSinkerTypeFromAlgo: invalid sinker type")
}

// NOTE: CreateAllIndexCdcTasks will create CDC task according to existing tableDef
func CreateAllIndexCdcTasks(c *Compile, indexes []*plan.IndexDef, dbname string, tablename string, tableid uint64, startFromNow bool, tableDef *plan.TableDef) error {
	idxmap := make(map[string]bool)
	for _, idx := range indexes {
		_, ok := idxmap[idx.IndexName]
		if ok {
			continue
		}

		valid, err := checkValidIndexCdcByIndexdef(idx)
		if err != nil {
			return err
		}

		if valid {
			idxmap[idx.IndexName] = true
			sinker_type := getSinkerTypeFromAlgo(idx.IndexAlgo)
			e := CreateIndexCdcTask(c, dbname, tablename, tableid, idx.IndexName, sinker_type, startFromNow, "", tableDef)
			if e != nil {
				return e
			}
		}
	}
	return nil
}

func checkValidIndexUpdateByIndexdef(idx *plan.IndexDef) (bool, error) {
	if !idx.TableExist {
		return false, nil
	}
	if p, ok := indexplugin.Get(idx.IndexAlgo); ok {
		return p.Catalog().SyncDescriptor().IdxcronAction != "", nil
	}
	return false, nil
}

// idxcron function
func CreateAllIndexUpdateTasks(c *Compile, indexes []*plan.IndexDef, dbname string, tablename string, tableid uint64) (err error) {
	// Background re-entry (idxcron's own ALTER REINDEX, ProcessInitSQL,
	// or any internal-SQL caller whose proc has IsFrontend=false) must
	// not re-register idxcron tasks here — IdxcronMetadata returns
	// (nil,nil) in background, the resulting string(metadata) is "",
	// and the REPLACE INTO mo_index_update would fail when its JSON
	// column rejects the empty literal. Mirror the alter.go /
	// ddl.go::AlterTableInplace IsFrontend gates (commit 2c8a55957).
	if !c.proc.Base.IsFrontend {
		return
	}

	idxmap := make(map[string]bool)
	// cctx is loop-invariant (depends only on c) — lazy-init so we
	// don't allocate when no index reaches the metadata fetch.
	var cctx *pluginCompileCtx
	for _, idx := range indexes {
		if _, ok := idxmap[idx.IndexName]; ok {
			continue
		}
		if len(idx.IndexName) == 0 {
			// alter reindex SQL doesn't support empty index names; skip.
			continue
		}

		p, ok := indexplugin.Get(idx.IndexAlgo)
		if !ok {
			continue
		}
		d := p.Catalog().SyncDescriptor()
		if d.IdxcronAction == "" {
			continue
		}
		if cctx == nil {
			cctx = newPluginCompileCtxForSync(c)
		}
		metadata, mErr := p.Compile().IdxcronMetadata(cctx)
		if mErr != nil {
			err = mErr
			return
		}
		// IsFrontend gate above covers the background re-entry case, but
		// BuildIdxcronMetadata can also return nil in frontend mode when
		// FrontendProbeVar resolves to nil (sub-Compile inheriting a
		// partial frontend resolver, e.g. CREATE TABLE CLONE). Passing
		// "" to RegisterUpdate would trip mo_index_update.metadata's
		// JSON NOT NULL — mirror the per-plugin registerIdxcronUpdate
		// guard and skip.
		if len(metadata) == 0 {
			continue
		}

		idxmap[idx.IndexName] = true
		err = idxcron.RegisterUpdate(c.proc.Ctx,
			c.proc.GetService(),
			c.proc.GetTxnOperator(),
			tableid,
			dbname,
			tablename,
			idx.IndexName,
			d.IdxcronAction,
			string(metadata))
		if err != nil {
			return
		}
	}
	return
}

// drop all cdc tasks according to tableDef
func DropAllIndexUpdateTasks(c *Compile, tabledef *plan.TableDef, dbname string, tablename string) (err error) {
	idxmap := make(map[string]bool)
	for _, idx := range tabledef.Indexes {
		if _, ok := idxmap[idx.IndexName]; ok {
			continue
		}

		p, ok := indexplugin.Get(idx.IndexAlgo)
		if !ok {
			continue
		}
		d := p.Catalog().SyncDescriptor()
		if d.IdxcronAction == "" {
			continue
		}
		action := d.IdxcronAction

		idxmap[idx.IndexName] = true
		err = idxcron.UnregisterUpdate(c.proc.Ctx,
			c.proc.GetService(),
			c.proc.GetTxnOperator(),
			tabledef.TblId,
			idx.IndexName,
			action)
		if err != nil {
			return
		}
	}
	return
}
