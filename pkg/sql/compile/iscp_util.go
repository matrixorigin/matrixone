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
	iscpRegisterJobFunc    = iscp.RegisterJob
	iscpUnregisterJobFunc  = iscp.UnregisterJob
	iscpLookupJobLogFunc   = iscp.LookupJobLog
	iscpGetExecutorFunc    = iscp.GetExecutorRuntime
	iscpGetTaskRunnerFunc  = iscp.GetTaskRunner
	iscpGetCNQueryAddress  = getCNQueryAddress
	iscpDrainReadyTimeout  = 10 * time.Second
	iscpDrainRetryInterval = 100 * time.Millisecond
	isTableInCCPRFunc      = isTableInCCPRImpl
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
		d := p.Catalog().SyncDescriptor()
		if !d.UsesCDC {
			return false, nil
		}
		if d.AlwaysAsync {
			return true, nil
		}
		return catalog.IsIndexAsync(idx.IndexAlgoParams)
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
	fencedTargets := make([]iscpDrainTarget, 0, 1)
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
		targets := fencedTargets
		if uncertainTarget != nil {
			targets = append(append([]iscpDrainTarget(nil), fencedTargets...), *uncertainTarget)
		}
		if len(targets) == 0 {
			return cause
		}
		cleanupCtx, cleanupCancel := context.WithTimeoutCause(
			context.Background(),
			iscpDrainReadyTimeout,
			moerr.NewInternalErrorNoCtx("iscp failed-drain fence cleanup timeout"),
		)
		defer cleanupCancel()
		return errors.Join(
			cause,
			removeISCPDrainTargetFences(cleanupCtx, targets, accountID, tableID, jobName, jobID),
		)
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
		fencedTargets = append(fencedTargets, target)

		// Start a new bounded readiness generation after every successful drain.
		// The drain itself may legitimately outlive the previous readiness window.
		cancel()
		readyCtx, cancel = context.WithTimeout(c.proc.Ctx, iscpDrainReadyTimeout)
		defer cancel()
	}
	if txnOp := c.proc.GetTxnOperator(); txnOp != nil {
		lease := startISCPJobFenceLease(func() error {
			ttl := iscp.RollbackFenceTTL()
			renewCtx, cancel := context.WithTimeoutCause(
				context.Background(),
				iscpFenceRenewTimeout(ttl),
				moerr.NewInternalErrorNoCtx("iscp fence lease renew timeout"),
			)
			defer cancel()
			return renewISCPDrainTargetFences(renewCtx, fencedTargets, accountID, tableID, jobName, jobID)
		})
		cleanup := client.NewTxnEventCallback(func(_ context.Context, _ client.TxnOperator, event client.TxnEvent, _ any) error {
			lease.Stop()
			if !event.CostEvent {
				return nil
			}
			cleanupCtx, cancel := context.WithTimeoutCause(
				context.Background(),
				iscp.DefaultRollbackFenceTTL,
				moerr.NewInternalErrorNoCtx("iscp rollback fence cleanup timeout"),
			)
			defer cancel()
			return removeISCPDrainTargetFences(cleanupCtx, fencedTargets, accountID, tableID, jobName, jobID)
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

func renewISCPDrainTargetFences(
	ctx context.Context,
	targets []iscpDrainTarget,
	accountID uint32,
	tableID uint64,
	jobName string,
	jobID uint64,
) error {
	key := iscp.NewJobRuntimeKey(accountID, tableID, jobName, jobID)
	var result error
	for _, target := range targets {
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
			result = errors.Join(result, fmt.Errorf("renew ISCP fence on runner %s: %w", target.runnerCN, err))
		}
	}
	return result
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
	var result error
	for _, target := range targets {
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
			result = errors.Join(result, fmt.Errorf("remove ISCP fence on runner %s: %w", target.runnerCN, err))
		}
	}
	return result
}

type iscpJobFenceLease struct {
	stop func()
}

func startISCPJobFenceLease(renew func() error) iscpJobFenceLease {
	ttl := iscp.RollbackFenceTTL()
	if ttl <= 0 || renew == nil {
		return iscpJobFenceLease{stop: func() {}}
	}
	interval := ttl / 2
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	stopCh := make(chan struct{})
	var once sync.Once
	go func() {
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if err := renew(); err != nil {
					logutil.Warnf("failed to renew ISCP job fence lease: %v", err)
				}
				timer.Reset(interval)
			case <-stopCh:
				return
			}
		}
	}()
	return iscpJobFenceLease{stop: func() {
		once.Do(func() {
			close(stopCh)
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
