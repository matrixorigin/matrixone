// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cdc

import (
	"context"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	defaultTableScanInterval      = 15 * time.Second
	DefaultSlowThreshold          = 10 * time.Minute
	DefaultPrintInterval          = 5 * time.Minute
	DefaultWatermarkCleanupPeriod = 5 * time.Minute
	DefaultCleanupWarnThreshold   = 5 * time.Second
)

type TableDetectorOptions struct {
	SlowThreshold        time.Duration
	PrintInterval        time.Duration
	CleanupPeriod        time.Duration
	CleanupWarnThreshold time.Duration
}

func normalizeTableDetectorOptions(opts *TableDetectorOptions) {
	if opts.SlowThreshold <= 0 {
		opts.SlowThreshold = DefaultSlowThreshold
	}
	if opts.PrintInterval <= 0 {
		opts.PrintInterval = DefaultPrintInterval
	}
	if opts.CleanupPeriod <= 0 {
		opts.CleanupPeriod = DefaultWatermarkCleanupPeriod
	}
	if opts.CleanupWarnThreshold <= 0 {
		opts.CleanupWarnThreshold = DefaultCleanupWarnThreshold
	}
}

var defaultTableDetectorOptions = TableDetectorOptions{
	SlowThreshold:        DefaultSlowThreshold,
	PrintInterval:        DefaultPrintInterval,
	CleanupPeriod:        DefaultWatermarkCleanupPeriod,
	CleanupWarnThreshold: DefaultCleanupWarnThreshold,
}

type TableDetectorOption func(*TableDetectorOptions)

func applyTableDetectorOptions(opts ...TableDetectorOption) TableDetectorOptions {
	options := defaultTableDetectorOptions
	for _, opt := range opts {
		opt(&options)
	}
	normalizeTableDetectorOptions(&options)
	return options
}

func WithTableDetectorSlowThreshold(d time.Duration) TableDetectorOption {
	return func(o *TableDetectorOptions) {
		o.SlowThreshold = d
	}
}

func WithTableDetectorPrintInterval(d time.Duration) TableDetectorOption {
	return func(o *TableDetectorOptions) {
		o.PrintInterval = d
	}
}

func WithTableDetectorCleanupPeriod(d time.Duration) TableDetectorOption {
	return func(o *TableDetectorOptions) {
		o.CleanupPeriod = d
	}
}

func WithTableDetectorCleanupWarnThreshold(d time.Duration) TableDetectorOption {
	return func(o *TableDetectorOptions) {
		o.CleanupWarnThreshold = d
	}
}

var cdcScanTableInjected = objectio.CDCScanTableInjected

func newTableDetector(exec executor.SQLExecutor, opts ...TableDetectorOption) *TableDetector {
	options := applyTableDetectorOptions(opts...)
	td := &TableDetector{
		Mp:                   make(map[uint32]TblMap),
		Callbacks:            make(map[string]TableCallback),
		exec:                 exec,
		CallBackAccountId:    make(map[string]uint32),
		SubscribedAccountIds: make(map[uint32][]string),
		CallBackDbName:       make(map[string][]string),
		SubscribedDbNames:    make(map[string][]string),
		CallBackTableName:    make(map[string][]string),
		SubscribedTableNames: make(map[string][]string),
		cdcStateManager:      NewCDCStateManager(),
		cleanupPeriod:        options.CleanupPeriod,
		cleanupWarn:          options.CleanupWarnThreshold,
		nowFn:                time.Now,
		scanInterval:         defaultTableScanInterval,
	}
	td.scanTableFn = td.scanTable
	return td
}

// InitTableDetector publishes discovery in the existing service runtime. It
// starts no workers until a task registers, and its CN owns shutdown.
func InitTableDetector(rt runtime.Runtime, exec executor.SQLExecutor) *TableDetector {
	d := newTableDetector(exec)
	rt.SetGlobalVariables(runtime.CDCTableDetector, d)
	return d
}

func TableDetectorFromRuntime(rt runtime.Runtime) *TableDetector {
	if rt == nil {
		return nil
	}
	v, _ := rt.GetGlobalVariables(runtime.CDCTableDetector)
	d, _ := v.(*TableDetector)
	return d
}

var GetTableDetector = func(cnUUID string) *TableDetector {
	d := TableDetectorFromRuntime(runtime.ServiceRuntime(cnUUID))
	if d == nil {
		panic(moerr.NewInvalidStateNoCtx("CDC detector is not initialized for CN " + cnUUID))
	}
	return d
}

// ResetTableDetectorForTest scopes a private scan cadence to the selected CN.
// Production restart constructs a new detector through the CN owner.
func ResetTableDetectorForTest(cnUUID string, scanInterval ...time.Duration) {
	rt := runtime.ServiceRuntime(cnUUID)
	if d := TableDetectorFromRuntime(rt); d != nil {
		ctx, cancel := context.WithTimeout(context.Background(), tableDetectorCloseTimeout)
		err := d.CloseAndWait(ctx)
		cancel()
		if err != nil {
			panic(err)
		}
	}
	v, _ := rt.GetGlobalVariables(runtime.InternalSQLExecutor)
	d := InitTableDetector(rt, v.(executor.SQLExecutor))
	if len(scanInterval) > 0 && scanInterval[0] > 0 {
		d.scanInterval = scanInterval[0]
	}
}

// TblMap key is dbName.tableName, e.g. db1.t1
type TblMap map[string]*DbTableInfo

type TableCallback func(map[uint32]TblMap) error

// cloneTableSnapshot copies only the subscribed account. Each callback gets
// independent descriptors while unrelated tenants add no fan-out cost.
func cloneTableSnapshot(src TblMap) TblMap {
	if src == nil {
		return nil
	}
	dst := make(TblMap, len(src))
	for key, info := range src {
		if info != nil {
			dst[key] = info.Clone()
		}
	}
	return dst
}

type TableIterationState struct {
	CreateAt time.Time
	EndAt    time.Time
	FromTs   types.TS
	ToTs     types.TS
}
type CDCStateManager struct {
	activeRunners map[string]*TableIterationState
	mu            sync.RWMutex
}

func NewCDCStateManager() *CDCStateManager {
	return &CDCStateManager{
		activeRunners: make(map[string]*TableIterationState),
		mu:            sync.RWMutex{},
	}
}

func (s *CDCStateManager) AddActiveRunner(tblInfo *DbTableInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := GenDbTblKey(tblInfo.SourceDbName, tblInfo.SourceTblName)
	s.activeRunners[key] = &TableIterationState{
		CreateAt: time.Now(),
		EndAt:    time.Now(),
		FromTs:   types.TS{},
		ToTs:     types.TS{},
	}
}

func (s *CDCStateManager) RemoveActiveRunner(tblInfo *DbTableInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := GenDbTblKey(tblInfo.SourceDbName, tblInfo.SourceTblName)
	delete(s.activeRunners, key)
}

func (s *CDCStateManager) UpdateActiveRunner(tblInfo *DbTableInfo, fromTs, toTs types.TS, start bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := GenDbTblKey(tblInfo.SourceDbName, tblInfo.SourceTblName)
	runner := s.activeRunners[key]
	if runner != nil {
		if start {
			runner.CreateAt = time.Now()
			runner.FromTs = fromTs
			runner.ToTs = toTs
			runner.EndAt = time.Time{}
		} else {
			runner.EndAt = time.Now()
		}
	}
}

func (s *CDCStateManager) PrintActiveRunners(slowThreshold time.Duration) {
	if s == nil {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	totalRunners := len(s.activeRunners)
	activeRunners := 0
	slowRunners := 0
	completedRunners := 0
	slowRunnersDetails := ""

	for info, state := range s.activeRunners {
		// Count active vs completed runners
		if state.EndAt.Equal(time.Time{}) {
			activeRunners++

			// Check if slow
			if state.CreateAt.Before(now.Add(-slowThreshold)) {
				slowRunners++
				duration := time.Since(state.CreateAt)
				slowRunnersDetails += fmt.Sprintf(
					"\n  %s: from=%s to=%s duration=%v",
					info,
					state.FromTs.ToString(),
					state.ToTs.ToString(),
					duration,
				)
			}
		} else {
			completedRunners++
		}
	}

	// Always log summary
	logutil.Info(
		"cdc.table_detector.state_summary",
		zap.Int("total-runners", totalRunners),
		zap.Int("active-runners", activeRunners),
		zap.Int("completed-runners", completedRunners),
		zap.Int("slow-runners", slowRunners),
		zap.Duration("slow-threshold", slowThreshold),
	)

	// Log slow runners details if any
	if slowRunners > 0 {
		logutil.Warn(
			"cdc.table_detector.slow_runners",
			zap.Int("count", slowRunners),
			zap.String("details", slowRunnersDetails),
		)
	}
}

type TableDetector struct {
	Mp        map[uint32]TblMap
	Callbacks map[string]TableCallback
	exec      executor.SQLExecutor
	cancel    context.CancelFunc

	CallBackAccountId    map[string]uint32
	SubscribedAccountIds map[uint32][]string

	// taskname -> [db1, db2 ...]
	CallBackDbName map[string][]string
	// dbname -> [taska, taskb ...]
	SubscribedDbNames map[string][]string

	// taskname -> [tbl1, tbl2 ...]
	CallBackTableName map[string][]string
	// tablename -> [taska, taskb ...]
	SubscribedTableNames map[string][]string

	scanTableFn  func(context.Context) error
	scanInterval time.Duration // Immutable after construction/test reset.

	// to make sure there is at most only one handleNewTables running, so the truncate info will not be lost
	handling        bool
	lastMp          map[uint32]TblMap
	mu              sync.Mutex
	cdcStateManager *CDCStateManager
	cleanupPeriod   time.Duration
	cleanupWarn     time.Duration
	nowFn           func() time.Time

	loopRunning atomic.Bool
	ctx         context.Context
	drained     chan struct{}
	active      int
	closed      bool
}

func (s *TableDetector) RegisterIfAbsent(id string, accountId uint32, dbs []string, tables []string, cb TableCallback) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	if _, exists := s.Callbacks[id]; exists {
		s.mu.Unlock()
		return false
	}
	startLoop := s.registerLocked(id, accountId, dbs, tables, cb)
	s.mu.Unlock()

	if startLoop {
		s.startScanLoop()
	}
	return true
}

func (s *TableDetector) registerLocked(id string, accountId uint32, dbs []string, tables []string, cb TableCallback) bool {
	startLoop := len(s.Callbacks) == 0

	s.SubscribedAccountIds[accountId] = append(s.SubscribedAccountIds[accountId], id)
	s.CallBackAccountId[id] = accountId

	for _, db := range dbs {
		s.SubscribedDbNames[db] = append(s.SubscribedDbNames[db], id)
	}
	s.CallBackDbName[id] = dbs

	for _, table := range tables {
		s.SubscribedTableNames[table] = append(s.SubscribedTableNames[table], id)
	}
	s.CallBackTableName[id] = tables

	s.Callbacks[id] = cb
	logutil.Debug(
		"cdc.table_detector.register",
		zap.String("task-id", id),
		zap.Uint32("account-id", accountId),
	)
	return startLoop
}

// IsTaskRegistered checks if a task is already registered
func (s *TableDetector) IsTaskRegistered(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.Callbacks[id]
	return exists
}

func (s *TableDetector) UnRegister(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if accountID, ok := s.CallBackAccountId[id]; ok {
		if tasks, ok := s.SubscribedAccountIds[accountID]; ok {
			s.SubscribedAccountIds[accountID] = slices.DeleteFunc(tasks, func(taskID string) bool {
				return taskID == id
			})
			if len(s.SubscribedAccountIds[accountID]) == 0 {
				delete(s.SubscribedAccountIds, accountID)
			}
		}
		delete(s.CallBackAccountId, id)
	}

	if dbs, ok := s.CallBackDbName[id]; ok {
		for _, db := range dbs {
			if tasks, ok := s.SubscribedDbNames[db]; ok {
				s.SubscribedDbNames[db] = slices.DeleteFunc(tasks, func(taskID string) bool {
					return taskID == id
				})
				if len(s.SubscribedDbNames[db]) == 0 {
					delete(s.SubscribedDbNames, db)
				}
			}
		}
		delete(s.CallBackDbName, id)
	}

	if tables, ok := s.CallBackTableName[id]; ok {
		for _, table := range tables {
			if tasks, ok := s.SubscribedTableNames[table]; ok {
				s.SubscribedTableNames[table] = slices.DeleteFunc(tasks, func(taskID string) bool {
					return taskID == id
				})
				if len(s.SubscribedTableNames[table]) == 0 {
					delete(s.SubscribedTableNames, table)
				}
			}
		}
		delete(s.CallBackTableName, id)
	}

	delete(s.Callbacks, id)

	logutil.Debug(
		"cdc.table_detector.unregister",
		zap.String("task-id", id),
	)
}

func (s *TableDetector) initLifetimeLocked() {
	if s.ctx == nil {
		s.ctx, s.cancel = context.WithCancel(defines.AttachAccountId(context.Background(), catalog.System_Account))
		s.drained = make(chan struct{})
	}
}

func (s *TableDetector) Context() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLifetimeLocked()
	return s.ctx
}

func (s *TableDetector) beginWork() (context.Context, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false
	}
	s.initLifetimeLocked()
	s.active++
	return s.ctx, true
}

func (s *TableDetector) endWork() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if s.closed && s.active == 0 {
		close(s.drained)
	}
}

func (s *TableDetector) startScanLoop() {
	ctx, ok := s.beginWork()
	if !ok {
		return
	}
	if !s.loopRunning.CompareAndSwap(false, true) {
		s.endWork()
		return
	}
	go func() {
		defer s.endWork()
		defer s.loopRunning.Store(false)
		s.scanTableLoop(ctx)
	}()
}

func (s *TableDetector) scanTableLoop(ctx context.Context) {
	logutil.Debug("cdc.table_detector.scan_start")
	defer logutil.Debug("cdc.table_detector.scan_end")

	var tickerDuration, retryTickerDuration time.Duration
	if msg, injected := cdcScanTableInjected(); injected || msg == "fast scan" {
		tickerDuration = 1 * time.Millisecond
		retryTickerDuration = 1 * time.Millisecond
	} else {
		tickerDuration = s.scanInterval
		if tickerDuration <= 0 {
			tickerDuration = defaultTableScanInterval
		}
		retryTickerDuration = 5 * time.Second
	}
	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()

	retryTicker := time.NewTicker(retryTickerDuration)
	defer retryTicker.Stop()

	printInterval := DefaultPrintInterval
	if printInterval <= 0 {
		logutil.Warn(
			"cdc.table_detector.print_interval_invalid",
			zap.Duration("interval", printInterval),
			zap.Duration("fallback", DefaultPrintInterval),
		)
		printInterval = DefaultPrintInterval
	}
	printStateTicker := time.NewTicker(printInterval)
	defer printStateTicker.Stop()

	cleanupPeriod := s.cleanupPeriod
	if cleanupPeriod <= 0 {
		logutil.Warn(
			"cdc.table_detector.cleanup_period_invalid",
			zap.Duration("period", cleanupPeriod),
			zap.Duration("fallback", DefaultWatermarkCleanupPeriod),
		)
		cleanupPeriod = DefaultWatermarkCleanupPeriod
	}
	cleanupTicker := time.NewTicker(cleanupPeriod)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()

			if s.handling {
				s.mu.Unlock()
				continue
			}

			s.mu.Unlock()

			s.scanAndProcess(ctx)
		case <-retryTicker.C:
			s.mu.Lock()
			handling, lastMp := s.handling, s.lastMp
			s.mu.Unlock()
			if handling || lastMp == nil {
				continue
			}

			s.startCallback(ctx, lastMp)
		case <-printStateTicker.C:
			s.cdcStateManager.PrintActiveRunners(DefaultSlowThreshold)
		case <-cleanupTicker.C:
			s.cleanupOrphanWatermarks(ctx)
		}
	}
}

func (s *TableDetector) scanAndProcess(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	// Defensive: ensure scanTableFn is set (for testing scenarios)
	if s.scanTableFn == nil {
		logutil.Warn("cdc.table_detector.scan_skipped", zap.String("reason", "scanTableFn is nil"))
		return
	}

	if err := s.scanTableFn(ctx); err != nil {
		logutil.Error("cdc.table_detector.scan_error", zap.Error(err))
		return
	}

	s.mu.Lock()
	s.lastMp = s.Mp
	mp := s.lastMp
	s.mu.Unlock()

	s.startCallback(ctx, mp)
}

// RunTableDetectorScanForTest performs one real catalog scan and callback
// synchronously.  SQL integration tests use this only to remove the polling
// interval from their admission ordering; the scan, table matching, and CDC
// pipeline callback remain the production implementations.
func RunTableDetectorScanForTest(cnUUID string) error {
	detector := GetTableDetector(cnUUID)
	ctx, ok := detector.beginWork()
	if !ok {
		return moerr.NewClientClosedNoCtx()
	}
	defer detector.endWork()
	if err := detector.scanTable(ctx); err != nil {
		return err
	}
	detector.mu.Lock()
	tables := detector.Mp
	callbackCount := len(detector.Callbacks)
	subscriptionCount := len(detector.SubscribedDbNames)
	detector.mu.Unlock()
	if len(tables) == 0 {
		return moerr.NewInternalErrorNoCtxf(
			"CDC test detector scan found no tables (callbacks=%d subscribed-databases=%d)",
			callbackCount, subscriptionCount,
		)
	}
	detector.processCallback(ctx, tables)
	return ctx.Err()
}

// Claim callback work before launching it, using the existing serialization
// flag. Retry ticks cannot accumulate callbacks and Close can join every owner.
func (s *TableDetector) admitCallback() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.handling {
		return false
	}
	s.initLifetimeLocked()
	s.active++
	s.handling = true
	return true
}

func (s *TableDetector) startCallback(ctx context.Context, tables map[uint32]TblMap) {
	if s.admitCallback() {
		go s.processAdmittedCallback(ctx, tables)
	}
}

func (s *TableDetector) processCallback(ctx context.Context, tables map[uint32]TblMap) {
	if s.admitCallback() {
		s.processAdmittedCallback(ctx, tables)
	}
}

func (s *TableDetector) processAdmittedCallback(ctx context.Context, tables map[uint32]TblMap) {
	defer s.endWork()
	s.mu.Lock()
	type subscriber struct {
		callback TableCallback
		account  uint32
	}
	callbacks := make([]subscriber, 0, len(s.Callbacks))
	tablesSnapshot := make(map[uint32]TblMap, len(s.SubscribedAccountIds))
	for id, cb := range s.Callbacks {
		account := s.CallBackAccountId[id]
		if _, copied := tablesSnapshot[account]; !copied {
			tablesSnapshot[account] = cloneTableSnapshot(tables[account])
		}
		callbacks = append(callbacks, subscriber{cb, account})
	}
	s.mu.Unlock()

	var err error
	var panicVal any
	defer func() {
		if r := recover(); r != nil {
			panicVal = r
			err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("callback panic: %v", r))
			logutil.Error(
				"cdc.table_detector.callback_panic",
				zap.Any("panic", r),
				zap.ByteString("stack", debug.Stack()),
			)
		}

		s.mu.Lock()
		if err != nil {
			logutil.Warn("cdc.table_detector.callback_failed", zap.Error(err))
		} else {
			logutil.Debug("cdc.table_detector.callback_success")
			s.lastMp = nil
		}
		s.handling = false
		s.mu.Unlock()

		if panicVal != nil {
			panic(panicVal)
		}
	}()

	for _, subscriber := range callbacks {
		if ctx.Err() != nil || s.Context().Err() != nil {
			return
		}
		view := map[uint32]TblMap{
			subscriber.account: cloneTableSnapshot(tablesSnapshot[subscriber.account]),
		}
		if cbErr := subscriber.callback(view); cbErr != nil {
			err = cbErr
		}
	}
}

func (s *TableDetector) cleanupOrphanWatermarks(ctx context.Context) {
	if s.exec == nil {
		logutil.Debug("cdc.table_detector.cleanup_watermark_skip", zap.String("reason", "executor nil"))
		return
	}

	for _, sql := range []string{
		CDCSQLBuilder.DeleteOrphanWatermarkSQL(),
		CDCSQLBuilder.DeleteOrphanSnapshotEpochSQL(),
	} {
		// Give each independent catalog cleanup its own budget. A slow watermark
		// cleanup must not deterministically starve snapshot-epoch cleanup on
		// every pass.
		// Preserve the historical ten-second worst-case scan-loop delay across
		// the two maintenance statements.
		cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		s.cleanupOrphanMetadata(cleanupCtx, sql)
		cancel()
	}
}

func (s *TableDetector) cleanupOrphanMetadata(cleanupCtx context.Context, sql string) {
	logutil.Debug(
		"cdc.table_detector.cleanup_watermark_execute",
		zap.String("sql", sql),
	)
	start := s.now()
	res, err := s.exec.Exec(
		cleanupCtx,
		sql,
		executor.Options{}.
			WithStatementOption(
				executor.StatementOption{}.
					WithAccountID(catalog.System_Account).
					WithDisableLog(),
			),
	)
	if err != nil {
		logutil.Error(
			"cdc.table_detector.cleanup_watermark_failed",
			zap.Error(err),
			zap.Duration("duration", s.now().Sub(start)),
		)
		return
	}

	if res.AffectedRows > 0 {
		duration := s.now().Sub(start)
		fields := []zap.Field{
			zap.Uint64("deleted-rows", res.AffectedRows),
			zap.Duration("duration", duration),
			zap.String("sql", commonutil.Abbreviate(sql, 500)),
		}
		if duration > s.cleanupWarn {
			logutil.Warn(
				"cdc.table_detector.cleanup_watermark_slow",
				fields...,
			)
		} else {
			logutil.Debug(
				"cdc.table_detector.cleanup_watermark_done",
				fields...,
			)
		}
	}
}

func (s *TableDetector) now() time.Time {
	if s.nowFn != nil {
		return s.nowFn()
	}
	return time.Now()
}

const tableDetectorCloseTimeout = 30 * time.Second

// Stop seals discovery before its task owner cancels and drains runner work.
func (s *TableDetector) Stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.initLifetimeLocked()
	s.closed = true
	cancel := s.cancel
	s.cancel = nil
	if s.active == 0 {
		close(s.drained)
	}
	cancel()
	s.mu.Unlock()
}

func (s *TableDetector) CloseAndWait(ctx context.Context) error {
	s.Stop()
	s.mu.Lock()
	done := s.drained
	s.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *TableDetector) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), tableDetectorCloseTimeout)
	defer cancel()
	if err := s.CloseAndWait(ctx); err != nil {
		logutil.Error("cdc.table_detector.close_incomplete", zap.Error(err))
	}
}

func (s *TableDetector) scanTable(parent context.Context) error {
	ctx, cancel := context.WithTimeoutCause(parent, 10*time.Second, moerr.CauseTableDetectorScan)
	defer cancel()
	var (
		accountIds string
		dbNames    string
		tableNames string
		mp         = make(map[uint32]TblMap)
	)

	s.mu.Lock()

	if len(s.SubscribedAccountIds) == 0 || len(s.SubscribedDbNames) == 0 || len(s.SubscribedTableNames) == 0 {
		s.Mp = mp
		s.mu.Unlock()
		return nil
	}
	var i int
	for accountId := range s.SubscribedAccountIds {
		if i != 0 {
			accountIds += ","
		}
		accountIds += fmt.Sprintf("%d", accountId)
		i++
	}
	dbNamesSlice := make([]string, 0, len(s.SubscribedDbNames))
	for dbName := range s.SubscribedDbNames {
		if dbName == "*" {
			dbNames = "*"
			break
		}
		if CDCSourceNameNeedsCatalogSuperset(dbName, 2) {
			dbNames = "*"
			break
		}
		// The detector serves tasks with different persisted case modes. Scan a
		// case-insensitive candidate superset; each task applies its own mode in
		// matchAnyPattern before it can create a pipeline.
		dbNamesSlice = append(dbNamesSlice, CDCSourceIdentifierKey(dbName, 2))
	}
	if dbNames != "*" {
		dbNames = AddSingleQuotesJoin(dbNamesSlice)
	}
	tableNamesSlice := make([]string, 0, len(s.SubscribedTableNames))
	for tableName := range s.SubscribedTableNames {
		if tableName == "*" {
			tableNames = "*"
			break
		}
		if CDCSourceNameNeedsCatalogSuperset(tableName, 2) {
			tableNames = "*"
			break
		}
		tableNamesSlice = append(tableNamesSlice, CDCSourceIdentifierKey(tableName, 2))
	}
	if tableNames != "*" {
		tableNames = AddSingleQuotesJoin(tableNamesSlice)
	}
	s.mu.Unlock()

	result, err := s.exec.Exec(
		ctx,
		CDCSQLBuilder.CollectTableInfoSQLCaseInsensitive(accountIds, dbNames, tableNames),
		executor.Options{}.WithStatementOption(executor.StatementOption{}.WithDisableLog()),
	)
	if err != nil {
		return err
	}
	defer result.Close()

	var scanErr error
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			tblId := vector.MustFixedColWithTypeCheck[uint64](cols[0])[i]
			tblName := cols[1].GetStringAt(i)
			dbId := vector.MustFixedColWithTypeCheck[uint64](cols[2])[i]
			dbName := cols[3].GetStringAt(i)
			createSql := cols[4].GetStringAt(i)
			accountId := vector.MustFixedColWithTypeCheck[uint32](cols[5])[i]
			hasForeignKey, decodeErr := TableHasForeignKeyConstraint(cols[6].GetBytesAt(i))
			if decodeErr != nil {
				scanErr = decodeErr
				logutil.Warn(
					"cdc.table_detector.scan_constraint_failed",
					zap.Uint32("account-id", accountId),
					zap.String("db", dbName),
					zap.String("table", tblName),
					zap.Error(decodeErr),
				)
				return false
			}

			// skip table with foreign key
			if hasForeignKey {
				continue
			}

			if _, ok := mp[accountId]; !ok {
				mp[accountId] = make(TblMap)
			}

			key := GenDbTblKey(dbName, tblName)

			// The callback may clear one-shot metadata while a scan is building
			// its next snapshot. Protect the shared map lookup and clone with the
			// same mutex used by publication/cleanup.
			mp[accountId][key] = &DbTableInfo{
				SourceDbId:        dbId,
				SourceDbName:      dbName,
				SourceTblId:       tblId,
				SourceTblName:     tblName,
				SourceCreateSql:   createSql,
				HasUserPrimaryKey: vector.MustFixedColNoTypeCheck[bool](cols[7])[i],
			}

		}
		return true
	})
	if scanErr != nil {
		return scanErr
	}

	s.mu.Lock()
	s.Mp = mp
	s.mu.Unlock()
	return nil
}

// TableHasForeignKeyConstraint decodes the persisted constraint metadata used
// by the runtime table scanner. CREATE CDC admission uses the same check so it
// does not reject foreign-key children that the scanner will never consume.
func TableHasForeignKeyConstraint(data []byte) (hasForeignKey bool, err error) {
	if len(data) == 0 {
		return false, nil
	}

	defer func() {
		if r := recover(); r != nil {
			err = moerr.NewInternalErrorNoCtxf("unmarshal table constraint failed: %v", r)
		}
	}()

	constraintDef := &engine.ConstraintDef{}
	if err := constraintDef.UnmarshalBinary(data); err != nil {
		return false, err
	}
	for _, constraint := range constraintDef.Cts {
		if foreignKeyDef, ok := constraint.(*engine.ForeignKeyDef); ok && len(foreignKeyDef.Fkeys) > 0 {
			return true, nil
		}
	}
	return false, nil
}
