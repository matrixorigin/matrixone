// Copyright 2022 Matrix Origin
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

package mometric

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

const (
	MetricDBConst    = metric.MetricDBConst
	SqlCreateDBConst = "create database if not exists " + MetricDBConst
	SqlDropDBConst   = "drop database if exists " + MetricDBConst
	ALL_IN_ONE_MODE  = "monolithic"
)

type CtxServiceType string

const ServiceTypeKey CtxServiceType = "ServiceTypeKey"
const LaunchMode = "ALL"

type statusServer struct {
	*http.Server
	sync.WaitGroup
}

var registry *prom.Registry
var moExporter metric.MetricExporter
var moCollector MetricCollector
var statsLogWriter *StatsLogWriter
var statusSvr *statusServer

// internalRegistry is the registry for metric.InternalCollectors, cooperated with internalExporter.
var internalRegistry *prom.Registry
var internalExporter metric.MetricExporter

var enable bool
var inited uint32

func InitMetric(ctx context.Context, ieFactory func() ie.InternalExecutor, SV *config.ObservabilityParameters, nodeUUID, role string, opts ...InitOption) (act bool) {
	// fix multi-init in standalone
	if !atomic.CompareAndSwapUint32(&inited, 0, 1) {
		return false
	}
	var initOpts InitOptions
	opts = append(opts,
		withExportInterval(SV.MetricExportInterval),
		withUpdateInterval(SV.MetricStorageUsageUpdateInterval.Duration),
		withCheckNewInterval(SV.MetricStorageUsageCheckNewInterval.Duration),
		WithInternalGatherInterval(SV.MetricInternalGatherInterval.Duration),
	)
	for _, opt := range opts {
		opt.ApplyTo(&initOpts)
	}
	// init global variables
	initConfigByParameterUnit(SV)
	registry = prom.NewRegistry()
	if initOpts.writerFactory != nil {
		moCollector = newMetricFSCollector(initOpts.writerFactory, WithFlushInterval(initOpts.exportInterval))
	} else {
		moCollector = newMetricCollector(ieFactory, WithFlushInterval(initOpts.exportInterval))
	}
	moExporter = newMetricExporter(registry, moCollector, nodeUUID, role, WithGatherInterval(metric.GetGatherInterval()))
	internalRegistry = prom.NewRegistry()
	internalExporter = newMetricExporter(internalRegistry, moCollector, nodeUUID, role, WithGatherInterval(initOpts.internalGatherInterval))
	statsLogWriter = newStatsLogWriter(stats.DefaultRegistry, runtime.ProcessLevelRuntime().Logger().Named("StatsLog"), metric.GetStatsGatherInterval())

	// register metrics and create tables
	registerAllMetrics()
	if initOpts.needInitTable {
		initTables(ctx, ieFactory)
	}

	// start the data flow
	if !SV.DisableMetric {
		serviceCtx := context.WithValue(context.Background(), ServiceTypeKey, role)
		moCollector.Start(serviceCtx)
		moExporter.Start(serviceCtx)
		internalExporter.Start(serviceCtx)
		statsLogWriter.Start(serviceCtx)
		metric.SetMetricExporter(moExporter)
	}

	if metric.EnableExportToProm() {
		// http.HandleFunc("/query", makeDebugHandleFunc(ieFactory))
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(v2.GetPrometheusGatherer(), promhttp.HandlerOpts{}))
		addr := fmt.Sprintf(":%d", SV.StatusPort)
		statusSvr = &statusServer{Server: &http.Server{Addr: addr, Handler: mux}}
		statusSvr.Add(1)
		go func() {
			defer statusSvr.Done()
			if err := statusSvr.ListenAndServe(); err != http.ErrServerClosed {
				panic(fmt.Sprintf("status server error: %v", err))
			}
		}()

		startCrossServicesMetricsTask(ctx)

		logutil.Debugf("[Metric] metrics scrape endpoint is ready at http://%s/metrics", addr)
	}

	enable = true
	SetUpdateStorageUsageInterval(initOpts.updateInterval)
	SetStorageUsageCheckNewInterval(initOpts.checkNewInterval)
	logutil.Debugf("metric with ExportInterval: %v", initOpts.exportInterval)
	logutil.Debugf("metric with UpdateStorageUsageInterval: %v", initOpts.updateInterval)
	return true
}

// this cron task can gather some service level metrics,
func startCrossServicesMetricsTask(ctx context.Context) {
	go func() {
		logutil.Info("cross service metrics task started")
		defer logutil.Info("cross service metrics task exiting")

		timer := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				mpoolRelatedMetrics()
			}
		}
	}()
}

func mpoolRelatedMetrics() {
	v2.MemTotalCrossPoolFreeCounter.Add(float64(mpool.TotalCrossPoolFreeCounter()))

	v2.MemGlobalStatsAllocatedGauge.Set(float64(mpool.GlobalStats().NumCurrBytes.Load()))
	v2.MemGlobalStatsHighWaterMarkGauge.Set(float64(mpool.GlobalStats().HighWaterMark.Load()))
}

func IsEnable() bool {
	return enable
}

func StopMetricSync() {
	if !atomic.CompareAndSwapUint32(&inited, 1, 0) {
		return
	}
	if moCollector != nil {
		if ch, effect := moCollector.Stop(true); effect {
			<-ch
		}
		moCollector = nil
	}
	if moExporter != nil {
		if ch, effect := moExporter.Stop(true); effect {
			<-ch
		}
		moExporter = nil
	}
	if internalExporter != nil {
		if ch, effect := internalExporter.Stop(true); effect {
			<-ch
		}
		internalExporter = nil
	}
	if statsLogWriter != nil {
		if ch, effect := statsLogWriter.Stop(true); effect {
			<-ch
		}
		statsLogWriter = nil
	}
	if statusSvr != nil {
		_ = statusSvr.Shutdown(context.TODO())
		statusSvr = nil
	}
	logutil.Info("Shutdown metric complete.")
}

func mustRegiterToProm(collector prom.Collector) {
	if err := v2.GetPrometheusRegistry().Register(collector); err != nil {
		// err is either registering a collector more than once or metrics have duplicate description.
		// in any case, we respect the existing collectors in the prom registry
		logutil.Debugf("[Metric] register to prom register: %v", err)
	}
}

func mustRegister(reg *prom.Registry, collector metric.Collector) {
	reg.MustRegister(collector)
	if metric.EnableExportToProm() {
		mustRegiterToProm(collector.CollectorToProm())
	} else {
		collector.CancelToProm()
	}
}

// register all defined collector here
func registerAllMetrics() {
	for _, c := range metric.InitCollectors {
		mustRegister(registry, c)
	}
	for _, c := range metric.InternalCollectors {
		mustRegister(internalRegistry, c)
	}
}

func initConfigByParameterUnit(SV *config.ObservabilityParameters) {
	metric.SetExportToProm(SV.EnableMetricToProm)
	metric.SetGatherInterval(time.Second * time.Duration(SV.MetricGatherInterval))
}

func InitSchema(ctx context.Context, txn executor.TxnExecutor) error {
	if metric.GetForceInit() {
		if _, err := txn.Exec(SqlDropDBConst, executor.StatementOption{}); err != nil {
			return err
		}
	}

	if _, err := txn.Exec(SqlCreateDBConst, executor.StatementOption{}); err != nil {
		return err
	}

	var createCost time.Duration
	defer func() {
		logutil.Debugf("[Metric] init metrics tables: create cost %d ms", createCost.Milliseconds())
	}()

	instant := time.Now()
	descChan := make(chan *prom.Desc, 10)
	go func() {
		for _, c := range metric.InitCollectors {
			c.Describe(descChan)
		}
		for _, c := range metric.InternalCollectors {
			c.Describe(descChan)
		}
		close(descChan)
	}()

	createSql := SingleMetricTable.ToCreateSql(ctx, true)
	if _, err := txn.Exec(createSql, executor.StatementOption{}); err != nil {
		//panic(fmt.Sprintf("[Metric] init metric tables error: %v, sql: %s", err, sql))
		return moerr.NewInternalError(ctx, "[Metric] init metric tables error: %v, sql: %s", err, createSql)
	}

	createSql = SqlStatementCUTable.ToCreateSql(ctx, true)
	if _, err := txn.Exec(createSql, executor.StatementOption{}); err != nil {
		//panic(fmt.Sprintf("[Metric] init metric tables error: %v, sql: %s", err, sql))
		return moerr.NewInternalError(ctx, "[Metric] init metric tables error: %v, sql: %s", err, createSql)
	}

	for desc := range descChan {
		view := getView(ctx, desc)
		sql := view.ToCreateSql(ctx, true)
		if _, err := txn.Exec(sql, executor.StatementOption{}); err != nil {
			return moerr.NewInternalError(ctx, "[Metric] init metric tables error: %v, sql: %s", err, sql)
		}
	}
	createCost = time.Since(instant)
	return nil
}

// initTables gathers all metrics and extract metadata to format create table sql
func initTables(ctx context.Context, ieFactory func() ie.InternalExecutor) {
	exec := ieFactory()
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(MetricDBConst).Internal(true).Finish())
	mustExec := func(sql string) {
		if err := exec.Exec(ctx, sql, ie.NewOptsBuilder().Finish()); err != nil {
			panic(fmt.Sprintf("[Metric] init metric tables error: %v, sql: %s", err, sql))
		}
	}
	if metric.GetForceInit() {
		mustExec(SqlDropDBConst)
	}
	mustExec(SqlCreateDBConst)
	var createCost time.Duration
	defer func() {
		logutil.Debugf(
			"[Metric] init metrics tables: create cost %d ms",
			createCost.Milliseconds())
	}()
	instant := time.Now()

	descChan := make(chan *prom.Desc, 10)

	go func() {
		for _, c := range metric.InitCollectors {
			c.Describe(descChan)
		}
		for _, c := range metric.InternalCollectors {
			c.Describe(descChan)
		}
		close(descChan)
	}()

	mustExec(SingleMetricTable.ToCreateSql(ctx, true))
	mustExec(SqlStatementCUTable.ToCreateSql(ctx, true))
	for desc := range descChan {
		view := getView(ctx, desc)
		sql := view.ToCreateSql(ctx, true)
		mustExec(sql)
	}

	createCost = time.Since(instant)
}

func getView(ctx context.Context, desc *prom.Desc) *table.View {
	extra := newDescExtra(desc)
	var labelNames = make([]string, 0, len(extra.labels))
	for _, lbl := range extra.labels {
		labelNames = append(labelNames, lbl.GetName())
	}
	return GetMetricViewWithLabels(ctx, extra.fqName, labelNames)
}

type descExtra struct {
	orig   *prom.Desc
	fqName string
	labels []*dto.LabelPair
}

// decode inner infomation of a prom.Desc
func newDescExtra(desc *prom.Desc) *descExtra {
	str := desc.String()[14:] // strip Desc{fqName: "
	fqName := str[:strings.Index(str, "\"")]
	str = str[strings.Index(str, "variableLabels: {")+17:] // spot varlbl list
	str = str[:strings.Index(str, "}")]
	varLblCnt := len(strings.Split(str, ","))
	labels := prom.MakeLabelPairs(desc, make([]string, varLblCnt))
	return &descExtra{orig: desc, fqName: fqName, labels: labels}
}

type InitOptions struct {
	writerFactory table.WriterFactory // see WithWriterFactory
	// needInitTable control to do the initTables
	// Deprecated: use InitSchema instead.
	needInitTable bool // see WithInitAction
	// exportInterval
	exportInterval time.Duration // see withExportInterval
	// updateInterval, update StorageUsage interval
	// set by withUpdateInterval
	updateInterval time.Duration
	// checkNewAccountInterval, check new account Internal to collect new account for metric StorageUsage
	// set by withCheckNewInterval
	checkNewInterval time.Duration
	// internalGatherInterval, handle metric.SubSystemMO gather interval
	internalGatherInterval time.Duration
}

type InitOption func(*InitOptions)

func (f InitOption) ApplyTo(opts *InitOptions) {
	f(opts)
}

func WithWriterFactory(factory table.WriterFactory) InitOption {
	return InitOption(func(options *InitOptions) {
		options.writerFactory = factory
	})
}

// Deprecated: Use InitSchema instead.
func WithInitAction(init bool) InitOption {
	return InitOption(func(options *InitOptions) {
		options.needInitTable = init
	})
}

func withExportInterval(sec int) InitOption {
	return InitOption(func(options *InitOptions) {
		options.exportInterval = time.Second * time.Duration(sec)
	})
}

func withUpdateInterval(interval time.Duration) InitOption {
	return InitOption(func(opts *InitOptions) {
		opts.updateInterval = interval
	})
}

func withCheckNewInterval(interval time.Duration) InitOption {
	return InitOption(func(opts *InitOptions) {
		opts.checkNewInterval = interval
	})
}

func WithInternalGatherInterval(interval time.Duration) InitOption {
	return InitOption(func(options *InitOptions) {
		options.internalGatherInterval = interval
	})
}

var (
	metricNameColumn        = table.StringDefaultColumn(`metric_name`, `sys`, `metric name, like: sql_statement_total, server_connections, process_cpu_percent, sys_memory_used, ...`)
	metricCollectTimeColumn = table.DatetimeColumn(`collecttime`, `metric data collect time`)
	metricValueColumn       = table.ValueColumn(`value`, `metric value`)
	metricNodeColumn        = table.StringDefaultColumn(`node`, ALL_IN_ONE_MODE, `mo node uuid`)
	metricRoleColumn        = table.StringDefaultColumn(`role`, ALL_IN_ONE_MODE, `mo node role, like: CN, DN, LOG`)
	metricAccountColumn     = table.StringDefaultColumn(`account`, `sys`, `account name`)
	metricTypeColumn        = table.StringColumn(`type`, `sql type, like: insert, select, ...`)

	sqlSourceTypeColumn = table.StringColumn(`sql_source_type`, `sql_source_type, val like: external_sql, cloud_nonuser_sql, cloud_user_sql, internal_sql, ...`)
)

var SingleMetricTable = &table.Table{
	Account:          table.AccountSys,
	Database:         MetricDBConst,
	Table:            `metric`,
	Columns:          []table.Column{metricNameColumn, metricCollectTimeColumn, metricValueColumn, metricNodeColumn, metricRoleColumn, metricAccountColumn, metricTypeColumn},
	PrimaryKeyColumn: []table.Column{},
	ClusterBy:        []table.Column{metricCollectTimeColumn, metricNameColumn, metricAccountColumn},
	Engine:           table.NormalTableEngine,
	Comment:          `metric data`,
	PathBuilder:      table.NewAccountDatePathBuilder(),
	AccountColumn:    &metricAccountColumn,
	// TimestampColumn
	TimestampColumn: &metricCollectTimeColumn,
	// SupportUserAccess
	SupportUserAccess: true,
	// SupportConstAccess
	SupportConstAccess: true,
}

var SqlStatementCUTable = &table.Table{
	Account:          table.AccountSys,
	Database:         MetricDBConst,
	Table:            catalog.MO_SQL_STMT_CU,
	Columns:          []table.Column{metricAccountColumn, metricCollectTimeColumn, metricValueColumn, metricNodeColumn, metricRoleColumn, sqlSourceTypeColumn},
	PrimaryKeyColumn: []table.Column{},
	ClusterBy:        []table.Column{metricAccountColumn, metricCollectTimeColumn},
	Engine:           table.NormalTableEngine,
	Comment:          `sql_statement_cu metric data`,
	PathBuilder:      table.NewAccountDatePathBuilder(),
	AccountColumn:    &metricAccountColumn,
	// TimestampColumn
	TimestampColumn: &metricCollectTimeColumn,
	// SupportUserAccess
	SupportUserAccess: true,
	// SupportConstAccess
	SupportConstAccess: true,
}

// GetAllTables
//
// Deprecated: use table.GetAllTables() instead.
func GetAllTables() []*table.Table {
	return []*table.Table{SingleMetricTable, SqlStatementCUTable}
}

func NewMetricView(tbl string, opts ...table.ViewOption) *table.View {
	view := &table.View{
		Database:    MetricDBConst,
		Table:       tbl,
		OriginTable: SingleMetricTable,
		Columns:     []table.Column{metricCollectTimeColumn, metricValueColumn, metricNodeColumn, metricRoleColumn},
		Condition:   &table.ViewSingleCondition{Column: metricNameColumn, Table: tbl},
	}
	for _, opt := range opts {
		opt.Apply(view)
	}
	return view
}

func NewMetricViewWithLabels(ctx context.Context, tbl string, lbls []string) *table.View {
	var options []table.ViewOption
	// check SubSystem
	var subSystem *metric.SubSystem = nil
	for _, ss := range metric.AllSubSystem {
		if strings.Index(tbl, ss.Name) == 0 {
			subSystem = ss
			break
		}
	}
	if subSystem == nil {
		panic(moerr.NewNotSupported(ctx, "metric unknown SubSystem: %s", tbl))
	}
	options = append(options, table.SupportUserAccess(subSystem.SupportUserAccess))
	// construct columns
	for _, label := range lbls {
		for _, col := range SingleMetricTable.Columns {
			if strings.EqualFold(label, col.Name) {
				options = append(options, table.WithColumn(col))
			}
		}
	}
	return NewMetricView(tbl, options...)
}

var gView struct {
	content map[string]*table.View
	mu      sync.Mutex
}

func GetMetricViewWithLabels(ctx context.Context, tbl string, lbls []string) *table.View {
	gView.mu.Lock()
	defer gView.mu.Unlock()
	if len(gView.content) == 0 {
		gView.content = make(map[string]*table.View)
	}
	view, exist := gView.content[tbl]
	if !exist {
		view = NewMetricViewWithLabels(ctx, tbl, lbls)
		gView.content[tbl] = view
	}
	return view
}

// GetSchemaForAccount return account's table, and view's schema
func GetSchemaForAccount(ctx context.Context, account string) []string {
	var sqls = make([]string, 0, 1)
	tbl := SingleMetricTable.Clone()
	tbl.Account = account
	sqls = append(sqls, tbl.ToCreateSql(ctx, true))
	tbl = SqlStatementCUTable.Clone()
	tbl.Account = account
	sqls = append(sqls, tbl.ToCreateSql(ctx, true))

	descChan := make(chan *prom.Desc, 10)
	go func() {
		for _, c := range metric.InitCollectors {
			c.Describe(descChan)
		}
		close(descChan)
	}()

	for desc := range descChan {
		view := getView(ctx, desc)

		if view.SupportUserAccess && view.OriginTable.SupportUserAccess {
			sqls = append(sqls, view.ToCreateSql(ctx, true))
		}
	}
	return sqls
}

func init() {
	if table.RegisterTableDefine(SingleMetricTable) != nil {
		panic(moerr.NewInternalError(context.Background(), "metric table already registered"))
	}
	if table.RegisterTableDefine(SqlStatementCUTable) != nil {
		panic(moerr.NewInternalError(context.Background(), "metric table 'sql_statement_cu' already registered"))
	}
}
