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
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"

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

type statusServer struct {
	*http.Server
	sync.WaitGroup
}

var registry *prom.Registry
var moExporter metric.MetricExporter
var moCollector MetricCollector
var statsLogWriter *StatsLogWriter
var statusSvr *statusServer
var multiTable = false // need set before newMetricFSCollector and initTables

var inited uint32

func InitMetric(ctx context.Context, ieFactory func() ie.InternalExecutor, SV *config.ObservabilityParameters, nodeUUID, role string, opts ...InitOption) {
	// fix multi-init in standalone
	if !atomic.CompareAndSwapUint32(&inited, 0, 1) {
		return
	}
	var initOpts InitOptions
	opts = append(opts,
		withExportInterval(SV.MetricExportInterval),
		withUpdateInterval(SV.MetricStorageUsageUpdateInterval.Duration),
		withCheckNewInterval(SV.MetricStorageUsageCheckNewInterval.Duration),
		withMultiTable(SV.MetricMultiTable),
	)
	for _, opt := range opts {
		opt.ApplyTo(&initOpts)
	}
	// init global variables
	initConfigByParamaterUnit(SV)
	registry = prom.NewRegistry()
	if initOpts.writerFactory != nil {
		moCollector = newMetricFSCollector(initOpts.writerFactory, WithFlushInterval(initOpts.exportInterval), ExportMultiTable(initOpts.multiTable))
	} else {
		moCollector = newMetricCollector(ieFactory, WithFlushInterval(initOpts.exportInterval))
	}
	moExporter = newMetricExporter(registry, moCollector, nodeUUID, role)
	statsLogWriter = newStatsLogWriter(&stats.DefaultRegistry, runtime.ProcessLevelRuntime().Logger().Named("StatsLog"), metric.GetStatsGatherInterval())

	// register metrics and create tables
	registerAllMetrics()
	multiTable = initOpts.multiTable
	if initOpts.needInitTable {
		initTables(ctx, ieFactory, SV.BatchProcessor)
	}

	// start the data flow
	serviceCtx := context.Background()
	moCollector.Start(serviceCtx)
	moExporter.Start(serviceCtx)
	statsLogWriter.Start(serviceCtx)
	metric.SetMetricExporter(moExporter)

	if metric.GetExportToProm() {
		// http.HandleFunc("/query", makeDebugHandleFunc(ieFactory))
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(prom.DefaultGatherer, promhttp.HandlerOpts{}))
		addr := fmt.Sprintf("%s:%d", SV.Host, SV.StatusPort)
		statusSvr = &statusServer{Server: &http.Server{Addr: addr, Handler: mux}}
		statusSvr.Add(1)
		go func() {
			defer statusSvr.Done()
			if err := statusSvr.ListenAndServe(); err != http.ErrServerClosed {
				panic(fmt.Sprintf("status server error: %v", err))
			}
		}()
		logutil.Infof("[Metric] metrics scrape endpoint is ready at http://%s/metrics", addr)
	}

	metric.SetUpdateStorageUsageInterval(initOpts.updateInterval)
	metric.SetStorageUsageCheckNewInterval(initOpts.checkNewInterval)
	logutil.Infof("metric with ExportInterval: %v", initOpts.exportInterval)
	logutil.Infof("metric with UpdateStorageUsageInterval: %v", initOpts.updateInterval)
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
	if err := prom.Register(collector); err != nil {
		// err is either registering a collector more than once or metrics have duplicate description.
		// in any case, we respect the existing collectors in the prom registry
		logutil.Debugf("[Metric] register to prom register: %v", err)
	}
}

func mustRegister(collector metric.Collector) {
	registry.MustRegister(collector)
	if metric.GetExportToProm() {
		mustRegiterToProm(collector.CollectorToProm())
	} else {
		collector.CancelToProm()
	}
}

// register all defined collector here
func registerAllMetrics() {
	for _, c := range metric.InitCollectors {
		mustRegister(c)
	}
}

func initConfigByParamaterUnit(SV *config.ObservabilityParameters) {
	metric.SetExportToProm(SV.EnableMetricToProm)
	metric.SetGatherInterval(time.Second * time.Duration(SV.MetricGatherInterval))
}

func InitSchema(ctx context.Context, ieFactory func() ie.InternalExecutor) error {
	initTables(ctx, ieFactory, motrace.FileService)
	return nil
}

// initTables gathers all metrics and extract metadata to format create table sql
func initTables(ctx context.Context, ieFactory func() ie.InternalExecutor, batchProcessMode string) {
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
		close(descChan)
	}()

	if !multiTable {
		mustExec(SingleMetricTable.ToCreateSql(ctx, true))
		for desc := range descChan {
			view := getView(ctx, desc)
			sql := view.ToCreateSql(ctx, true)
			mustExec(sql)
		}
	} else {
		optFactory := table.GetOptionFactory(ctx, table.ExternalTableEngine)
		buf := new(bytes.Buffer)
		for desc := range descChan {
			sql := createTableSqlFromMetricFamily(desc, buf, optFactory)
			mustExec(sql)
		}
	}

	createCost = time.Since(instant)
}

type optionsFactory func(db, tbl, account string) table.TableOptions

// instead MetricFamily, Desc is used to create tables because we don't want collect errors come into the picture.
func createTableSqlFromMetricFamily(desc *prom.Desc, buf *bytes.Buffer, optionsFactory optionsFactory) string {
	buf.Reset()
	extra := newDescExtra(desc)
	opts := optionsFactory(MetricDBConst, extra.fqName, table.AccountAll)
	buf.WriteString("create ")
	buf.WriteString(opts.GetCreateOptions())
	buf.WriteString(fmt.Sprintf(
		"table if not exists %s.%s (`%s` datetime(6), `%s` double, `%s` varchar(36), `%s` varchar(20)",
		MetricDBConst, extra.fqName, metric.LblTimeConst, metric.LblValueConst, metric.LblNodeConst, metric.LblRoleConst,
	))
	for _, lbl := range extra.labels {
		buf.WriteString(", `")
		buf.WriteString(lbl.GetName())
		buf.WriteString("` varchar(20)")
	}
	buf.WriteRune(')')
	buf.WriteString(opts.GetTableOptions(nil))
	return buf.String()
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
	str = str[strings.Index(str, "variableLabels: [")+17:] // spot varlbl list
	str = str[:strings.Index(str, "]")]
	varLblCnt := len(strings.Split(str, " "))
	labels := prom.MakeLabelPairs(desc, make([]string, varLblCnt))
	return &descExtra{orig: desc, fqName: fqName, labels: labels}
}

type InitOptions struct {
	writerFactory table.WriterFactory // see WithWriterFactory
	// needInitTable control to do the initTables
	needInitTable bool // see WithInitAction
	// initSingleTable
	multiTable bool // see WithMultiTable
	// exportInterval
	exportInterval time.Duration // see withExportInterval
	// updateInterval, update StorageUsage interval
	// set by withUpdateInterval
	updateInterval time.Duration
	// checkNewAccountInterval, check new account Internal to collect new account for metric StorageUsage
	// set by withCheckNewInterval
	checkNewInterval time.Duration
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

func WithInitAction(init bool) InitOption {
	return InitOption(func(options *InitOptions) {
		options.needInitTable = init
	})
}

func withMultiTable(multi bool) InitOption {
	return InitOption(func(options *InitOptions) {
		options.multiTable = multi
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

var (
	metricNameColumn        = table.StringDefaultColumn(`metric_name`, `sys`, `metric name, like: sql_statement_total, server_connections, process_cpu_percent, sys_memory_used, ...`)
	metricCollectTimeColumn = table.DatetimeColumn(`collecttime`, `metric data collect time`)
	metricValueColumn       = table.ValueColumn(`value`, `metric value`)
	metricNodeColumn        = table.StringDefaultColumn(`node`, ALL_IN_ONE_MODE, `mo node uuid`)
	metricRoleColumn        = table.StringDefaultColumn(`role`, ALL_IN_ONE_MODE, `mo node role, like: CN, DN, LOG`)
	metricAccountColumn     = table.StringDefaultColumn(`account`, `sys`, `account name`)
	metricTypeColumn        = table.StringColumn(`type`, `sql type, like: insert, select, ...`)
)

var SingleMetricTable = &table.Table{
	Account:          table.AccountAll,
	Database:         MetricDBConst,
	Table:            `metric`,
	Columns:          []table.Column{metricNameColumn, metricCollectTimeColumn, metricValueColumn, metricNodeColumn, metricRoleColumn, metricAccountColumn, metricTypeColumn},
	PrimaryKeyColumn: []table.Column{},
	Engine:           table.ExternalTableEngine,
	Comment:          `metric data`,
	PathBuilder:      table.NewAccountDatePathBuilder(),
	AccountColumn:    &metricAccountColumn,
	// SupportUserAccess
	SupportUserAccess: true,
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
}
