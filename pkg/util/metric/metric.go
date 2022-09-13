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

package metric

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

const (
	MetricDBConst    = "system_metrics"
	sqlCreateDBConst = "create database if not exists " + MetricDBConst
	sqlDropDBConst   = "drop database if exists " + MetricDBConst
	ALL_IN_ONE_MODE  = "monolithic"
)

var (
	lblNodeConst  = "node"
	lblRoleConst  = "role"
	lblValueConst = "value"
	lblTimeConst  = "collecttime"
	occupiedLbls  = map[string]struct{}{lblTimeConst: {}, lblValueConst: {}, lblNodeConst: {}, lblRoleConst: {}}
)

type Collector interface {
	prom.Collector
	// cancelToProm remove the cost introduced by being compatible with prometheus
	CancelToProm()
	// collectorForProm returns a collector used in prometheus scrape registry
	CollectorToProm() prom.Collector
}

type selfAsPromCollector struct {
	self prom.Collector
}

func (s *selfAsPromCollector) init(self prom.Collector)        { s.self = self }
func (s *selfAsPromCollector) CancelToProm()                   {}
func (s *selfAsPromCollector) CollectorToProm() prom.Collector { return s.self }

type statusServer struct {
	*http.Server
	sync.WaitGroup
}

var registry *prom.Registry
var moExporter MetricExporter
var moCollector MetricCollector
var statusSvr *statusServer

var inited uint32

func InitMetric(ctx context.Context, ieFactory func() ie.InternalExecutor, SV *config.ObservabilityParameters, nodeUUID, role string, opts ...InitOption) {
	// fix multi-init in standalone
	if !atomic.CompareAndSwapUint32(&inited, 0, 1) {
		return
	}
	var initOpts InitOptions
	for _, opt := range opts {
		opt.ApplyTo(&initOpts)
	}
	// init global variables
	initConfigByParamaterUnit(SV)
	registry = prom.NewRegistry()
	if initOpts.writerFactory != nil {
		moCollector = newMetricFSCollector(initOpts.writerFactory)
	} else {
		moCollector = newMetricCollector(ieFactory)
	}
	moExporter = newMetricExporter(registry, moCollector, nodeUUID, role)

	// register metrics and create tables
	registerAllMetrics()
	if initOpts.needInitTable {
		initTables(ctx, ieFactory, SV.BatchProcessor)
	}

	// start the data flow
	moCollector.Start(ctx)
	moExporter.Start(ctx)

	if getExportToProm() {
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
}

func StopMetricSync() {
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
	if statusSvr != nil {
		_ = statusSvr.Shutdown(context.TODO())
		statusSvr = nil
	}
}

func mustRegiterToProm(collector prom.Collector) {
	if err := prom.Register(collector); err != nil {
		// err is either registering a collector more than once or metrics have duplicate description.
		// in any case, we respect the existing collectors in the prom registry
		logutil.Debugf("[Metric] register to prom register: %v", err)
	}
}

func mustRegister(collector Collector) {
	registry.MustRegister(collector)
	if getExportToProm() {
		mustRegiterToProm(collector.CollectorToProm())
	} else {
		collector.CancelToProm()
	}
}

func InitSchema(ctx context.Context, ieFactory func() ie.InternalExecutor) error {
	initTables(ctx, ieFactory, trace.FileService)
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
	if getForceInit() {
		mustExec(sqlDropDBConst)
	}
	mustExec(sqlCreateDBConst)
	var createCost time.Duration
	defer func() {
		logutil.Debugf(
			"[Metric] init metrics tables: create cost %d ms",
			createCost.Milliseconds())
	}()
	instant := time.Now()

	descChan := make(chan *prom.Desc, 10)

	go func() {
		for _, c := range initCollectors {
			c.Describe(descChan)
		}
		close(descChan)
	}()

	optFactory := trace.GetOptionFactory(batchProcessMode)
	buf := new(bytes.Buffer)
	for desc := range descChan {
		sql := createTableSqlFromMetricFamily(desc, buf, optFactory)
		mustExec(sql)
	}

	createCost = time.Since(instant)
}

type optionsFactory func(db, tbl string) trace.TableOptions

// instead MetricFamily, Desc is used to create tables because we don't want collect errors come into the picture.
func createTableSqlFromMetricFamily(desc *prom.Desc, buf *bytes.Buffer, optionsFactory optionsFactory) string {
	buf.Reset()
	extra := newDescExtra(desc)
	opts := optionsFactory(MetricDBConst, extra.fqName)
	buf.WriteString("create ")
	buf.WriteString(opts.GetCreateOptions())
	buf.WriteString(fmt.Sprintf(
		"table if not exists %s.%s (`%s` datetime(6), `%s` double, `%s` varchar(36), `%s` varchar(20)",
		MetricDBConst, extra.fqName, lblTimeConst, lblValueConst, lblNodeConst, lblRoleConst,
	))
	for _, lbl := range extra.labels {
		buf.WriteString(", `")
		buf.WriteString(lbl.GetName())
		buf.WriteString("` varchar(20)")
	}
	buf.WriteRune(')')
	buf.WriteString(opts.GetTableOptions())
	return buf.String()
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

func mustValidLbls(name string, consts prom.Labels, vars []string) {
	mustNotOccupied := func(lblName string) {
		if _, ok := occupiedLbls[strings.ToLower(lblName)]; ok {
			panic(fmt.Sprintf("%s contains a occupied label: %s", name, lblName))
		}
	}
	for k := range consts {
		mustNotOccupied(k)
	}
	for _, v := range vars {
		mustNotOccupied(v)
	}
}

type InitOptions struct {
	writerFactory export.FSWriterFactory // see WithWriterFactory
	// needInitTable control to do the initTables
	needInitTable bool // see WithInitAction
}

type InitOption func(*InitOptions)

func (f InitOption) ApplyTo(opts *InitOptions) {
	f(opts)
}

func WithWriterFactory(factory export.FSWriterFactory) InitOption {
	return InitOption(func(options *InitOptions) {
		options.writerFactory = factory
	})
}

func WithInitAction(init bool) InitOption {
	return InitOption(func(options *InitOptions) {
		options.needInitTable = init
	})
}
