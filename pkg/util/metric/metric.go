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
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

const (
	metricDBConst    = "system_metrics"
	sqlCreateDBConst = "create database if not exists " + metricDBConst
	sqlDropDBConst   = "drop database if exists " + metricDBConst
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

func InitMetric(ieFactory func() ie.InternalExecutor, pu *config.ParameterUnit, nodeId int, role string) {
	// init global variables
	initConfigByParamaterUnit(pu)
	registry = prom.NewRegistry()
	moCollector = newMetricCollector(ieFactory)
	moExporter = newMetricExporter(registry, moCollector, int32(nodeId), role)

	// register metrics and create tables
	registerAllMetrics()
	initTables(ieFactory)

	// start the data flow
	moCollector.Start()
	moExporter.Start()

	if getExportToProm() {
		// http.HandleFunc("/query", makeDebugHandleFunc(ieFactory))
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(prom.DefaultGatherer, promhttp.HandlerOpts{}))
		addr := fmt.Sprintf("%s:%d", pu.SV.GetHost(), pu.SV.GetStatusPort())
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

// initTables gathers all metrics and extract metadata to format create table sql
func initTables(ieFactory func() ie.InternalExecutor) {
	exec := ieFactory()
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(metricDBConst).Internal(true).Finish())
	mustExec := func(sql string) {
		if err := exec.Exec(sql, ie.NewOptsBuilder().Finish()); err != nil {
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

	buf := new(bytes.Buffer)
	for desc := range descChan {
		sql := createTableSqlFromMetricFamily(desc, buf)
		mustExec(sql)
	}

	createCost = time.Since(instant)
}

// instead MetricFamily, Desc is used to create tables because we don't want collect errors come into the picture.
func createTableSqlFromMetricFamily(desc *prom.Desc, buf *bytes.Buffer) string {
	buf.Reset()
	extra := newDescExtra(desc)
	buf.WriteString(fmt.Sprintf(
		"create table if not exists %s.%s (`%s` datetime, `%s` double, `%s` int, `%s` varchar(20)",
		metricDBConst, extra.fqName, lblTimeConst, lblValueConst, lblNodeConst, lblRoleConst,
	))
	for _, lbl := range extra.labels {
		buf.WriteString(", `")
		buf.WriteString(lbl.GetName())
		buf.WriteString("` varchar(20)")
	}
	buf.WriteRune(')')
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
