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

package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/K-Phoen/grabana"
	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/graph"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/target/prometheus"
	"github.com/K-Phoen/grabana/timeseries"
	tsaxis "github.com/K-Phoen/grabana/timeseries/axis"
	"github.com/K-Phoen/grabana/timeseries/fields"
	"github.com/K-Phoen/grabana/variable/datasource"
	"github.com/K-Phoen/grabana/variable/interval"
	"github.com/K-Phoen/grabana/variable/query"
	"github.com/K-Phoen/sdk"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	defaultMoFolderName = "Matrixone"
	localFolderName     = "Matrixone-Standalone"
)

var UnitPercent01 = axis.Unit("percentunit")
var UnitPercent0100 = axis.Unit("percent")

type DashboardCreator struct {
	cli             *grabana.Client
	host            string
	username        string
	password        string
	dataSource      string
	extraFilterFunc func() string
	by              string
	filterOptions   []dashboard.Option
	folderName      string
}

func NewCloudDashboardCreator(
	host,
	username,
	password,
	dataSource,
	folderName string) *DashboardCreator {
	dc := &DashboardCreator{
		cli:        grabana.NewClient(http.DefaultClient, host, grabana.WithBasicAuth(username, password)),
		host:       host,
		username:   username,
		password:   password,
		dataSource: dataSource,
	}
	dc.extraFilterFunc = dc.getCloudFilters
	dc.by = "pod"
	dc.folderName = folderName
	dc.initCloudFilterOptions()
	return dc
}

func NewLocalDashboardCreator(
	host,
	username,
	password,
	folderName string) *DashboardCreator {
	datasourceVariableName := "datasource"
	dc := &DashboardCreator{
		cli:        grabana.NewClient(http.DefaultClient, host, grabana.WithBasicAuth(username, password)),
		host:       host,
		username:   username,
		password:   password,
		dataSource: fmt.Sprintf("${%s}", datasourceVariableName),
	}
	dc.extraFilterFunc = dc.getLocalFilters
	dc.by = "instance"
	dc.folderName = folderName
	dc.initLocalFilterOptions(datasourceVariableName)
	return dc
}

func NewK8SDashboardCreator(
	host,
	username,
	password,
	dataSource,
	folderName string) *DashboardCreator {
	dc := &DashboardCreator{
		cli:        grabana.NewClient(http.DefaultClient, host, grabana.WithBasicAuth(username, password)),
		host:       host,
		username:   username,
		password:   password,
		dataSource: dataSource,
	}
	dc.extraFilterFunc = dc.getK8SFilters
	dc.by = "pod"
	dc.folderName = folderName
	dc.initK8SFilterOptions()
	return dc
}

func NewCloudCtrlPlaneDashboardCreator(
	host,
	username,
	password,
	dataSource,
	folderName string) *DashboardCreator {
	dc := &DashboardCreator{
		cli:        grabana.NewClient(http.DefaultClient, host, grabana.WithBasicAuth(username, password)),
		host:       host,
		username:   username,
		password:   password,
		dataSource: AutoUnitPrometheusDatasource,
	}
	dc.extraFilterFunc = dc.getCloudFilters
	dc.by = "pod"
	dc.folderName = folderName
	dc.initCloudCtrlPlaneFilterOptions(dataSource)
	return dc
}

func (c *DashboardCreator) Create() error {
	if err := c.initTxnDashboard(); err != nil {
		return err
	}

	if err := c.initLogTailDashboard(); err != nil {
		return err
	}

	if err := c.initTaskDashboard(); err != nil {
		return err
	}

	if err := c.initFileServiceDashboard(); err != nil {
		return err
	}

	if err := c.initRPCDashboard(); err != nil {
		return err
	}

	if err := c.initMemDashboard(); err != nil {
		return err
	}

	if err := c.initRuntimeDashboard(); err != nil {
		return err
	}

	if err := c.initTraceDashboard(); err != nil {
		return err
	}

	if err := c.initProxyDashboard(); err != nil {
		return err
	}

	if err := c.initFrontendDashboard(); err != nil {
		return err
	}

	if err := c.initCDCDashboard(); err != nil {
		return err
	}

	if err := c.initShardingDashboard(); err != nil {
		return err
	}

	if err := c.initPipelineDashBoard(); err != nil {
		return err
	}

	return nil
}

func (c *DashboardCreator) createFolder(name string) (*grabana.Folder, error) {
	return c.cli.FindOrCreateFolder(context.Background(), name)
}

func (c *DashboardCreator) withGraph(
	title string,
	span float32,
	pql string,
	legend string,
	opts ...axis.Option) row.Option {
	return c.withMultiGraph(
		title,
		span,
		[]string{pql},
		[]string{legend},
		opts...,
	)
}

func (c *DashboardCreator) withMultiGraph(
	title string,
	span float32,
	queries []string,
	legends []string,
	axisOpts ...axis.Option) row.Option {
	opts := []graph.Option{
		graph.Span(span),
		graph.DataSource(c.dataSource),
		graph.LeftYAxis(axisOpts...)}

	for i, query := range queries {
		opts = append(opts,
			graph.WithPrometheusTarget(
				query,
				prometheus.Legend(legends[i]),
			))
	}

	return row.WithGraph(
		title,
		opts...,
	)
}

// withTimeSeries
// cc github.com/K-Phoen/grabana/cmd/builder-example
func (c *DashboardCreator) withTimeSeries(
	title string,
	span float32,
	queries []string,
	legends []string,
	tsOpts ...timeseries.Option,
) row.Option {

	opts := []timeseries.Option{
		timeseries.Span(span),
		timeseries.DataSource(c.dataSource),
		timeseries.Tooltip(timeseries.AllSeries), // default show all metrics' value.
	}
	opts = append(opts, tsOpts...)

	for i, query := range queries {
		opts = append(opts,
			timeseries.WithPrometheusTarget(
				query,
				prometheus.Legend(legends[i]),
			))
	}

	return row.WithTimeSeries(
		title,
		opts...,
	)
}

func ScaleDistributionLinear() fields.OverrideOption {
	return func(field *sdk.FieldConfigOverride) {
		field.Properties = append(field.Properties,
			sdk.FieldConfigOverrideProperty{
				ID: "custom.scaleDistribution",
				Value: struct {
					Type string `json:"type"`
				}{Type: "linear"},
			})
	}
}

func (c *DashboardCreator) getHistogram(
	title string,
	metric string,
	percents []float64,
	column float32,
	axisOptions ...axis.Option) row.Option {
	return c.getHistogramWithExtraBy(title, metric, percents, column, "", axisOptions...)
}

func SpanNulls(always bool) timeseries.Option {
	return func(ts *timeseries.TimeSeries) error {
		ts.Builder.TimeseriesPanel.FieldConfig.Defaults.Custom.SpanNulls = always
		return nil
	}
}

func (c *DashboardCreator) getPercentHist(
	title string,
	metric string,
	percents []float64,
	opts ...timeseries.Option) row.Option {
	options := []timeseries.Option{
		timeseries.DataSource(c.dataSource),
		timeseries.FillOpacity(0),
		timeseries.Height("300px"),
		timeseries.Axis(tsaxis.Unit("s")),
	}
	options = append(options, opts...)
	for i := 0; i < len(percents); i++ {
		percent := percents[i]
		query := fmt.Sprintf("histogram_quantile(%f, sum(rate(%s[$interval])) by (le))", percent, metric)
		legend := fmt.Sprintf("P%.0f", percent*100)
		options = append(options, timeseries.WithPrometheusTarget(
			query,
			prometheus.Legend(legend),
		))
	}
	return row.WithTimeSeries(title, options...)
}

func (c *DashboardCreator) getTimeSeries(
	title string, pql []string, legend []string,
	opts ...timeseries.Option) row.Option {
	options := []timeseries.Option{
		timeseries.DataSource(c.dataSource),
		timeseries.FillOpacity(0),
		timeseries.Height("300px"),
	}
	options = append(options, opts...)
	for i := range pql {
		options = append(options, timeseries.WithPrometheusTarget(
			pql[i],
			prometheus.Legend(legend[i]),
		))
	}
	return row.WithTimeSeries(title, options...)
}

func (c *DashboardCreator) getHistogramWithExtraBy(
	title string,
	metric string,
	percents []float64,
	column float32,
	extraBy string,
	axisOptions ...axis.Option) row.Option {

	var queries []string
	var legends []string
	for i := 0; i < len(percents); i++ {
		percent := percents[i]

		query := fmt.Sprintf("histogram_quantile(%f, sum(rate(%s[$interval])) by (le))", percent, metric)
		legend := fmt.Sprintf("P%.2f%%", percent*100)
		if len(extraBy) > 0 {
			query = fmt.Sprintf("histogram_quantile(%f, sum(rate(%s[$interval])) by (le, %s))", percent, metric, extraBy)
			legend = fmt.Sprintf("{{ "+extraBy+" }}(P%.2f%%)", percent*100)
		}
		queries = append(queries, query)
		legends = append(legends, legend)
	}
	return c.withMultiGraph(
		title,
		column,
		queries,
		legends,
		axisOptions...,
	)
}

func (c *DashboardCreator) getMultiHistogram(
	metrics []string,
	legends []string,
	percents []float64,
	columns []float32,
	axisOptions ...axis.Option) []row.Option {
	var options []row.Option
	for i := 0; i < len(percents); i++ {
		percent := percents[i]

		var queries []string
		for _, metric := range metrics {
			queries = append(queries,
				fmt.Sprintf("histogram_quantile(%f, sum(rate(%s[$interval]))  by (le))", percent, metric))
		}

		// format title
		// default: P50.000000 time
		// bytes:   P50.000000 size
		title := fmt.Sprintf("P%f time", percent*100)
		if axis.New(axisOptions...).Builder.Format == "bytes" {
			title = fmt.Sprintf("P%f size", percent*100)
		}

		options = append(options,
			c.withMultiGraph(
				title,
				columns[i],
				queries,
				legends,
				axisOptions...))
	}
	return options
}

func (c *DashboardCreator) withRowOptions(rows ...dashboard.Option) []dashboard.Option {
	rows = append(rows,
		dashboard.AutoRefresh("30s"),
		dashboard.Time("now-30m", "now"),
		dashboard.VariableAsInterval(
			"interval",
			interval.Default("1m"),
			interval.Values([]string{"1m", "5m", "10m", "30m", "1h", "6h", "12h"}),
		))
	return append(rows, c.filterOptions...)
}

func (c *DashboardCreator) getMetricWithFilter(name string, filter string) string {
	var metric bytes.Buffer
	extraFilters := c.extraFilterFunc()

	if len(filter) == 0 && len(extraFilters) == 0 {
		return name
	}

	metric.WriteString(name)
	metric.WriteString("{")
	if filter != "" {
		metric.WriteString(filter)
		if len(extraFilters) > 0 {
			metric.WriteString(",")
		}
	}
	if len(extraFilters) > 0 {
		metric.WriteString(c.extraFilterFunc())
	}
	metric.WriteString("}")
	return metric.String()
}

func (c *DashboardCreator) getCloudFilters() string {
	return `matrixone_cloud_main_cluster=~"$physicalCluster", matrixorigin_io_owner=~"$owner", pod=~"$pod"`
}

func (c *DashboardCreator) initCloudFilterOptions() {
	c.filterOptions = append(c.filterOptions,
		dashboard.VariableAsQuery(
			"physicalCluster",
			query.DataSource(c.dataSource),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Multiple(),
			query.Label("main_cluster"),
			query.Request(`label_values(up, matrixone_cloud_main_cluster)`),
		),
		dashboard.VariableAsQuery(
			"owner",
			query.DataSource(c.dataSource),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Multiple(),
			query.Label("owner"),
			query.Request(`label_values(up{matrixone_cloud_main_cluster=~"$physicalCluster"}, matrixorigin_io_owner)`),
			query.AllValue(".*"),
			query.Refresh(query.TimeChange),
		),
		dashboard.VariableAsQuery(
			"pod",
			query.DataSource(c.dataSource),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Multiple(),
			query.Label("pod"),
			query.Request(`label_values(up{matrixone_cloud_main_cluster="$physicalCluster", matrixorigin_io_owner=~"$owner"},pod)`),
			query.Refresh(query.TimeChange),
		))
}

const Prometheus = "prometheus"
const AutoUnitPrometheusDatasource = `${ds_prom}`

func (c *DashboardCreator) initCloudCtrlPlaneFilterOptions(metaDatasource string) {
	c.filterOptions = append(c.filterOptions,
		dashboard.VariableAsQuery(
			"unit",
			query.DataSource(metaDatasource),
			query.Label("unit"),
			query.Request(`label_values(mo_cluster_info, unit)`),
		),
		dashboard.VariableAsDatasource(
			"ds_prom",
			datasource.Type(Prometheus),
			datasource.Regex(`/$unit-prometheus/`),
		),
		dashboard.VariableAsQuery(
			"physicalCluster",
			query.DataSource(`${ds_prom}`),
			query.Label("main_cluster"),
			query.Request(`label_values(up, matrixone_cloud_main_cluster)`),
		),
		dashboard.VariableAsQuery(
			"owner",
			query.DataSource(`${ds_prom}`),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Multiple(),
			query.Label("owner"),
			query.Request(`label_values(up{matrixone_cloud_main_cluster=~"$physicalCluster"}, matrixorigin_io_owner)`),
			query.AllValue(".*"),
			query.Refresh(query.TimeChange),
		),
		dashboard.VariableAsQuery(
			"pod",
			query.DataSource(`${ds_prom}`),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Multiple(),
			query.Label("pod"),
			query.Request(`label_values(up{matrixone_cloud_main_cluster="$physicalCluster", matrixorigin_io_owner=~"$owner"},pod)`),
			query.Refresh(query.TimeChange),
		))
}

func (c *DashboardCreator) getLocalFilters() string {
	return `instance=~"$instance"`
}

func (c *DashboardCreator) initLocalFilterOptions(datasourceVariableName string) {
	c.filterOptions = append(c.filterOptions,
		dashboard.VariableAsDatasource(
			datasourceVariableName,
			datasource.Type(Prometheus),
			datasource.Label(datasourceVariableName),
		),
		dashboard.VariableAsQuery(
			"instance",
			query.DataSource(fmt.Sprintf("${%s}", datasourceVariableName)),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Label("instance"),
			query.Request("label_values(instance)"),
		))
}

func (c *DashboardCreator) getK8SFilters() string {
	return `namespace=~"$namespace", pod=~"$pod"`
}

func (c *DashboardCreator) initK8SFilterOptions() {
	c.filterOptions = append(c.filterOptions,
		dashboard.VariableAsQuery(
			"namespace",
			query.DataSource(c.dataSource),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Multiple(),
			query.Label("namespace"),
			query.Request("label_values(namespace)"),
			query.Refresh(query.TimeChange),
		),
		dashboard.VariableAsQuery(
			"pod",
			query.DataSource(c.dataSource),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Multiple(),
			query.Label("pod"),
			query.Request(`label_values(kube_pod_info{namespace="$namespace"},pod)`),
			query.Refresh(query.TimeChange),
		))
}

// Delete deletes all dashboards in the specified folder using Grafana HTTP API.
func (c *DashboardCreator) Delete() error {
	client := &http.Client{}

	// Step 1: Find the folder by name
	folder, err := c.findFolderByName(client, c.folderName)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to find folder %s: %v", c.folderName, err)
	}
	if folder == nil {
		return moerr.NewInternalErrorNoCtxf("folder %s not found", c.folderName)
	}

	// Step 2: List all dashboards in the folder
	dashboards, err := c.listDashboardsInFolder(client, folder.UID)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to list dashboards in folder %s: %v", c.folderName, err)
	}

	if len(dashboards) == 0 {
		return moerr.NewInternalErrorNoCtxf("no dashboards found in folder %s", c.folderName)
	}

	// Step 3: Delete each dashboard
	var deletedCount int
	var skippedCount int
	var errors []error
	for _, dash := range dashboards {
		if err := c.deleteDashboard(client, dash.UID); err != nil {
			// Check if it's a provisioned dashboard error
			errStr := err.Error()
			if strings.Contains(errStr, "provisioned dashboard cannot be deleted") {
				skippedCount++
				// Log but don't treat as fatal error
				continue
			}
			errors = append(errors, moerr.NewInternalErrorNoCtxf("failed to delete dashboard %s (UID: %s): %v", dash.Title, dash.UID, err))
			continue
		}
		deletedCount++
	}

	// Report summary if there were any issues
	if len(errors) > 0 {
		return moerr.NewInternalErrorNoCtxf("deleted %d/%d dashboards, skipped %d provisioned dashboards, errors: %v", deletedCount, len(dashboards), skippedCount, errors)
	}

	if skippedCount > 0 {
		// Return a special error that indicates success but with warnings
		// The main.go will check for this and display it as a warning
		return moerr.NewInternalErrorNoCtxf("deleted %d/%d dashboards, skipped %d provisioned dashboard(s) (they cannot be deleted via API and will be recreated on next provisioning)", deletedCount, len(dashboards), skippedCount)
	}

	return nil
}

// List lists all dashboards in the specified folder.
// Returns a slice of dashboard information including UID and Title.
func (c *DashboardCreator) List() ([]DashboardInfo, error) {
	client := &http.Client{}

	// Step 1: Find the folder by name
	folder, err := c.findFolderByName(client, c.folderName)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("failed to find folder %s: %v", c.folderName, err)
	}
	if folder == nil {
		return nil, moerr.NewInternalErrorNoCtxf("folder %s not found", c.folderName)
	}

	// Step 2: List all dashboards in the folder
	dashboards, err := c.listDashboardsInFolder(client, folder.UID)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("failed to list dashboards in folder %s: %v", c.folderName, err)
	}

	// Convert internal dashboardInfo to exported DashboardInfo
	result := make([]DashboardInfo, len(dashboards))
	for i, dash := range dashboards {
		result[i] = DashboardInfo{
			UID:   dash.UID,
			Title: dash.Title,
		}
	}

	return result, nil
}

type folderInfo struct {
	UID   string `json:"uid"`
	Title string `json:"title"`
}

type dashboardInfo struct {
	UID   string `json:"uid"`
	Title string `json:"title"`
}

// DashboardInfo represents a dashboard in Grafana
type DashboardInfo struct {
	UID   string
	Title string
}

func (c *DashboardCreator) findFolderByName(client *http.Client, folderName string) (*folderInfo, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/folders", c.host), nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.username, c.password)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, moerr.NewInternalErrorNoCtxf("failed to retrieve folders, status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var folders []folderInfo
	if err := json.NewDecoder(resp.Body).Decode(&folders); err != nil {
		return nil, err
	}

	for _, folder := range folders {
		if folder.Title == folderName {
			return &folder, nil
		}
	}

	return nil, nil
}

func (c *DashboardCreator) listDashboardsInFolder(client *http.Client, folderUID string) ([]dashboardInfo, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/search?folderIds=%s", c.host, folderUID), nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.username, c.password)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, moerr.NewInternalErrorNoCtxf("failed to retrieve dashboards, status code: %d, body: %s", resp.StatusCode, string(body))
	}

	var dashboards []dashboardInfo
	if err := json.NewDecoder(resp.Body).Decode(&dashboards); err != nil {
		return nil, err
	}

	return dashboards, nil
}

func (c *DashboardCreator) deleteDashboard(client *http.Client, dashboardUID string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/api/dashboards/uid/%s", c.host, dashboardUID), nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.username, c.password)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return moerr.NewInternalErrorNoCtxf("failed to delete dashboard, status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteDashboardByUID deletes a single dashboard by its UID.
func (c *DashboardCreator) DeleteDashboardByUID(dashboardUID string) error {
	client := &http.Client{}
	return c.deleteDashboard(client, dashboardUID)
}

// DeleteDashboardByUIDWithAuth deletes a single dashboard by its UID using provided credentials.
// This is a standalone function that doesn't require a DashboardCreator instance.
func DeleteDashboardByUIDWithAuth(host, username, password, dashboardUID string) error {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/api/dashboards/uid/%s", host, dashboardUID), nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return moerr.NewInternalErrorNoCtxf("failed to delete dashboard, status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteFolder deletes the entire folder and all dashboards in it.
// Provisioned dashboards (created via configuration files) cannot be deleted via API
// and will be skipped with a warning.
func (c *DashboardCreator) DeleteFolder() error {
	client := &http.Client{}

	// Step 1: Find the folder by name
	folder, err := c.findFolderByName(client, c.folderName)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to find folder %s: %v", c.folderName, err)
	}
	if folder == nil {
		return moerr.NewInternalErrorNoCtxf("folder %s not found", c.folderName)
	}

	// Step 2: List all dashboards in the folder
	dashboards, err := c.listDashboardsInFolder(client, folder.UID)
	if err != nil {
		return moerr.NewInternalErrorNoCtxf("failed to list dashboards in folder %s: %v", c.folderName, err)
	}

	// Step 3: Delete each dashboard
	var deletedCount int
	var skippedCount int
	var errors []error
	for _, dash := range dashboards {
		err := c.deleteDashboard(client, dash.UID)
		if err != nil {
			// Check if it's a provisioned dashboard error
			errStr := err.Error()
			if strings.Contains(errStr, "provisioned dashboard cannot be deleted") {
				skippedCount++
				// Log but don't treat as fatal error
				continue
			}
			errors = append(errors, moerr.NewInternalErrorNoCtxf("failed to delete dashboard %s (UID: %s): %v", dash.Title, dash.UID, err))
			continue
		}
		deletedCount++
	}

	// Step 4: Delete the folder itself (even if some dashboards couldn't be deleted)
	if err := c.deleteFolder(client, folder.UID); err != nil {
		// If folder deletion fails, return error with summary
		if len(errors) > 0 || skippedCount > 0 {
			return moerr.NewInternalErrorNoCtxf("deleted %d/%d dashboards, skipped %d provisioned dashboards, failed to delete folder: %v, errors: %v", deletedCount, len(dashboards), skippedCount, err, errors)
		}
		return moerr.NewInternalErrorNoCtxf("failed to delete folder %s: %v", c.folderName, err)
	}

	// Report summary if there were any issues
	if len(errors) > 0 {
		return moerr.NewInternalErrorNoCtxf("folder deleted, but deleted %d/%d dashboards, skipped %d provisioned dashboards, errors: %v", deletedCount, len(dashboards), skippedCount, errors)
	}

	if skippedCount > 0 {
		// Return a special error that indicates success but with warnings
		// The main.go will check for this and display it as a warning
		return moerr.NewInternalErrorNoCtxf("folder deleted successfully, but skipped %d provisioned dashboard(s) (they cannot be deleted via API and will be recreated on next provisioning)", skippedCount)
	}

	return nil
}

func (c *DashboardCreator) deleteFolder(client *http.Client, folderUID string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/api/folders/%s", c.host, folderUID), nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.username, c.password)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return moerr.NewInternalErrorNoCtxf("failed to delete folder, status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}
