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

package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/testutil"
)

const (
	crossEngineEnableEnv       = "MO_ICEBERG_CROSS_ENGINE"
	crossEngineMOCommandEnv    = "MO_ICEBERG_MO_SQL_CMD"
	crossEngineSparkCommandEnv = "MO_ICEBERG_SPARK_SQL_CMD"
	crossEngineTrinoCommandEnv = "MO_ICEBERG_TRINO_SQL_CMD"
	crossEngineScenarioEnv     = "MO_ICEBERG_CROSS_ENGINE_SCENARIOS"
	crossEngineTimeoutEnv      = "MO_ICEBERG_CROSS_ENGINE_TIMEOUT"
	crossEngineReportDirEnv    = "MO_ICEBERG_REPORT_DIR"

	crossEngineComparisonOrdered   = "ordered"
	crossEngineComparisonUnordered = "unordered"
)

type crossEngineScenario struct {
	ID                   string   `json:"id,omitempty"`
	CaseID               string   `json:"case_id,omitempty"`
	Name                 string   `json:"name"`
	Catalog              string   `json:"catalog,omitempty"`
	Namespace            string   `json:"namespace,omitempty"`
	Table                string   `json:"table,omitempty"`
	Ref                  string   `json:"ref,omitempty"`
	ResolvedSnapshotID   string   `json:"resolved_snapshot_id,omitempty"`
	MetadataLocation     string   `json:"metadata_location,omitempty"`
	MetadataLocationHash string   `json:"metadata_location_hash,omitempty"`
	ComparisonMode       string   `json:"comparison_mode"`
	NullSentinel         string   `json:"null_sentinel,omitempty"`
	TimestampNormalize   string   `json:"timestamp_normalization,omitempty"`
	MOActions            []string `json:"mo_actions,omitempty"`
	SparkActions         []string `json:"spark_actions,omitempty"`
	TrinoActions         []string `json:"trino_actions,omitempty"`
	CompareSQL           []string `json:"compare_sql"`
	MOCompareSQL         []string `json:"mo_compare_sql,omitempty"`
	SparkCompareSQL      []string `json:"spark_compare_sql,omitempty"`
	TrinoCompareSQL      []string `json:"trino_compare_sql,omitempty"`
}

type crossEngineCommandResult struct {
	Raw  string
	Rows []string
	Err  error
}

type crossEngineCompareQuery struct {
	SQL     map[string]string
	Display string
}

type crossEngineScenarioReport struct {
	ReportSchemaVersion      string                   `json:"report_schema_version"`
	CaseID                   string                   `json:"case_id"`
	Scenario                 string                   `json:"scenario"`
	Catalog                  string                   `json:"catalog,omitempty"`
	Namespace                string                   `json:"namespace,omitempty"`
	Table                    string                   `json:"table,omitempty"`
	Ref                      string                   `json:"ref,omitempty"`
	ResolvedSnapshotID       string                   `json:"resolved_snapshot_id,omitempty"`
	MetadataLocationHash     string                   `json:"metadata_location_hash,omitempty"`
	MetadataLocationRedacted string                   `json:"metadata_location_redacted,omitempty"`
	ComparisonMode           string                   `json:"comparison_mode"`
	NullSentinel             string                   `json:"null_sentinel"`
	TimestampNormalization   string                   `json:"timestamp_normalization"`
	Status                   string                   `json:"status"`
	FailureCategory          string                   `json:"failure_category,omitempty"`
	GeneratedAtUTC           string                   `json:"generated_at_utc"`
	SourceScenarioPath       string                   `json:"source_scenario_path,omitempty"`
	EngineVersions           map[string]string        `json:"engine_versions,omitempty"`
	Queries                  []crossEngineQueryReport `json:"queries,omitempty"`
	SampleMismatch           []string                 `json:"sample_mismatch,omitempty"`
	CommandTemplateHashes    map[string]string        `json:"command_template_hashes,omitempty"`
}

type crossEngineQueryReport struct {
	Index           int               `json:"index"`
	SQL             map[string]string `json:"sql"`
	RowCount        map[string]int    `json:"row_count"`
	Checksum        map[string]string `json:"checksum"`
	Fingerprint     map[string]string `json:"fingerprint"`
	Status          string            `json:"status"`
	FailureCategory string            `json:"failure_category,omitempty"`
	SampleMismatch  []string          `json:"sample_mismatch,omitempty"`
}

func TestIcebergDMLCrossEngineDiff(t *testing.T) {
	if os.Getenv(crossEngineEnableEnv) != "1" {
		t.Skipf("set %s=1 and %s/%s/%s to run Iceberg DML Spark/Trino cross-engine diff",
			crossEngineEnableEnv, crossEngineMOCommandEnv, crossEngineSparkCommandEnv, crossEngineTrinoCommandEnv)
	}
	commands := map[string]string{
		"mo":    strings.TrimSpace(os.Getenv(crossEngineMOCommandEnv)),
		"spark": strings.TrimSpace(os.Getenv(crossEngineSparkCommandEnv)),
		"trino": strings.TrimSpace(os.Getenv(crossEngineTrinoCommandEnv)),
	}
	for name, command := range commands {
		if command == "" {
			t.Fatalf("%s command template is required when %s=1", name, crossEngineEnableEnv)
		}
		if !strings.Contains(command, "{sql}") {
			t.Fatalf("%s command template must contain {sql}: %s", name, command)
		}
	}
	scenarios := loadCrossEngineScenarios(t)
	timeout := crossEngineTimeout()
	for _, scenario := range scenarios {
		t.Run(crossEngineScenarioCaseID(scenario)+"_"+scenario.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeoutCause(context.Background(), timeout, api.CauseForCode(api.ErrPlanningTimeout))
			defer cancel()
			report := newCrossEngineScenarioReport(scenario, commands)
			rawOutputs := map[string][]string{"mo": nil, "spark": nil, "trino": nil}
			for _, action := range []struct {
				engine  string
				command string
				sqls    []string
			}{
				{engine: "mo", command: commands["mo"], sqls: scenario.MOActions},
				{engine: "spark", command: commands["spark"], sqls: scenario.SparkActions},
				{engine: "trino", command: commands["trino"], sqls: scenario.TrinoActions},
			} {
				for idx, sql := range action.sqls {
					sql = expandCrossEngineScenarioSQL(t, scenario, sql)
					result := runCrossEngineSQLResult(ctx, action.engine, action.command, sql)
					rawOutputs[action.engine] = append(rawOutputs[action.engine], crossEngineRawSection("action", idx, sql, result.Raw))
					if result.Err != nil {
						report.Status = "failed"
						report.FailureCategory = classifyCrossEngineFailure(result.Err, result.Raw)
						report.SampleMismatch = []string{fmt.Sprintf("%s action failed: %v", action.engine, result.Err)}
						writeCrossEngineReport(t, scenario, report, rawOutputs)
						t.Fatalf("%s action failed: %v\nSQL:\n%s\nOutput:\n%s", action.engine, result.Err, sql, result.Raw)
					}
				}
			}
			compareQueries, err := crossEngineCompareQueries(scenario)
			if err != nil {
				t.Fatal(err)
			}
			for _, query := range compareQueries {
				queryIndex := len(report.Queries)
				querySQL := expandCrossEngineCompareSQL(t, scenario, query.SQL)
				moResult := runCrossEngineSQLResult(ctx, "mo", commands["mo"], querySQL["mo"])
				sparkResult := runCrossEngineSQLResult(ctx, "spark", commands["spark"], querySQL["spark"])
				trinoResult := runCrossEngineSQLResult(ctx, "trino", commands["trino"], querySQL["trino"])
				results := map[string]crossEngineCommandResult{
					"mo":    moResult,
					"spark": sparkResult,
					"trino": trinoResult,
				}
				for engine, result := range results {
					rawOutputs[engine] = append(rawOutputs[engine], crossEngineRawSection("query", queryIndex, querySQL[engine], result.Raw))
					if result.Err != nil {
						queryReport := newCrossEngineQueryReport(queryIndex, querySQL, scenario.ComparisonMode, results)
						queryReport.Status = "failed"
						queryReport.FailureCategory = classifyCrossEngineFailure(result.Err, result.Raw)
						queryReport.SampleMismatch = []string{fmt.Sprintf("%s query failed: %v", engine, result.Err)}
						report.Queries = append(report.Queries, queryReport)
						report.Status = "failed"
						report.FailureCategory = queryReport.FailureCategory
						report.SampleMismatch = queryReport.SampleMismatch
						writeCrossEngineReport(t, scenario, report, rawOutputs)
						t.Fatalf("%s query failed: %v\nSQL:\n%s\nOutput:\n%s", engine, result.Err, querySQL[engine], result.Raw)
					}
				}
				queryReport := newCrossEngineQueryReport(queryIndex, querySQL, scenario.ComparisonMode, results)
				if !crossEngineResultsMatch(scenario.ComparisonMode, moResult.Rows, sparkResult.Rows, trinoResult.Rows) {
					queryReport.Status = "failed"
					queryReport.FailureCategory = "data_mismatch"
					queryReport.SampleMismatch = crossEngineMismatchSample(scenario.ComparisonMode, moResult.Rows, sparkResult.Rows, trinoResult.Rows)
					report.Queries = append(report.Queries, queryReport)
					report.Status = "failed"
					report.FailureCategory = "data_mismatch"
					report.SampleMismatch = queryReport.SampleMismatch
					writeCrossEngineReport(t, scenario, report, rawOutputs)
					t.Fatalf("cross-engine diff mismatch for %q mode=%s\nMO:    %s\nSpark: %s\nTrino: %s\nMismatch sample:\n%s\nMO rows:\n%s\nSpark rows:\n%s\nTrino rows:\n%s",
						query.Display,
						scenario.ComparisonMode,
						queryReport.Fingerprint["mo"],
						queryReport.Fingerprint["spark"],
						queryReport.Fingerprint["trino"],
						strings.Join(queryReport.SampleMismatch, "\n"),
						strings.Join(moResult.Rows, "\n"),
						strings.Join(sparkResult.Rows, "\n"),
						strings.Join(trinoResult.Rows, "\n"))
				}
				queryReport.Status = "success"
				report.Queries = append(report.Queries, queryReport)
			}
			writeCrossEngineReport(t, scenario, report, rawOutputs)
		})
	}
}

func TestCrossEngineFingerprintIsOrderInsensitive(t *testing.T) {
	left := normalizeCrossEngineRows("mysql: [Warning] Using a password on the command line interface can be insecure.\n 2\tb \n1   a\nTime taken: 1.2 seconds, Fetched 2 row(s)\n")
	right := normalizeCrossEngineRows("1 a\n2 b\n")
	if got, want := crossEngineFingerprint(left), crossEngineFingerprint(right); got != want {
		t.Fatalf("expected normalized fingerprints to match, got %s want %s", got, want)
	}
	withHeader := normalizeCrossEngineRowsForEngine("mysql: [Warning] Using a password on the command line interface can be insecure.\norder_id amount\n1 10\n2 20\n", "mo", "mysql -h 127.0.0.1 -e {sql}")
	noHeader := normalizeCrossEngineRowsForEngine("1 10\n2 20\n", "spark", "spark-sql -e {sql}")
	if got, want := crossEngineFingerprint(withHeader), crossEngineFingerprint(noHeader); got != want {
		t.Fatalf("expected mysql header to be ignored, got %s want %s", got, want)
	}
	alreadySkipped := normalizeCrossEngineRowsForEngine("1 10\n2 20\n", "mo", "mysql -N -e {sql}")
	if got, want := crossEngineFingerprint(alreadySkipped), crossEngineFingerprint(noHeader); got != want {
		t.Fatalf("expected mysql -N rows to remain intact, got %s want %s", got, want)
	}
	sparkWithShutdownNoise := normalizeCrossEngineRows("1 a\norg.apache.spark.SparkException: Exception thrown in awaitResult:\nat org.apache.spark.executor.Executor.reportHeartBeat(Executor.scala:1219)\nCaused by: org.apache.spark.SparkException: Could not find HeartbeatReceiver.\n2 b\n")
	if got, want := crossEngineFingerprint(sparkWithShutdownNoise), crossEngineFingerprint(right); got != want {
		t.Fatalf("expected Spark shutdown stack trace to be ignored, got %s want %s rows=%v", got, want, sparkWithShutdownNoise)
	}
	trinoWithJavaWarning := normalizeCrossEngineRows("WARNING: Final field resourceEstimates has been mutated reflectively\nWARNING: A restricted method in java.lang.System has been called\n1 a\n2 b\n")
	if got, want := crossEngineFingerprint(trinoWithJavaWarning), crossEngineFingerprint(right); got != want {
		t.Fatalf("expected Trino Java warnings to be ignored, got %s want %s rows=%v", got, want, trinoWithJavaWarning)
	}
	if quote := shellQuote("select 'ksa'"); quote != `'select '\''ksa'\'''` {
		t.Fatalf("unexpected shell quote: %s", quote)
	}
}

func TestCrossEngineScenarioValidationRequiresComparisonMode(t *testing.T) {
	err := validateCrossEngineScenarioList([]crossEngineScenario{{
		ID:         "ICE-TEST-150",
		Name:       "missing-mode",
		CompareSQL: []string{"select 1"},
	}})
	if err == nil || !strings.Contains(err.Error(), "comparison_mode") {
		t.Fatalf("expected comparison_mode validation error, got %v", err)
	}
	err = validateCrossEngineScenarioList([]crossEngineScenario{{
		ID:             "ICE-TEST-150",
		Name:           "ok",
		ComparisonMode: crossEngineComparisonOrdered,
		CompareSQL:     []string{"select 1"},
	}})
	if err != nil {
		t.Fatalf("expected valid scenario: %v", err)
	}
	err = validateCrossEngineScenarioList([]crossEngineScenario{{
		ID:             "ICE-TEST-151",
		Name:           "engine-specific",
		ComparisonMode: crossEngineComparisonOrdered,
		MOCompareSQL:   []string{"select * from mo_writer_gold_kpi"},
		SparkCompareSQL: []string{
			"select * from tiera.writer.gold_kpi",
		},
		TrinoCompareSQL: []string{"select * from iceberg.writer.gold_kpi"},
	}})
	if err != nil {
		t.Fatalf("expected engine-specific scenario: %v", err)
	}
	err = validateCrossEngineScenarioList([]crossEngineScenario{{
		ID:              "ICE-TEST-151",
		Name:            "bad-engine-specific",
		ComparisonMode:  crossEngineComparisonOrdered,
		MOCompareSQL:    []string{"select 1"},
		SparkCompareSQL: []string{"select 1"},
	}})
	if err == nil || !strings.Contains(err.Error(), "mo_compare_sql/spark_compare_sql/trino_compare_sql") {
		t.Fatalf("expected engine-specific length validation error, got %v", err)
	}
}

func TestGoldenRealScenarioExampleValidates(t *testing.T) {
	path := filepath.Join("..", "..", "..", "test", "iceberg", "golden_real_scenarios.example.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var scenarios []crossEngineScenario
	if err := json.Unmarshal(data, &scenarios); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if err := validateCrossEngineScenarioList(scenarios); err != nil {
		t.Fatalf("validate %s: %v", path, err)
	}
	got := make(map[string]bool, len(scenarios))
	for _, scenario := range scenarios {
		got[scenario.ID] = true
	}
	for _, want := range []string{"ICE-TEST-156", "ICE-TEST-157", "ICE-TEST-158", "ICE-TEST-159"} {
		if !got[want] {
			t.Fatalf("golden-real example missing %s", want)
		}
	}
}

func TestCrossEngineComparisonModes(t *testing.T) {
	left := []string{"1 a", "2 b"}
	right := []string{"2 b", "1 a"}
	if !crossEngineResultsMatch(crossEngineComparisonUnordered, left, right, right) {
		t.Fatalf("unordered mode should ignore row order")
	}
	if crossEngineResultsMatch(crossEngineComparisonOrdered, left, right, right) {
		t.Fatalf("ordered mode should preserve row order")
	}
}

func TestCrossEngineReportWritesStandardArtifactsAndRedacts(t *testing.T) {
	reportDir := t.TempDir()
	t.Setenv(crossEngineReportDirEnv, reportDir)
	t.Setenv("MO_ICEBERG_CATALOG_TOKEN", "raw-token")
	t.Setenv("MO_ICEBERG_S3_SK", "raw-secret-key")
	t.Setenv("MO_ICEBERG_TRINO_VERSION", "trino-test")

	scenario := crossEngineScenario{
		ID:                 "ICE-TEST-160",
		CaseID:             "ICE-TEST-160_report_artifacts",
		Name:               "report artifacts",
		Catalog:            "nessie",
		Namespace:          "tpch_sf01",
		Table:              "orders",
		Ref:                "main",
		ResolvedSnapshotID: "123",
		MetadataLocation:   "s3://mo-iceberg/warehouse/tpch/orders/metadata/00001.metadata.json",
		ComparisonMode:     crossEngineComparisonOrdered,
	}
	report := newCrossEngineScenarioReport(scenario, map[string]string{
		"mo":    "mysql -e {sql}",
		"spark": "spark-sql -e {sql}",
		"trino": "trino --execute {sql}",
	})
	results := map[string]crossEngineCommandResult{
		"mo":    {Rows: []string{"1 a"}},
		"spark": {Rows: []string{"1 a"}},
		"trino": {Rows: []string{"1 a"}},
	}
	queryReport := newCrossEngineQueryReport(0, sameCrossEngineSQL("select * from orders order by 1"), scenario.ComparisonMode, results)
	queryReport.Status = "success"
	report.Queries = append(report.Queries, queryReport)
	writeCrossEngineReport(t, scenario, report, map[string][]string{
		"mo": {
			"raw-token raw-secret-key Bearer raw-token s3://mo-iceberg/warehouse/tpch/orders/data/part-1.parquet",
		},
		"spark": {
			"https://minio.local/mo-iceberg/warehouse/tpch/orders/metadata/00001.metadata.json?X-Amz-Signature=abcdef&AWSAccessKeyId=raw-token",
		},
		"trino": {
			"/warehouse/tpch/orders/manifests/manifest.avro secret=raw-secret-key",
		},
	})

	caseDir := filepath.Join(reportDir, "ICE-TEST-160_report_artifacts_report_artifacts")
	for _, name := range []string{"metadata.json", "diff.json", "summary.md", "mo.out", "spark.out", "trino.out"} {
		path := filepath.Join(caseDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read report artifact %s: %v", path, err)
		}
		text := string(data)
		testutil.AssertNoIcebergSensitiveLeak(t, "cross-engine report artifact "+path, text,
			"raw-token",
			"raw-secret-key",
			"X-Amz-Signature=",
			"AWSAccessKeyId=",
			"/warehouse/tpch/orders/manifests/manifest.avro",
		)
	}
	metadata := readCrossEngineReportFile(t, filepath.Join(caseDir, "metadata.json"))
	for _, want := range []string{
		`"case_id": "ICE-TEST-160_report_artifacts"`,
		`"comparison_mode": "ordered"`,
		`"metadata_location_hash":`,
		`"trino": "trino-test"`,
	} {
		if !strings.Contains(metadata, want) {
			t.Fatalf("metadata report missing %q:\n%s", want, metadata)
		}
	}
}

func readCrossEngineReportFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func loadCrossEngineScenarios(t *testing.T) []crossEngineScenario {
	t.Helper()
	if path := strings.TrimSpace(os.Getenv(crossEngineScenarioEnv)); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var scenarios []crossEngineScenario
		if err := json.Unmarshal(data, &scenarios); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		validateCrossEngineScenarios(t, scenarios)
		return scenarios
	}
	scenarios := []crossEngineScenario{
		{
			ID:             "ICE-TEST-151",
			CaseID:         "ICE-TEST-151_append_gold_kpi_history_preserved",
			Name:           "append-gold-kpi-history-preserved",
			Namespace:      "writer",
			Table:          "gold_kpi",
			Ref:            "main",
			ComparisonMode: crossEngineComparisonOrdered,
			MOActions: []string{
				"insert into ${MO_ICEBERG_TIER_A_MO_WRITER_GOLD_KPI} values (cast(9001 as bigint), 'ksa', cast('2026-06-29' as date), cast(900 as bigint), 'mo_append'), (cast(9002 as bigint), 'uae', cast('2026-06-29' as date), cast(901 as bigint), 'mo_append')",
			},
			MOCompareSQL: []string{
				"select source, region, count(*) as c, sum(cast(kpi_value as bigint)) as value_sum from ${MO_ICEBERG_TIER_A_MO_WRITER_GOLD_KPI} where source in ('spark_base', 'mo_append') group by source, region order by source, region",
			},
			SparkCompareSQL: []string{
				"select source, region, count(*) as c, sum(cast(kpi_value as bigint)) as value_sum from ${MO_ICEBERG_TIER_A_SPARK_WRITER_GOLD_KPI} where source in ('spark_base', 'mo_append') group by source, region order by source, region",
			},
			TrinoCompareSQL: []string{
				"select source, region, count(*) as c, sum(cast(kpi_value as bigint)) as value_sum from ${MO_ICEBERG_TIER_A_TRINO_WRITER_GOLD_KPI} where source in ('spark_base', 'mo_append') group by source, region order by source, region",
			},
		},
		{
			ID:             "ICE-TEST-152",
			CaseID:         "ICE-TEST-152_dml_delete_update_merge_accounts",
			Name:           "dml-delete-update-merge-accounts",
			Namespace:      "dml",
			Table:          "accounts",
			Ref:            "main",
			ComparisonMode: crossEngineComparisonOrdered,
			MOActions: []string{
				"delete from ${MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS} where account_id = -9001",
				"update ${MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS} set balance = balance + 1 where account_id = -9002",
				"merge into ${MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS} t using (select cast(-9999 as bigint) as account_id, cast(0 as bigint) as balance where false) s on t.account_id = s.account_id when not matched then insert (account_id, balance) values (s.account_id, s.balance)",
			},
			MOCompareSQL: []string{
				"select count(*) as c, sum(cast(account_id as bigint)) as id_sum, sum(cast(balance as bigint)) as balance_sum from ${MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS}",
				"select account_id, balance, case when region is null then '<NULL>' else region end as region from ${MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS} order by account_id",
			},
			SparkCompareSQL: []string{
				"select count(*) as c, sum(cast(account_id as bigint)) as id_sum, sum(cast(balance as bigint)) as balance_sum from ${MO_ICEBERG_TIER_A_SPARK_DML_ACCOUNTS}",
				"select account_id, balance, case when region is null then '<NULL>' else region end as region from ${MO_ICEBERG_TIER_A_SPARK_DML_ACCOUNTS} order by account_id",
			},
			TrinoCompareSQL: []string{
				"select count(*) as c, sum(cast(account_id as bigint)) as id_sum, sum(cast(balance as bigint)) as balance_sum from ${MO_ICEBERG_TIER_A_TRINO_DML_ACCOUNTS}",
				"select account_id, balance, case when region is null then '<NULL>' else region end as region from ${MO_ICEBERG_TIER_A_TRINO_DML_ACCOUNTS} order by account_id",
			},
		},
		{
			ID:             "ICE-TEST-154",
			CaseID:         "ICE-TEST-154_merge_on_read_delete_apply",
			Name:           "merge-on-read-delete-apply",
			Namespace:      "delete_files",
			Table:          "orders_mor",
			Ref:            "main",
			ComparisonMode: crossEngineComparisonOrdered,
			MOCompareSQL: []string{
				"select count(*) as c, sum(cast(order_id as bigint)) as order_sum, sum(cast(hidden_key as bigint)) as hidden_sum, sum(cast(amount as bigint)) as amount_sum from ${MO_ICEBERG_TIER_A_MO_DB}.delete_files_orders_mor",
				"select order_id, hidden_key, bucket, amount from ${MO_ICEBERG_TIER_A_MO_DB}.delete_files_orders_mor order by order_id",
			},
			SparkCompareSQL: []string{
				"select count(*) as c, sum(cast(order_id as bigint)) as order_sum, sum(cast(hidden_key as bigint)) as hidden_sum, sum(cast(amount as bigint)) as amount_sum from ${MO_ICEBERG_TIER_A_SPARK_DELETE_ORDERS}",
				"select order_id, hidden_key, bucket, amount from ${MO_ICEBERG_TIER_A_SPARK_DELETE_ORDERS} order by order_id",
			},
			TrinoCompareSQL: []string{
				"select count(*) as c, sum(cast(order_id as bigint)) as order_sum, sum(cast(hidden_key as bigint)) as hidden_sum, sum(cast(amount as bigint)) as amount_sum from ${MO_ICEBERG_TIER_A_TRINO_DELETE_ORDERS}",
				"select order_id, hidden_key, bucket, amount from ${MO_ICEBERG_TIER_A_TRINO_DELETE_ORDERS} order by order_id",
			},
		},
		{
			ID:             "ICE-TEST-153",
			CaseID:         "ICE-TEST-153_maintenance_rewrite_expire_orders_small",
			Name:           "maintenance-rewrite-expire",
			Namespace:      "maintenance",
			Table:          "orders_small",
			Ref:            "main",
			ComparisonMode: crossEngineComparisonOrdered,
			MOActions: []string{
				"${MO_ICEBERG_TIER_A_MAINTENANCE_REWRITE_DATA_MO_SQL}",
				"${MO_ICEBERG_TIER_A_MAINTENANCE_REWRITE_MANIFESTS_MO_SQL}",
				"${MO_ICEBERG_TIER_A_MAINTENANCE_EXPIRE_MO_SQL}",
			},
			MOCompareSQL: []string{
				"${MO_ICEBERG_TIER_A_MAINTENANCE_MO_SQL}",
				"select order_id, bucket, amount from ${MO_ICEBERG_TIER_A_MO_MAINTENANCE_ORDERS} order by order_id",
			},
			SparkCompareSQL: []string{
				"${MO_ICEBERG_TIER_A_MAINTENANCE_SPARK_SQL}",
				"select order_id, bucket, amount from ${MO_ICEBERG_TIER_A_SPARK_MAINTENANCE_ORDERS} order by order_id",
			},
			TrinoCompareSQL: []string{
				"${MO_ICEBERG_TIER_A_MAINTENANCE_TRINO_SQL}",
				"select order_id, bucket, amount from ${MO_ICEBERG_TIER_A_TRINO_MAINTENANCE_ORDERS} order by order_id",
			},
		},
		{
			ID:             "ICE-TEST-162",
			CaseID:         "ICE-TEST-162_dml_partition_overwrite_history_preserved",
			Name:           "dml-partition-overwrite",
			Namespace:      "dml",
			Table:          "accounts_by_region",
			Ref:            "main",
			ComparisonMode: crossEngineComparisonOrdered,
			MOActions: []string{
				"insert overwrite ${MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS_BY_REGION} partition(region = 'ksa') select account_id, balance, region from ${MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS_BY_REGION_STAGE} where region = 'ksa'",
			},
			MOCompareSQL: []string{
				"select region, count(*) as c, sum(cast(account_id as bigint)) as id_sum, sum(cast(balance as bigint)) as balance_sum from ${MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS_BY_REGION} group by region order by region",
			},
			SparkCompareSQL: []string{
				"select region, count(*) as c, sum(cast(account_id as bigint)) as id_sum, sum(cast(balance as bigint)) as balance_sum from ${MO_ICEBERG_TIER_A_SPARK_DML_ACCOUNTS_BY_REGION} group by region order by region",
			},
			TrinoCompareSQL: []string{
				"select region, count(*) as c, sum(cast(account_id as bigint)) as id_sum, sum(cast(balance as bigint)) as balance_sum from ${MO_ICEBERG_TIER_A_TRINO_DML_ACCOUNTS_BY_REGION} group by region order by region",
			},
		},
	}
	validateCrossEngineScenarios(t, scenarios)
	return scenarios
}

func validateCrossEngineScenarios(t *testing.T, scenarios []crossEngineScenario) {
	t.Helper()
	if err := validateCrossEngineScenarioList(scenarios); err != nil {
		t.Fatalf("%v", err)
	}
}

func validateCrossEngineScenarioList(scenarios []crossEngineScenario) error {
	if len(scenarios) == 0 {
		return moerr.NewInternalErrorNoCtx("cross-engine scenarios must not be empty")
	}
	for _, scenario := range scenarios {
		if strings.TrimSpace(scenario.Name) == "" {
			return moerr.NewInternalErrorNoCtx("cross-engine scenario name is required")
		}
		if strings.TrimSpace(crossEngineScenarioCaseID(scenario)) == "" {
			return moerr.NewInternalErrorNoCtxf("cross-engine scenario %s requires case_id or id", scenario.Name)
		}
		switch strings.TrimSpace(scenario.ComparisonMode) {
		case crossEngineComparisonOrdered, crossEngineComparisonUnordered:
		default:
			return moerr.NewInternalErrorNoCtxf("cross-engine scenario %s requires comparison_mode %q or %q", scenario.Name, crossEngineComparisonOrdered, crossEngineComparisonUnordered)
		}
		if len(scenario.CompareSQL) == 0 {
			if _, err := crossEngineCompareQueries(scenario); err != nil {
				return err
			}
			continue
		}
		if _, err := crossEngineCompareQueries(scenario); err != nil {
			return err
		}
	}
	return nil
}

func crossEngineCompareQueries(scenario crossEngineScenario) ([]crossEngineCompareQuery, error) {
	hasEngineSpecific := len(scenario.MOCompareSQL) != 0 || len(scenario.SparkCompareSQL) != 0 || len(scenario.TrinoCompareSQL) != 0
	if hasEngineSpecific {
		count := len(scenario.MOCompareSQL)
		if count == 0 || len(scenario.SparkCompareSQL) != count || len(scenario.TrinoCompareSQL) != count {
			return nil, moerr.NewInternalErrorNoCtxf("cross-engine scenario %s requires equal non-empty mo_compare_sql/spark_compare_sql/trino_compare_sql lengths", scenario.Name)
		}
		out := make([]crossEngineCompareQuery, 0, count)
		for idx := 0; idx < count; idx++ {
			sql := map[string]string{
				"mo":    strings.TrimSpace(scenario.MOCompareSQL[idx]),
				"spark": strings.TrimSpace(scenario.SparkCompareSQL[idx]),
				"trino": strings.TrimSpace(scenario.TrinoCompareSQL[idx]),
			}
			for engine, query := range sql {
				if query == "" {
					return nil, moerr.NewInternalErrorNoCtxf("cross-engine scenario %s has empty %s compare sql at index %d", scenario.Name, engine, idx)
				}
			}
			out = append(out, crossEngineCompareQuery{
				SQL:     sql,
				Display: sql["mo"],
			})
		}
		return out, nil
	}
	if len(scenario.CompareSQL) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("cross-engine scenario %s requires compare_sql", scenario.Name)
	}
	out := make([]crossEngineCompareQuery, 0, len(scenario.CompareSQL))
	for idx, query := range scenario.CompareSQL {
		query = strings.TrimSpace(query)
		if query == "" {
			return nil, moerr.NewInternalErrorNoCtxf("cross-engine scenario %s has empty compare_sql at index %d", scenario.Name, idx)
		}
		out = append(out, crossEngineCompareQuery{
			SQL:     sameCrossEngineSQL(query),
			Display: query,
		})
	}
	return out, nil
}

func sameCrossEngineSQL(query string) map[string]string {
	return map[string]string{
		"mo":    query,
		"spark": query,
		"trino": query,
	}
}

func expandCrossEngineCompareSQL(t *testing.T, scenario crossEngineScenario, query map[string]string) map[string]string {
	t.Helper()
	out := make(map[string]string, len(query))
	for _, engine := range []string{"mo", "spark", "trino"} {
		out[engine] = expandCrossEngineScenarioSQL(t, scenario, query[engine])
	}
	return out
}

func expandCrossEngineScenarioSQL(t *testing.T, scenario crossEngineScenario, value string) string {
	t.Helper()
	missing := make([]string, 0)
	expanded := os.Expand(value, func(key string) string {
		envValue := strings.TrimSpace(os.Getenv(key))
		if envValue == "" {
			missing = append(missing, key)
		}
		return envValue
	})
	if len(missing) > 0 {
		t.Fatalf("cross-engine scenario %s missing environment variables: %s", crossEngineScenarioCaseID(scenario), strings.Join(missing, ", "))
	}
	if strings.TrimSpace(expanded) == "" {
		t.Fatalf("cross-engine scenario %s SQL expanded to empty string from %q", crossEngineScenarioCaseID(scenario), value)
	}
	return expanded
}

func crossEngineScenarioCaseID(scenario crossEngineScenario) string {
	if value := strings.TrimSpace(scenario.CaseID); value != "" {
		return value
	}
	return strings.TrimSpace(scenario.ID)
}

func crossEngineTimeout() time.Duration {
	if raw := strings.TrimSpace(os.Getenv(crossEngineTimeoutEnv)); raw != "" {
		if timeout, err := time.ParseDuration(raw); err == nil && timeout > 0 {
			return timeout
		}
	}
	return 20 * time.Minute
}

func runCrossEngineSQL(t *testing.T, ctx context.Context, engine, command, sql string) []string {
	t.Helper()
	result := runCrossEngineSQLResult(ctx, engine, command, sql)
	if result.Err != nil {
		t.Fatalf("%s query failed: %v\nSQL:\n%s\nOutput:\n%s", engine, result.Err, sql, result.Raw)
	}
	return result.Rows
}

func runCrossEngineSQLResult(ctx context.Context, engine, command, sql string) crossEngineCommandResult {
	rendered := strings.ReplaceAll(command, "{sql}", shellQuote(sql))
	cmd := exec.CommandContext(ctx, "sh", "-c", rendered)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return crossEngineCommandResult{Raw: string(out), Rows: normalizeCrossEngineRowsForEngine(string(out), engine, command), Err: err}
	}
	return crossEngineCommandResult{Raw: string(out), Rows: normalizeCrossEngineRowsForEngine(string(out), engine, command)}
}

func normalizeCrossEngineRowsForEngine(output, engine, command string) []string {
	rows := normalizeCrossEngineRows(output)
	if strings.EqualFold(engine, "mo") && mysqlCommandPrintsColumnNames(command) && len(rows) > 0 {
		return rows[1:]
	}
	return rows
}

func mysqlCommandPrintsColumnNames(command string) bool {
	lower := strings.ToLower(command)
	if !strings.Contains(lower, "mysql") {
		return false
	}
	if strings.Contains(lower, "--skip-column-names") || strings.Contains(lower, "--column-names=false") {
		return false
	}
	fields := strings.Fields(lower)
	for _, field := range fields {
		if field == "-n" || field == "--skip-column-names" {
			return false
		}
		if strings.HasPrefix(field, "-") && !strings.HasPrefix(field, "--") && strings.Contains(field[1:], "n") {
			return false
		}
	}
	return true
}

func normalizeCrossEngineRows(output string) []string {
	lines := strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n")
	rows := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if isCrossEngineNoiseLine(line) {
			continue
		}
		line = strings.Join(strings.Fields(line), " ")
		rows = append(rows, line)
	}
	return rows
}

func isCrossEngineNoiseLine(line string) bool {
	if strings.HasPrefix(line, "mysql: [Warning]") ||
		strings.HasPrefix(line, ":: ") ||
		strings.HasPrefix(line, "Ivy Default Cache set to:") ||
		strings.HasPrefix(line, "The jars for the packages stored in:") ||
		strings.HasPrefix(line, "confs: ") ||
		strings.HasPrefix(line, "found ") ||
		strings.HasPrefix(line, "Setting default log level") ||
		strings.HasPrefix(line, "To adjust logging level") ||
		strings.HasPrefix(line, "Spark Web UI available") ||
		strings.HasPrefix(line, "Spark master:") ||
		strings.HasPrefix(line, "org.apache.spark.") ||
		strings.HasPrefix(line, "at org.apache.spark.") ||
		strings.HasPrefix(line, "Caused by: org.apache.spark.") ||
		strings.HasPrefix(line, "Time taken:") ||
		strings.HasPrefix(line, "SLF4J:") ||
		strings.HasPrefix(line, "WARNING:") ||
		strings.HasPrefix(line, "|") ||
		strings.HasPrefix(line, "---") {
		return true
	}
	if strings.Contains(line, " added as a dependency") ||
		strings.Contains(line, " artifacts copied, ") ||
		(strings.Contains(line, " from ") && strings.Contains(line, " in [default]")) {
		return true
	}
	if len(line) >= len("26/06/26 18:41:37 WARN") &&
		line[2] == '/' && line[5] == '/' && line[8] == ' ' &&
		strings.Contains(line, " WARN ") {
		return true
	}
	return false
}

func crossEngineFingerprint(rows []string) string {
	sorted := append([]string(nil), rows...)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(sum[:])
}

func crossEngineChecksum(rows []string, mode string) string {
	selected := append([]string(nil), rows...)
	if mode == crossEngineComparisonUnordered {
		sort.Strings(selected)
	}
	sum := sha256.Sum256([]byte(strings.Join(selected, "\n")))
	return hex.EncodeToString(sum[:])
}

func crossEngineResultsMatch(mode string, moRows, sparkRows, trinoRows []string) bool {
	mo := crossEngineChecksum(moRows, mode)
	return crossEngineChecksum(sparkRows, mode) == mo && crossEngineChecksum(trinoRows, mode) == mo
}

func newCrossEngineQueryReport(index int, sql map[string]string, mode string, results map[string]crossEngineCommandResult) crossEngineQueryReport {
	rowCount := make(map[string]int, len(results))
	checksum := make(map[string]string, len(results))
	fingerprint := make(map[string]string, len(results))
	reportSQL := make(map[string]string, 3)
	for _, engine := range []string{"mo", "spark", "trino"} {
		reportSQL[engine] = strings.TrimSpace(sql[engine])
	}
	for _, engine := range []string{"mo", "spark", "trino"} {
		result := results[engine]
		rowCount[engine] = len(result.Rows)
		checksum[engine] = crossEngineChecksum(result.Rows, mode)
		fingerprint[engine] = crossEngineChecksum(result.Rows, mode)
	}
	return crossEngineQueryReport{
		Index:       index,
		SQL:         reportSQL,
		RowCount:    rowCount,
		Checksum:    checksum,
		Fingerprint: fingerprint,
	}
}

func crossEngineMismatchSample(mode string, moRows, sparkRows, trinoRows []string) []string {
	rowsByEngine := map[string][]string{
		"mo":    append([]string(nil), moRows...),
		"spark": append([]string(nil), sparkRows...),
		"trino": append([]string(nil), trinoRows...),
	}
	if mode == crossEngineComparisonUnordered {
		for engine := range rowsByEngine {
			sort.Strings(rowsByEngine[engine])
		}
	}
	maxRows := 5
	out := make([]string, 0, maxRows)
	maxLen := len(rowsByEngine["mo"])
	for _, engine := range []string{"spark", "trino"} {
		if len(rowsByEngine[engine]) > maxLen {
			maxLen = len(rowsByEngine[engine])
		}
	}
	for idx := 0; idx < maxLen && len(out) < maxRows; idx++ {
		mo := crossEngineRowAt(rowsByEngine["mo"], idx)
		spark := crossEngineRowAt(rowsByEngine["spark"], idx)
		trino := crossEngineRowAt(rowsByEngine["trino"], idx)
		if mo != spark || mo != trino {
			out = append(out, fmt.Sprintf("row[%d] mo=%q spark=%q trino=%q", idx, mo, spark, trino))
		}
	}
	if len(out) == 0 {
		out = append(out, "fingerprints differ but no row-level sample was available")
	}
	return out
}

func crossEngineRowAt(rows []string, idx int) string {
	if idx >= len(rows) {
		return "<missing>"
	}
	return rows[idx]
}

func newCrossEngineScenarioReport(scenario crossEngineScenario, commands map[string]string) crossEngineScenarioReport {
	nullSentinel := strings.TrimSpace(scenario.NullSentinel)
	if nullSentinel == "" {
		nullSentinel = "<NULL>"
	}
	timestampNormalization := strings.TrimSpace(scenario.TimestampNormalize)
	if timestampNormalization == "" {
		timestampNormalization = "iceberg_semantics"
	}
	metadataLocationHash := strings.TrimSpace(scenario.MetadataLocationHash)
	if metadataLocationHash == "" && strings.TrimSpace(scenario.MetadataLocation) != "" {
		metadataLocationHash = api.PathHash(scenario.MetadataLocation)
	}
	return crossEngineScenarioReport{
		ReportSchemaVersion:      "iceberg-cross-engine-v1",
		CaseID:                   crossEngineScenarioCaseID(scenario),
		Scenario:                 scenario.Name,
		Catalog:                  scenario.Catalog,
		Namespace:                scenario.Namespace,
		Table:                    scenario.Table,
		Ref:                      scenario.Ref,
		ResolvedSnapshotID:       scenario.ResolvedSnapshotID,
		MetadataLocationHash:     metadataLocationHash,
		MetadataLocationRedacted: crossEngineRedactPathOrEmpty(scenario.MetadataLocation),
		ComparisonMode:           scenario.ComparisonMode,
		NullSentinel:             nullSentinel,
		TimestampNormalization:   timestampNormalization,
		Status:                   "success",
		GeneratedAtUTC:           time.Now().UTC().Format(time.RFC3339Nano),
		SourceScenarioPath:       strings.TrimSpace(os.Getenv(crossEngineScenarioEnv)),
		EngineVersions:           crossEngineEngineVersions(),
		CommandTemplateHashes:    crossEngineCommandTemplateHashes(commands),
	}
}

func crossEngineCommandTemplateHashes(commands map[string]string) map[string]string {
	if len(commands) == 0 {
		return nil
	}
	out := make(map[string]string, len(commands))
	for engine, command := range commands {
		sum := sha256.Sum256([]byte(command))
		out[engine] = hex.EncodeToString(sum[:8])
	}
	return out
}

func crossEngineEngineVersions() map[string]string {
	out := make(map[string]string)
	for key, env := range map[string]string{
		"mo":      "MO_ICEBERG_MO_VERSION",
		"spark":   "MO_ICEBERG_SPARK_VERSION",
		"trino":   "MO_ICEBERG_TRINO_VERSION",
		"iceberg": "MO_ICEBERG_ICEBERG_VERSION",
		"nessie":  "MO_ICEBERG_NESSIE_VERSION",
		"minio":   "MO_ICEBERG_MINIO_VERSION",
	} {
		if value := strings.TrimSpace(os.Getenv(env)); value != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func crossEngineRawSection(kind string, index int, sql, raw string) string {
	return fmt.Sprintf("## %s %d\nSQL: %s\n%s", kind, index, sql, raw)
}

func classifyCrossEngineFailure(err error, output string) string {
	lower := strings.ToLower(output)
	if err != nil {
		lower += " " + strings.ToLower(err.Error())
	}
	switch {
	case strings.Contains(lower, "context deadline exceeded"),
		strings.Contains(lower, "timeout"),
		strings.Contains(lower, "connection refused"),
		strings.Contains(lower, "connection reset"),
		strings.Contains(lower, "no such host"),
		strings.Contains(lower, "service unavailable"),
		strings.Contains(lower, "temporarily unavailable"):
		return "environment_unavailable"
	case strings.Contains(lower, "unauthorized"),
		strings.Contains(lower, "forbidden"),
		strings.Contains(lower, "access denied"),
		strings.Contains(lower, "permission denied"),
		strings.Contains(lower, "residency_denied"),
		strings.Contains(lower, "principal_not_mapped"):
		return "permission_error"
	default:
		return "engine_error"
	}
}

func writeCrossEngineReport(t *testing.T, scenario crossEngineScenario, report crossEngineScenarioReport, rawOutputs map[string][]string) {
	t.Helper()
	root := strings.TrimSpace(os.Getenv(crossEngineReportDirEnv))
	if root == "" {
		return
	}
	caseDir := filepath.Join(root, crossEngineSafeReportName(crossEngineScenarioCaseID(scenario)+"_"+scenario.Name))
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		t.Fatalf("create cross-engine report dir %s: %v", caseDir, err)
	}
	for _, engine := range []string{"mo", "spark", "trino"} {
		writeCrossEngineReportFile(t, filepath.Join(caseDir, engine+".out"), strings.Join(rawOutputs[engine], "\n\n"))
	}
	writeCrossEngineReportJSON(t, filepath.Join(caseDir, "metadata.json"), report)
	writeCrossEngineReportJSON(t, filepath.Join(caseDir, "diff.json"), crossEngineDiffSummary(report))
	writeCrossEngineReportFile(t, filepath.Join(caseDir, "summary.md"), crossEngineSummaryMarkdown(report))
	assertCrossEngineReportRedacted(t, caseDir)
}

func crossEngineDiffSummary(report crossEngineScenarioReport) map[string]any {
	return map[string]any{
		"case_id":          report.CaseID,
		"scenario":         report.Scenario,
		"status":           report.Status,
		"failure_category": report.FailureCategory,
		"comparison_mode":  report.ComparisonMode,
		"queries":          report.Queries,
		"sample_mismatch":  report.SampleMismatch,
	}
}

func crossEngineSummaryMarkdown(report crossEngineScenarioReport) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "# %s %s\n\n", report.CaseID, report.Scenario)
	fmt.Fprintf(&builder, "| Field | Value |\n| --- | --- |\n")
	fmt.Fprintf(&builder, "| status | %s |\n", report.Status)
	fmt.Fprintf(&builder, "| failure_category | %s |\n", report.FailureCategory)
	fmt.Fprintf(&builder, "| comparison_mode | %s |\n", report.ComparisonMode)
	fmt.Fprintf(&builder, "| catalog | %s |\n", report.Catalog)
	fmt.Fprintf(&builder, "| namespace | %s |\n", report.Namespace)
	fmt.Fprintf(&builder, "| table | %s |\n", report.Table)
	fmt.Fprintf(&builder, "| ref | %s |\n", report.Ref)
	fmt.Fprintf(&builder, "| resolved_snapshot_id | %s |\n", report.ResolvedSnapshotID)
	fmt.Fprintf(&builder, "| metadata_location_hash | %s |\n", report.MetadataLocationHash)
	fmt.Fprintf(&builder, "| generated_at_utc | %s |\n\n", report.GeneratedAtUTC)
	if len(report.Queries) > 0 {
		builder.WriteString("## Queries\n\n")
		builder.WriteString("| Index | Status | MO rows | Spark rows | Trino rows | MO checksum | Spark checksum | Trino checksum |\n")
		builder.WriteString("| --- | --- | ---: | ---: | ---: | --- | --- | --- |\n")
		for _, query := range report.Queries {
			fmt.Fprintf(&builder, "| %d | %s | %d | %d | %d | `%s` | `%s` | `%s` |\n",
				query.Index,
				query.Status,
				query.RowCount["mo"],
				query.RowCount["spark"],
				query.RowCount["trino"],
				query.Checksum["mo"],
				query.Checksum["spark"],
				query.Checksum["trino"])
		}
		builder.WriteString("\n")
	}
	if len(report.SampleMismatch) > 0 {
		builder.WriteString("## Sample Mismatch\n\n")
		for _, sample := range report.SampleMismatch {
			fmt.Fprintf(&builder, "- `%s`\n", sample)
		}
	}
	return builder.String()
}

func writeCrossEngineReportJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal cross-engine report %s: %v", path, err)
	}
	writeCrossEngineReportFile(t, path, string(data)+"\n")
}

func writeCrossEngineReportFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(redactCrossEngineReportText(value)), 0o644); err != nil {
		t.Fatalf("write cross-engine report %s: %v", path, err)
	}
}

func crossEngineSafeReportName(value string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, value)
}

func crossEngineRedactPathOrEmpty(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	return api.RedactPath(path)
}

var (
	crossEngineBearerRE           = regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`)
	crossEngineSecretAssignmentRE = regexp.MustCompile(`(?i)((?:token|secret|password|credential|authorization|access[_-]?key|session[_-]?token)\s*=\s*)[^\s;&]+`)
	crossEngineS3PathRE           = regexp.MustCompile(`(?i)\b(?:s3|s3a|oss|cos|gs|abfs)://[^\s'"<>]+`)
	crossEngineFilePathRE         = regexp.MustCompile(`(?i)(?:/[^\s'"<>]*)?(?:metadata|manifest|manifests|data|delete)[^\s'"<>]*\.(?:metadata\.json|json|avro|parquet)`)
	crossEngineURLRE              = regexp.MustCompile(`https?://[^\s'"<>]+`)
)

func redactCrossEngineReportText(value string) string {
	for _, key := range []string{
		"MO_ICEBERG_CATALOG_TOKEN",
		"MO_ICEBERG_TOKEN",
		"MO_ICEBERG_S3_AK",
		"MO_ICEBERG_S3_SK",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
		"MINIO_ROOT_USER",
		"MINIO_ROOT_PASSWORD",
	} {
		if secret := strings.TrimSpace(os.Getenv(key)); secret != "" {
			value = strings.ReplaceAll(value, secret, "<redacted>")
		}
	}
	value = crossEngineBearerRE.ReplaceAllString(value, "${1}<redacted>")
	value = crossEngineSecretAssignmentRE.ReplaceAllString(value, "${1}<redacted>")
	value = crossEngineS3PathRE.ReplaceAllStringFunc(value, api.RedactPath)
	value = crossEngineURLRE.ReplaceAllStringFunc(value, redactCrossEngineURL)
	value = crossEngineFilePathRE.ReplaceAllStringFunc(value, api.RedactPath)
	return value
}

func redactCrossEngineURL(raw string) string {
	if strings.Contains(raw, "?") {
		return api.RedactPath(raw)
	}
	lower := strings.ToLower(raw)
	if strings.Contains(lower, "/metadata/") ||
		strings.Contains(lower, "/manifest") ||
		strings.Contains(lower, "/data/") ||
		strings.Contains(lower, "/delete/") ||
		strings.HasSuffix(lower, ".metadata.json") ||
		strings.HasSuffix(lower, ".avro") ||
		strings.HasSuffix(lower, ".parquet") {
		return api.RedactPath(raw)
	}
	return raw
}

func assertCrossEngineReportRedacted(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(data)
		for _, forbidden := range crossEngineForbiddenReportSubstrings() {
			if strings.Contains(text, forbidden) {
				return moerr.NewInternalErrorNoCtxf("cross-engine report %s leaked %q", path, forbidden)
			}
		}
		if crossEngineS3PathRE.MatchString(text) || crossEngineFilePathRE.MatchString(text) {
			return moerr.NewInternalErrorNoCtxf("cross-engine report %s leaked an object path: %s", path, text)
		}
		for _, rawURL := range crossEngineURLRE.FindAllString(text, -1) {
			if strings.Contains(rawURL, "?") {
				return moerr.NewInternalErrorNoCtxf("cross-engine report %s leaked a signed URL: %s", path, text)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func crossEngineForbiddenReportSubstrings() []string {
	out := []string{"X-Amz-Signature=", "AWSAccessKeyId=", "sessionToken=", "Bearer raw"}
	for _, key := range []string{
		"MO_ICEBERG_CATALOG_TOKEN",
		"MO_ICEBERG_TOKEN",
		"MO_ICEBERG_S3_AK",
		"MO_ICEBERG_S3_SK",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
		"MINIO_ROOT_USER",
		"MINIO_ROOT_PASSWORD",
	} {
		if secret := strings.TrimSpace(os.Getenv(key)); secret != "" {
			out = append(out, secret)
		}
	}
	return out
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
