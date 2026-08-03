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
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestIcebergTierASeedScriptGeneratesScenarioEnvironment(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	script := filepath.Join(repoRoot, "etc", "launch-minio-local", "tier-a", "seed-iceberg-tier-a.sh")
	tempDir := t.TempDir()
	fakeSpark := filepath.Join(tempDir, "spark-sql")
	fakeMO := filepath.Join(tempDir, "mo-sql")
	envPath := filepath.Join(tempDir, "tier_a.generated.env")
	sparkLog := filepath.Join(tempDir, "spark.log")
	renderedSeed := filepath.Join(tempDir, "rendered_seed.sql")
	moSQLLog := filepath.Join(tempDir, "mo_setup.sql")

	writeExecutable(t, fakeSpark, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FAKE_SPARK_LOG"
for ((i=1; i <= $#; i++)); do
  arg="${!i}"
  if [[ "$arg" == "-f" ]]; then
    next=$((i + 1))
    cp "${!next}" "$FAKE_SPARK_RENDERED"
    exit 0
  fi
  if [[ "$arg" == "-e" ]]; then
    next=$((i + 1))
    query="${!next}"
    if [[ "$query" == *"order by committed_at asc"* ]]; then
      printf 'snapshot_id\n101\n'
    elif [[ "$query" == *"order by committed_at desc"* ]]; then
      printf 'snapshot_id\n102\n'
    elif [[ "$query" == *"where snapshot_id = 101"* ]]; then
      printf 'committed_at\n2026-01-01 00:00:00.000\n'
    else
      printf '1\n'
    fi
    exit 0
  fi
done
`)
	writeExecutable(t, fakeMO, `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$1" > "$FAKE_MO_SQL_LOG"
`)

	cmd := exec.Command("bash", script)
	cmd.Env = append(os.Environ(),
		"SPARK_SQL_BIN="+fakeSpark,
		"FAKE_SPARK_LOG="+sparkLog,
		"FAKE_SPARK_RENDERED="+renderedSeed,
		"FAKE_MO_SQL_LOG="+moSQLLog,
		"MO_ICEBERG_TIER_A_ENV="+envPath,
		"MO_ICEBERG_MO_SQL_CMD="+fakeMO+" {sql}",
		"MO_ICEBERG_SPARK_CATALOG=nightly",
		"MO_ICEBERG_TIER_A_MO_DB=moice",
		"MO_ICEBERG_TIER_A_MO_CATALOG=mocat",
		"MO_ICEBERG_CATALOG_URI=http://catalog.test/iceberg",
		"MO_ICEBERG_WAREHOUSE=s3://bucket/wh",
		"MO_ICEBERG_S3_ENDPOINT=http://s3.test:9000",
		"MO_ICEBERG_SPARK_PACKAGES=org.example:iceberg-runtime:test,org.example:aws:test",
		"SPARK_LOCAL_IP=127.0.0.1",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run seed script: %v\n%s", err, string(out))
	}

	rendered := readFile(t, renderedSeed)
	sparkLogText := readFile(t, sparkLog)
	if !strings.Contains(sparkLogText, "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") {
		t.Fatalf("seed script did not enable Iceberg Spark SQL extensions\n%s", sparkLogText)
	}
	for _, want := range []string{
		"CREATE NAMESPACE IF NOT EXISTS nightly.tpch_sf01",
		"CREATE TABLE nightly.evolution.users",
		"DELETE FROM nightly.delete_files.orders_mor",
		"CREATE TABLE nightly.writer.gold_kpi",
		"CREATE TABLE nightly.dml.accounts_by_region",
		"CREATE TABLE nightly.maintenance.orders_small",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered seed SQL missing %q\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "CREATE BRANCH tier_a_branch") || strings.Contains(rendered, "CREATE TAG tier_a_tag") {
		t.Fatalf("rendered seed SQL should skip branch/tag refs by default\n%s", rendered)
	}

	moSetup := readFile(t, moSQLLog)
	for _, want := range []string{
		"CREATE ICEBERG CATALOG mocat",
		"'uri'='http://catalog.test/iceberg'",
		"DROP TABLE IF EXISTS moice.tpch_sf01_orders_imported_native",
		"CREATE EXTERNAL TABLE moice.delete_files_orders_mor",
		"CREATE EXTERNAL TABLE moice.writer_gold_kpi",
		"CREATE EXTERNAL TABLE moice.dml_accounts_by_region",
		"CREATE EXTERNAL TABLE moice.maintenance_orders_small",
		"'read_mode'='merge_on_read'",
	} {
		if !strings.Contains(moSetup, want) {
			t.Fatalf("rendered MO setup SQL missing %q\n%s", want, moSetup)
		}
	}

	tierAEnv := readFile(t, envPath)
	for _, want := range []string{
		"export MO_ICEBERG_IT='1'",
		"export MO_ICEBERG_ALLOW_PLAIN_HTTP='1'",
		"export MO_ICEBERG_SPARK_SQL_CMD='",
		"SPARK_LOCAL_IP='\\''127.0.0.1'\\''",
		"org.example:iceberg-runtime:test,org.example:aws:test",
		"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
		"-e {sql}",
		"export MO_ICEBERG_MO_SQL_CMD='",
		"export MO_ICEBERG_TIER_A_TIME_TRAVEL_SNAPSHOT_MO_SQL='select count(*) from moice.tpch_sf01_orders for iceberg snapshot 101'",
		"export MO_ICEBERG_TIER_A_TIME_TRAVEL_TIMESTAMP_MO_ACTION_SQL='set time_zone = '\\''+03:00'\\'''",
		"export MO_ICEBERG_TIER_A_DELETE_APPLY_SPARK_SQL='select order_id, hidden_key, bucket, amount from nightly.delete_files.orders_mor order by order_id'",
		"export MO_ICEBERG_TIER_A_TRINO_DELETE_ORDERS='iceberg.delete_files.orders_mor'",
		"export MO_ICEBERG_TIER_A_MO_ORDERS_IMPORT_NATIVE='moice.tpch_sf01_orders_imported_native'",
		"export MO_ICEBERG_TIER_A_IMPORT_NATIVE_CREATE_MO_SQL='create table moice.tpch_sf01_orders_imported_native as select order_id, cust_id, order_status, order_date, total_price, bucket from moice.tpch_sf01_orders for iceberg snapshot 101'",
		"export MO_ICEBERG_TIER_A_IMPORT_NATIVE_SPARK_SQL='select count(*) as c, sum(cast(order_id as bigint)) as order_sum",
		"export MO_ICEBERG_TIER_A_MO_DML_ACCOUNTS='moice.dml_accounts'",
		"export MO_ICEBERG_TIER_A_TRINO_DML_ACCOUNTS='iceberg.dml.accounts'",
		"export MO_ICEBERG_TIER_A_MO_WRITER_GOLD_KPI='moice.writer_gold_kpi'",
		"export MO_ICEBERG_TIER_A_SPARK_WRITER_GOLD_KPI='nightly.writer.gold_kpi'",
		"export MO_ICEBERG_TIER_A_TRINO_WRITER_GOLD_KPI='iceberg.writer.gold_kpi'",
		"export MO_ICEBERG_TIER_A_APPEND_MO_ACTION_SQL='insert into moice.writer_gold_kpi",
		"export MO_ICEBERG_TIER_A_MO_MAINTENANCE_ORDERS='moice.maintenance_orders_small'",
		"export MO_ICEBERG_TIER_A_SPARK_MAINTENANCE_ORDERS='nightly.maintenance.orders_small'",
		"export MO_ICEBERG_TIER_A_TRINO_MAINTENANCE_ORDERS='iceberg.maintenance.orders_small'",
		"export MO_ICEBERG_TIER_A_MAINTENANCE_REWRITE_DATA_MO_SQL='call iceberg_rewrite_data_files",
		"export MO_ICEBERG_TIER_A_MAINTENANCE_EXPIRE_MO_SQL='call iceberg_expire_snapshots",
	} {
		if !strings.Contains(tierAEnv, want) {
			t.Fatalf("generated env missing %q\n%s", want, tierAEnv)
		}
	}

	envOutput := sourceEnvFile(t, envPath)
	for _, line := range strings.Split(envOutput, "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok || !strings.HasPrefix(key, "MO_ICEBERG_") {
			continue
		}
		t.Setenv(key, value)
	}
	scenarios := loadTierAScenarios(t)
	for _, scenario := range scenarios {
		for _, action := range scenario.MOActions {
			_ = expandTierAEnv(t, action)
		}
		_ = expandTierAEnv(t, scenario.MOSQL)
		if scenario.ExpectMOErr != "" {
			_ = expandTierAEnv(t, scenario.ExpectMOErr)
			continue
		}
		_ = expandTierAEnv(t, scenario.SparkSQL)
	}

	refRenderedSeed := filepath.Join(tempDir, "rendered_seed_refs.sql")
	refEnvPath := filepath.Join(tempDir, "tier_a.refs.generated.env")
	refSparkLog := filepath.Join(tempDir, "spark_refs.log")
	cmd = exec.Command("bash", script)
	cmd.Env = append(os.Environ(),
		"SPARK_SQL_BIN="+fakeSpark,
		"FAKE_SPARK_LOG="+refSparkLog,
		"FAKE_SPARK_RENDERED="+refRenderedSeed,
		"MO_ICEBERG_TIER_A_ENV="+refEnvPath,
		"MO_ICEBERG_SPARK_CATALOG=nightly",
		"MO_ICEBERG_MO_SQL_CMD=",
		"MO_ICEBERG_SEED_REFS=1",
	)
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run seed script with refs: %v\n%s", err, string(out))
	}
	refRendered := readFile(t, refRenderedSeed)
	for _, want := range []string{
		"ALTER TABLE nightly.refs.orders_branch CREATE BRANCH tier_a_branch",
		"ALTER TABLE nightly.refs.orders_branch CREATE TAG tier_a_tag",
	} {
		if !strings.Contains(refRendered, want) {
			t.Fatalf("rendered ref seed SQL missing %q\n%s", want, refRendered)
		}
	}
}

func TestIcebergTierAScenarioOverrideAllowsTargetedRun(t *testing.T) {
	scenarioPath := filepath.Join(t.TempDir(), "tier_a_119_only.json")
	if err := os.WriteFile(scenarioPath, []byte(`[
  {
    "id": "ICE-TEST-119",
    "name": "dml-targeted",
    "mo_sql": "select 1",
    "spark_sql": "select 1"
  }
]`), 0o644); err != nil {
		t.Fatalf("write targeted Tier A scenario: %v", err)
	}
	t.Setenv(tierAScenarioEnv, scenarioPath)

	scenarios := loadTierAScenarios(t)
	if len(scenarios) != 1 {
		t.Fatalf("expected targeted scenario override to load one scenario, got %d", len(scenarios))
	}
	if scenarios[0].ID != "ICE-TEST-119" {
		t.Fatalf("expected ICE-TEST-119 override scenario, got %+v", scenarios[0])
	}
}

func TestIcebergTierAReportWritesRedactedArtifacts(t *testing.T) {
	reportDir := t.TempDir()
	t.Setenv(tierAReportDirEnv, reportDir)
	t.Setenv("MO_ICEBERG_S3_SK", "secret-key")
	t.Setenv("MO_ICEBERG_CATALOG_TOKEN", "catalog-token")

	scenario := tierAScenario{ID: "ICE-TEST-113", Name: "read/current"}
	writeTierAReport(t, scenario, tierAComparisonReport{
		CaseID:         scenario.ID,
		Scenario:       scenario.Name,
		Status:         "success",
		MOSQL:          "select 1",
		SparkSQL:       "select 1",
		MORowCount:     1,
		SparkRowCount:  1,
		MOChecksum:     "abc",
		SparkChecksum:  "abc",
		ComparisonMode: "unordered_fingerprint",
		GeneratedAtUTC: "2026-01-01T00:00:00Z",
	}, []string{"1 secret-key"}, []string{"1 catalog-token"})

	caseDir := filepath.Join(reportDir, "ICE-TEST-113_read_current")
	for _, name := range []string{"metadata.json", "diff.json", "mo.out", "spark.out", "summary.md"} {
		path := filepath.Join(caseDir, name)
		data := readFile(t, path)
		if strings.Contains(data, "secret-key") || strings.Contains(data, "catalog-token") {
			t.Fatalf("report file %s leaked a secret: %s", path, data)
		}
	}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func sourceEnvFile(t *testing.T, path string) string {
	t.Helper()
	cmd := exec.Command("bash", "-c", "source \"$TIER_A_ENV\"; env")
	cmd.Env = append(os.Environ(), "TIER_A_ENV="+path)
	out, err := cmd.Output()
	if err != nil {
		var stderr bytes.Buffer
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr.Write(exitErr.Stderr)
		}
		t.Fatalf("source %s: %v %s", path, err, stderr.String())
	}
	return string(out)
}
