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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const (
	tierAEnableEnv       = "MO_ICEBERG_IT"
	tierACatalogURIEnv   = "MO_ICEBERG_CATALOG_URI"
	tierACatalogTokenEnv = "MO_ICEBERG_CATALOG_TOKEN"
	tierAS3EndpointEnv   = "MO_ICEBERG_S3_ENDPOINT"
	tierAScenarioEnv     = "MO_ICEBERG_TIER_A_SCENARIOS"
	tierAReportDirEnv    = "MO_ICEBERG_REPORT_DIR"
)

type tierAScenario struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	MOActions   []string `json:"mo_actions,omitempty"`
	MOSQL       string   `json:"mo_sql"`
	SparkSQL    string   `json:"spark_sql"`
	ExpectMOErr string   `json:"expect_mo_error,omitempty"`
}

func TestIcebergTierALocalNessieMinIOPreflight(t *testing.T) {
	requireTierAEnabled(t)
	catalogURI := requireEnv(t, tierACatalogURIEnv)
	s3Endpoint := requireEnv(t, tierAS3EndpointEnv)

	tierAHTTPGet(t, tierACatalogConfigURL(t, catalogURI), strings.TrimSpace(os.Getenv(tierACatalogTokenEnv)))
	tierAHTTPGet(t, tierAMinIOHealthURL(t, s3Endpoint), "")
}

func TestIcebergTierADeterministicDatasetAvailable(t *testing.T) {
	requireTierAEnabled(t)
	sparkCommand := requireSQLCommand(t, "spark", crossEngineSparkCommandEnv)
	ctx, cancel := context.WithTimeoutCause(context.Background(), crossEngineTimeout(), api.CauseForCode(api.ErrPlanningTimeout))
	defer cancel()

	for _, table := range []struct {
		id  string
		env string
	}{
		{id: "ICE-TEST-111", env: "MO_ICEBERG_TIER_A_SPARK_ORDERS"},
		{id: "ICE-TEST-111", env: "MO_ICEBERG_TIER_A_SPARK_TAXI"},
		{id: "ICE-TEST-111", env: "MO_ICEBERG_TIER_A_SPARK_USERS"},
		{id: "ICE-TEST-112", env: "MO_ICEBERG_TIER_A_SPARK_DELETE_ORDERS"},
		{id: "ICE-TEST-112", env: "MO_ICEBERG_TIER_A_SPARK_DML_ACCOUNTS"},
		{id: "ICE-TEST-112", env: "MO_ICEBERG_TIER_A_SPARK_DML_ACCOUNTS_BY_REGION"},
		{id: "ICE-TEST-112", env: "MO_ICEBERG_TIER_A_SPARK_REF_ORDERS"},
		{id: "ICE-TEST-117", env: "MO_ICEBERG_TIER_A_SPARK_WRITER_GOLD_KPI"},
		{id: "ICE-TEST-120", env: "MO_ICEBERG_TIER_A_SPARK_MAINTENANCE_ORDERS"},
	} {
		tableName := requireEnv(t, table.env)
		rows := runCrossEngineSQL(t, ctx, "spark", sparkCommand, "select count(*) from "+tableName)
		if len(rows) == 0 {
			t.Fatalf("%s dataset table %s returned no count rows", table.id, tableName)
		}
	}
}

func TestIcebergTierAReadTimeTravelSchemaEvolutionDeleteApply(t *testing.T) {
	requireTierAEnabled(t)
	commands := map[string]string{
		"mo":    requireSQLCommand(t, "mo", crossEngineMOCommandEnv),
		"spark": requireSQLCommand(t, "spark", crossEngineSparkCommandEnv),
	}
	scenarios := loadTierAScenarios(t)
	ctx, cancel := context.WithTimeoutCause(context.Background(), crossEngineTimeout(), api.CauseForCode(api.ErrPlanningTimeout))
	defer cancel()

	for _, scenario := range scenarios {
		t.Run(scenario.ID+"_"+scenario.Name, func(t *testing.T) {
			moSQL := expandTierAEnv(t, scenario.MOSQL)
			moActions := expandTierAActions(t, scenario.MOActions)
			if len(scenario.MOActions) > 0 {
				runTierAMOActions(t, ctx, commands["mo"], moActions)
			}
			if scenario.ExpectMOErr != "" {
				moOutput := requireTierAMOError(t, ctx, commands["mo"], moSQL, expandTierAEnv(t, scenario.ExpectMOErr))
				writeTierAReport(t, scenario, tierAComparisonReport{
					CaseID:          scenario.ID,
					Scenario:        scenario.Name,
					Status:          "success",
					FailureCategory: "expected_mo_error",
					MOActions:       moActions,
					MOSQL:           moSQL,
					MOChecksum:      crossEngineFingerprint(normalizeCrossEngineRows(moOutput)),
					MORowCount:      len(normalizeCrossEngineRows(moOutput)),
					ComparisonMode:  "expected_error",
					GeneratedAtUTC:  time.Now().UTC().Format(time.RFC3339Nano),
					EngineVersions:  tierAEngineVersions(),
				}, normalizeCrossEngineRows(moOutput), nil)
				return
			}
			sparkSQL := expandTierAEnv(t, scenario.SparkSQL)
			moRows := runCrossEngineSQL(t, ctx, "mo", commands["mo"], moSQL)
			sparkRows := runCrossEngineSQL(t, ctx, "spark", commands["spark"], sparkSQL)
			moFingerprint := crossEngineFingerprint(moRows)
			sparkFingerprint := crossEngineFingerprint(sparkRows)
			report := tierAComparisonReport{
				CaseID:                 scenario.ID,
				Scenario:               scenario.Name,
				Status:                 "success",
				MOActions:              moActions,
				MOSQL:                  moSQL,
				SparkSQL:               sparkSQL,
				MOChecksum:             moFingerprint,
				SparkChecksum:          sparkFingerprint,
				MORowCount:             len(moRows),
				SparkRowCount:          len(sparkRows),
				ComparisonMode:         "unordered_fingerprint",
				NullSentinel:           "<NULL>",
				TimestampNormalization: "iceberg_semantics",
				GeneratedAtUTC:         time.Now().UTC().Format(time.RFC3339Nano),
				EngineVersions:         tierAEngineVersions(),
			}
			if sparkFingerprint != moFingerprint {
				report.Status = "failed"
				report.FailureCategory = "data_mismatch"
				writeTierAReport(t, scenario, report, moRows, sparkRows)
				t.Fatalf("%s %s Spark diff mismatch\nMO SQL:\n%s\nSpark SQL:\n%s\nMO:    %s\nSpark: %s\nMO rows:\n%s\nSpark rows:\n%s",
					scenario.ID, scenario.Name, moSQL, sparkSQL, moFingerprint, sparkFingerprint, strings.Join(moRows, "\n"), strings.Join(sparkRows, "\n"))
			}
			writeTierAReport(t, scenario, report, moRows, sparkRows)
		})
	}
}

func expandTierAActions(t *testing.T, actions []string) []string {
	t.Helper()
	if len(actions) == 0 {
		return nil
	}
	expanded := make([]string, 0, len(actions))
	for _, action := range actions {
		if strings.TrimSpace(action) == "" {
			continue
		}
		expanded = append(expanded, expandTierAEnv(t, action))
	}
	return expanded
}

func runTierAMOActions(t *testing.T, ctx context.Context, command string, actions []string) {
	t.Helper()
	for _, action := range actions {
		if strings.TrimSpace(action) == "" {
			continue
		}
		if out, err := runTierACommandOutput(ctx, command, action); err != nil {
			t.Fatalf("MO action failed: %v\nSQL:\n%s\nOutput:\n%s", err, action, string(out))
		}
	}
}

func requireTierAEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv(tierAEnableEnv) != "1" {
		t.Skipf("set %s=1 to run Local Nessie + MinIO Iceberg Tier A integration tests", tierAEnableEnv)
	}
}

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		t.Fatalf("%s is required when %s=1", key, tierAEnableEnv)
	}
	return value
}

func requireSQLCommand(t *testing.T, engine, key string) string {
	t.Helper()
	command := requireEnv(t, key)
	if !strings.Contains(command, "{sql}") {
		t.Fatalf("%s command template %s must contain {sql}: %s", engine, key, command)
	}
	return command
}

func tierAHTTPGet(t *testing.T, rawURL, bearerToken string) {
	t.Helper()
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, api.CauseForCode(api.ErrCatalogUnavailable))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("build GET %s: %v", rawURL, err)
	}
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", rawURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("GET %s returned %s", rawURL, resp.Status)
	}
}

func tierACatalogConfigURL(t *testing.T, rawURI string) string {
	t.Helper()
	parsed, err := url.Parse(strings.TrimRight(rawURI, "/"))
	if err != nil {
		t.Fatalf("parse %s: %v", tierACatalogURIEnv, err)
	}
	path := strings.TrimRight(parsed.Path, "/")
	if strings.HasSuffix(path, "/v1") || path == "/v1" {
		parsed.Path = path + "/config"
	} else {
		parsed.Path = path + "/v1/config"
	}
	return parsed.String()
}

func tierAMinIOHealthURL(t *testing.T, rawEndpoint string) string {
	t.Helper()
	endpoint := strings.TrimRight(rawEndpoint, "/")
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse %s: %v", tierAS3EndpointEnv, err)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/minio/health/live"
	return parsed.String()
}

func loadTierAScenarios(t *testing.T) []tierAScenario {
	t.Helper()
	path := strings.TrimSpace(os.Getenv(tierAScenarioEnv))
	requireFullCoverage := path == ""
	if path == "" {
		path = filepath.Join("testdata", "tier_a_scenarios.json")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Tier A scenarios %s: %v", path, err)
	}
	var scenarios []tierAScenario
	if err := json.Unmarshal(data, &scenarios); err != nil {
		t.Fatalf("parse Tier A scenarios %s: %v", path, err)
	}
	validateTierAScenarios(t, scenarios, requireFullCoverage)
	return scenarios
}

func validateTierAScenarios(t *testing.T, scenarios []tierAScenario, requireFullCoverage bool) {
	t.Helper()
	if len(scenarios) == 0 {
		t.Fatalf("Tier A scenarios must not be empty")
	}
	covered := make(map[string]bool)
	for _, scenario := range scenarios {
		if strings.TrimSpace(scenario.ID) == "" || strings.TrimSpace(scenario.Name) == "" {
			t.Fatalf("Tier A scenario id and name are required: %+v", scenario)
		}
		if strings.TrimSpace(scenario.MOSQL) == "" || strings.TrimSpace(scenario.SparkSQL) == "" {
			if strings.TrimSpace(scenario.ExpectMOErr) != "" && strings.TrimSpace(scenario.MOSQL) != "" {
				covered[scenario.ID] = true
				continue
			}
			t.Fatalf("Tier A scenario %s/%s requires mo_sql and spark_sql", scenario.ID, scenario.Name)
		}
		covered[scenario.ID] = true
	}
	if !requireFullCoverage {
		return
	}
	for _, required := range []string{"ICE-TEST-113", "ICE-TEST-114", "ICE-TEST-115", "ICE-TEST-116", "ICE-TEST-117", "ICE-TEST-118", "ICE-TEST-119", "ICE-TEST-120"} {
		if !covered[required] {
			t.Fatalf("Tier A scenarios must cover %s", required)
		}
	}
}

func expandTierAEnv(t *testing.T, value string) string {
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
		t.Fatalf("missing Tier A scenario environment variables: %s", strings.Join(missing, ", "))
	}
	if strings.TrimSpace(expanded) == "" {
		t.Fatalf("Tier A scenario SQL expanded to empty string from %q", value)
	}
	return expanded
}

func requireTierAMOError(t *testing.T, ctx context.Context, command, sql, expected string) string {
	t.Helper()
	out, err := runTierACommandOutput(ctx, command, sql)
	if err == nil {
		t.Fatalf("expected MO query to fail with %q, but it succeeded\nSQL:\n%s\nOutput:\n%s", expected, sql, string(out))
	}
	if !strings.Contains(string(out), expected) {
		t.Fatalf("expected MO query failure to contain %q\nSQL:\n%s\nOutput:\n%s", expected, sql, string(out))
	}
	return string(out)
}

func runTierACommandOutput(ctx context.Context, command, sql string) ([]byte, error) {
	rendered := strings.ReplaceAll(command, "{sql}", shellQuote(sql))
	cmd := exec.CommandContext(ctx, "sh", "-c", rendered)
	return cmd.CombinedOutput()
}

type tierAComparisonReport struct {
	CaseID                 string            `json:"case_id"`
	Scenario               string            `json:"scenario"`
	Status                 string            `json:"status"`
	FailureCategory        string            `json:"failure_category,omitempty"`
	MOActions              []string          `json:"mo_actions,omitempty"`
	MOSQL                  string            `json:"mo_sql,omitempty"`
	SparkSQL               string            `json:"spark_sql,omitempty"`
	MORowCount             int               `json:"mo_row_count,omitempty"`
	SparkRowCount          int               `json:"spark_row_count,omitempty"`
	MOChecksum             string            `json:"mo_checksum,omitempty"`
	SparkChecksum          string            `json:"spark_checksum,omitempty"`
	ComparisonMode         string            `json:"comparison_mode"`
	NullSentinel           string            `json:"null_sentinel,omitempty"`
	TimestampNormalization string            `json:"timestamp_normalization,omitempty"`
	GeneratedAtUTC         string            `json:"generated_at_utc"`
	EngineVersions         map[string]string `json:"engine_versions,omitempty"`
}

func writeTierAReport(t *testing.T, scenario tierAScenario, report tierAComparisonReport, moRows, sparkRows []string) {
	t.Helper()
	root := strings.TrimSpace(os.Getenv(tierAReportDirEnv))
	if root == "" {
		return
	}
	caseDir := filepath.Join(root, safeTierAReportName(scenario.ID+"_"+scenario.Name))
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		t.Fatalf("create Tier A report dir %s: %v", caseDir, err)
	}
	writeTierAReportFile(t, filepath.Join(caseDir, "mo.out"), strings.Join(redactTierAReportRows(moRows), "\n"))
	writeTierAReportFile(t, filepath.Join(caseDir, "spark.out"), strings.Join(redactTierAReportRows(sparkRows), "\n"))
	writeTierAReportJSON(t, filepath.Join(caseDir, "metadata.json"), report)
	writeTierAReportJSON(t, filepath.Join(caseDir, "diff.json"), map[string]any{
		"status":           report.Status,
		"failure_category": report.FailureCategory,
		"mo_checksum":      report.MOChecksum,
		"spark_checksum":   report.SparkChecksum,
		"mo_row_count":     report.MORowCount,
		"spark_row_count":  report.SparkRowCount,
	})
	summary := fmt.Sprintf("# %s %s\n\nstatus: %s\ncomparison_mode: %s\nmo_rows: %d\nspark_rows: %d\nmo_checksum: `%s`\nspark_checksum: `%s`\n",
		report.CaseID, report.Scenario, report.Status, report.ComparisonMode, report.MORowCount, report.SparkRowCount, report.MOChecksum, report.SparkChecksum)
	writeTierAReportFile(t, filepath.Join(caseDir, "summary.md"), summary)
}

func writeTierAReportJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal Tier A report %s: %v", path, err)
	}
	writeTierAReportFile(t, path, string(data)+"\n")
}

func writeTierAReportFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(redactTierAReportText(value)), 0o644); err != nil {
		t.Fatalf("write Tier A report %s: %v", path, err)
	}
}

func safeTierAReportName(value string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, value)
}

func tierAEngineVersions() map[string]string {
	out := make(map[string]string)
	for key, env := range map[string]string{
		"mo":      "MO_ICEBERG_MO_VERSION",
		"spark":   "MO_ICEBERG_SPARK_VERSION",
		"iceberg": "MO_ICEBERG_ICEBERG_VERSION",
		"nessie":  "MO_ICEBERG_NESSIE_VERSION",
		"minio":   "MO_ICEBERG_MINIO_VERSION",
	} {
		if value := strings.TrimSpace(os.Getenv(env)); value != "" {
			out[key] = value
		}
	}
	return out
}

func redactTierAReportRows(rows []string) []string {
	out := make([]string, len(rows))
	for idx, row := range rows {
		out[idx] = redactTierAReportText(row)
	}
	return out
}

func redactTierAReportText(value string) string {
	for _, key := range []string{
		"MO_ICEBERG_CATALOG_TOKEN",
		"MO_ICEBERG_S3_AK",
		"MO_ICEBERG_S3_SK",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SESSION_TOKEN",
	} {
		if secret := strings.TrimSpace(os.Getenv(key)); secret != "" {
			value = strings.ReplaceAll(value, secret, "<redacted>")
		}
	}
	return value
}
