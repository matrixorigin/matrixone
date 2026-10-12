// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

type staticDrainStateReader struct {
	rowsValue        []row
	restOffsetValue  int64
	groupOffsetValue int64
	groupFound       bool
	logEndValue      int64
	logEndFound      bool
}

func (r staticDrainStateReader) rows(context.Context) ([]row, error) {
	return r.rowsValue, nil
}

func (r staticDrainStateReader) restOffset(context.Context) (int64, error) {
	return r.restOffsetValue, nil
}

func (r staticDrainStateReader) groupOffset(context.Context) (int64, bool, error) {
	return r.groupOffsetValue, r.groupFound, nil
}

func (r staticDrainStateReader) logEndOffset(context.Context) (int64, bool, error) {
	return r.logEndValue, r.logEndFound, nil
}

func TestWaitForDrainReturnsEveryVerifiedOffset(t *testing.T) {
	rows := expectedFinalRows()
	reader := staticDrainStateReader{
		rowsValue:        sortedRows(rows),
		restOffsetValue:  7,
		groupOffsetValue: 7,
		groupFound:       true,
		logEndValue:      7,
		logEndFound:      true,
	}
	receipt, err := waitForDrain(t.Context(), reader, rows, 7)
	if err != nil {
		t.Fatal(err)
	}
	if want := (drainReceipt{RESTOffset: 7, KafkaGroupOffset: 7, KafkaLogEndOffset: 7}); !reflect.DeepEqual(receipt, want) {
		t.Fatalf("drain receipt = %+v, want %+v", receipt, want)
	}
}

func TestWaitForDrainRejectsGroupOffsetBehindSinkAndLogEnd(t *testing.T) {
	reader := staticDrainStateReader{
		rowsValue:        sortedRows(expectedFinalRows()),
		restOffsetValue:  7,
		groupOffsetValue: 6,
		groupFound:       true,
		logEndValue:      7,
		logEndFound:      true,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	if _, err := waitForDrain(ctx, reader, expectedFinalRows(), 7); err == nil {
		t.Fatal("waitForDrain accepted a lagging Kafka consumer group")
	}
}

func TestPhaseReportIncludesZeroOffsets(t *testing.T) {
	data, err := json.Marshal(phase{})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"expected_offset", "rest_offset", "kafka_group_offset", "kafka_log_end_offset"} {
		if _, exists := fields[name]; !exists {
			t.Errorf("phase receipt omitted %s at offset zero: %s", name, data)
		}
	}
}

func TestRunResultMarksFailedReport(t *testing.T) {
	report := runReport{Status: "running"}
	setRunResult(&report, errors.New("sink offset did not reach the expected value"))
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "failed" || got.Error != "sink offset did not reach the expected value" {
		t.Fatalf("failed run report = %+v, want failed status and error", got)
	}
}

func TestKafkaConnectRunnerFailsWhenOnlyPortBlockIsLeased(t *testing.T) {
	allocatePorts := kafkaConnectRunnerFunction(t, "allocate_ports", "collect_artifacts()")
	script := strings.Join([]string{
		"set -euo pipefail",
		"PORT_BLOCK_WIDTH=80",
		"PORT_BLOCK_BASE=''",
		"MO_PORT='' STATUS_PORT='' KAFKA_PORT='' CONNECT_PORT=''",
		"die() { echo \"$*\" >&2; exit 1; }",
		"log() { :; }",
		"acquire_port_lease() { return 1; }",
		"mo_port_block_candidates() { echo 36000; }",
		"mo_port_block_is_available() { return 0; }",
		"mo_port_block_release_lease() { :; }",
		allocatePorts,
		"allocate_ports",
	}, "\n")
	cmd := exec.Command("bash", "-c", script)
	output, runErr := cmd.CombinedOutput()
	if runErr == nil {
		t.Fatalf("runner succeeded despite the only port block being leased:\n%s", output)
	}
	if !strings.Contains(string(output), "could not find an available 80-port range") {
		t.Fatalf("runner did not report exhausted port candidates (error %v):\n%s", runErr, output)
	}
}

func TestKafkaConnectRunnerMarksCleanupFailureInReport(t *testing.T) {
	cleanup := kafkaConnectRunnerFunction(t, "cleanup", "run_e2e()")
	root := t.TempDir()
	reportDir := filepath.Join(root, "reports")
	if err := os.Mkdir(reportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	tmpDir := filepath.Join(root, "mo-kafka-connect-e2e.test")
	if err := os.Mkdir(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(reportDir, "report.json"), []byte(`{"status":"passed"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	script := strings.Join([]string{
		"set -u",
		"CLEANUP_ERRORS=()",
		"log() { :; }",
		"record_cleanup_error() { CLEANUP_ERRORS+=(\"$1\"); }",
		"mo_e2e_stop_service() { return 0; }",
		"collect_artifacts() { return 0; }",
		"compose() { return 19; }",
		"mo_port_block_release_lease() { return 0; }",
		cleanup,
		"cleanup",
	}, "\n")
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append(os.Environ(),
		"REPORT_DIR="+reportDir,
		"TMP_DIR="+tmpDir,
		"MO_PID=",
		"CURRENT_STAGE=run_recovery_driver",
		"COMPOSE_PROJECT_NAME=mo-kc-test",
		"SOURCE_SHA=test-revision",
	)
	output, runErr := cmd.CombinedOutput()
	if runErr == nil {
		t.Fatalf("cleanup returned success when Compose cleanup failed:\n%s", output)
	}
	var report struct {
		Status        string   `json:"status"`
		CleanupStatus string   `json:"cleanup_status"`
		CleanupErrors []string `json:"cleanup_errors"`
		Error         string   `json:"error"`
	}
	data, err := os.ReadFile(filepath.Join(reportDir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Status != "failed" || report.CleanupStatus != "failed" || len(report.CleanupErrors) == 0 || !strings.Contains(report.Error, "docker compose down failed") {
		t.Fatalf("cleanup failure report = %+v, want failed status and Compose cleanup error; runner output: %s", report, output)
	}
}

func TestKafkaConnectRunnerFailsWhenArtifactRedactionFails(t *testing.T) {
	collectArtifacts := kafkaConnectRunnerFunction(t, "collect_artifacts", "cleanup()")
	root := t.TempDir()
	reportDir := filepath.Join(root, "reports")
	tmpDir := filepath.Join(root, "run")
	if err := os.Mkdir(reportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	psCalled := filepath.Join(root, "ps-called")
	script := strings.Join([]string{
		"set -u",
		"TMP_DIR=\"$TEST_TMP_DIR\"",
		"REPORT_DIR=\"$TEST_REPORT_DIR\"",
		"compose() { case \"$1\" in logs) return 0 ;; ps) : >\"$TEST_PS_CALLED\"; return 0 ;; esac; }",
		"python3() { return 17; }",
		collectArtifacts,
		"collect_artifacts",
	}, "\n")
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append(os.Environ(),
		"TEST_TMP_DIR="+tmpDir,
		"TEST_REPORT_DIR="+reportDir,
		"TEST_PS_CALLED="+psCalled,
	)
	output, runErr := cmd.CombinedOutput()
	if runErr == nil {
		t.Fatalf("artifact collection succeeded when log redaction failed:\n%s", output)
	}
	if _, err := os.Stat(psCalled); !os.IsNotExist(err) {
		t.Fatalf("Compose ps ran after log redaction failed; got stat error %v", err)
	}
}

func TestWaitForDatabaseRetriesTransientPingFailure(t *testing.T) {
	pinger := &sequenceDatabasePinger{}
	if err := waitForDatabase(t.Context(), pinger); err != nil {
		t.Fatalf("waitForDatabase() returned error after a transient PingContext failure: %v", err)
	}
	if pinger.calls != 2 {
		t.Fatalf("PingContext calls = %d, want first failed ping followed by successful retry", pinger.calls)
	}
}

type sequenceDatabasePinger struct {
	calls int
}

func (p *sequenceDatabasePinger) PingContext(context.Context) error {
	p.calls++
	if p.calls == 1 {
		return errors.New("transient connection refusal")
	}
	return nil
}

func TestSinkConfigUsesDebeziumJdbcBatchSize(t *testing.T) {
	config := sinkConfig(runOptions{dsn: "root:111@tcp(127.0.0.1:6001)/"})
	if got := config["batch.size"]; got != "1" {
		t.Fatalf("batch.size = %q, want 1", got)
	}
	if _, exists := config["max.batch.size"]; exists {
		t.Fatal("sink config contains unsupported max.batch.size property")
	}
}

func kafkaConnectRunnerFunction(t *testing.T, name, next string) string {
	t.Helper()
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate test source")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(testFile), "..", ".."))
	runner, err := os.ReadFile(filepath.Join(repoRoot, "optools", "kafka_connect_ci.bash"))
	if err != nil {
		t.Fatal(err)
	}
	startToken := name + "() {"
	start := strings.Index(string(runner), startToken)
	if start < 0 {
		t.Fatalf("could not find %s in Kafka Connect runner", startToken)
	}
	endToken := "\n}\n\n" + next
	end := strings.Index(string(runner)[start:], endToken)
	if end < 0 {
		t.Fatalf("could not find end marker %q for %s", next, name)
	}
	return string(runner[start : start+end+2])
}

func TestExpectedFinalRowsHaveUniqueIDsAndCoverRecoveryContract(t *testing.T) {
	rows := expectedFinalRows()
	if err := validateUniqueRowIDs(rows); err != nil {
		t.Fatalf("final recovery fixture must contain each primary key once: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("final row count = %d, want 4", len(rows))
	}
	byID := make(map[int64]row, len(rows))
	for _, item := range rows {
		byID[item.ID] = item
	}
	if _, exists := byID[1]; exists {
		t.Fatal("final recovery fixture must omit the tombstoned row with ID 1")
	}
	if got := byID[2].Note; got == nil || *got != "" {
		t.Fatalf("ID 2 note = %v, want a non-NULL empty string", got)
	}
	if got := byID[3]; got.Name != "after-update" || got.Counter != 33 || got.Note == nil || *got.Note != "更新" {
		t.Fatalf("ID 3 = %+v, want the updated row", got)
	}
	if got := byID[5].Note; got != nil {
		t.Fatalf("ID 5 note = %q, want SQL NULL", *got)
	}

	duplicate := append(append([]row(nil), rows...), rows[0])
	if err := validateUniqueRowIDs(duplicate); err == nil || !errors.Is(err, errDuplicateRowID) {
		t.Fatalf("duplicate primary key error = %v, want duplicate-row error", err)
	}
}

func TestJSONConverterEnvelopesPreserveKeyAndNullSemantics(t *testing.T) {
	key, err := encodeKey(17)
	if err != nil {
		t.Fatal(err)
	}
	var decodedKey struct {
		Schema struct {
			Fields []struct {
				Field string `json:"field"`
			} `json:"fields"`
		} `json:"schema"`
		Payload map[string]int64 `json:"payload"`
	}
	if err := json.Unmarshal(key, &decodedKey); err != nil {
		t.Fatal(err)
	}
	if len(decodedKey.Schema.Fields) != 1 || decodedKey.Schema.Fields[0].Field != "id" || decodedKey.Payload["id"] != 17 {
		t.Fatalf("unexpected Kafka key envelope: %s", key)
	}

	for _, test := range []struct {
		name string
		note *string
		want any
	}{
		{name: "SQL NULL", note: nil, want: nil},
		{name: "empty string", note: stringPointer(""), want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, err := encodeValue(row{ID: 17, Name: "fixture", Note: test.note, Counter: 1})
			if err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				Schema struct {
					Fields []struct {
						Field    string `json:"field"`
						Optional bool   `json:"optional"`
					} `json:"fields"`
				} `json:"schema"`
				Payload map[string]any `json:"payload"`
			}
			if err := json.Unmarshal(value, &decoded); err != nil {
				t.Fatal(err)
			}
			if len(decoded.Schema.Fields) != 4 || decoded.Schema.Fields[2].Field != "note" || !decoded.Schema.Fields[2].Optional {
				t.Fatalf("note must be an optional schema field: %s", value)
			}
			if decoded.Payload["note"] != test.want {
				t.Fatalf("note payload = %#v, want %#v", decoded.Payload["note"], test.want)
			}
		})
	}
}
