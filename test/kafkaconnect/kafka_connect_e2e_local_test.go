// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
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

func TestWaitForDrainRejectsRESTOffsetBehindSinkAndLogEnd(t *testing.T) {
	reader := staticDrainStateReader{
		rowsValue:        sortedRows(expectedFinalRows()),
		restOffsetValue:  6,
		groupOffsetValue: 7,
		groupFound:       true,
		logEndValue:      7,
		logEndFound:      true,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	if _, err := waitForDrain(ctx, reader, expectedFinalRows(), 7); err == nil {
		t.Fatal("waitForDrain accepted a lagging Connect REST offset")
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
	setRunResult(&report, nil)
	if report.Status != "passed" || report.Error != "sink offset did not reach the expected value" {
		t.Fatalf("successful run result = %+v, want passed status", report)
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

func TestKafkaConnectRunnerUsesFreshReportBeforePreflight(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate test source")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(testFile), "..", ".."))
	reportDir := filepath.Join(t.TempDir(), "reports")
	if err := os.Mkdir(reportDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{
		"report.json":        `{"status":"passed"}`,
		"setup-failure.json": `{"status":"failed","stage":"previous-run"}`,
		"keep-me.txt":        "unrelated evidence\n",
	} {
		if err := os.WriteFile(filepath.Join(reportDir, name), []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Keep docker out of PATH to fail in preflight before creating any owned
	// service, while preserving the small set of tools needed to write evidence.
	pathDir := t.TempDir()
	for _, name := range []string{"dirname", "mkdir", "python3", "rm"} {
		path, err := exec.LookPath(name)
		if err != nil {
			t.Fatalf("look up %s: %v", name, err)
		}
		if err := os.Symlink(path, filepath.Join(pathDir, name)); err != nil {
			t.Fatalf("link %s: %v", name, err)
		}
	}

	cmd := exec.Command("/bin/bash", filepath.Join(repoRoot, "optools", "kafka_connect_ci.bash"), "e2e-local")
	for _, item := range os.Environ() {
		if strings.HasPrefix(item, "PATH=") || strings.HasPrefix(item, "MO_KAFKA_CONNECT_REPORT_DIR=") {
			continue
		}
		cmd.Env = append(cmd.Env, item)
	}
	cmd.Env = append(cmd.Env, "PATH="+pathDir, "MO_KAFKA_CONNECT_REPORT_DIR="+reportDir)
	output, runErr := cmd.CombinedOutput()
	if runErr == nil || !strings.Contains(string(output), "required command not found: docker") {
		t.Fatalf("runner preflight error = %v, output:\n%s", runErr, output)
	}
	if _, err := os.Stat(filepath.Join(reportDir, "report.json")); !os.IsNotExist(err) {
		t.Fatalf("stale report.json remains after failed run; stat error = %v", err)
	}
	var failure struct {
		Status    string `json:"status"`
		Stage     string `json:"stage"`
		SourceSHA string `json:"source_sha"`
		ExitCode  int    `json:"exit_status"`
	}
	failureData, err := os.ReadFile(filepath.Join(reportDir, "setup-failure.json"))
	if err != nil {
		t.Fatalf("read current setup failure: %v; runner output:\n%s", err, output)
	}
	if err := json.Unmarshal(failureData, &failure); err != nil {
		t.Fatalf("decode current setup failure: %v", err)
	}
	if failure.Status != "failed" || failure.Stage != "preflight" || failure.ExitCode == 0 {
		t.Fatalf("setup-failure.json = %+v, want current failed preflight evidence", failure)
	}
	if got, err := os.ReadFile(filepath.Join(reportDir, "keep-me.txt")); err != nil || string(got) != "unrelated evidence\n" {
		t.Fatalf("unrelated report artifact = %q, %v; want it preserved", got, err)
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

type recoveryTestEvent struct {
	offset      int64
	value       *row
	tombstoneID *int64
}

type recoveryTestRuntime struct {
	rowsValue        []row
	rowsByID         map[int64]row
	events           []recoveryTestEvent
	nextOffset       int64
	groupOffsetValue int64
	restOffsetValue  int64
	groupFound       bool
	workerStopped    bool
	actions          []string
	prepareErr       error
	createTopicErr   error
}

func (r *recoveryTestRuntime) prepareTarget(context.Context) error {
	r.actions = append(r.actions, "prepare")
	if r.prepareErr != nil {
		return r.prepareErr
	}
	r.rowsByID = make(map[int64]row)
	r.rowsValue = nil
	r.events = nil
	r.nextOffset = 0
	r.groupOffsetValue = 0
	r.restOffsetValue = 0
	r.groupFound = false
	r.workerStopped = false
	return nil
}

func (r *recoveryTestRuntime) createTopic(context.Context) error {
	r.actions = append(r.actions, "topic")
	return r.createTopicErr
}

func (r *recoveryTestRuntime) produceRows(_ context.Context, values []row) ([]producedRecord, error) {
	records := make([]producedRecord, 0, len(values))
	for _, value := range values {
		copyValue := value
		r.events = append(r.events, recoveryTestEvent{offset: r.nextOffset, value: &copyValue})
		records = append(records, producedRecord{Partition: 0, Offset: r.nextOffset, ID: value.ID})
		r.nextOffset++
		if !r.workerStopped {
			r.apply(r.events[len(r.events)-1])
		}
	}
	if !r.workerStopped {
		r.groupOffsetValue = r.nextOffset
		r.restOffsetValue = r.nextOffset
		r.groupFound = true
	}
	r.actions = append(r.actions, "produce")
	return records, nil
}

func (r *recoveryTestRuntime) produceTombstone(_ context.Context, id int64) ([]producedRecord, error) {
	idCopy := id
	r.events = append(r.events, recoveryTestEvent{offset: r.nextOffset, tombstoneID: &idCopy})
	record := producedRecord{Partition: 0, Offset: r.nextOffset, ID: id, Tombstone: true}
	r.nextOffset++
	if !r.workerStopped {
		r.apply(r.events[len(r.events)-1])
		r.groupOffsetValue = r.nextOffset
		r.restOffsetValue = r.nextOffset
		r.groupFound = true
	}
	r.actions = append(r.actions, "tombstone")
	return []producedRecord{record}, nil
}

func (r *recoveryTestRuntime) killWorker(context.Context) error {
	r.workerStopped = true
	r.actions = append(r.actions, "kill")
	return nil
}

func (r *recoveryTestRuntime) startWorker(context.Context) error {
	r.workerStopped = false
	r.rowsByID = make(map[int64]row)
	for _, event := range r.events {
		r.apply(event)
	}
	r.groupOffsetValue = r.nextOffset
	r.restOffsetValue = r.nextOffset
	r.groupFound = true
	r.actions = append(r.actions, "start")
	return nil
}

func (r *recoveryTestRuntime) rows(context.Context) ([]row, error) {
	return append([]row(nil), r.rowsValue...), nil
}

func (r *recoveryTestRuntime) restOffset(context.Context) (int64, error) {
	return r.restOffsetValue, nil
}

func (r *recoveryTestRuntime) groupOffset(context.Context) (int64, bool, error) {
	return r.groupOffsetValue, r.groupFound, nil
}

func (r *recoveryTestRuntime) logEndOffset(context.Context) (int64, bool, error) {
	return r.nextOffset, r.nextOffset > 0, nil
}

func (r *recoveryTestRuntime) apply(event recoveryTestEvent) {
	if event.tombstoneID != nil {
		delete(r.rowsByID, *event.tombstoneID)
	} else if event.value != nil {
		r.rowsByID[event.value.ID] = *event.value
	}
	values := make([]row, 0, len(r.rowsByID))
	for _, value := range r.rowsByID {
		values = append(values, value)
	}
	r.rowsValue = sortedRows(values)
}

type recoveryTestConnect struct {
	runtime      *recoveryTestRuntime
	state        string
	patchErr     error
	actions      []string
	registers    int
	patchStopped bool
	stoppedSeen  bool
}

func (c *recoveryTestConnect) handler(w http.ResponseWriter, request *http.Request) {
	writeJSON := func(value any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(value)
	}
	switch {
	case request.Method == http.MethodPost && request.URL.Path == "/connectors":
		c.registers++
		c.actions = append(c.actions, "register")
		c.state = "RUNNING"
		w.WriteHeader(http.StatusCreated)
	case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/status"):
		statusState := c.state
		if c.state == "STOPPING" {
			c.state = "STOPPED"
		} else if statusState == "STOPPED" {
			c.stoppedSeen = true
		}
		writeJSON(connectorStatusResponse{
			Connector: connectorStatus{Name: "fixture", State: statusState, Worker: "worker-0"},
			Tasks:     []taskStatus{{ID: 0, State: statusState, Worker: "worker-0"}},
		})
	case request.Method == http.MethodPut && strings.HasSuffix(request.URL.Path, "/stop"):
		c.actions = append(c.actions, "stop")
		c.state = "STOPPING"
		w.WriteHeader(http.StatusAccepted)
	case request.Method == http.MethodPut && strings.HasSuffix(request.URL.Path, "/resume"):
		c.actions = append(c.actions, "resume")
		c.state = "RUNNING"
		c.runtime.restOffsetValue = c.runtime.nextOffset
		w.WriteHeader(http.StatusAccepted)
	case request.Method == http.MethodPatch && strings.HasSuffix(request.URL.Path, "/offsets"):
		c.actions = append(c.actions, "patch")
		if c.state != "STOPPED" {
			http.Error(w, "connector is not stopped", http.StatusConflict)
			return
		}
		if c.patchErr != nil {
			http.Error(w, c.patchErr.Error(), http.StatusInternalServerError)
			return
		}
		var offsets connectorOffsets
		if err := json.NewDecoder(request.Body).Decode(&offsets); err != nil || len(offsets.Offsets) != 1 || offsets.Offsets[0].Offset == nil {
			http.Error(w, "malformed offsets", http.StatusBadRequest)
			return
		}
		c.runtime.restOffsetValue = offsets.Offsets[0].Offset.KafkaOffset
		c.patchStopped = true
		w.WriteHeader(http.StatusAccepted)
	case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/offsets"):
		writeJSON(connectorOffsets{Offsets: []connectorOffset{{
			Partition: offsetPartition{KafkaTopic: topicName, KafkaPartition: 0},
			Offset:    &offsetValue{KafkaOffset: c.runtime.restOffsetValue},
		}}})
	default:
		http.NotFound(w, request)
	}
}

func newRecoveryFixture(t *testing.T, patchErr error) (*recoveryTestRuntime, *recoveryTestConnect, connectClient, runOptions, *runReport, func()) {
	t.Helper()
	runtime := &recoveryTestRuntime{}
	serverState := &recoveryTestConnect{runtime: runtime, patchErr: patchErr}
	server := httptest.NewServer(http.HandlerFunc(serverState.handler))
	opts := runOptions{connectorName: "fixture", reportDir: t.TempDir(), phaseTimeout: 3 * time.Second}
	report := &runReport{SinkConsumerGroup: "connect-fixture", Phases: []phase{}}
	connect := connectClient{base: server.URL, client: server.Client()}
	return runtime, serverState, connect, opts, report, server.Close
}

func TestRunRecoveryExercisesSuccessfulRecoveryAndReplay(t *testing.T) {
	runtime, server, connect, opts, report, closeServer := newRecoveryFixture(t, nil)
	defer closeServer()
	if err := runRecovery(t.Context(), opts, report, connect, runtime); err != nil {
		t.Fatal(err)
	}
	if runtime.rowsValue == nil || !reflect.DeepEqual(runtime.rowsValue, sortedRows(expectedFinalRows())) {
		t.Fatalf("final fake rows = %v, want %v", runtime.rowsValue, sortedRows(expectedFinalRows()))
	}
	if server.registers != 1 {
		t.Fatalf("connector registrations = %d, want one registration across recovery", server.registers)
	}
	wantActions := []string{"register", "stop", "patch", "resume"}
	for _, want := range wantActions {
		found := false
		for _, action := range server.actions {
			if action == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("REST actions = %v, missing %q", server.actions, want)
		}
	}
	patchIndex := -1
	resumeIndex := -1
	stopIndex := -1
	for index, action := range server.actions {
		switch action {
		case "stop":
			stopIndex = index
		case "patch":
			patchIndex = index
		case "resume":
			resumeIndex = index
		}
	}
	if !(stopIndex >= 0 && stopIndex < patchIndex && patchIndex < resumeIndex) {
		t.Fatalf("REST actions = %v, want STOPPED before PATCH before resume", server.actions)
	}
	if !server.patchStopped {
		t.Fatalf("PATCH was accepted without a confirmed STOPPED status: %v", server.actions)
	}
	if !server.stoppedSeen {
		t.Fatalf("PATCH proceeded without observing a STOPPED status response: %v", server.actions)
	}
	if len(report.Phases) != 4 || report.Phases[0].Name != "baseline" || report.Phases[1].Name != "worker-crash-catch-up" || report.Phases[2].Name != "controlled-replay" || report.Phases[3].Name != "empty-work-restart" {
		t.Fatalf("recovery phases = %+v, want baseline, catch-up, replay, and empty restart", report.Phases)
	}
}

func TestRunRecoveryStopsWhenPatchIsRejected(t *testing.T) {
	patchErr := errors.New("offset patch rejected")
	runtime, server, connect, opts, report, closeServer := newRecoveryFixture(t, patchErr)
	defer closeServer()
	if err := runRecovery(t.Context(), opts, report, connect, runtime); err == nil || !strings.Contains(err.Error(), "rewind owned sink offset") {
		t.Fatalf("runRecovery error = %v, want offset rewind failure", err)
	}
	for _, action := range server.actions {
		if action == "resume" {
			t.Fatalf("REST resumed after rejected patch: %v", server.actions)
		}
	}
}

func TestRunRecoveryStopsBeforeConnectorWhenTargetPreparationFails(t *testing.T) {
	runtime := &recoveryTestRuntime{prepareErr: errors.New("target unavailable")}
	opts := runOptions{connectorName: "fixture", reportDir: t.TempDir(), phaseTimeout: time.Second}
	report := &runReport{SinkConsumerGroup: "connect-fixture", Phases: []phase{}}
	connect := connectClient{base: "http://127.0.0.1:1", client: &http.Client{}}
	if err := runRecovery(t.Context(), opts, report, connect, runtime); err == nil || !strings.Contains(err.Error(), "target unavailable") {
		t.Fatalf("runRecovery error = %v, want target preparation failure", err)
	}
	if len(runtime.actions) != 1 || runtime.actions[0] != "prepare" {
		t.Fatalf("runtime actions = %v, want preparation to be the only action", runtime.actions)
	}
}

func TestPrepareTargetAndReadRows(t *testing.T) {
	t.Run("prepare succeeds", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, statement := range []string{
			"CREATE DATABASE IF NOT EXISTS " + databaseName,
			"DROP TABLE IF EXISTS " + databaseName + "." + tableName,
			"CREATE TABLE " + databaseName + "." + tableName,
		} {
			mock.ExpectExec(statement).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		if err := prepareTarget(t.Context(), db); err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("prepare stops on first failed statement", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectExec("CREATE DATABASE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("DROP TABLE IF EXISTS").WillReturnError(errors.New("table locked"))
		if err := prepareTarget(t.Context(), db); err == nil || !strings.Contains(err.Error(), "table locked") {
			t.Fatalf("prepareTarget error = %v, want table lock failure", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("read rows preserves null and empty values", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		empty := ""
		mock.ExpectQuery("SELECT id, name, note, counter").WillReturnRows(
			sqlmock.NewRows([]string{"id", "name", "note", "counter"}).
				AddRow(int64(1), "null-note", nil, int64(1)).
				AddRow(int64(2), "empty-note", empty, int64(2)),
		)
		got, err := readRows(t.Context(), db)
		if err != nil {
			t.Fatal(err)
		}
		want := []row{{ID: 1, Name: "null-note", Counter: 1}, {ID: 2, Name: "empty-note", Note: &empty, Counter: 2}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("readRows = %+v, want %+v", got, want)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("read rows reports query and scan failures", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SELECT id, name, note, counter").WillReturnError(errors.New("database offline"))
		if _, err := readRows(t.Context(), db); err == nil || !strings.Contains(err.Error(), "database offline") {
			t.Fatalf("readRows query error = %v", err)
		}
		mock.ExpectQuery("SELECT id, name, note, counter").WillReturnRows(
			sqlmock.NewRows([]string{"id", "name", "note", "counter"}).AddRow("bad-id", "name", nil, int64(1)),
		)
		if _, err := readRows(t.Context(), db); err == nil {
			t.Fatal("readRows accepted a row that cannot be scanned")
		}
		_ = db.Close()
	})
}

func TestConnectClientReadinessAndConnectorOperations(t *testing.T) {
	var receivedConfig map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/":
			_, _ = w.Write([]byte(`{"version":"3.8.1","kafka_cluster_id":"cluster-1"}`))
		case request.Method == http.MethodGet && request.URL.Path == "/connector-plugins":
			_, _ = w.Write([]byte(`[{"class":"io.debezium.connector.jdbc.JdbcSinkConnector","type":"sink","version":"2.6"}]`))
		case request.Method == http.MethodPost && request.URL.Path == "/connectors":
			if err := json.NewDecoder(request.Body).Decode(&receivedConfig); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
		case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/status"):
			_, _ = w.Write([]byte(`{"connector":{"name":"fixture","state":"RUNNING","worker_id":"worker-0"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"worker-0"}]}`))
		case request.Method == http.MethodPut && (strings.HasSuffix(request.URL.Path, "/stop") || strings.HasSuffix(request.URL.Path, "/resume")):
			w.WriteHeader(http.StatusNoContent)
		case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/offsets"):
			_, _ = w.Write([]byte(`{"offsets":[{"partition":{"kafka_topic":"mo_issue_29848_events","kafka_partition":0},"offset":{"kafka_offset":12}}]}`))
		case request.Method == http.MethodPatch && strings.HasSuffix(request.URL.Path, "/offsets"):
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	connect := connectClient{base: server.URL, client: server.Client()}
	root, err := waitForConnect(t.Context(), connect)
	if err != nil || root.Version != "3.8.1" || root.KafkaClusterID != "cluster-1" {
		t.Fatalf("waitForConnect = %+v, %v", root, err)
	}
	plugins, err := connect.plugins(t.Context())
	if err != nil || len(plugins) != 1 || plugins[0].Version != "2.6" {
		t.Fatalf("plugins = %+v, %v", plugins, err)
	}
	if err := connect.createConnector(t.Context(), "fixture", map[string]string{"topics": topicName}); err != nil {
		t.Fatal(err)
	}
	if receivedConfig["name"] != "fixture" {
		t.Fatalf("connector request = %#v, want connector name", receivedConfig)
	}
	status, err := connect.status(t.Context(), "fixture")
	if err != nil || status.Connector.State != "RUNNING" || len(status.Tasks) != 1 {
		t.Fatalf("status = %+v, %v", status, err)
	}
	if err := connect.stopConnector(t.Context(), "fixture"); err != nil {
		t.Fatal(err)
	}
	if err := connect.resumeConnector(t.Context(), "fixture"); err != nil {
		t.Fatal(err)
	}
	offset, err := connect.restOffset(t.Context(), "fixture", topicName, 0)
	if err != nil || offset != 12 {
		t.Fatalf("restOffset = %d, %v", offset, err)
	}
	if err := connect.patchOffset(t.Context(), "fixture", topicName, 0, 7); err != nil {
		t.Fatal(err)
	}

	badJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/connector-plugins" || strings.HasSuffix(request.URL.Path, "/status") {
			_, _ = w.Write([]byte("{"))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer badJSON.Close()
	badConnect := connectClient{base: badJSON.URL, client: badJSON.Client()}
	if _, err := badConnect.plugins(t.Context()); err == nil {
		t.Fatal("plugins accepted malformed JSON")
	}
	if _, err := badConnect.status(t.Context(), "fixture"); err == nil {
		t.Fatal("status accepted malformed JSON")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := waitForConnect(ctx, badConnect); err == nil {
		t.Fatal("waitForConnect accepted a canceled readiness context")
	}
	if _, err := (connectClient{base: "http://[", client: http.DefaultClient}).request(t.Context(), http.MethodGet, "/", nil, http.StatusOK); err == nil {
		t.Fatal("request accepted an invalid URL")
	}
}

func TestConnectorDrainReaderAndLiveRuntimeDelegations(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT id, name, note, counter").WillReturnRows(
		sqlmock.NewRows([]string{"id", "name", "note", "counter"}).AddRow(int64(1), "one", nil, int64(1)),
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"offsets":[{"partition":{"kafka_topic":"mo_issue_29848_events","kafka_partition":0},"offset":{"kafka_offset":1}}]}`))
	}))
	defer server.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reader := connectorDrainReader{
		db: db, admin: kadm.NewClient(client),
		connect: &connectClient{base: server.URL, client: server.Client()}, name: "fixture",
	}
	if rows, err := reader.rows(t.Context()); err != nil || len(rows) != 1 {
		t.Fatalf("connectorDrainReader.rows = %+v, %v", rows, err)
	}
	if offset, err := reader.restOffset(t.Context()); err != nil || offset != 1 {
		t.Fatalf("connectorDrainReader.restOffset = %d, %v", offset, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := reader.groupOffset(canceled); err == nil {
		t.Fatal("connectorDrainReader.groupOffset unexpectedly succeeded")
	}
	if _, _, err := reader.logEndOffset(canceled); err == nil {
		t.Fatal("connectorDrainReader.logEndOffset unexpectedly succeeded")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	for _, statement := range []string{
		"CREATE DATABASE IF NOT EXISTS " + databaseName,
		"DROP TABLE IF EXISTS " + databaseName + "." + tableName,
		"CREATE TABLE " + databaseName + "." + tableName,
	} {
		mock.ExpectExec(statement).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	runtime := liveRecoveryRuntime{connectorDrainReader: reader, kafka: client, opts: runOptions{composeProject: "fixture", composeBase: t.TempDir() + "/base.yaml", composeConnect: t.TempDir() + "/connect.yaml"}}
	if err := runtime.prepareTarget(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runtime.createTopic(ctx); err == nil {
		t.Fatal("live runtime createTopic unexpectedly succeeded")
	}
	if _, err := runtime.produceRows(ctx, []row{{ID: 1, Name: "one", Counter: 1}}); err == nil {
		t.Fatal("live runtime produceRows unexpectedly succeeded")
	}
	if _, err := runtime.produceTombstone(ctx, 1); err == nil {
		t.Fatal("live runtime produceTombstone unexpectedly succeeded")
	}
	dockerDir := t.TempDir()
	docker := filepath.Join(dockerDir, "docker")
	if err := os.WriteFile(docker, []byte("#!/bin/sh\ncase \"$1\" in\ncompose) printf 'container-1\\n' ;;\nkill) ;;\ninspect) printf 'exited\\n' ;;\nesac\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dockerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := runtime.killWorker(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.startWorker(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeAndKafkaHelperFailures(t *testing.T) {
	if _, err := decodeKeyID([]byte("{")); err == nil {
		t.Fatal("decodeKeyID accepted malformed JSON")
	}
	if got, err := decodeKeyID([]byte(`{"payload":{"id":17}}`)); err != nil || got != 17 {
		t.Fatalf("decodeKeyID = %d, %v, want 17", got, err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	records := []*kgo.Record{{Topic: topicName, Key: []byte(`{"payload":{"id":1}}`), Value: []byte(`{}`)}}
	if _, err := produce(ctx, client, records); err == nil {
		t.Fatal("produce unexpectedly succeeded with a canceled context")
	}
	if _, err := produceRows(ctx, client, []row{{ID: 1, Name: "one", Counter: 1}}); err == nil {
		t.Fatal("produceRows unexpectedly succeeded with a canceled context")
	}
	if _, err := produceTombstone(ctx, client, 1); err == nil {
		t.Fatal("produceTombstone unexpectedly succeeded with a canceled context")
	}
	_, admin := canceledKafkaAdmin(t)
	if err := createTopic(ctx, admin); err == nil {
		t.Fatal("createTopic unexpectedly succeeded with a canceled context")
	}
	if _, _, err := groupOffset(ctx, admin, "fixture"); err == nil {
		t.Fatal("groupOffset unexpectedly succeeded with a canceled context")
	}
	if _, _, err := topicLogEndOffset(ctx, admin); err == nil {
		t.Fatal("topicLogEndOffset unexpectedly succeeded with a canceled context")
	}
}

func TestKafkaHelpersAgainstInProcessBroker(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := createTopic(ctx, admin); err != nil {
		t.Fatal(err)
	}
	if err := createTopic(ctx, admin); err != nil {
		t.Fatalf("idempotent topic creation failed: %v", err)
	}
	records, err := produceRows(ctx, client, []row{{ID: 1, Name: "one", Counter: 1}, {ID: 2, Name: "two", Note: stringPointer(""), Counter: 2}})
	if err != nil || len(records) != 2 || records[0].Offset != 0 || records[1].Offset != 1 {
		t.Fatalf("produceRows records = %+v, err=%v", records, err)
	}
	tombstone, err := produceTombstone(ctx, client, 1)
	if err != nil || len(tombstone) != 1 || !tombstone[0].Tombstone || tombstone[0].Offset != 2 {
		t.Fatalf("produceTombstone records = %+v, err=%v", tombstone, err)
	}
	end, found, err := topicLogEndOffset(ctx, admin)
	if err != nil || !found || end != 3 {
		t.Fatalf("topicLogEndOffset = %d, found=%v, err=%v", end, found, err)
	}
	responses, err := admin.CommitOffsets(ctx, "fixture", (kadm.OffsetsList{{Topic: topicName, Partition: 0, At: 3}}).Offsets())
	if err != nil || !responses.Ok() {
		t.Fatalf("commit fixture offset = %+v, err=%v", responses, err)
	}
	group, found, err := groupOffset(ctx, admin, "fixture")
	if err != nil || !found || group != 3 {
		t.Fatalf("groupOffset = %d, found=%v, err=%v", group, found, err)
	}
}

func TestWaitForDatabaseFailureAndPortParsing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := waitForDatabase(ctx, alwaysFailPinger{}); err == nil || !strings.Contains(err.Error(), "last ping") {
		t.Fatalf("waitForDatabase error = %v, want last ping context", err)
	}
	for _, test := range []struct {
		dsn  string
		want string
	}{
		{dsn: "root@tcp(127.0.0.1:6601)/", want: "6601"},
		{dsn: "root@tcp(127.0.0.1)/", want: "6001"},
		{dsn: "root@/", want: "6001"},
	} {
		if got := portFromDSN(test.dsn); got != test.want {
			t.Fatalf("portFromDSN(%q) = %q, want %q", test.dsn, got, test.want)
		}
	}
}

func TestPollAndWriteReportFailureBranches(t *testing.T) {
	if err := poll(t.Context(), "scripted check", time.Second, func(context.Context) (bool, error) {
		return false, errors.New("check failed")
	}); err == nil || !strings.Contains(err.Error(), "scripted check") {
		t.Fatalf("poll error = %v, want scripted check failure", err)
	}
	reportDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(reportDir, "report.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeReport(reportDir, runReport{Status: "failed"}); err == nil {
		t.Fatal("writeReport replaced an existing report directory")
	}
}

type alwaysFailPinger struct{}

func (alwaysFailPinger) PingContext(context.Context) error { return errors.New("ping failed") }

func canceledKafkaAdmin(t *testing.T) (*kgo.Client, *kadm.Client) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, kadm.NewClient(client)
}

func TestOwnedConnectDockerHelpers(t *testing.T) {
	composeDir := t.TempDir()
	opts := runOptions{composeProject: "fixture", composeBase: filepath.Join(composeDir, "base.yaml"), composeConnect: filepath.Join(composeDir, "connect.yaml")}
	dockerDir := t.TempDir()
	docker := filepath.Join(dockerDir, "docker")
	if err := os.WriteFile(docker, []byte("#!/bin/sh\ncase \"$1\" in\ncompose) case \"$*\" in *\"ps -q connect\"*) printf 'container-1\\n' ;; *\"start connect\"*) printf 'started\\n' ;; *) printf 'compose-output\\n' ;; esac ;;\nkill) ;;\ninspect) printf 'exited\\n' ;;\nesac\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dockerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if output, err := composeOutput(t.Context(), opts, "config"); err != nil || !strings.Contains(output, "compose-output") {
		t.Fatalf("composeOutput = %q, %v", output, err)
	}
	if err := startOwnedConnect(t.Context(), opts); err != nil {
		t.Fatal(err)
	}
	if err := killOwnedConnect(t.Context(), opts); err != nil {
		t.Fatal(err)
	}

	emptyDockerDir := t.TempDir()
	emptyDocker := filepath.Join(emptyDockerDir, "docker")
	if err := os.WriteFile(emptyDocker, []byte("#!/bin/sh\nif [ \"$1\" = compose ]; then exit 0; fi\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", emptyDockerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := killOwnedConnect(t.Context(), opts); err == nil || !strings.Contains(err.Error(), "container was not found") {
		t.Fatal("killOwnedConnect accepted an empty container id")
	}

	failingDockerDir := t.TempDir()
	failingDocker := filepath.Join(failingDockerDir, "docker")
	if err := os.WriteFile(failingDocker, []byte("#!/bin/sh\nprintf 'docker failed\\n'\nexit 17\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", failingDockerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if _, err := composeOutput(t.Context(), opts, "config"); err == nil {
		t.Fatal("composeOutput accepted a failed docker command")
	}
}

func TestComposeOutputResolvesRelativePathsBeforeChangingDirectory(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate test source")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(testFile), "..", ".."))
	originalWorkingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(repoRoot); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalWorkingDirectory); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})

	const relativeBase = "etc/launch-kafka-local/compose.yaml"
	const relativeConnect = "etc/launch-kafka-local/compose.connect.yaml"
	basePath, err := filepath.Abs(relativeBase)
	if err != nil {
		t.Fatal(err)
	}
	connectPath, err := filepath.Abs(relativeConnect)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{basePath, connectPath} {
		if !filepath.IsAbs(path) {
			t.Fatalf("Compose path %q is not absolute", path)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected Compose file %q to exist: %v", path, err)
		}
	}

	dockerDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "docker-args.txt")
	dockerPath := filepath.Join(dockerDir, "docker")
	dockerScript := "#!/bin/sh\nprintf '%s\\n' \"$PWD\" >\"$TEST_DOCKER_LOG\"\nprintf '%s\\n' \"$@\" >>\"$TEST_DOCKER_LOG\"\n"
	if err := os.WriteFile(dockerPath, []byte(dockerScript), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dockerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TEST_DOCKER_LOG", logPath)
	opts := runOptions{
		composeProject: "fixture",
		composeBase:    relativeBase,
		composeConnect: relativeConnect,
	}
	if output, err := composeOutput(t.Context(), opts, "config"); err != nil {
		t.Fatalf("composeOutput = %q, %v", output, err)
	}
	log, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(log)), "\n")
	want := []string{filepath.Dir(basePath), "compose", "-p", "fixture", "-f", basePath, "-f", connectPath, "config"}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("docker working directory and arguments = %q, want %q", lines, want)
	}
}

func TestRunRejectsUnsafeOrUnavailableSetup(t *testing.T) {
	report := &runReport{Phases: []phase{}}
	if err := run(t.Context(), runOptions{reportDir: t.TempDir()}, report); err == nil || !strings.Contains(err.Error(), "compose project is required") {
		t.Fatal("run accepted an empty compose project")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report = &runReport{Phases: []phase{}}
	opts := runOptions{
		composeProject: "fixture",
		dsn:            "root:111@tcp(127.0.0.1:1)/?timeout=1ms",
		reportDir:      t.TempDir(),
		phaseTimeout:   time.Second,
	}
	if err := run(ctx, opts, report); err == nil || !strings.Contains(err.Error(), "MatrixOne SQL readiness") {
		t.Fatalf("run setup error = %v, want MatrixOne readiness failure", err)
	}
}

func TestRunMainValidatesArgumentsAndRecordsFailures(t *testing.T) {
	if code := runMain([]string{"-h"}); code != 0 {
		t.Fatalf("runMain help code = %d, want 0", code)
	}
	if code := runMain([]string{"-timeout=0"}); code != 2 {
		t.Fatalf("runMain timeout validation code = %d, want 2", code)
	}
	if code := runMain([]string{"-unknown-flag"}); code != 2 {
		t.Fatalf("runMain flag parsing code = %d, want 2", code)
	}
	badReportPath := filepath.Join(t.TempDir(), "report-file")
	if err := os.WriteFile(badReportPath, []byte("occupied"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := runMain([]string{"-report-dir", badReportPath}); code != 1 {
		t.Fatalf("runMain report setup code = %d, want 1", code)
	}
	if code := runMain([]string{"-report-dir", t.TempDir()}); code != 1 {
		t.Fatalf("runMain setup failure code = %d, want 1", code)
	}
}

func TestRunUsesInjectableReadinessAndRecoveryDependencies(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectPing().WillReturnError(nil)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/":
			_, _ = w.Write([]byte(`{"version":"3.8.1","kafka_cluster_id":"cluster-1"}`))
		case "/connector-plugins":
			_, _ = w.Write([]byte(`[{"class":"io.debezium.connector.jdbc.JdbcSinkConnector","type":"sink","version":"2.6"}]`))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	recoveryCalled := false
	dependencies := runDependencies{
		openDatabase: func(string) (*sql.DB, error) { return db, nil },
		newKafkaClient: func(...kgo.Opt) (*kgo.Client, error) {
			return client, nil
		},
		runRecoveryFunc: func(_ context.Context, opts runOptions, report *runReport, connect connectClient, runtime recoveryRuntime) error {
			recoveryCalled = true
			if opts.connectorName != "fixture" || report.ConnectVersion != "3.8.1" || connect.base != server.URL || runtime == nil {
				return errors.New("run did not pass initialized dependencies")
			}
			return nil
		},
	}
	report := &runReport{Phases: []phase{}}
	opts := runOptions{
		composeProject: "fixture", connectorName: "fixture", connectURL: server.URL,
		reportDir: t.TempDir(), dsn: "ignored", phaseTimeout: time.Second,
	}
	if err := run(t.Context(), opts, report, dependencies); err != nil {
		t.Fatal(err)
	}
	if !recoveryCalled || report.ConnectVersion != "3.8.1" || report.KafkaClusterID != "cluster-1" || report.JDBCPluginVersion != "2.6" {
		t.Fatalf("run report/dependency state = %+v, recovery called=%v", report, recoveryCalled)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRunReportsDependencyFailureBranches(t *testing.T) {
	baseOptions := func(reportDir string, connectURL string) runOptions {
		return runOptions{composeProject: "fixture", connectURL: connectURL, reportDir: reportDir, phaseTimeout: 20 * time.Millisecond}
	}
	newDatabase := func(t *testing.T) *sql.DB {
		t.Helper()
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectPing().WillReturnError(nil)
		t.Cleanup(func() {
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
			_ = db.Close()
		})
		return db
	}
	root := `{"version":"3.8.1","kafka_cluster_id":"cluster-1"}`
	plugins := `[{"class":"io.debezium.connector.jdbc.JdbcSinkConnector","type":"sink","version":"2.6"}]`
	newServer := func(t *testing.T, rootBody, pluginsBody string, pluginStatus int) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch request.URL.Path {
			case "/":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(rootBody))
			case "/connector-plugins":
				w.WriteHeader(pluginStatus)
				_, _ = w.Write([]byte(pluginsBody))
			default:
				http.NotFound(w, request)
			}
		}))
	}

	t.Run("open database", func(t *testing.T) {
		dependencies := defaultRunDependencies()
		dependencies.openDatabase = func(string) (*sql.DB, error) { return nil, errors.New("driver unavailable") }
		report := &runReport{Phases: []phase{}}
		err := run(t.Context(), baseOptions(t.TempDir(), "http://127.0.0.1:1"), report, dependencies)
		if err == nil || !strings.Contains(err.Error(), "open MatrixOne DSN") {
			t.Fatalf("run error = %v, want database open failure", err)
		}
	})

	t.Run("initial report stage", func(t *testing.T) {
		db, _, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		badPath := filepath.Join(t.TempDir(), "report-file")
		if err := os.WriteFile(badPath, []byte("occupied"), 0o644); err != nil {
			t.Fatal(err)
		}
		dependencies := defaultRunDependencies()
		dependencies.openDatabase = func(string) (*sql.DB, error) { return db, nil }
		report := &runReport{Phases: []phase{}}
		runErr := run(t.Context(), baseOptions(badPath, "http://127.0.0.1:1"), report, dependencies)
		if runErr == nil {
			t.Fatal("run accepted a report path that is a file")
		}
	})

	t.Run("connect readiness", func(t *testing.T) {
		db := newDatabase(t)
		server := newServer(t, `{"version":""}`, plugins, http.StatusOK)
		defer server.Close()
		dependencies := defaultRunDependencies()
		dependencies.openDatabase = func(string) (*sql.DB, error) { return db, nil }
		report := &runReport{Phases: []phase{}}
		err := run(t.Context(), baseOptions(t.TempDir(), server.URL), report, dependencies)
		if err == nil || !strings.Contains(err.Error(), "wait for Kafka Connect") {
			t.Fatalf("run error = %v, want Connect readiness failure", err)
		}
	})

	t.Run("plugin REST failure", func(t *testing.T) {
		db := newDatabase(t)
		server := newServer(t, root, "plugin unavailable", http.StatusServiceUnavailable)
		defer server.Close()
		dependencies := defaultRunDependencies()
		dependencies.openDatabase = func(string) (*sql.DB, error) { return db, nil }
		report := &runReport{Phases: []phase{}}
		err := run(t.Context(), baseOptions(t.TempDir(), server.URL), report, dependencies)
		if err == nil || !strings.Contains(err.Error(), "list Kafka Connect plugins") {
			t.Fatalf("run error = %v, want plugin REST failure", err)
		}
	})

	t.Run("missing sink plugin", func(t *testing.T) {
		db := newDatabase(t)
		server := newServer(t, root, `[]`, http.StatusOK)
		defer server.Close()
		dependencies := defaultRunDependencies()
		dependencies.openDatabase = func(string) (*sql.DB, error) { return db, nil }
		report := &runReport{Phases: []phase{}}
		err := run(t.Context(), baseOptions(t.TempDir(), server.URL), report, dependencies)
		if err == nil || !strings.Contains(err.Error(), "required sink plugin") {
			t.Fatalf("run error = %v, want missing plugin failure", err)
		}
	})

	t.Run("Kafka client creation", func(t *testing.T) {
		db := newDatabase(t)
		server := newServer(t, root, plugins, http.StatusOK)
		defer server.Close()
		dependencies := defaultRunDependencies()
		dependencies.openDatabase = func(string) (*sql.DB, error) { return db, nil }
		dependencies.newKafkaClient = func(...kgo.Opt) (*kgo.Client, error) { return nil, errors.New("broker unavailable") }
		report := &runReport{Phases: []phase{}}
		err := run(t.Context(), baseOptions(t.TempDir(), server.URL), report, dependencies)
		if err == nil || !strings.Contains(err.Error(), "create Kafka client") {
			t.Fatalf("run error = %v, want Kafka client failure", err)
		}
	})
}
