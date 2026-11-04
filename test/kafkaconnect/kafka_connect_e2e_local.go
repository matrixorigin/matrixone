// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command kafka_connect_e2e_local qualifies one real Kafka Connect JDBC sink
// recovery path against an owned MatrixOne, Kafka broker, and Connect worker.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topicName      = "mo_issue_29848_events"
	databaseName   = "kafka_connect_recovery"
	tableName      = "events"
	connectorClass = "io.debezium.connector.jdbc.JdbcSinkConnector"
)

var errDuplicateRowID = errors.New("duplicate expected row ID")

type row struct {
	ID      int64
	Name    string
	Note    *string
	Counter int64
}

type producedRecord struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
	ID        int64 `json:"id"`
	Tombstone bool  `json:"tombstone"`
}

type phase struct {
	Name              string           `json:"name"`
	Status            string           `json:"status"`
	ExpectedOffset    int64            `json:"expected_offset"`
	RESTOffset        int64            `json:"rest_offset"`
	KafkaGroupOffset  int64            `json:"kafka_group_offset"`
	KafkaLogEndOffset int64            `json:"kafka_log_end_offset"`
	Produced          []producedRecord `json:"produced,omitempty"`
	Rows              []rowReport      `json:"rows,omitempty"`
	ConnectorState    string           `json:"connector_state,omitempty"`
	WorkerRestarted   bool             `json:"worker_restarted,omitempty"`
	ControlledReplay  bool             `json:"controlled_replay,omitempty"`
	Detail            string           `json:"detail,omitempty"`
}

type rowReport struct {
	ID      int64   `json:"id"`
	Name    string  `json:"name"`
	Note    *string `json:"note"`
	Counter int64   `json:"counter"`
}

type runReport struct {
	Status            string  `json:"status"`
	CurrentStage      string  `json:"current_stage,omitempty"`
	StartedAt         string  `json:"started_at"`
	FinishedAt        string  `json:"finished_at,omitempty"`
	SourceSHA         string  `json:"source_sha"`
	HarnessSHA        string  `json:"harness_sha"`
	MOBinarySHA256    string  `json:"mo_binary_sha256"`
	KafkaBootstrap    string  `json:"kafka_bootstrap"`
	ConnectURL        string  `json:"connect_url"`
	ConnectorName     string  `json:"connector_name"`
	SinkConsumerGroup string  `json:"sink_consumer_group"`
	ConnectVersion    string  `json:"connect_version,omitempty"`
	KafkaClusterID    string  `json:"kafka_cluster_id,omitempty"`
	JDBCPluginVersion string  `json:"jdbc_plugin_version,omitempty"`
	Phases            []phase `json:"phases"`
	Error             string  `json:"error,omitempty"`
}

type connectRoot struct {
	Version        string `json:"version"`
	Commit         string `json:"commit"`
	KafkaClusterID string `json:"kafka_cluster_id"`
}

type connectorPlugin struct {
	Class   string `json:"class"`
	Type    string `json:"type"`
	Version string `json:"version"`
}

type connectorStatus struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Worker string `json:"worker_id"`
}

type taskStatus struct {
	ID     int    `json:"id"`
	State  string `json:"state"`
	Worker string `json:"worker_id"`
	Trace  string `json:"trace,omitempty"`
}

type connectorStatusResponse struct {
	Connector connectorStatus `json:"connector"`
	Tasks     []taskStatus    `json:"tasks"`
}

type offsetPartition struct {
	KafkaTopic     string `json:"kafka_topic"`
	KafkaPartition int32  `json:"kafka_partition"`
}

type offsetValue struct {
	KafkaOffset int64 `json:"kafka_offset"`
}

type connectorOffset struct {
	Partition offsetPartition `json:"partition"`
	Offset    *offsetValue    `json:"offset"`
}

type connectorOffsets struct {
	Offsets []connectorOffset `json:"offsets"`
}

type connectClient struct {
	base   string
	client *http.Client
}

type drainReceipt struct {
	RESTOffset        int64
	KafkaGroupOffset  int64
	KafkaLogEndOffset int64
}

type drainStateReader interface {
	rows(context.Context) ([]row, error)
	restOffset(context.Context) (int64, error)
	groupOffset(context.Context) (int64, bool, error)
	logEndOffset(context.Context) (int64, bool, error)
}

type connectorDrainReader struct {
	db      *sql.DB
	admin   *kadm.Client
	connect *connectClient
	name    string
}

func (r connectorDrainReader) rows(ctx context.Context) ([]row, error) {
	return readRows(ctx, r.db)
}

func (r connectorDrainReader) restOffset(ctx context.Context) (int64, error) {
	return r.connect.restOffset(ctx, r.name, topicName, 0)
}

func (r connectorDrainReader) groupOffset(ctx context.Context) (int64, bool, error) {
	return groupOffset(ctx, r.admin, "connect-"+r.name)
}

func (r connectorDrainReader) logEndOffset(ctx context.Context) (int64, bool, error) {
	return topicLogEndOffset(ctx, r.admin)
}

type recoveryRuntime interface {
	drainStateReader
	prepareTarget(context.Context) error
	createTopic(context.Context) error
	produceRows(context.Context, []row) ([]producedRecord, error)
	produceTombstone(context.Context, int64) ([]producedRecord, error)
	killWorker(context.Context) error
	startWorker(context.Context) error
}

type liveRecoveryRuntime struct {
	connectorDrainReader
	kafka *kgo.Client
	opts  runOptions
}

func (r liveRecoveryRuntime) prepareTarget(ctx context.Context) error {
	return prepareTarget(ctx, r.db)
}

func (r liveRecoveryRuntime) createTopic(ctx context.Context) error {
	return createTopic(ctx, r.admin)
}

func (r liveRecoveryRuntime) produceRows(ctx context.Context, rows []row) ([]producedRecord, error) {
	return produceRows(ctx, r.kafka, rows)
}

func (r liveRecoveryRuntime) produceTombstone(ctx context.Context, id int64) ([]producedRecord, error) {
	return produceTombstone(ctx, r.kafka, id)
}

func (r liveRecoveryRuntime) killWorker(ctx context.Context) error {
	return killOwnedConnect(ctx, r.opts)
}

func (r liveRecoveryRuntime) startWorker(ctx context.Context) error {
	return startOwnedConnect(ctx, r.opts)
}

type runOptions struct {
	dsn, bootstrap, connectURL, composeBase, composeConnect, composeProject string
	connectorName, sourceSHA, harnessSHA, moBinarySHA, reportDir            string
	totalTimeout, phaseTimeout                                              time.Duration
}

type runDependencies struct {
	openDatabase    func(string) (*sql.DB, error)
	newKafkaClient  func(...kgo.Opt) (*kgo.Client, error)
	runRecoveryFunc func(context.Context, runOptions, *runReport, connectClient, recoveryRuntime) error
}

func defaultRunDependencies() runDependencies {
	return runDependencies{
		openDatabase:    func(dsn string) (*sql.DB, error) { return sql.Open("mysql", dsn) },
		newKafkaClient:  kgo.NewClient,
		runRecoveryFunc: runRecovery,
	}
}

func main() {
	os.Exit(runMain(os.Args[1:]))
}

func runMain(args []string) int {
	var opts runOptions
	flags := flag.NewFlagSet("kafka_connect_e2e_local", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&opts.dsn, "dsn", "root:111@tcp(127.0.0.1:6001)/?timeout=5s&readTimeout=30s&writeTimeout=30s", "owned MatrixOne MySQL DSN")
	flags.StringVar(&opts.bootstrap, "bootstrap", "127.0.0.1:9092", "owned Kafka broker bootstrap address")
	flags.StringVar(&opts.connectURL, "connect-url", "http://127.0.0.1:8083", "owned Kafka Connect REST URL")
	flags.StringVar(&opts.composeBase, "compose-base", "etc/launch-kafka-local/compose.yaml", "base Kafka Compose file")
	flags.StringVar(&opts.composeConnect, "compose-connect", "etc/launch-kafka-local/compose.connect.yaml", "Connect Compose file")
	flags.StringVar(&opts.composeProject, "compose-project", "", "owned Compose project")
	flags.StringVar(&opts.connectorName, "connector-name", "mo-issue-29848-recovery", "owned sink connector name")
	flags.StringVar(&opts.sourceSHA, "source-sha", "unknown", "MatrixOne source revision")
	flags.StringVar(&opts.harnessSHA, "harness-sha", "unknown", "harness source revision")
	flags.StringVar(&opts.moBinarySHA, "mo-binary-sha", "unknown", "built MatrixOne binary SHA-256")
	flags.StringVar(&opts.reportDir, "report-dir", "test/kafkaconnect/reports/local", "evidence output directory")
	flags.DurationVar(&opts.totalTimeout, "timeout", 10*time.Minute, "maximum duration for the complete recovery run")
	flags.DurationVar(&opts.phaseTimeout, "phase-timeout", 90*time.Second, "maximum duration for each readiness or drain phase")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if opts.totalTimeout <= 0 || opts.phaseTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "timeout values must be positive")
		return 2
	}

	report := runReport{
		Status:            "running",
		StartedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		SourceSHA:         opts.sourceSHA,
		HarnessSHA:        opts.harnessSHA,
		MOBinarySHA256:    opts.moBinarySHA,
		KafkaBootstrap:    opts.bootstrap,
		ConnectURL:        opts.connectURL,
		ConnectorName:     opts.connectorName,
		SinkConsumerGroup: "connect-" + opts.connectorName,
		Phases:            []phase{},
	}
	if err := writeReport(opts.reportDir, report); err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("write initial evidence report: %w", err))
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), opts.totalTimeout)
	defer cancel()

	err := run(ctx, opts, &report)
	setRunResult(&report, err)
	report.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if writeErr := writeReport(opts.reportDir, report); writeErr != nil {
		if err == nil {
			err = fmt.Errorf("write evidence report: %w", writeErr)
		} else {
			err = errors.Join(err, fmt.Errorf("write evidence report: %w", writeErr))
		}
	}
	if err == nil {
		fmt.Printf("Kafka Connect recovery report: %s\n", filepath.Join(opts.reportDir, "report.json"))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func setRunResult(report *runReport, runErr error) {
	if runErr != nil {
		report.Status = "failed"
		report.Error = runErr.Error()
		return
	}
	report.Status = "passed"
}

func run(ctx context.Context, opts runOptions, report *runReport, provided ...runDependencies) error {
	dependencies := defaultRunDependencies()
	if len(provided) > 0 {
		dependencies = provided[0]
	}
	if opts.composeProject == "" {
		return errors.New("compose project is required to operate only on run-owned containers")
	}
	connect := connectClient{base: strings.TrimRight(opts.connectURL, "/"), client: &http.Client{Timeout: 10 * time.Second}}
	db, err := dependencies.openDatabase(opts.dsn)
	if err != nil {
		return fmt.Errorf("open MatrixOne DSN: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := setStage(opts, report, "wait-for-matrixone"); err != nil {
		return err
	}
	phaseCtx, phaseCancel := context.WithTimeout(ctx, opts.phaseTimeout)
	err = waitForDatabase(phaseCtx, db)
	phaseCancel()
	if err != nil {
		return err
	}
	if err := setStage(opts, report, "wait-for-connect"); err != nil {
		return err
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	root, err := waitForConnect(phaseCtx, connect)
	phaseCancel()
	if err != nil {
		return err
	}
	report.ConnectVersion = root.Version
	report.KafkaClusterID = root.KafkaClusterID
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	plugins, err := connect.plugins(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("list Kafka Connect plugins: %w", err)
	}
	pluginVersion := ""
	for _, plugin := range plugins {
		if plugin.Class == connectorClass && plugin.Type == "sink" {
			pluginVersion = plugin.Version
			break
		}
	}
	if pluginVersion == "" {
		return fmt.Errorf("required sink plugin %q is absent from /connector-plugins", connectorClass)
	}
	report.JDBCPluginVersion = pluginVersion
	kafka, err := dependencies.newKafkaClient(kgo.SeedBrokers(opts.bootstrap), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer kafka.Close()
	admin := kadm.NewClient(kafka)
	runtime := liveRecoveryRuntime{
		connectorDrainReader: connectorDrainReader{db: db, admin: admin, connect: &connect, name: opts.connectorName},
		kafka:                kafka,
		opts:                 opts,
	}
	return dependencies.runRecoveryFunc(ctx, opts, report, connect, runtime)
}

func runRecovery(ctx context.Context, opts runOptions, report *runReport, connect connectClient, runtime recoveryRuntime) error {
	if err := setStage(opts, report, "prepare-target-table"); err != nil {
		return err
	}
	phaseCtx, phaseCancel := context.WithTimeout(ctx, opts.phaseTimeout)
	err := runtime.prepareTarget(phaseCtx)
	phaseCancel()
	if err != nil {
		return err
	}
	if err := setStage(opts, report, "create-topic-and-connector"); err != nil {
		return err
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = runtime.createTopic(phaseCtx)
	phaseCancel()
	if err != nil {
		return err
	}
	config := sinkConfig(opts)
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = connect.createConnector(phaseCtx, opts.connectorName, config)
	phaseCancel()
	if err != nil {
		return err
	}
	if err := setStage(opts, report, "wait-for-initial-connector"); err != nil {
		return err
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = waitForConnector(phaseCtx, connect, opts.connectorName, "RUNNING")
	phaseCancel()
	if err != nil {
		return err
	}

	baseline := []row{
		{ID: 1, Name: "baseline-null", Note: nil, Counter: 1},
		{ID: 2, Name: "雪-empty", Note: stringPointer(""), Counter: 2},
		{ID: 3, Name: "before-update", Note: stringPointer("before"), Counter: 3},
	}
	if err := setStage(opts, report, "produce-baseline"); err != nil {
		return err
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	baselineRecords, err := runtime.produceRows(phaseCtx, baseline)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("produce baseline: %w", err)
	}
	baselineWant := baseline
	baselineOffset := lastOffset(baselineRecords) + 1
	if err := setStage(opts, report, "baseline-drain"); err != nil {
		return err
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	baselineReceipt, err := waitForDrain(phaseCtx, runtime, baselineWant, baselineOffset)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("baseline drain: %w", err)
	}
	if err := recordPhase(opts, report, phase{
		Name: "baseline", Status: "passed", ExpectedOffset: baselineOffset,
		RESTOffset: baselineReceipt.RESTOffset, KafkaGroupOffset: baselineReceipt.KafkaGroupOffset,
		KafkaLogEndOffset: baselineReceipt.KafkaLogEndOffset, Produced: baselineRecords,
		Rows: toRowReports(baselineWant), ConnectorState: "RUNNING",
	}); err != nil {
		return err
	}

	if err := setStage(opts, report, "stop-worker-and-produce-offline-records"); err != nil {
		return err
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = runtime.killWorker(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("SIGKILL owned Connect worker: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	currentOffset, ok, err := runtime.groupOffset(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("read sink consumer group offset after worker crash: %w", err)
	}
	if !ok || currentOffset != baselineOffset {
		return fmt.Errorf("crash checkpoint changed: got %d (present=%v), want baseline %d", currentOffset, ok, baselineOffset)
	}
	offline := []row{
		{ID: 4, Name: "offline-insert", Note: stringPointer("while worker stopped"), Counter: 4},
		{ID: 3, Name: "after-update", Note: stringPointer("更新"), Counter: 33},
		{ID: 5, Name: "offline-null", Note: nil, Counter: 5},
	}
	if err := setStage(opts, report, "produce-offline-range"); err != nil {
		return err
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	offlineRecords, err := runtime.produceRows(phaseCtx, offline[:1])
	phaseCancel()
	if err != nil {
		return fmt.Errorf("produce offline insert: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	updateRecords, err := runtime.produceRows(phaseCtx, offline[1:2])
	phaseCancel()
	if err != nil {
		return fmt.Errorf("produce offline update: %w", err)
	}
	offlineRecords = append(offlineRecords, updateRecords...)
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	tombstoneRecords, err := runtime.produceTombstone(phaseCtx, 1)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("produce keyed tombstone: %w", err)
	}
	offlineRecords = append(offlineRecords, tombstoneRecords...)
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	lastRecords, err := runtime.produceRows(phaseCtx, offline[2:])
	phaseCancel()
	if err != nil {
		return fmt.Errorf("produce final offline insert: %w", err)
	}
	offlineRecords = append(offlineRecords, lastRecords...)
	finalOffset := lastOffset(offlineRecords) + 1
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	unchangedRows, err := runtime.rows(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("read MatrixOne during worker outage: %w", err)
	}
	if !reflect.DeepEqual(unchangedRows, sortedRows(baselineWant)) {
		return fmt.Errorf("MatrixOne changed while Connect was stopped: got %v, want baseline %v", unchangedRows, sortedRows(baselineWant))
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	got, ok, err := runtime.groupOffset(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("read sink group offset while worker stopped: %w", err)
	} else if !ok || got != baselineOffset {
		return fmt.Errorf("sink checkpoint advanced while worker was stopped: got %d (present=%v), want %d", got, ok, baselineOffset)
	}
	allOffline := expectedFinalRows()
	if err := validateUniqueRowIDs(allOffline); err != nil {
		return fmt.Errorf("invalid expected final rows: %w", err)
	}
	if err := setStage(opts, report, "restart-worker-and-catch-up"); err != nil {
		return err
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = runtime.startWorker(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("restart same Connect worker: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = waitForConnector(phaseCtx, connect, opts.connectorName, "RUNNING")
	phaseCancel()
	if err != nil {
		return fmt.Errorf("connector recovery: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	catchUpReceipt, err := waitForDrain(phaseCtx, runtime, allOffline, finalOffset)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("post-crash catch-up: %w", err)
	}
	if err := recordPhase(opts, report, phase{
		Name: "worker-crash-catch-up", Status: "passed", ExpectedOffset: finalOffset,
		RESTOffset: catchUpReceipt.RESTOffset, KafkaGroupOffset: catchUpReceipt.KafkaGroupOffset,
		KafkaLogEndOffset: catchUpReceipt.KafkaLogEndOffset,
		Produced:          offlineRecords, Rows: toRowReports(allOffline), ConnectorState: "RUNNING", WorkerRestarted: true,
		Detail: "same broker volume, Connect worker container, connector name/config, and sink consumer group; no connector re-registration or offset reset",
	}); err != nil {
		return err
	}

	if err := setStage(opts, report, "stop-connector-for-replay"); err != nil {
		return err
	}
	if err := connect.stopConnector(ctx, opts.connectorName); err != nil {
		return fmt.Errorf("stop connector for controlled replay: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = waitForConnector(phaseCtx, connect, opts.connectorName, "STOPPED")
	phaseCancel()
	if err != nil {
		return fmt.Errorf("wait for stopped connector before replay: %w", err)
	}
	if err := setStage(opts, report, "rewind-offset-for-replay"); err != nil {
		return err
	}
	if err := connect.patchOffset(ctx, opts.connectorName, topicName, 0, offlineRecords[0].Offset); err != nil {
		return fmt.Errorf("rewind owned sink offset for replay: %w", err)
	}
	replayedOffset, err := connect.restOffset(ctx, opts.connectorName, topicName, 0)
	if err != nil {
		return fmt.Errorf("verify replay offset reset: %w", err)
	}
	if replayedOffset != offlineRecords[0].Offset {
		return fmt.Errorf("replay offset is %d, want %d", replayedOffset, offlineRecords[0].Offset)
	}
	if err := setStage(opts, report, "resume-connector-for-replay"); err != nil {
		return err
	}
	if err := connect.resumeConnector(ctx, opts.connectorName); err != nil {
		return fmt.Errorf("resume connector after controlled replay: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = waitForConnector(phaseCtx, connect, opts.connectorName, "RUNNING")
	phaseCancel()
	if err != nil {
		return fmt.Errorf("wait for connector after controlled replay: %w", err)
	}
	if err := setStage(opts, report, "replay-drain"); err != nil {
		return err
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	replayReceipt, err := waitForDrain(phaseCtx, runtime, allOffline, finalOffset)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("controlled replay: %w", err)
	}
	if err := recordPhase(opts, report, phase{
		Name: "controlled-replay", Status: "passed", ExpectedOffset: finalOffset,
		RESTOffset: replayReceipt.RESTOffset, KafkaGroupOffset: replayReceipt.KafkaGroupOffset,
		KafkaLogEndOffset: replayReceipt.KafkaLogEndOffset, Rows: toRowReports(allOffline),
		ConnectorState: "RUNNING", ControlledReplay: true,
		Detail: "connector stopped before the official offsets PATCH; replayed the owned offline range; exact destination rows remained unchanged",
	}); err != nil {
		return err
	}

	if err := setStage(opts, report, "empty-work-worker-restart"); err != nil {
		return err
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = runtime.killWorker(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("empty-work restart: stop owned Connect worker: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = runtime.startWorker(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("empty-work restart: start same Connect worker: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	err = waitForConnector(phaseCtx, connect, opts.connectorName, "RUNNING")
	phaseCancel()
	if err != nil {
		return fmt.Errorf("empty-work restart: connector did not recover: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	stableRows, err := runtime.rows(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("empty-work restart: read final rows: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	stableGroup, ok, err := runtime.groupOffset(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("empty-work restart: read sink offset: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	stableREST, err := connect.restOffset(phaseCtx, opts.connectorName, topicName, 0)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("empty-work restart: read Connect REST offset: %w", err)
	}
	phaseCtx, phaseCancel = context.WithTimeout(ctx, opts.phaseTimeout)
	stableLogEnd, endFound, err := runtime.logEndOffset(phaseCtx)
	phaseCancel()
	if err != nil {
		return fmt.Errorf("empty-work restart: read topic log end: %w", err)
	}
	if !ok || stableREST != finalOffset || stableGroup != finalOffset || !endFound || stableLogEnd != finalOffset || !reflect.DeepEqual(stableRows, sortedRows(allOffline)) {
		return fmt.Errorf("empty-work restart changed state: rows=%v REST offset=%d, group offset=%d (present=%v), log-end=%d (present=%v), want rows=%v offsets=%d", stableRows, stableREST, stableGroup, ok, stableLogEnd, endFound, sortedRows(allOffline), finalOffset)
	}
	if err := recordPhase(opts, report, phase{
		Name: "empty-work-restart", Status: "passed", ExpectedOffset: finalOffset,
		RESTOffset: stableREST, KafkaGroupOffset: stableGroup, KafkaLogEndOffset: stableLogEnd,
		Rows: toRowReports(allOffline), ConnectorState: "RUNNING", WorkerRestarted: true,
	}); err != nil {
		return err
	}
	if err := setStage(opts, report, "complete"); err != nil {
		return err
	}
	return nil
}

func sinkConfig(opts runOptions) map[string]string {
	return map[string]string{
		"connector.class":      connectorClass,
		"tasks.max":            "1",
		"connection.url":       fmt.Sprintf("jdbc:mysql://host.docker.internal:%s/%s?useSSL=false&serverTimezone=UTC&useServerPrepStmts=true&cachePrepStmts=true&rewriteBatchedStatements=false", portFromDSN(opts.dsn), databaseName),
		"connection.username":  "root",
		"connection.password":  "111",
		"insert.mode":          "upsert",
		"delete.enabled":       "true",
		"primary.key.mode":     "record_key",
		"schema.evolution":     "none",
		"use.time.zone":        "UTC",
		"topics":               topicName,
		"table.name.format":    tableName,
		"flush.max.retries":    "3",
		"flush.retry.delay.ms": "500",
		"batch.size":           "1",
	}
}

func portFromDSN(dsn string) string {
	start := strings.LastIndex(dsn, "tcp(")
	if start < 0 {
		return "6001"
	}
	start += len("tcp(")
	end := strings.Index(dsn[start:], ")")
	if end < 0 {
		return "6001"
	}
	address := dsn[start : start+end]
	_, port, err := net.SplitHostPort(address)
	if err == nil && port != "" {
		return port
	}
	return "6001"
}

type databasePinger interface {
	PingContext(context.Context) error
}

func waitForDatabase(ctx context.Context, db databasePinger) error {
	var lastPingErr error
	err := poll(ctx, "MatrixOne SQL readiness", time.Second, func(ctx context.Context) (bool, error) {
		if err := db.PingContext(ctx); err != nil {
			lastPingErr = err
			return false, nil
		}
		return true, nil
	})
	if err != nil && lastPingErr != nil {
		return fmt.Errorf("%w (last ping: %v)", err, lastPingErr)
	}
	return err
}

func prepareTarget(ctx context.Context, db *sql.DB) error {
	for _, statement := range []string{
		"CREATE DATABASE IF NOT EXISTS " + databaseName,
		"DROP TABLE IF EXISTS " + databaseName + "." + tableName,
		"CREATE TABLE " + databaseName + "." + tableName + " (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(128) NOT NULL, note VARCHAR(255), counter BIGINT NOT NULL)",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("prepare run-owned target (%s): %w", statement, err)
		}
	}
	return nil
}

func createTopic(ctx context.Context, admin *kadm.Client) error {
	resp, err := admin.CreateTopic(ctx, 1, 1, nil, topicName)
	if err != nil && !strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
		return fmt.Errorf("create one-partition fixture topic: %w", err)
	}
	if err == nil && resp.Err != nil && !strings.Contains(resp.Err.Error(), "TOPIC_ALREADY_EXISTS") {
		return fmt.Errorf("create fixture topic: %w", resp.Err)
	}
	return nil
}

func produceRows(ctx context.Context, client *kgo.Client, rows []row) ([]producedRecord, error) {
	records := make([]*kgo.Record, 0, len(rows))
	for _, value := range rows {
		key, err := encodeKey(value.ID)
		if err != nil {
			return nil, err
		}
		payload, err := encodeValue(value)
		if err != nil {
			return nil, err
		}
		records = append(records, &kgo.Record{Topic: topicName, Key: key, Value: payload})
	}
	return produce(ctx, client, records)
}

func produceTombstone(ctx context.Context, client *kgo.Client, id int64) ([]producedRecord, error) {
	key, err := encodeKey(id)
	if err != nil {
		return nil, err
	}
	return produce(ctx, client, []*kgo.Record{{Topic: topicName, Key: key}})
}

func produce(ctx context.Context, client *kgo.Client, records []*kgo.Record) ([]producedRecord, error) {
	results := client.ProduceSync(ctx, records...)
	out := make([]producedRecord, 0, len(results))
	for i, result := range results {
		if result.Err != nil {
			return nil, fmt.Errorf("produce record %d: %w", i, result.Err)
		}
		id, err := decodeKeyID(records[i].Key)
		if err != nil {
			return nil, err
		}
		out = append(out, producedRecord{Partition: result.Record.Partition, Offset: result.Record.Offset, ID: id, Tombstone: result.Record.Value == nil})
	}
	return out, nil
}

func encodeKey(id int64) ([]byte, error) {
	return json.Marshal(map[string]any{
		"schema":  map[string]any{"type": "struct", "name": "mo.issue29848.Key", "optional": false, "fields": []map[string]any{{"type": "int64", "optional": false, "field": "id"}}},
		"payload": map[string]any{"id": id},
	})
}

func decodeKeyID(raw []byte) (int64, error) {
	var envelope struct {
		Payload struct {
			ID int64 `json:"id"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return 0, err
	}
	return envelope.Payload.ID, nil
}

func encodeValue(value row) ([]byte, error) {
	fields := []map[string]any{
		{"type": "int64", "optional": false, "field": "id"},
		{"type": "string", "optional": false, "field": "name"},
		{"type": "string", "optional": true, "field": "note"},
		{"type": "int64", "optional": false, "field": "counter"},
	}
	return json.Marshal(map[string]any{
		"schema":  map[string]any{"type": "struct", "name": "mo.issue29848.Value", "optional": false, "fields": fields},
		"payload": map[string]any{"id": value.ID, "name": value.Name, "note": value.Note, "counter": value.Counter},
	})
}

func sortedRows(rows []row) []row {
	out := append([]row(nil), rows...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func expectedFinalRows() []row {
	return []row{
		{ID: 2, Name: "雪-empty", Note: stringPointer(""), Counter: 2},
		{ID: 3, Name: "after-update", Note: stringPointer("更新"), Counter: 33},
		{ID: 4, Name: "offline-insert", Note: stringPointer("while worker stopped"), Counter: 4},
		{ID: 5, Name: "offline-null", Note: nil, Counter: 5},
	}
}

func validateUniqueRowIDs(rows []row) error {
	seen := make(map[int64]struct{}, len(rows))
	for _, item := range rows {
		if _, exists := seen[item.ID]; exists {
			return fmt.Errorf("%w %d", errDuplicateRowID, item.ID)
		}
		seen[item.ID] = struct{}{}
	}
	return nil
}

func readRows(ctx context.Context, db *sql.DB) ([]row, error) {
	rows, err := db.QueryContext(ctx, "SELECT id, name, note, counter FROM "+databaseName+"."+tableName+" ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []row
	for rows.Next() {
		var item row
		var note sql.NullString
		if err := rows.Scan(&item.ID, &item.Name, &note, &item.Counter); err != nil {
			return nil, err
		}
		if note.Valid {
			item.Note = &note.String
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func waitForConnect(ctx context.Context, connect connectClient) (connectRoot, error) {
	var root connectRoot
	var lastErr error
	err := poll(ctx, "Kafka Connect REST readiness", time.Second, func(ctx context.Context) (bool, error) {
		body, err := connect.request(ctx, http.MethodGet, "/", nil, http.StatusOK)
		if err != nil {
			lastErr = err
			return false, nil
		}
		if err := json.Unmarshal(body, &root); err != nil {
			return false, err
		}
		return root.Version != "", nil
	})
	if err != nil {
		return root, fmt.Errorf("wait for Kafka Connect: %w (last REST error: %v)", err, lastErr)
	}
	return root, nil
}

func (c connectClient) plugins(ctx context.Context) ([]connectorPlugin, error) {
	body, err := c.request(ctx, http.MethodGet, "/connector-plugins", nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	var plugins []connectorPlugin
	if err := json.Unmarshal(body, &plugins); err != nil {
		return nil, err
	}
	return plugins, nil
}

func (c connectClient) createConnector(ctx context.Context, name string, config map[string]string) error {
	body, err := json.Marshal(map[string]any{"name": name, "config": config})
	if err != nil {
		return err
	}
	_, err = c.request(ctx, http.MethodPost, "/connectors", body, http.StatusCreated)
	return err
}

func (c connectClient) status(ctx context.Context, name string) (connectorStatusResponse, error) {
	body, err := c.request(ctx, http.MethodGet, "/connectors/"+url.PathEscape(name)+"/status", nil, http.StatusOK)
	if err != nil {
		return connectorStatusResponse{}, err
	}
	var status connectorStatusResponse
	if err := json.Unmarshal(body, &status); err != nil {
		return status, err
	}
	return status, nil
}

func (c connectClient) stopConnector(ctx context.Context, name string) error {
	_, err := c.request(ctx, http.MethodPut, "/connectors/"+url.PathEscape(name)+"/stop", nil, http.StatusAccepted, http.StatusNoContent)
	return err
}

func (c connectClient) resumeConnector(ctx context.Context, name string) error {
	_, err := c.request(ctx, http.MethodPut, "/connectors/"+url.PathEscape(name)+"/resume", nil, http.StatusAccepted, http.StatusNoContent)
	return err
}

func (c connectClient) restOffset(ctx context.Context, name, topic string, partition int32) (int64, error) {
	body, err := c.request(ctx, http.MethodGet, "/connectors/"+url.PathEscape(name)+"/offsets", nil, http.StatusOK)
	if err != nil {
		return 0, err
	}
	var response connectorOffsets
	if err := json.Unmarshal(body, &response); err != nil {
		return 0, err
	}
	for _, item := range response.Offsets {
		if item.Partition.KafkaTopic == topic && item.Partition.KafkaPartition == partition && item.Offset != nil {
			return item.Offset.KafkaOffset, nil
		}
	}
	return 0, fmt.Errorf("connector offsets omit %s[%d]", topic, partition)
}

func (c connectClient) patchOffset(ctx context.Context, name, topic string, partition int32, offset int64) error {
	body, err := json.Marshal(connectorOffsets{Offsets: []connectorOffset{{
		Partition: offsetPartition{KafkaTopic: topic, KafkaPartition: partition},
		Offset:    &offsetValue{KafkaOffset: offset},
	}}})
	if err != nil {
		return err
	}
	_, err = c.request(ctx, http.MethodPatch, "/connectors/"+url.PathEscape(name)+"/offsets", body, http.StatusAccepted, http.StatusOK, http.StatusNoContent)
	return err
}

func (c connectClient) request(ctx context.Context, method, path string, body []byte, expected ...int) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	for _, status := range expected {
		if resp.StatusCode == status {
			return data, nil
		}
	}
	return nil, fmt.Errorf("connect REST %s %s returned HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
}

func waitForConnector(ctx context.Context, connect connectClient, name, want string) error {
	return poll(ctx, "connector state "+want, time.Second, func(ctx context.Context) (bool, error) {
		status, err := connect.status(ctx, name)
		if err != nil {
			return false, nil
		}
		if status.Connector.State != want {
			return false, nil
		}
		for _, task := range status.Tasks {
			if task.State != want && !(want == "STOPPED" && task.State == "STOPPED") {
				return false, nil
			}
		}
		if want == "RUNNING" && len(status.Tasks) != 1 {
			return false, nil
		}
		return true, nil
	})
}

func waitForDrain(ctx context.Context, reader drainStateReader, expected []row, expectedOffset int64) (drainReceipt, error) {
	var receipt drainReceipt
	err := poll(ctx, "target rows and checkpoint", time.Second, func(ctx context.Context) (bool, error) {
		actual, err := reader.rows(ctx)
		if err != nil {
			return false, nil
		}
		if !reflect.DeepEqual(actual, sortedRows(expected)) {
			return false, nil
		}
		restOffset, err := reader.restOffset(ctx)
		if err != nil || restOffset != expectedOffset {
			return false, nil
		}
		kafkaOffset, groupFound, err := reader.groupOffset(ctx)
		if err != nil || !groupFound || kafkaOffset != expectedOffset {
			return false, nil
		}
		logEndOffset, endFound, err := reader.logEndOffset(ctx)
		if err != nil || !endFound || logEndOffset != expectedOffset {
			return false, nil
		}
		receipt = drainReceipt{
			RESTOffset:        restOffset,
			KafkaGroupOffset:  kafkaOffset,
			KafkaLogEndOffset: logEndOffset,
		}
		return true, nil
	})
	return receipt, err
}

func groupOffset(ctx context.Context, admin *kadm.Client, group string) (int64, bool, error) {
	response, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		if strings.Contains(err.Error(), "GROUP_ID_NOT_FOUND") {
			return 0, false, nil
		}
		return 0, false, err
	}
	offset, ok := response.Lookup(topicName, 0)
	if !ok || offset.Err != nil {
		return 0, false, offset.Err
	}
	return offset.At, true, nil
}

func topicLogEndOffset(ctx context.Context, admin *kadm.Client) (int64, bool, error) {
	offsets, err := admin.ListEndOffsets(ctx, topicName)
	if err != nil {
		return 0, false, err
	}
	offset, ok := offsets.Lookup(topicName, 0)
	if !ok {
		return 0, false, nil
	}
	if offset.Err != nil {
		return 0, false, offset.Err
	}
	return offset.Offset, true, nil
}

func killOwnedConnect(ctx context.Context, opts runOptions) error {
	containerID, err := composeOutput(ctx, opts, "ps", "-q", "connect")
	if err != nil {
		return err
	}
	containerID = strings.TrimSpace(containerID)
	if containerID == "" {
		return errors.New("owned Connect container was not found in the run's Compose project")
	}
	if _, err := exec.CommandContext(ctx, "docker", "kill", "--signal", "KILL", containerID).CombinedOutput(); err != nil {
		return fmt.Errorf("kill owned container %s: %w", containerID, err)
	}
	return poll(ctx, "owned Connect worker exit", 250*time.Millisecond, func(ctx context.Context) (bool, error) {
		out, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.Status}}", containerID).CombinedOutput()
		if err != nil {
			return false, nil
		}
		return strings.TrimSpace(string(out)) == "exited", nil
	})
}

func startOwnedConnect(ctx context.Context, opts runOptions) error {
	_, err := composeOutput(ctx, opts, "start", "connect")
	return err
}

func composeOutput(ctx context.Context, opts runOptions, args ...string) (string, error) {
	composeBase, err := filepath.Abs(opts.composeBase)
	if err != nil {
		return "", fmt.Errorf("resolve base Compose path %q: %w", opts.composeBase, err)
	}
	composeConnect, err := filepath.Abs(opts.composeConnect)
	if err != nil {
		return "", fmt.Errorf("resolve Connect Compose path %q: %w", opts.composeConnect, err)
	}
	commandArgs := make([]string, 0, 7+len(args))
	commandArgs = append(commandArgs, "compose", "-p", opts.composeProject, "-f", composeBase, "-f", composeConnect)
	commandArgs = append(commandArgs, args...)
	cmd := exec.CommandContext(ctx, "docker", commandArgs...)
	cmd.Dir = filepath.Dir(composeBase)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("docker %s: %w: %s", strings.Join(commandArgs, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func lastOffset(records []producedRecord) int64 {
	last := int64(-1)
	for _, record := range records {
		if record.Offset > last {
			last = record.Offset
		}
	}
	return last
}

func toRowReports(rows []row) []rowReport {
	sorted := sortedRows(rows)
	out := make([]rowReport, 0, len(sorted))
	for _, item := range sorted {
		out = append(out, rowReport(item))
	}
	return out
}

func stringPointer(value string) *string { return &value }

func poll(ctx context.Context, description string, interval time.Duration, check func(context.Context) (bool, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ok, err := check(ctx)
		if err != nil {
			return fmt.Errorf("%s: %w", description, err)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s timed out: %w", description, ctx.Err())
		case <-ticker.C:
		}
	}
}

func setStage(opts runOptions, report *runReport, name string) error {
	report.CurrentStage = name
	fmt.Fprintf(os.Stdout, "Kafka Connect recovery stage: %s\n", name)
	return writeReport(opts.reportDir, *report)
}

func recordPhase(opts runOptions, report *runReport, completed phase) error {
	report.Phases = append(report.Phases, completed)
	report.CurrentStage = completed.Name + "-passed"
	fmt.Fprintf(os.Stdout, "Kafka Connect recovery phase passed: %s\n", completed.Name)
	return writeReport(opts.reportDir, *report)
}

func writeReport(dir string, report runReport) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".report-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, "report.json")); err != nil {
		return err
	}
	return nil
}
