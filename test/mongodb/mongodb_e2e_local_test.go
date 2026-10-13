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

package main

import (
	"bufio"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoDBLocalE2ERunnerDoesNotImportKernelPackages(t *testing.T) {
	repoRoot := mongoDBTestRepoRoot(t)
	file, err := parser.ParseFile(
		token.NewFileSet(),
		filepath.Join(repoRoot, "test/mongodb/mongodb_e2e_local.go"),
		nil,
		parser.ImportsOnly,
	)
	require.NoError(t, err)
	for _, imported := range file.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		require.NoError(t, err)
		require.Falsef(t, strings.HasPrefix(path, "github.com/matrixorigin/matrixone/"),
			"standalone E2E runner must not import kernel package %s", path)
	}
}

func TestMongoDBLocalE2EKeyfileMountUsesDirectory(t *testing.T) {
	repoRoot := mongoDBTestRepoRoot(t)
	script, err := os.ReadFile(filepath.Join(repoRoot, "optools", "mongodb_ci.bash"))
	require.NoError(t, err)
	require.Contains(t, string(script), "MONGODB_KEYFILE_DIR=\"$(mktemp -d \"$ROOT_DIR/../.mo-mongodb-key-source.XXXXXX\")\"")
	require.Contains(t, string(script), "MONGODB_KEYFILE=\"$MONGODB_KEYFILE_DIR/mongodb-keyfile\"")
	require.Contains(t, string(script), "$(basename \"$MONGODB_KEYFILE_DIR\")\" == .mo-mongodb-key-source.*")

	compose, err := os.ReadFile(filepath.Join(repoRoot, "etc", "launch-mongodb-local", "compose.yaml"))
	require.NoError(t, err)
	require.Contains(t, string(compose), "cp /run/key-source/mongodb-keyfile /tmp/mongodb-keyfile")
	require.Contains(t, string(compose), "${MONGODB_KEYFILE_DIR}:/run/key-source:ro")
}

func TestMongoDBLocalE2EPortPlanUsesOneReservedBlock(t *testing.T) {
	for range 5 {
		ports, output, err := runMongoDBPortPlan(t, nil)
		require.NoError(t, err, output)

		base := ports["LOG_PORT_BASE"]
		require.GreaterOrEqual(t, base, 1)
		require.LessOrEqual(t, base+79, 65535)
		require.Equal(t, base, ports["LOG_RAFT_PORT"])
		require.Equal(t, base+1, ports["LOG_SERVICE_PORT"])
		require.Equal(t, base+2, ports["LOG_GOSSIP_PORT"])
		require.Equal(t, base+24, ports["TN_PORT_BASE"])
		require.Equal(t, base+48, ports["CN_PORT_BASE"])
		require.Equal(t, base+72, ports["MO_PORT"])
		require.Equal(t, base+73, ports["STATUS_PORT"])

		for name, port := range ports {
			require.GreaterOrEqualf(t, port, base, "%s is below the reserved block", name)
			require.LessOrEqualf(t, port, base+79, "%s is above the reserved block", name)
		}

		ranges := []struct {
			name       string
			start, end int
		}{
			{name: "LogService", start: base, end: base + 2},
			{name: "TN", start: ports["TN_PORT_BASE"], end: ports["TN_PORT_BASE"] + 20},
			{name: "CN", start: ports["CN_PORT_BASE"], end: ports["CN_PORT_BASE"] + 20},
			{name: "frontend", start: ports["MO_PORT"], end: ports["MO_PORT"]},
			{name: "status", start: ports["STATUS_PORT"], end: ports["STATUS_PORT"]},
		}
		for left := range ranges {
			for right := left + 1; right < len(ranges); right++ {
				require.Truef(t,
					ranges[left].end < ranges[right].start || ranges[right].end < ranges[left].start,
					"%s range %d-%d overlaps %s range %d-%d",
					ranges[left].name, ranges[left].start, ranges[left].end,
					ranges[right].name, ranges[right].start, ranges[right].end,
				)
			}
		}
	}
}

func TestMongoDBLocalE2EPortPlanValidatesOverrides(t *testing.T) {
	for _, test := range []struct {
		name      string
		overrides map[string]string
		want      string
	}{
		{
			name:      "frontend above TCP range",
			overrides: map[string]string{"MO_MONGODB_FRONTEND_PORT": "65536"},
			want:      "MO_MONGODB_FRONTEND_PORT must be between 1 and 65535",
		},
		{
			name:      "status below TCP range",
			overrides: map[string]string{"MO_MONGODB_STATUS_PORT": "0"},
			want:      "MO_MONGODB_STATUS_PORT must be between 1 and 65535",
		},
		{
			name:      "port block exceeds TCP range",
			overrides: map[string]string{"MO_MONGODB_LOG_PORT_BASE": "65457"},
			want:      "MO_MONGODB_LOG_PORT_BASE must leave room for the 80-port block",
		},
		{
			name: "frontend overlaps LogService",
			overrides: map[string]string{
				"MO_MONGODB_LOG_PORT_BASE": "40000",
				"MO_MONGODB_FRONTEND_PORT": "40000",
			},
			want: "LogService overlaps frontend",
		},
		{
			name: "frontend overlaps status",
			overrides: map[string]string{
				"MO_MONGODB_LOG_PORT_BASE": "40000",
				"MO_MONGODB_FRONTEND_PORT": "40072",
				"MO_MONGODB_STATUS_PORT":   "40072",
			},
			want: "frontend overlaps status",
		},
		{
			name: "frontend outside reserved block",
			overrides: map[string]string{
				"MO_MONGODB_LOG_PORT_BASE": "40000",
				"MO_MONGODB_FRONTEND_PORT": "50000",
			},
			want: "MO_MONGODB_FRONTEND_PORT must be inside the reserved 80-port block",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, output, err := runMongoDBPortPlan(t, test.overrides)
			require.Error(t, err)
			require.Contains(t, output, test.want)
		})
	}
}

func TestMongoDBLocalE2EPortPlanLeasesPortBlockAcrossProcesses(t *testing.T) {
	temporaryDirectory := t.TempDir()
	firstTempDirectory := filepath.Join(temporaryDirectory, "first-tmp")
	secondTempDirectory := filepath.Join(temporaryDirectory, "second-tmp")
	require.NoError(t, os.MkdirAll(firstTempDirectory, 0o700))
	require.NoError(t, os.MkdirAll(secondTempDirectory, 0o700))
	// Production defaults to this host-user-wide directory, independently of
	// TMPDIR. The lease-only owner has to pass it explicitly so it never probes
	// host ports; the contender exercises port-plan's implicit production path.
	leaseDirectory := filepath.Join("/tmp", fmt.Sprintf("mo-mongodb-port-leases-%d", os.Geteuid()))
	const firstBase = 57000
	const lastBase = 65456
	const baseStep = 80
	baseCount := (lastBase-firstBase)/baseStep + 1
	startOffset := os.Getpid() % baseCount
	var owner *mongoDBPortLeaseTestProcess
	for attempt := range baseCount {
		base := firstBase + ((startOffset+attempt)%baseCount)*baseStep
		readyFile := filepath.Join(firstTempDirectory, fmt.Sprintf("port-lease-ready-%d", base))
		candidate := startMongoDBPortLeaseTestProcess(
			t,
			strconv.Itoa(base),
			leaseDirectory,
			firstTempDirectory,
			readyFile,
		)
		if candidate.waitForReady(t) {
			owner = candidate
			break
		}
		output := candidate.output(t)
		require.Error(t, candidate.waitErr, "lease owner exited without acquiring or reporting a conflict: %s", output)
		require.Containsf(t, output, "MO_MONGODB_LOG_PORT_BASE is already leased by another MongoDB E2E run",
			"unexpected failure reserving candidate base %d", base)
		candidate.mayHaveDescendants = false
	}
	require.NotNil(t, owner, "could not reserve a free test lease base in %d attempts", baseCount)

	_, output, err := runMongoDBPortPlan(t, map[string]string{
		"MO_MONGODB_LOG_PORT_BASE": owner.base,
		"TMPDIR":                   secondTempDirectory,
	})
	require.Error(t, err)
	require.Contains(t, output, "MO_MONGODB_LOG_PORT_BASE is already leased by another MongoDB E2E run")
	owner.releaseAndWait(t)
}

func TestMongoDBLocalE2EPortPlanReleasesLeaseAfterParentDeath(t *testing.T) {
	const base = "62000"
	temporaryDirectory := t.TempDir()
	tempDirectory := filepath.Join(temporaryDirectory, "tmp")
	require.NoError(t, os.MkdirAll(tempDirectory, 0o700))
	readyFile := filepath.Join(temporaryDirectory, "port-plan-ready")
	leaseDirectory := filepath.Join(temporaryDirectory, "leases")
	leaseFile := filepath.Join(leaseDirectory, base+".lock")
	owner := startMongoDBPortLeaseTestProcess(t, base, leaseDirectory, tempDirectory, readyFile)
	require.True(t, owner.waitForReady(t), "lease owner did not become ready: %s", owner.output(t))
	observer := startMongoDBLeaseLockObserver(t, leaseFile)
	observer.readPhase(t, "blocked")

	owner.killParent(t)

	observer.readPhase(t, "acquired")
	require.NoError(t, observer.wait(), "lease observer failed: %s", observer.stderr())
	owner.mayHaveDescendants = false
}

const mongoDBPortLeaseLockObserver = `import errno
import fcntl
import sys

lease_path = sys.argv[1]
with open(lease_path, "r+") as lease:
    try:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as err:
        if err.errno not in (errno.EACCES, errno.EAGAIN):
            raise
        print("blocked", flush=True)
    else:
        print("unexpectedly-acquired", flush=True)
        raise SystemExit(1)

    fcntl.flock(lease, fcntl.LOCK_EX)
    print("acquired", flush=True)
`

type mongoDBLeaseLockObserver struct {
	command    *exec.Cmd
	stdout     *bufio.Reader
	stderrPath string
	cancel     context.CancelFunc
	waited     bool
	waitErr    error
}

type mongoDBPortLeaseTestProcess struct {
	command            *exec.Cmd
	input              io.WriteCloser
	readyFile          string
	outputPath         string
	waitDone           chan error
	waited             bool
	waitErr            error
	inputClosed        bool
	ownsLease          bool
	mayHaveDescendants bool
	base               string
}

func startMongoDBPortLeaseTestProcess(
	t *testing.T,
	base, leaseDirectory, tempDirectory, readyFile string,
) *mongoDBPortLeaseTestProcess {
	t.Helper()
	repoRoot := mongoDBTestRepoRoot(t)
	command := exec.Command("bash", filepath.Join(repoRoot, "optools", "mongodb_ci.bash"), "port-lease-test")
	command.Dir = repoRoot
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Env = mongoDBPortPlanEnv(map[string]string{
		"MO_MONGODB_LOG_PORT_BASE":        base,
		"MO_MONGODB_PORT_LEASE_DIR":       leaseDirectory,
		"MO_MONGODB_PORT_PLAN_READY_FILE": readyFile,
		"TMPDIR":                          tempDirectory,
	})
	outputFile, err := os.CreateTemp(t.TempDir(), "port-lease-test-output")
	require.NoError(t, err)
	t.Cleanup(func() { _ = outputFile.Close() })
	input, err := command.StdinPipe()
	require.NoError(t, err)
	command.Stdout = outputFile
	command.Stderr = outputFile
	if err := command.Start(); err != nil {
		_ = input.Close()
		_ = outputFile.Close()
		require.NoError(t, err)
	}
	process := &mongoDBPortLeaseTestProcess{
		command:            command,
		input:              input,
		readyFile:          readyFile,
		outputPath:         outputFile.Name(),
		waitDone:           make(chan error, 1),
		mayHaveDescendants: true,
		base:               base,
	}
	go func() { process.waitDone <- command.Wait() }()
	t.Cleanup(process.cleanup)
	require.NoError(t, outputFile.Close())
	return process
}

func (p *mongoDBPortLeaseTestProcess) waitForReady(t *testing.T) bool {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(p.readyFile)
		if err == nil {
			if string(contents) == "ready\n" {
				p.ownsLease = true
				return true
			}
			require.Emptyf(t, contents, "lease owner wrote unexpected readiness contents: %q", contents)
		} else {
			require.True(t, os.IsNotExist(err), "could not read lease readiness file: %v", err)
		}
		if p.observeExit() {
			p.closeInput()
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lease owner did not become ready before deadline: %s", p.output(t))
	return false
}

func (p *mongoDBPortLeaseTestProcess) observeExit() bool {
	if p.waited {
		return true
	}
	select {
	case p.waitErr = <-p.waitDone:
		p.waited = true
		return true
	default:
		return false
	}
}

func (p *mongoDBPortLeaseTestProcess) waitForExit(timeout time.Duration) bool {
	if p.observeExit() {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case p.waitErr = <-p.waitDone:
		p.waited = true
		return true
	case <-timer.C:
		return false
	}
}

func (p *mongoDBPortLeaseTestProcess) output(t *testing.T) string {
	t.Helper()
	contents, err := os.ReadFile(p.outputPath)
	require.NoError(t, err)
	return string(contents)
}

func (p *mongoDBPortLeaseTestProcess) closeInput() {
	if !p.inputClosed {
		_ = p.input.Close()
		p.inputClosed = true
	}
}

func (p *mongoDBPortLeaseTestProcess) releaseAndWait(t *testing.T) {
	t.Helper()
	require.True(t, p.ownsLease, "cannot release a lease not owned by this test process")
	_, err := io.WriteString(p.input, "release\n")
	require.NoError(t, err)
	p.closeInput()
	require.True(t, p.waitForExit(3*time.Second), "lease owner did not exit after release: %s", p.output(t))
	require.NoError(t, p.waitErr, p.output(t))
	p.ownsLease = false
	p.mayHaveDescendants = false
}

func (p *mongoDBPortLeaseTestProcess) killParent(t *testing.T) {
	t.Helper()
	p.ownsLease = false
	require.NoError(t, p.command.Process.Kill())
	require.True(t, p.waitForExit(3*time.Second), "lease owner parent did not die")
	require.Error(t, p.waitErr, "lease owner parent unexpectedly exited successfully")
	p.closeInput()
}

func (p *mongoDBPortLeaseTestProcess) cleanup() {
	if p.ownsLease && !p.waited {
		_, _ = io.WriteString(p.input, "release\n")
		p.closeInput()
	}
	if p.mayHaveDescendants {
		_ = syscall.Kill(-p.command.Process.Pid, syscall.SIGKILL)
	}
	if !p.waited {
		p.closeInput()
		_ = p.waitForExit(3 * time.Second)
	}
}

func startMongoDBLeaseLockObserver(t *testing.T, leaseFile string) *mongoDBLeaseLockObserver {
	t.Helper()
	stderrFile, err := os.CreateTemp(t.TempDir(), "lease-observer-stderr")
	require.NoError(t, err)
	t.Cleanup(func() { _ = stderrFile.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, "python3", "-c", mongoDBPortLeaseLockObserver, leaseFile)
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	command.Stderr = stderrFile
	require.NoError(t, command.Start())

	observer := &mongoDBLeaseLockObserver{
		command:    command,
		stdout:     bufio.NewReader(stdout),
		stderrPath: stderrFile.Name(),
		cancel:     cancel,
	}
	t.Cleanup(func() {
		if !observer.waited {
			observer.cancel()
			if observer.command.Process != nil {
				_ = observer.command.Process.Kill()
			}
			_ = observer.wait()
		}
	})
	return observer
}

func (o *mongoDBLeaseLockObserver) readPhase(t *testing.T, want string) {
	t.Helper()
	line, err := o.stdout.ReadString('\n')
	if err != nil {
		waitErr := o.wait()
		t.Fatalf("lease observer did not report %q phase: read error %v, wait error %v, stderr: %s",
			want, err, waitErr, o.stderr())
	}
	require.Equal(t, want+"\n", line)
}

func (o *mongoDBLeaseLockObserver) wait() error {
	if !o.waited {
		o.waitErr = o.command.Wait()
		o.waited = true
		o.cancel()
	}
	return o.waitErr
}

func (o *mongoDBLeaseLockObserver) stderr() string {
	data, err := os.ReadFile(o.stderrPath)
	if err != nil {
		return fmt.Sprintf("could not read observer stderr: %v", err)
	}
	return string(data)
}

func runMongoDBPortPlan(t *testing.T, overrides map[string]string) (map[string]int, string, error) {
	t.Helper()
	repoRoot := mongoDBTestRepoRoot(t)
	command := exec.Command("bash", filepath.Join(repoRoot, "optools", "mongodb_ci.bash"), "port-plan")
	command.Dir = repoRoot
	command.Env = mongoDBPortPlanEnv(overrides)
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, string(output), err
	}

	ports := make(map[string]int)
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, string(output), fmt.Errorf("unexpected MongoDB port-plan output line %q", line)
		}
		port, err := strconv.Atoi(value)
		if err != nil {
			return nil, string(output), fmt.Errorf("parse MongoDB port-plan %s: %w", name, err)
		}
		if _, exists := ports[name]; exists {
			return nil, string(output), fmt.Errorf("duplicate MongoDB port-plan value %q", name)
		}
		ports[name] = port
	}
	for _, name := range []string{
		"LOG_PORT_BASE", "LOG_RAFT_PORT", "LOG_SERVICE_PORT", "LOG_GOSSIP_PORT",
		"TN_PORT_BASE", "CN_PORT_BASE", "MO_PORT", "STATUS_PORT",
	} {
		if _, exists := ports[name]; !exists {
			return nil, string(output), fmt.Errorf("MongoDB port-plan omitted %s", name)
		}
	}
	return ports, string(output), nil
}

func mongoDBPortPlanEnv(overrides map[string]string) []string {
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "MO_MONGODB_") || name == "LC_ALL" {
			continue
		}
		environment = append(environment, entry)
	}
	environment = append(environment, "LC_ALL=C")
	for name, value := range overrides {
		environment = append(environment, name+"="+value)
	}
	return environment
}

type testTransferMonitor struct {
	reset             func(context.Context) error
	documentsReturned func(context.Context) (int64, error)
}

func (m testTransferMonitor) Reset(ctx context.Context) error { return m.reset(ctx) }

func (m testTransferMonitor) DocumentsReturned(ctx context.Context) (int64, error) {
	return m.documentsReturned(ctx)
}

func TestMongoDBLocalE2ERunContract(t *testing.T) {
	repoRoot := mongoDBTestRepoRoot(t)
	previous, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(repoRoot))
	t.Cleanup(func() { require.NoError(t, os.Chdir(previous)) })

	manifest, err := loadFixtureManifest("test/mongodb/fixture_manifest.json")
	require.NoError(t, err)
	db, mock := newMongoDBE2ESQLMock(t)

	for range 10 {
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery("show mongodb connections").WillReturnRows(sqlmock.NewRows([]string{
		"name", "discovery_mode", "auth_mechanism", "tls_mode",
		"read_preference", "read_concern", "version", "disabled",
	}).
		AddRow("existing", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 1, 0).
		AddRow("mongodb_ci", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 3, 0))
	mock.ExpectQuery("show create table").WillReturnRows(sqlmock.NewRows([]string{"table", "ddl"}).AddRow(
		"events", "CREATE EXTERNAL TABLE events (id CHAR(24) MONGODB_PATH '_id') ENGINE = MONGODB WITH ('connection'='mongodb_ci')"))
	expectMongoDBE2EScalar(mock, "STRING")
	expectMongoDBE2EScalar(mock, "text")
	expectMongoDBE2EScalar(mock, "2")
	expectMongoDBE2EScalar(mock, "1")
	expectMongoDBE2EScalar(mock, "4")
	expectMongoDBE2EScalar(mock, "0")
	expectMongoDBE2EScalar(mock, "5")
	mock.ExpectQuery("truncate table mongodb_ci.events").WillReturnError(
		errors.New("invalid input: cannot insert/update/delete from external table"))
	expectMongoDBE2EScalar(mock, "5")
	fixtureRows := sqlmock.NewRows([]string{"id", "device_id", "site_id", "ts", "measurement", "source_batch"})
	for _, row := range manifest.Rows {
		require.Len(t, row, 6)
		fixtureRows.AddRow(row[0], row[1], row[2], row[3], row[4], row[5])
	}
	mock.ExpectQuery("select mongo_id").WillReturnRows(fixtureRows)
	expectMongoDBE2EScalar(mock, "3")
	expectMongoDBE2EScalar(mock, "3")
	prepared := mock.ExpectPrepare("select count")
	prepared.ExpectQuery().WithArgs(int64(13)).WillReturnRows(
		sqlmock.NewRows([]string{"count(*)"}).AddRow("3"))
	prepared.ExpectQuery().WithArgs(int64(19)).WillReturnRows(
		sqlmock.NewRows([]string{"count(*)"}).AddRow("2"))
	prepared.ExpectQuery().WithArgs(int64(29)).WillReturnRows(
		sqlmock.NewRows([]string{"count(*)"}).AddRow("1"))
	mock.ExpectExec("prepare mongo_pruned_no_params").WillReturnResult(sqlmock.NewResult(0, 0))
	expectMongoDBE2EScalar(mock, "4")
	expectMongoDBE2EScalar(mock, "4")
	mock.ExpectExec("deallocate prepare mongo_pruned_no_params").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("prepare mongo_pruned_text").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("set @mongo_measurement = 13").WillReturnResult(sqlmock.NewResult(0, 0))
	expectMongoDBE2EScalar(mock, "3")
	mock.ExpectExec("set @mongo_measurement = 19").WillReturnResult(sqlmock.NewResult(0, 0))
	expectMongoDBE2EScalar(mock, "2")
	mock.ExpectExec("deallocate prepare mongo_pruned_text").WillReturnResult(sqlmock.NewResult(0, 0))
	expectMongoDBE2EScalar(mock, "1")
	expectMongoDBE2EScalar(mock, "1")
	expectMongoDBE2EScalar(mock, `{"filter":{"site_id":"site-west"}}`)
	expectMongoDBE2EScalar(mock, "device-001|4|18.5")
	expectMongoDBE2EScalar(mock, "device-001|4|18.5")
	expectMongoDBE2EScalar(mock, "1")
	expectMongoDBE2EScalar(mock, "64b000000000000000000005")
	expectMongoDBE2EScalar(mock, "5")
	expectMongoDBE2EScalar(mock, "10")
	expectMongoDBE2EScalar(mock, "5")
	mock.ExpectQuery("explain select").WillReturnRows(sqlmock.NewRows([]string{"QUERY PLAN"}).
		AddRow("MongoDB Scan: operation=aggregate query_digest=0123456789ab").
		AddRow("Filter Cond: event_count >= 1"))
	for range 4 {
		mock.ExpectQuery("select count").WillReturnError(errors.New("MongoDB pipeline stage is not allowed"))
	}
	mock.ExpectQuery("select count").WillReturnError(errors.New("MongoDB $sort requires 1 to 32 fields"))
	mock.ExpectQuery("select count").WillReturnError(errors.New("MongoDB $unwind requires a valid field path"))
	mock.ExpectQuery("select count").WillReturnError(errors.New("MongoDB __mo_query must contain only a filter or pipeline field"))
	mock.ExpectQuery("select count").WillReturnError(errors.New("MongoDB __mo_query must be strict Extended JSON"))
	expectMongoDBE2EScalar(mock, "5")
	mock.ExpectExec("create table mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 1))
	expectMongoDBE2EScalar(mock, "1")
	mock.ExpectExec("create table mongodb_ci.events_composite_insert_target").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("insert into mongodb_ci.events_composite_insert_target").WillReturnResult(sqlmock.NewResult(0, 1))
	expectMongoDBE2EScalar(mock, "1")
	mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnError(errors.New("duplicate entry"))
	expectMongoDBE2EScalar(mock, "1")
	mock.ExpectExec("set time_zone").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("select id,date_format\\(d.*order by id").WillReturnRows(sqlmock.NewRows([]string{"id", "d", "t"}).
		AddRow("a", "2026-03-08 01:59:59.123000", "2026-03-08 01:59:59.123000").
		AddRow("b", "2026-03-08 03:00:00.456000", "2026-03-08 03:00:00.456000").
		AddRow("c", "2026-11-01 01:59:59.789000", "2026-11-01 01:59:59.789000").
		AddRow("d", "2026-11-01 02:00:00.012000", "2026-11-01 02:00:00.012000"))
	expectMongoDBE2EScalar(mock, "1")
	mock.ExpectQuery("select payload_1").WillReturnError(errors.New("MongoDB decoded batch byte limit exceeded"))
	// A pre-canceled context is rejected by database/sql before it reaches the
	// driver, so no sqlmock expectation is consumed here.
	expectMongoDBE2EScalar(mock, "5")
	expectMongoDBE2EScalar(mock, "4")
	for range 3 {
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectBegin()
	mock.ExpectExec("replace into mongodb_ci.minute_aggregate").WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectExec("update mongodb_ci.ingest_watermark").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectMongoDBE2EScalar(mock, "2026-07-27 10:03:00")
	mock.ExpectExec("create external table mongodb_ci.events_strict").WillReturnResult(sqlmock.NewResult(0, 0))
	expectMongoDBE2EScalar(mock, "4")
	expectMongoDBE2EScalar(mock, "2026-07-27 10:03:00")
	mock.ExpectBegin()
	mock.ExpectExec("replace into mongodb_ci.minute_aggregate.*events_strict").WillReturnError(errors.New("strict conversion failed"))
	mock.ExpectRollback()
	expectMongoDBE2EScalar(mock, "4")
	expectMongoDBE2EScalar(mock, "2026-07-27 10:03:00")
	mock.ExpectBegin()
	mock.ExpectExec("replace into mongodb_ci.minute_aggregate").WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectExec("update mongodb_ci.ingest_watermark").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectMongoDBE2EScalar(mock, "4")
	mock.ExpectExec("alter mongodb connection mongodb_ci set").WillReturnResult(sqlmock.NewResult(0, 1))
	expectMongoDBE2EScalar(mock, "5")
	mock.ExpectExec("alter mongodb connection mongodb_ci disable").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("select count").WillReturnError(errors.New("MongoDB connection is disabled"))
	mock.ExpectExec("alter mongodb connection mongodb_ci enable").WillReturnResult(sqlmock.NewResult(0, 1))
	expectMongoDBE2EScalar(mock, "5")

	transferSteps := 0
	monitor := testTransferMonitor{
		reset: func(context.Context) error {
			transferSteps++
			return nil
		},
		documentsReturned: func(context.Context) (int64, error) {
			transferSteps++
			if transferSteps == 2 {
				return 5, nil
			}
			return 1, nil
		},
	}
	result := report{}
	require.NoError(t, runWithDSNAndTransferMonitor(t.Context(), db, "", "127.0.0.1:27017", &result, monitor))
	require.Equal(t, []string{
		"secret-backed-ddl",
		"show-connections-admin-metadata-redaction",
		"show-create-redaction-roundtrip",
		"json-relaxed-extended-conversion",
		"fixed-binary-padding",
		"truncate-read-only-source-preserved",
		"scan-projection-pushdown-null-conversion",
		"prepared-scan-binary-and-text-reuse-recovery-metadata",
		"explicit-filter-residual",
		"explicit-filter-and-query-column",
		"explicit-reducing-aggregation-pipeline",
		"explicit-sort-and-unwind-pipeline",
		"explicit-query-explain-redaction",
		"explicit-query-fail-closed",
		"insert-select-primary-key-targets",
		"date-format-order-by",
		"low-precision-temporal-residual",
		"decoded-vector-budget-enforced",
		"multi-batch-cancel-recovery",
		"mongoscan-timewin-gapfill",
		"atomic-aggregate-watermark",
		"conversion-error-atomic-rollback",
		"bounded-idempotent-replay",
		"credential-generation-rotation",
		"connection-disable-enable",
	}, result.Cases)
	require.Equal(t, &transferEvidence{RawScanDocuments: 5, PipelineDocuments: 1}, result.Transfer)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMongoDBLocalE2ERunPropagatesRelaxedJSONQueryFailures(t *testing.T) {
	for _, tc := range []struct {
		name        string
		failedQuery string
	}{
		{name: "payload", failedQuery: "json_unquote"},
		{name: "array", failedQuery: "json_contains"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repoRoot := mongoDBTestRepoRoot(t)
			previous, err := os.Getwd()
			require.NoError(t, err)
			require.NoError(t, os.Chdir(repoRoot))
			t.Cleanup(func() { require.NoError(t, os.Chdir(previous)) })

			db, mock := newMongoDBE2ESQLMock(t)
			for range 10 {
				mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
			}
			mock.ExpectQuery("show mongodb connections").WillReturnRows(sqlmock.NewRows([]string{
				"name", "discovery_mode", "auth_mechanism", "tls_mode",
				"read_preference", "read_concern", "version", "disabled",
			}).AddRow("mongodb_ci", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 3, 0))
			mock.ExpectQuery("show create table").WillReturnRows(sqlmock.NewRows([]string{"table", "ddl"}).AddRow(
				"events", "CREATE EXTERNAL TABLE events (id CHAR(24) MONGODB_PATH '_id') ENGINE = MONGODB WITH ('connection'='mongodb_ci')"))
			expectMongoDBE2EScalar(mock, "STRING")
			expectMongoDBE2EScalar(mock, "text")
			if tc.failedQuery == "json_contains" {
				expectMongoDBE2EScalar(mock, "2")
			}
			mock.ExpectQuery(tc.failedQuery).WillReturnError(errors.New("relaxed JSON query failed"))

			err = run(t.Context(), db, "127.0.0.1:27017", &report{})
			require.ErrorContains(t, err, "relaxed JSON query failed")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMongoDBPrimaryKeyInsertSelect(t *testing.T) {
	prepareSuccessfulTargets := func(mock sqlmock.Sqlmock) {
		mock.ExpectExec("create table mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("select count\\(\\*\\) from mongodb_ci.events_insert_target").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("1"))
		mock.ExpectExec("create table mongodb_ci.events_composite_insert_target").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("insert into mongodb_ci.events_composite_insert_target").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("select count\\(\\*\\) from mongodb_ci.events_composite_insert_target").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("1"))
	}

	tests := []struct {
		name    string
		prepare func(sqlmock.Sqlmock)
		wantErr string
	}{
		{
			name: "create target fails",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("create table mongodb_ci.events_insert_target").
					WillReturnError(errors.New("catalog unavailable"))
			},
			wantErr: "create single primary key target: catalog unavailable",
		},
		{
			name: "insert select fails",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("create table mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into mongodb_ci.events_insert_target").
					WillReturnError(errors.New("source scan failed"))
			},
			wantErr: "insert-select into single primary key target: source scan failed",
		},
		{
			name: "inserted row count is validated",
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("create table mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("select count\\(\\*\\) from mongodb_ci.events_insert_target").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("0"))
			},
			wantErr: "expected \"1\"",
		},
		{
			name: "both primary key layouts and duplicate rejection succeed",
			prepare: func(mock sqlmock.Sqlmock) {
				prepareSuccessfulTargets(mock)
				mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnError(errors.New("duplicate entry"))
				mock.ExpectQuery("select count\\(\\*\\) from mongodb_ci.events_insert_target").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow("1"))
			},
		},
		{
			name: "duplicate insert unexpectedly succeeds",
			prepare: func(mock sqlmock.Sqlmock) {
				prepareSuccessfulTargets(mock)
				mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantErr: "duplicate insert-select into primary-key target unexpectedly succeeded",
		},
		{
			name: "duplicate insert times out",
			prepare: func(mock sqlmock.Sqlmock) {
				prepareSuccessfulTargets(mock)
				mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnError(context.DeadlineExceeded)
			},
			wantErr: "duplicate insert-select into primary-key target timed out",
		},
		{
			name: "duplicate insert returns non duplicate error",
			prepare: func(mock sqlmock.Sqlmock) {
				prepareSuccessfulTargets(mock)
				mock.ExpectExec("insert into mongodb_ci.events_insert_target").WillReturnError(errors.New("connection reset"))
			},
			wantErr: "duplicate insert-select into primary-key target returned unexpected error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock := newMongoDBE2ESQLMock(t)
			test.prepare(mock)
			err := verifyPrimaryKeyInsertSelect(t.Context(), db)
			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMongoDBLocalE2EHelpers(t *testing.T) {
	t.Run("wait succeeds", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		mock.ExpectPing()
		require.NoError(t, waitForMO(t.Context(), db))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("wait observes cancellation", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		mock.ExpectPing().WillDelayFor(10 * time.Millisecond).WillReturnError(errors.New("not ready"))
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		defer cancel()
		require.ErrorIs(t, waitForMO(ctx, db), context.DeadlineExceeded)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("fixture manifest failures", func(t *testing.T) {
		_, err := loadFixtureManifest(filepath.Join(t.TempDir(), "missing.json"))
		require.ErrorContains(t, err, "read MongoDB fixture manifest")
		invalid := filepath.Join(t.TempDir(), "invalid.json")
		require.NoError(t, os.WriteFile(invalid, []byte("{"), 0o600))
		_, err = loadFixtureManifest(invalid)
		require.ErrorContains(t, err, "decode MongoDB fixture manifest")
		empty := filepath.Join(t.TempDir(), "empty.json")
		require.NoError(t, os.WriteFile(empty, []byte(`{"rows":[]}`), 0o600))
		_, err = loadFixtureManifest(empty)
		require.ErrorContains(t, err, "has no rows")
	})

	t.Run("row comparison failures", func(t *testing.T) {
		db, mock := newMongoDBE2ESQLMock(t)
		mock.ExpectQuery("query-error").WillReturnError(errors.New("source offline"))
		require.ErrorContains(t, expectRows(t.Context(), db, "query-error", nil), "source offline")
		mock.ExpectQuery("scan-error").WillReturnRows(sqlmock.NewRows([]string{"only"}).AddRow("value"))
		require.Error(t, expectRows(t.Context(), db, "scan-error", nil))
		mock.ExpectQuery("row-error").WillReturnRows(
			sqlmock.NewRows([]string{"a", "b", "c", "d", "e", "f"}).
				AddRow("1", "2", "3", "4", "5", "6").
				AddRow("1", "2", "3", "4", "5", "6").
				RowError(1, errors.New("getMore failed")))
		require.ErrorContains(t, expectRows(t.Context(), db, "row-error", nil), "getMore failed")
		mock.ExpectQuery("mismatch").WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c", "d", "e", "f"}))
		require.ErrorContains(t, expectRows(t.Context(), db, "mismatch", [][]string{{"expected"}}), "fixture result mismatch")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("row comparison supports temporal projection width", func(t *testing.T) {
		db, mock := newMongoDBE2ESQLMock(t)
		mock.ExpectQuery("temporal-width").WillReturnRows(sqlmock.NewRows([]string{"id", "d", "t"}).
			AddRow("a", "2026-03-08 01:59:59.123000", "2026-03-08 01:59:59.123000"))
		require.NoError(t, expectRows(t.Context(), db, "temporal-width", [][]string{{"a", "2026-03-08 01:59:59.123000", "2026-03-08 01:59:59.123000"}}))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("show create validation", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			row  *sqlmock.Rows
			err  error
		}{
			{name: "query error", err: errors.New("catalog offline")},
			{name: "incomplete", row: sqlmock.NewRows([]string{"table", "ddl"}).AddRow("raw", "CREATE TABLE raw(a int)")},
			{name: "secret leak", row: sqlmock.NewRows([]string{"table", "ddl"}).AddRow("raw", "ENGINE = MONGODB MONGODB_PATH connection secret://env/key")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db, mock := newMongoDBE2ESQLMock(t)
				expectation := mock.ExpectQuery("show create table")
				if tc.err != nil {
					expectation.WillReturnError(tc.err)
				} else {
					expectation.WillReturnRows(tc.row)
				}
				require.Error(t, verifyShowCreate(t.Context(), db))
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})

	t.Run("show connections validation", func(t *testing.T) {
		columns := []string{
			"name", "discovery_mode", "auth_mechanism", "tls_mode",
			"read_preference", "read_concern", "version", "disabled",
		}
		tests := []struct {
			name string
			rows *sqlmock.Rows
			err  error
			want string
		}{
			{name: "query error", err: errors.New("catalog offline"), want: "catalog offline"},
			{name: "unexpected columns", rows: sqlmock.NewRows([]string{"name"}).AddRow("mongodb_ci"), want: "unexpected columns"},
			{name: "metadata mismatch", rows: sqlmock.NewRows(columns).AddRow("mongodb_ci", "srv", "SCRAM-SHA-256", "disabled", "primary", "majority", 1, 0), want: "metadata mismatch"},
			{name: "invalid version", rows: sqlmock.NewRows(columns).AddRow("mongodb_ci", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 0, 0), want: "invalid version"},
			{name: "row error", rows: sqlmock.NewRows(columns).AddRow("mongodb_ci", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 1, 0).RowError(0, errors.New("getMore failed")), want: "getMore failed"},
			{name: "connection omitted", rows: sqlmock.NewRows(columns).AddRow("existing", "seeds", "SCRAM-SHA-256", "disabled", "primary", "majority", 1, 0), want: "omitted mongodb_ci"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				db, mock := newMongoDBE2ESQLMock(t)
				expectation := mock.ExpectQuery("show mongodb connections")
				if tc.err != nil {
					expectation.WillReturnError(tc.err)
				} else {
					expectation.WillReturnRows(tc.rows)
				}
				require.ErrorContains(t, verifyShowMongoDBConnections(t.Context(), db), tc.want)
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})

	t.Run("scalar and expected failure", func(t *testing.T) {
		db, mock := newMongoDBE2ESQLMock(t)
		mock.ExpectQuery("scalar-error").WillReturnError(errors.New("query failed"))
		require.ErrorContains(t, expectScalar(t.Context(), db, "scalar-error", "1"), "query failed")
		mock.ExpectQuery("scalar-mismatch").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("2"))
		require.ErrorContains(t, expectScalar(t.Context(), db, "scalar-mismatch", "1"), "expected")
		mock.ExpectQuery("select __mo_query").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("secret-actual"))
		err := expectScalar(t.Context(), db, "select __mo_query /* secret-query */", "secret-expected")
		require.ErrorContains(t, err, "result mismatch")
		require.NotContains(t, err.Error(), "secret-")
		mock.ExpectQuery("unexpected-success").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("1"))
		require.ErrorContains(t, expectQueryFailure(t.Context(), db, "unexpected-success", ""), "unexpectedly succeeded")
		mock.ExpectQuery("wrong-error").WillReturnError(errors.New("different"))
		require.ErrorContains(t, expectQueryFailure(t.Context(), db, "wrong-error", "disabled"), "without")
		mock.ExpectQuery("expected-error").WillReturnError(errors.New("connection disabled"))
		require.NoError(t, expectQueryFailure(t.Context(), db, "expected-error", "DISABLED"))
		mock.ExpectQuery("unexpected-statement-success").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("1"))
		require.ErrorContains(t, expectStatementRejected(t.Context(), db, "unexpected-statement-success", "privilege"), "unexpectedly succeeded")
		mock.ExpectQuery("statement-row-error").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("1").RowError(0, errors.New("read failed")))
		require.ErrorContains(t, expectStatementRejected(t.Context(), db, "statement-row-error", "privilege"), "failed while reading rows")
		mock.ExpectQuery("wrong-statement-error").WillReturnError(errors.New("network offline"))
		require.ErrorContains(t, expectStatementRejected(t.Context(), db, "wrong-statement-error", "privilege"), "without")
		mock.ExpectQuery("denied-statement").WillReturnError(errors.New("do not have privilege to execute the statement"))
		require.NoError(t, expectStatementRejected(t.Context(), db, "denied-statement", "PRIVILEGE"))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("prepared MongoDB scan failures", func(t *testing.T) {
		t.Run("prepare", func(t *testing.T) {
			db, mock := newMongoDBE2ESQLMock(t)
			mock.ExpectPrepare("select count").WillReturnError(errors.New("prepare failed"))
			require.ErrorContains(t, verifyPreparedMongoDBScan(t.Context(), db), "prepare MongoDB scan")
			require.NoError(t, mock.ExpectationsWereMet())
		})

		for _, tc := range []struct {
			name  string
			setup func(*sqlmock.ExpectedPrepare)
			want  string
		}{
			{
				name: "execute",
				setup: func(prepared *sqlmock.ExpectedPrepare) {
					prepared.ExpectQuery().WithArgs(int64(13)).WillReturnError(errors.New("execute failed"))
				},
				want: "execute prepared MongoDB scan",
			},
			{
				name: "metadata",
				setup: func(prepared *sqlmock.ExpectedPrepare) {
					prepared.ExpectQuery().WithArgs(int64(13)).WillReturnRows(
						sqlmock.NewRows([]string{"value"}).AddRow("3"))
				},
				want: "result metadata mismatch",
			},
			{
				name: "empty result",
				setup: func(prepared *sqlmock.ExpectedPrepare) {
					prepared.ExpectQuery().WithArgs(int64(13)).WillReturnRows(
						sqlmock.NewRows([]string{"count(*)"}))
				},
				want: "returned no rows",
			},
			{
				name: "row error",
				setup: func(prepared *sqlmock.ExpectedPrepare) {
					prepared.ExpectQuery().WithArgs(int64(13)).WillReturnRows(
						sqlmock.NewRows([]string{"count(*)"}).AddRow("3").RowError(0, errors.New("read failed")))
				},
				want: "read prepared MongoDB result",
			},
			{
				name: "scan",
				setup: func(prepared *sqlmock.ExpectedPrepare) {
					prepared.ExpectQuery().WithArgs(int64(13)).WillReturnRows(
						sqlmock.NewRows([]string{"count(*)"}).AddRow(nil))
				},
				want: "scan prepared MongoDB result",
			},
			{
				name: "value mismatch",
				setup: func(prepared *sqlmock.ExpectedPrepare) {
					prepared.ExpectQuery().WithArgs(int64(13)).WillReturnRows(
						sqlmock.NewRows([]string{"count(*)"}).AddRow("2"))
				},
				want: "expected \"3\", got \"2\"",
			},
			{
				name: "multiple aggregate rows",
				setup: func(prepared *sqlmock.ExpectedPrepare) {
					prepared.ExpectQuery().WithArgs(int64(13)).WillReturnRows(
						sqlmock.NewRows([]string{"count(*)"}).AddRow("3").AddRow("3"))
				},
				want: "aggregate returned more than one row",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				db, mock := newMongoDBE2ESQLMock(t)
				tc.setup(mock.ExpectPrepare("select count"))
				require.ErrorContains(t, verifyPreparedMongoDBScan(t.Context(), db), tc.want)
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	})

	t.Run("redaction and report", func(t *testing.T) {
		require.Equal(t, "<redacted MongoDB DDL>", redact("CREDENTIAL_SECRET_REF='secret'"))
		require.Equal(t, "<redacted MongoDB __mo_query statement>", redact("select __mo_query from t"))
		require.Equal(t, "select 1", redact("select 1"))
		dir := t.TempDir()
		require.NoError(t, writeReport(dir, report{Status: "passed", Cases: []string{"scan"}}))
		data, err := os.ReadFile(filepath.Join(dir, "report.json"))
		require.NoError(t, err)
		require.Contains(t, string(data), `"status": "passed"`)
		summary, err := os.ReadFile(filepath.Join(dir, "summary.md"))
		require.NoError(t, err)
		require.Contains(t, string(summary), "Status: **passed**")
		fileParent := filepath.Join(t.TempDir(), "not-a-directory")
		require.NoError(t, os.WriteFile(fileParent, []byte("x"), 0o600))
		require.Error(t, writeReport(filepath.Join(fileParent, "child"), report{}))
	})
}

func TestMongoDBTransferProfilerProtocol(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	host, commands := newMongoDBTransferTestServer(t)
	_, err := newMongoTransferMonitor(ctx, host, "", "secret")
	require.ErrorContains(t, err, "requires both root credentials")

	profiler, err := newMongoTransferMonitor(ctx, host, "root", "secret")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, profiler.Close(context.Background())) })
	require.NoError(t, profiler.Reset(ctx))
	returned, err := profiler.DocumentsReturned(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 6, returned)

	var received []string
	for len(commands) > 0 {
		received = append(received, <-commands)
	}
	joined := strings.Join(received, "\n")
	require.Contains(t, joined, "profile")
	require.Contains(t, joined, "drop")
	require.Contains(t, joined, "find")
	require.NoError(t, (*mongoProfiler)(nil).Close(ctx))
	require.NoError(t, (*mongoProfiler)(nil).Reset(ctx))
	returned, err = (*mongoProfiler)(nil).DocumentsReturned(ctx)
	require.NoError(t, err)
	require.Zero(t, returned)
}

func newMongoDBTransferTestServer(t *testing.T) (string, chan string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	commands := make(chan string, 16)
	digest := fmt.Sprintf("%x", md5.Sum([]byte("root:mongo:secret")))
	client, err := scram.SHA1.NewClientUnprepped("root", digest, "")
	require.NoError(t, err)
	credentials := client.GetStoredCredentials(scram.KeyFactors{Salt: "test-salt", Iters: 4096})
	server, err := scram.SHA1.NewServer(func(username string) (scram.StoredCredentials, error) {
		if username != "root" {
			return scram.StoredCredentials{}, fmt.Errorf("unknown test user %q", username)
		}
		return credentials, nil
	})
	require.NoError(t, err)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveMongoDBTransferTestConnection(conn, commands, server)
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String(), commands
}

func serveMongoDBTransferTestConnection(conn net.Conn, commands chan<- string, server *scram.Server) {
	defer conn.Close()
	var conversation *scram.ServerConversation
	for {
		header := make([]byte, 16)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		length := int(binary.LittleEndian.Uint32(header[:4]))
		if length < len(header) {
			return
		}
		body := make([]byte, length-len(header))
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		command := string(body)
		commands <- command
		response := bson.D{{Key: "ok", Value: 1}, {Key: "isWritablePrimary", Value: true}, {Key: "minWireVersion", Value: 0}, {Key: "maxWireVersion", Value: 13}}
		request, err := mongoDBTransferCommand(header, body)
		if err != nil {
			return
		}
		if payload, ok := request["payload"].(bson.Binary); ok {
			if _, started := request["saslStart"]; started {
				conversation = server.NewConversation()
			}
			if conversation != nil {
				reply, err := conversation.Step(string(payload.Data))
				if err != nil {
					return
				}
				response = bson.D{{Key: "ok", Value: 1}, {Key: "conversationId", Value: 1}, {Key: "done", Value: conversation.Done()}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte(reply)}}}
			}
		}
		if strings.Contains(command, "find") {
			response = bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "mongodb_source.system.profile"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "nreturned", Value: 5}}, bson.D{{Key: "nreturned", Value: 1}}}}}}}
		}
		encoded, err := bson.Marshal(response)
		if err != nil {
			return
		}
		if _, err := conn.Write(mongoDBTransferWireResponse(header, encoded)); err != nil {
			return
		}
	}
}

func mongoDBTransferCommand(header, body []byte) (bson.M, error) {
	var document []byte
	if binary.LittleEndian.Uint32(header[12:16]) == 2004 { // OP_QUERY
		if len(body) < 4 {
			return nil, errors.New("short OP_QUERY test command")
		}
		remainder := body[4:]
		if end := strings.IndexByte(string(remainder), 0); end >= 0 && len(remainder) >= end+9 {
			document = remainder[end+9:]
		}
	} else if len(body) >= 5 { // OP_MSG flags followed by a document section
		document = body[5:]
	}
	var command bson.M
	if err := bson.Unmarshal(document, &command); err != nil {
		return nil, err
	}
	return command, nil
}

func mongoDBTransferWireResponse(requestHeader, document []byte) []byte {
	requestID := binary.LittleEndian.Uint32(requestHeader[4:8])
	if binary.LittleEndian.Uint32(requestHeader[12:16]) == 2004 { // OP_QUERY
		message := make([]byte, 16+4+8+4+4+len(document))
		binary.LittleEndian.PutUint32(message[:4], uint32(len(message)))
		binary.LittleEndian.PutUint32(message[8:12], requestID)
		binary.LittleEndian.PutUint32(message[12:16], 1) // OP_REPLY
		binary.LittleEndian.PutUint32(message[32:36], 1)
		copy(message[36:], document)
		return message
	}
	message := make([]byte, 16+4+1+len(document))
	binary.LittleEndian.PutUint32(message[:4], uint32(len(message)))
	binary.LittleEndian.PutUint32(message[8:12], requestID)
	binary.LittleEndian.PutUint32(message[12:16], 2013) // OP_MSG
	message[20] = 0                                     // BSON document section
	copy(message[21:], document)
	return message
}

func mongoDBTestRepoRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func newMongoDBE2ESQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func expectMongoDBE2EScalar(mock sqlmock.Sqlmock, value string) {
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(value))
}

func TestMongoDBLocalE2ERedactionIsCaseInsensitive(t *testing.T) {
	require.True(t, strings.HasPrefix(redact("CrEdEnTiAl_SeCrEt_ReF=x"), "<redacted"))
}
