package run

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/datasync/internal/config"
	"github.com/matrixorigin/datasync/internal/plan"
	"github.com/matrixorigin/datasync/internal/report"
)

func TestTaskPathsUseStableTableDirectory(t *testing.T) {
	dir := t.TempDir()
	task := plan.Task{
		SourceName:     "tenant_a",
		SourceHost:     "127.0.0.1",
		SourcePort:     6001,
		SourceUser:     "a:admin",
		SourcePassword: "111",
		SourceDatabase: "src_db",
		SourceTable:    "t1",
		TargetDatabase: "dst_db",
	}
	r := Runner{Config: &config.Config{OutputDir: dir}}

	tableDir, sqlFile, csvFile := r.taskPaths("run1", task)

	if !strings.HasSuffix(tableDir, filepath.Join("run1", "exports", "tenant_a", "src_db", "t1")) {
		t.Fatalf("tableDir = %s", tableDir)
	}
	if filepath.Base(sqlFile) != "t1.sql" {
		t.Fatalf("sqlFile = %s", sqlFile)
	}
	if filepath.Base(csvFile) != "src_db_t1.csv" {
		t.Fatalf("csvFile = %s", csvFile)
	}
}

func TestRunTaskRetriesExportAndImports(t *testing.T) {
	dir := t.TempDir()
	exec := &fakeExecutor{failMoDumpCalls: 1}
	db := &fakeDB{sourceRows: 2, targetRows: 2}
	r := Runner{
		Config:   testConfig(dir),
		Mode:     ModeSync,
		Executor: exec,
		DB:       db,
	}

	row := r.runTask(context.Background(), "run1", testTask())

	if row.ExportStatus != report.StatusSuccess || row.ImportStatus != report.StatusSuccess {
		t.Fatalf("row = %+v", row)
	}
	if row.ExportAttempts != 2 || row.ImportAttempts != 1 {
		t.Fatalf("attempts export=%d import=%d", row.ExportAttempts, row.ImportAttempts)
	}
	if row.CSVFileSize != 4 {
		t.Fatalf("CSVFileSize = %d, want 4", row.CSVFileSize)
	}
	if exec.moDumpCalls != 2 || exec.mysqlCalls != 1 {
		t.Fatalf("moDumpCalls=%d mysqlCalls=%d", exec.moDumpCalls, exec.mysqlCalls)
	}
	if db.ensureTargetCalls != 1 {
		t.Fatalf("ensureTargetCalls = %d, want 1", db.ensureTargetCalls)
	}
}

func TestRunTaskExportModeSkipsImport(t *testing.T) {
	dir := t.TempDir()
	exec := &fakeExecutor{}
	db := &fakeDB{sourceRows: 2, targetRows: 0}
	r := Runner{
		Config:   testConfig(dir),
		Mode:     ModeExport,
		Executor: exec,
		DB:       db,
	}

	row := r.runTask(context.Background(), "run1", testTask())

	if row.ExportStatus != report.StatusSuccess || row.ImportStatus != report.StatusSkipped {
		t.Fatalf("row = %+v", row)
	}
	if exec.mysqlCalls != 0 || db.ensureTargetCalls != 0 {
		t.Fatalf("mysqlCalls=%d ensureTargetCalls=%d, want skipped import", exec.mysqlCalls, db.ensureTargetCalls)
	}
}

func TestRunTaskImportModeSkipsExportAndImportsExistingFiles(t *testing.T) {
	dir := t.TempDir()
	task := testTask()
	task.SourceRows = 2
	paths := Runner{Config: testConfig(dir)}
	tableDir, sqlFile, csvFile := paths.taskPaths("run1", task)
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sqlFile, []byte("DROP TABLE IF EXISTS `t1`;"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(csvFile, []byte("1\n2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	exec := &fakeExecutor{}
	db := &fakeDB{sourceRows: 2, targetRows: 2}
	r := Runner{
		Config:   testConfig(dir),
		Mode:     ModeImport,
		Executor: exec,
		DB:       db,
	}

	row := r.runTask(context.Background(), "run1", task)

	if row.ExportStatus != report.StatusSkipped || row.ImportStatus != report.StatusSuccess {
		t.Fatalf("row = %+v", row)
	}
	if row.SourceRows != 2 {
		t.Fatalf("SourceRows = %d, want preserved source row count", row.SourceRows)
	}
	if row.CSVFileSize != 4 {
		t.Fatalf("CSVFileSize = %d, want 4", row.CSVFileSize)
	}
	if exec.moDumpCalls != 0 || exec.mysqlCalls != 1 {
		t.Fatalf("moDumpCalls=%d mysqlCalls=%d", exec.moDumpCalls, exec.mysqlCalls)
	}
}

func TestRunImportModeCountsSkippedExportAsSuccess(t *testing.T) {
	dir := t.TempDir()
	task := testTask()
	task.SourceRows = 2
	paths := Runner{Config: testConfig(dir)}
	tableDir, sqlFile, csvFile := paths.taskPaths("run1", task)
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sqlFile, []byte("DROP TABLE IF EXISTS `t1`;"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(csvFile, []byte("1\n2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := Runner{
		Config:   testConfig(dir),
		Mode:     ModeImport,
		Executor: &fakeExecutor{},
		DB:       &fakeDB{targetRows: 2},
	}

	out, err := r.Run(context.Background(), "run1", []plan.Task{task})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if out.Summary.SucceededTasks != 1 || out.Summary.FailedTasks != 0 {
		t.Fatalf("summary = %+v, want import task counted as success", out.Summary)
	}
	if out.Summary.TotalSourceRows != 2 {
		t.Fatalf("TotalSourceRows = %d, want preserved source row count", out.Summary.TotalSourceRows)
	}
}

func TestRunSummaryCountsFailures(t *testing.T) {
	dir := t.TempDir()
	r := Runner{
		Config:   testConfig(dir),
		Mode:     ModeSync,
		Executor: &fakeExecutor{},
		DB:       &fakeDB{sourceRows: 2, targetRows: 1},
	}

	out, err := r.Run(context.Background(), "run1", []plan.Task{testTask()})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if out.Summary.TotalTasks != 1 || out.Summary.FailedTasks != 1 || out.Summary.SucceededTasks != 0 {
		t.Fatalf("summary = %+v", out.Summary)
	}
	if !strings.Contains(out.Tables[0].Error, "row count mismatch") {
		t.Fatalf("table error = %q, want row count mismatch", out.Tables[0].Error)
	}
}

func TestRunTaskRetriesImportWhenTargetRowCountMismatches(t *testing.T) {
	dir := t.TempDir()
	exec := &fakeExecutor{}
	db := &fakeDB{sourceRows: 2, targetRowsByCall: []int64{1, 2}}
	r := Runner{
		Config:   testConfig(dir),
		Mode:     ModeSync,
		Executor: exec,
		DB:       db,
	}

	row := r.runTask(context.Background(), "run1", testTask())

	if row.ImportStatus != report.StatusSuccess {
		t.Fatalf("ImportStatus = %q error=%q, want success after retry", row.ImportStatus, row.Error)
	}
	if exec.mysqlCalls != 2 {
		t.Fatalf("mysqlCalls = %d, want row-count mismatch retry", exec.mysqlCalls)
	}
	if row.ImportAttempts != 2 {
		t.Fatalf("ImportAttempts = %d, want 2", row.ImportAttempts)
	}
	if row.TargetRows != 2 {
		t.Fatalf("TargetRows = %d, want final matching row count", row.TargetRows)
	}
}

func TestRunTaskKeepsExportFilesByDefaultAfterSyncImportSuccess(t *testing.T) {
	dir := t.TempDir()
	r := Runner{
		Config:   testConfig(dir),
		Mode:     ModeSync,
		Executor: &fakeExecutor{},
		DB:       &fakeDB{sourceRows: 2, targetRows: 2},
	}

	row := r.runTask(context.Background(), "run1", testTask())

	if row.ImportStatus != report.StatusSuccess {
		t.Fatalf("row = %+v", row)
	}
	if _, err := os.Stat(filepath.Dir(row.SQLFile)); err != nil {
		t.Fatalf("export dir missing after default sync import: %v", err)
	}
}

func TestRunTaskCleansExportFilesAfterSyncImportSuccessWhenEnabled(t *testing.T) {
	dir := t.TempDir()
	r := Runner{
		Config:   testConfig(dir),
		Mode:     ModeSync,
		Options:  Options{CleanupExportAfterImport: true},
		Executor: &fakeExecutor{},
		DB:       &fakeDB{sourceRows: 2, targetRows: 2},
	}

	row := r.runTask(context.Background(), "run1", testTask())

	if row.ImportStatus != report.StatusSuccess {
		t.Fatalf("row = %+v", row)
	}
	if _, err := os.Stat(filepath.Dir(row.SQLFile)); !os.IsNotExist(err) {
		t.Fatalf("export dir exists after cleanup, stat err=%v", err)
	}
	if row.CSVFileSize != 4 {
		t.Fatalf("CSVFileSize = %d, want report to keep pre-cleanup size", row.CSVFileSize)
	}
}

func TestRunTaskKeepsExportFilesAfterSyncImportFailureWhenCleanupEnabled(t *testing.T) {
	dir := t.TempDir()
	r := Runner{
		Config:   testConfig(dir),
		Mode:     ModeSync,
		Options:  Options{CleanupExportAfterImport: true},
		Executor: &fakeExecutor{},
		DB:       &fakeDB{sourceRows: 2, targetRows: 1},
	}

	row := r.runTask(context.Background(), "run1", testTask())

	if row.ImportStatus != report.StatusFailed {
		t.Fatalf("row = %+v, want failed import", row)
	}
	if _, err := os.Stat(filepath.Dir(row.SQLFile)); err != nil {
		t.Fatalf("export dir missing after failed sync import: %v", err)
	}
}

func TestRunTaskReturnsSourceCountError(t *testing.T) {
	errBoom := errors.New("count failed")
	r := Runner{
		Config:   testConfig(t.TempDir()),
		Mode:     ModeSync,
		Executor: &fakeExecutor{},
		DB:       &fakeDB{sourceErr: errBoom},
	}

	row := r.runTask(context.Background(), "run1", testTask())
	if row.ExportStatus != report.StatusFailed || !strings.Contains(row.Error, "count failed") {
		t.Fatalf("row = %+v, want source count failure", row)
	}
}

func TestRunTaskReturnsMissingSQLFileError(t *testing.T) {
	r := Runner{
		Config:   testConfig(t.TempDir()),
		Mode:     ModeSync,
		Executor: &fakeExecutor{skipSQL: true},
		DB:       &fakeDB{sourceRows: 2},
	}

	row := r.runTask(context.Background(), "run1", testTask())
	if row.ExportStatus != report.StatusFailed || !strings.Contains(row.Error, "t1.sql") {
		t.Fatalf("row = %+v, want missing SQL failure", row)
	}
}

func TestRunTaskReturnsMissingCSVFileError(t *testing.T) {
	r := Runner{
		Config:   testConfig(t.TempDir()),
		Mode:     ModeSync,
		Executor: &fakeExecutor{skipCSV: true},
		DB:       &fakeDB{sourceRows: 2},
	}

	row := r.runTask(context.Background(), "run1", testTask())
	if row.ExportStatus != report.StatusFailed || !strings.Contains(row.Error, "src_db_t1.csv") {
		t.Fatalf("row = %+v, want missing CSV failure", row)
	}
}

func TestRunTaskReturnsEnsureTargetError(t *testing.T) {
	errBoom := errors.New("ensure failed")
	r := Runner{
		Config:   testConfig(t.TempDir()),
		Mode:     ModeSync,
		Executor: &fakeExecutor{},
		DB:       &fakeDB{sourceRows: 2, ensureTargetErr: errBoom},
	}

	row := r.runTask(context.Background(), "run1", testTask())
	if row.ImportStatus != report.StatusFailed || !strings.Contains(row.Error, "ensure failed") {
		t.Fatalf("row = %+v, want ensure target failure", row)
	}
}

func TestRunTaskReturnsMySQLSourceError(t *testing.T) {
	errBoom := errors.New("mysql failed")
	r := Runner{
		Config:   testConfig(t.TempDir()),
		Mode:     ModeSync,
		Executor: &fakeExecutor{mysqlErr: errBoom},
		DB:       &fakeDB{sourceRows: 2},
	}

	row := r.runTask(context.Background(), "run1", testTask())
	if row.ImportStatus != report.StatusFailed || !strings.Contains(row.Error, "mysql failed") {
		t.Fatalf("row = %+v, want mysql failure", row)
	}
}

func TestRunTaskReturnsTargetCountError(t *testing.T) {
	errBoom := errors.New("target count failed")
	r := Runner{
		Config:   testConfig(t.TempDir()),
		Mode:     ModeSync,
		Executor: &fakeExecutor{},
		DB:       &fakeDB{sourceRows: 2, targetErr: errBoom},
	}

	row := r.runTask(context.Background(), "run1", testTask())
	if row.ImportStatus != report.StatusFailed || !strings.Contains(row.Error, "target count failed") {
		t.Fatalf("row = %+v, want target count failure", row)
	}
}

func TestRequireFileRejectsDirectory(t *testing.T) {
	dir := t.TempDir()
	if _, err := requireFile(dir); err == nil {
		t.Fatal("requireFile() error = nil, want directory error")
	}
}

func TestLocalExecutorMoDumpRunsBinaryInWorkDirAndWritesStdout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script test")
	}
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args.txt")
	pwdFile := filepath.Join(dir, "pwd.txt")
	binary := writeShellScript(t, dir, "mo-dump", `#!/bin/sh
printf '%s\n' "$@" > "`+argsFile+`"
pwd > "`+pwdFile+`"
printf 'DROP TABLE IF EXISTS t1;\n'
`)
	sqlFile := filepath.Join(dir, "out.sql")
	task := testTask()

	err := LocalExecutor{}.MoDump(context.Background(), MoDumpRequest{
		Binary:  binary,
		Task:    task,
		WorkDir: dir,
		SQLFile: sqlFile,
	})
	if err != nil {
		t.Fatalf("MoDump() error = %v", err)
	}

	sqlBytes, err := os.ReadFile(sqlFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sqlBytes), "DROP TABLE") {
		t.Fatalf("SQL output = %q, want dump stdout", string(sqlBytes))
	}
	argsBytes, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	args := string(argsBytes)
	for _, want := range []string{"-u\na:admin", "-p\n111", "-h\n127.0.0.1", "-P\n6001", "-db\nsrc_db", "-tbl\nt1", "-csv", "--local-infile=true"} {
		if !strings.Contains(args, want) {
			t.Fatalf("args = %q, missing %q", args, want)
		}
	}
	pwdBytes, err := os.ReadFile(pwdFile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(pwdBytes)) != dir {
		t.Fatalf("pwd = %q, want %q", strings.TrimSpace(string(pwdBytes)), dir)
	}
}

func TestLocalExecutorMySQLSourceRunsSourceCommand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script test")
	}
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "mysql-args.txt")
	binary := writeShellScript(t, dir, "mysql", `#!/bin/sh
printf '%s\n' "$@" > "`+argsFile+`"
`)

	err := LocalExecutor{}.MySQLSource(context.Background(), MySQLSourceRequest{
		Binary:   binary,
		Target:   testConfig(dir).Target,
		Database: "dst_db",
		SQLFile:  "/tmp/t1.sql",
	})
	if err != nil {
		t.Fatalf("MySQLSource() error = %v", err)
	}

	argsBytes, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	args := string(argsBytes)
	for _, want := range []string{"--local-infile=1", "-h\n127.0.0.1", "-P\n6001", "-u\ntarget:admin", "-p111", "dst_db", "-e\nsource /tmp/t1.sql"} {
		if !strings.Contains(args, want) {
			t.Fatalf("args = %q, missing %q", args, want)
		}
	}
}

func TestParseMode(t *testing.T) {
	tests := []struct {
		value string
		want  Mode
	}{
		{value: "", want: ModeSync},
		{value: "sync", want: ModeSync},
		{value: "export", want: ModeExport},
		{value: "import", want: ModeImport},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			got, err := ParseMode(tt.value)
			if err != nil {
				t.Fatalf("ParseMode() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ParseMode() = %q, want %q", got, tt.want)
			}
		})
	}

	if _, err := ParseMode("copy"); err == nil {
		t.Fatal("ParseMode(copy) error = nil, want invalid mode error")
	}
}

func writeShellScript(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

type fakeExecutor struct {
	moDumpCalls     int
	mysqlCalls      int
	failMoDumpCalls int
	skipSQL         bool
	skipCSV         bool
	mysqlErr        error
}

func (f *fakeExecutor) MoDump(ctx context.Context, req MoDumpRequest) error {
	f.moDumpCalls++
	if f.moDumpCalls <= f.failMoDumpCalls {
		return errors.New("temporary dump failure")
	}
	if err := os.MkdirAll(req.WorkDir, 0o755); err != nil {
		return err
	}
	if !f.skipSQL {
		if err := os.WriteFile(req.SQLFile, []byte("DROP TABLE IF EXISTS `t1`;"), 0o644); err != nil {
			return err
		}
	}
	if !f.skipCSV {
		return os.WriteFile(filepath.Join(req.WorkDir, "src_db_t1.csv"), []byte("1\n2\n"), 0o644)
	}
	return nil
}

func (f *fakeExecutor) MySQLSource(context.Context, MySQLSourceRequest) error {
	f.mysqlCalls++
	return f.mysqlErr
}

type fakeDB struct {
	sourceRows        int64
	targetRows        int64
	targetRowsByCall  []int64
	ensureTargetCalls int
	sourceErr         error
	ensureTargetErr   error
	targetErr         error
	targetCalls       int
}

func (f *fakeDB) CountSourceRows(context.Context, plan.Task) (int64, error) {
	if f.sourceErr != nil {
		return 0, f.sourceErr
	}
	return f.sourceRows, nil
}

func (f *fakeDB) EnsureTargetDatabase(context.Context, string) error {
	f.ensureTargetCalls++
	return f.ensureTargetErr
}

func (f *fakeDB) CountTargetRows(context.Context, string, string) (int64, error) {
	f.targetCalls++
	if f.targetErr != nil {
		return 0, f.targetErr
	}
	if len(f.targetRowsByCall) > 0 {
		idx := f.targetCalls - 1
		if idx >= len(f.targetRowsByCall) {
			idx = len(f.targetRowsByCall) - 1
		}
		return f.targetRowsByCall[idx], nil
	}
	return f.targetRows, nil
}

func testConfig(dir string) *config.Config {
	return &config.Config{
		MoDumpPath:  "/bin/mo-dump",
		MySQLPath:   "/bin/mysql",
		OutputDir:   dir,
		Parallelism: 1,
		Retry: config.RetryConfig{
			MaxAttempts: 2,
			Backoff:     time.Nanosecond,
		},
		Target: config.Endpoint{
			Name:     "target",
			Host:     "127.0.0.1",
			Port:     6001,
			User:     "target:admin",
			Password: "111",
		},
	}
}

func testTask() plan.Task {
	return plan.Task{
		SourceName:     "tenant_a",
		SourceHost:     "127.0.0.1",
		SourcePort:     6001,
		SourceUser:     "a:admin",
		SourcePassword: "111",
		SourceDatabase: "src_db",
		SourceTable:    "t1",
		TargetDatabase: "dst_db",
	}
}
