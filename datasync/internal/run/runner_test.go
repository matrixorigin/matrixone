package run

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

type fakeExecutor struct {
	moDumpCalls     int
	mysqlCalls      int
	failMoDumpCalls int
}

func (f *fakeExecutor) MoDump(ctx context.Context, req MoDumpRequest) error {
	f.moDumpCalls++
	if f.moDumpCalls <= f.failMoDumpCalls {
		return errors.New("temporary dump failure")
	}
	if err := os.MkdirAll(req.WorkDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(req.SQLFile, []byte("DROP TABLE IF EXISTS `t1`;"), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(req.WorkDir, "src_db_t1.csv"), []byte("1\n2\n"), 0o644)
}

func (f *fakeExecutor) MySQLSource(context.Context, MySQLSourceRequest) error {
	f.mysqlCalls++
	return nil
}

type fakeDB struct {
	sourceRows        int64
	targetRows        int64
	ensureTargetCalls int
}

func (f *fakeDB) CountSourceRows(context.Context, plan.Task) (int64, error) {
	return f.sourceRows, nil
}

func (f *fakeDB) EnsureTargetDatabase(context.Context, string) error {
	f.ensureTargetCalls++
	return nil
}

func (f *fakeDB) CountTargetRows(context.Context, string, string) (int64, error) {
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
