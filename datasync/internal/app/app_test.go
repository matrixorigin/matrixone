package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/datasync/internal/config"
	"github.com/matrixorigin/datasync/internal/db"
	"github.com/matrixorigin/datasync/internal/plan"
	"github.com/matrixorigin/datasync/internal/report"
	"github.com/matrixorigin/datasync/internal/run"
)

func TestRunBuildsTasksFromDiscoveredTablesAndWritesReport(t *testing.T) {
	cfg := &config.Config{
		OutputDir:   t.TempDir(),
		Parallelism: 1,
		MoDumpPath:  "/bin/mo-dump",
		MySQLPath:   "/bin/mysql",
		Target: config.Endpoint{
			Name:     "target",
			Host:     "127.0.0.1",
			Port:     6001,
			User:     "target:admin",
			Password: "111",
		},
		Sources: []config.Source{{
			Endpoint: config.Endpoint{
				Name:     "tenant_a",
				Host:     "127.0.0.1",
				Port:     6001,
				User:     "a:admin",
				Password: "111",
			},
			Databases: []config.Database{{
				Name:          "src_db",
				Target:        "dst_db",
				ExcludeTables: []string{"skip"},
			}},
		}},
	}
	runner := &fakeRunner{}
	app := App{
		Config:    cfg,
		Mode:      run.ModeExport,
		Discovery: fakeDiscovery{tables: []string{"keep", "skip"}},
		Runner:    runner,
	}

	result, err := app.Run(context.Background(), "run1")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.PlannedTasks != 1 {
		t.Fatalf("PlannedTasks = %d, want 1", result.PlannedTasks)
	}
	if runner.mode != run.ModeExport {
		t.Fatalf("runner mode = %q, want export", runner.mode)
	}
	if filepath.Base(result.Report.Summary.JSONReportPath) != "report.json" {
		t.Fatalf("JSONReportPath = %q, want report.json", result.Report.Summary.JSONReportPath)
	}
	if got := runner.tasks[0].SourceTable; got != "keep" {
		t.Fatalf("planned table = %q, want keep", got)
	}
}

func TestRunPassesOptionsToDefaultRunner(t *testing.T) {
	originalOpenDB := openDB
	defer func() { openDB = originalOpenDB }()

	dir := t.TempDir()
	cfg := &config.Config{
		OutputDir:   dir,
		Parallelism: 1,
		MoDumpPath:  filepath.Join(dir, "mo-dump"),
		MySQLPath:   filepath.Join(dir, "mysql"),
		Retry:       config.RetryConfig{MaxAttempts: 1},
		Target:      config.Endpoint{Host: "target", Port: 6001, User: "target:admin", Password: "111"},
		Sources: []config.Source{{
			Endpoint:  config.Endpoint{Name: "tenant_a", Host: "source", Port: 6001, User: "source:admin", Password: "111"},
			Databases: []config.Database{{Name: "src_db", Target: "dst_db"}},
		}},
	}
	openDB = func(ctx context.Context, endpoint db.Endpoint, database string) (dbClient, error) {
		return &fakeDBClient{
			tables: []db.Table{{Name: "t1", Kind: "r"}},
			count:  2,
		}, nil
	}
	if err := os.WriteFile(cfg.MoDumpPath, []byte("#!/bin/sh\nprintf 'DROP TABLE IF EXISTS t1;\\n'\nprintf '1\\n2\\n' > src_db_t1.csv\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.MySQLPath, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	app := App{
		Config: cfg,
		Mode:   run.ModeSync,
		Options: run.Options{
			CleanupExportAfterImport: true,
		},
		Discovery: fakeDiscovery{tables: []string{"t1"}},
	}

	result, err := app.Run(context.Background(), "run1")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if _, err := os.Stat(filepath.Dir(result.Report.Tables[0].SQLFile)); !os.IsNotExist(err) {
		t.Fatalf("export dir exists after default runner cleanup, stat err=%v", err)
	}
}

func TestRunReturnsErrorWhenTableTaskFailsAfterWritingReport(t *testing.T) {
	cfg := &config.Config{
		OutputDir: t.TempDir(),
		Sources: []config.Source{{
			Endpoint:  config.Endpoint{Name: "tenant_a", Host: "127.0.0.1", Port: 6001, User: "a:admin", Password: "111"},
			Databases: []config.Database{{Name: "src_db", Target: "dst_db"}},
		}},
	}
	app := App{
		Config:    cfg,
		Discovery: fakeDiscovery{tables: []string{"t1"}},
		Runner: &fakeRunner{
			failedTasks: 1,
		},
	}

	result, err := app.Run(context.Background(), "run1")

	if err == nil {
		t.Fatal("Run() error = nil, want failed task error")
	}
	if !strings.Contains(err.Error(), "1 table tasks failed") {
		t.Fatalf("Run() error = %v", err)
	}
	if filepath.Base(result.Report.Summary.JSONReportPath) != "report.json" {
		t.Fatalf("JSONReportPath = %q, want report.json after failure", result.Report.Summary.JSONReportPath)
	}
}

func TestNewRunID(t *testing.T) {
	now := time.Date(2026, 6, 6, 7, 8, 9, 0, time.UTC)
	if got := NewRunID(now); got != "20260606-070809" {
		t.Fatalf("NewRunID() = %q, want 20260606-070809", got)
	}
}

func TestRunReturnsDiscoveryError(t *testing.T) {
	errBoom := errors.New("discover failed")
	app := App{
		Config: &config.Config{
			Sources: []config.Source{{
				Endpoint:  config.Endpoint{Name: "tenant_a"},
				Databases: []config.Database{{Name: "src_db"}},
			}},
		},
		Discovery: fakeDiscovery{err: errBoom},
		Runner:    &fakeRunner{},
	}

	_, err := app.Run(context.Background(), "run1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run() error = %v, want discovery error", err)
	}
}

func TestRunReturnsRunnerError(t *testing.T) {
	errBoom := errors.New("runner failed")
	app := App{
		Config: &config.Config{
			OutputDir: t.TempDir(),
			Sources: []config.Source{{
				Endpoint:  config.Endpoint{Name: "tenant_a"},
				Databases: []config.Database{{Name: "src_db", Target: "dst_db"}},
			}},
		},
		Discovery: fakeDiscovery{tables: []string{"t1"}},
		Runner:    &fakeRunner{err: errBoom},
	}

	_, err := app.Run(context.Background(), "run1")
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run() error = %v, want runner error", err)
	}
}

func TestRunReturnsReportWriteError(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	app := App{
		Config: &config.Config{
			OutputDir: file,
			Sources: []config.Source{{
				Endpoint:  config.Endpoint{Name: "tenant_a"},
				Databases: []config.Database{{Name: "src_db", Target: "dst_db"}},
			}},
		},
		Discovery: fakeDiscovery{tables: []string{"t1"}},
		Runner:    &fakeRunner{},
	}
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := app.Run(context.Background(), "run1")
	if err == nil {
		t.Fatal("Run() error = nil, want report write error")
	}
	if result.RunID != "run1" || result.PlannedTasks != 1 {
		t.Fatalf("result = %+v, want partial result", result)
	}
}

func TestRunImportModeBuildsTasksFromExistingReportWithoutDiscovery(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run1")
	existingReport := report.RunReport{
		RunID: "run1",
		Summary: report.Summary{
			TotalSourceRows: 7,
		},
		Tables: []report.TableReport{{
			SourceName:     "tenant_a",
			SourceHost:     "127.0.0.1",
			SourcePort:     6001,
			SourceDatabase: "src_db",
			SourceTable:    "t1",
			TargetDatabase: "dst_db",
			SourceRows:     7,
		}},
	}
	if _, err := report.Write(runDir, existingReport); err != nil {
		t.Fatal(err)
	}
	runner := &fakeRunner{}
	app := App{
		Config: &config.Config{OutputDir: dir},
		Mode:   run.ModeImport,
		Discovery: fakeDiscovery{
			err: errors.New("source discovery should not run in import mode"),
		},
		Runner: runner,
	}

	result, err := app.Run(context.Background(), "run1")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if result.PlannedTasks != 1 {
		t.Fatalf("PlannedTasks = %d, want 1", result.PlannedTasks)
	}
	if len(runner.tasks) != 1 {
		t.Fatalf("runner task count = %d, want 1", len(runner.tasks))
	}
	task := runner.tasks[0]
	if task.SourceName != "tenant_a" ||
		task.SourceHost != "127.0.0.1" ||
		task.SourcePort != 6001 ||
		task.SourceDatabase != "src_db" ||
		task.SourceTable != "t1" ||
		task.TargetDatabase != "dst_db" {
		t.Fatalf("task = %+v", task)
	}
	if result.Report.Tables[0].SourceRows != 7 {
		t.Fatalf("SourceRows = %d, want preserved source row count", result.Report.Tables[0].SourceRows)
	}
	if result.Report.Summary.TotalSourceRows != 7 {
		t.Fatalf("TotalSourceRows = %d, want preserved source row count", result.Report.Summary.TotalSourceRows)
	}
}

func TestMatrixOneDiscoveryListsTableNames(t *testing.T) {
	originalOpenDB := openDB
	defer func() { openDB = originalOpenDB }()
	fake := &fakeDBClient{
		tables: []db.Table{{Name: "t1", Kind: "r"}, {Name: "t2", Kind: "r"}},
	}
	openDB = func(ctx context.Context, endpoint db.Endpoint, database string) (dbClient, error) {
		if endpoint.User != "a:admin" || database != "src_db" {
			t.Fatalf("openDB endpoint=%+v database=%q", endpoint, database)
		}
		return fake, nil
	}

	tables, err := MatrixOneDiscovery{}.ListTables(context.Background(), config.Source{
		Endpoint: config.Endpoint{Host: "127.0.0.1", Port: 6001, User: "a:admin", Password: "111"},
	}, "src_db")
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}

	if !reflect.DeepEqual(tables, []string{"t1", "t2"}) {
		t.Fatalf("tables = %#v, want t1/t2", tables)
	}
	if !fake.closed {
		t.Fatal("client was not closed")
	}
}

func TestMatrixOneRunDBUsesConfiguredEndpoints(t *testing.T) {
	originalOpenDB := openDB
	defer func() { openDB = originalOpenDB }()
	var calls []string
	openDB = func(ctx context.Context, endpoint db.Endpoint, database string) (dbClient, error) {
		calls = append(calls, endpoint.User+"@"+database)
		return &fakeDBClient{count: 5}, nil
	}
	runDB := MatrixOneRunDB{Config: &config.Config{
		Target: config.Endpoint{Host: "target", Port: 6001, User: "target:admin", Password: "111"},
	}}
	task := plan.Task{
		SourceHost:     "source",
		SourcePort:     6002,
		SourceUser:     "source:admin",
		SourcePassword: "222",
		SourceDatabase: "src_db",
		SourceTable:    "t1",
	}

	sourceRows, err := runDB.CountSourceRows(context.Background(), task)
	if err != nil {
		t.Fatalf("CountSourceRows() error = %v", err)
	}
	if sourceRows != 5 {
		t.Fatalf("CountSourceRows() = %d, want 5", sourceRows)
	}
	if err := runDB.EnsureTargetDatabase(context.Background(), "dst_db"); err != nil {
		t.Fatalf("EnsureTargetDatabase() error = %v", err)
	}
	targetRows, err := runDB.CountTargetRows(context.Background(), "dst_db", "t1")
	if err != nil {
		t.Fatalf("CountTargetRows() error = %v", err)
	}
	if targetRows != 5 {
		t.Fatalf("CountTargetRows() = %d, want 5", targetRows)
	}

	want := []string{"source:admin@src_db", "target:admin@", "target:admin@dst_db"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("openDB calls = %#v, want %#v", calls, want)
	}
}

type fakeDiscovery struct {
	tables []string
	err    error
}

func (f fakeDiscovery) ListTables(context.Context, config.Source, string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.tables, nil
}

type fakeRunner struct {
	mode        run.Mode
	tasks       []plan.Task
	failedTasks int
	err         error
}

func (f *fakeRunner) Run(ctx context.Context, mode run.Mode, runID string, tasks []plan.Task) (report.RunReport, error) {
	f.mode = mode
	f.tasks = tasks
	if f.err != nil {
		return report.RunReport{}, f.err
	}
	var sourceRows int64
	if len(tasks) > 0 {
		sourceRows = tasks[0].SourceRows
	}
	return report.RunReport{
		RunID: runID,
		Summary: report.Summary{
			TotalTasks:      len(tasks),
			SucceededTasks:  len(tasks) - f.failedTasks,
			FailedTasks:     f.failedTasks,
			TotalSourceRows: sourceRows,
		},
		Tables: []report.TableReport{{
			RunID:          runID,
			SourceName:     "tenant_a",
			SourceDatabase: "src_db",
			SourceTable:    "t1",
			TargetDatabase: "dst_db",
			SourceRows:     sourceRows,
			ExportStatus:   report.StatusSuccess,
			ImportStatus:   report.StatusSkipped,
		}},
	}, nil
}

type fakeDBClient struct {
	tables []db.Table
	count  int64
	closed bool
}

func (f *fakeDBClient) Close() error {
	f.closed = true
	return nil
}

func (f *fakeDBClient) ListOrdinaryTables(context.Context, string) ([]db.Table, error) {
	return f.tables, nil
}

func (f *fakeDBClient) CountRows(context.Context, string, string) (int64, error) {
	return f.count, nil
}

func (f *fakeDBClient) EnsureDatabase(context.Context, string) error {
	return nil
}
