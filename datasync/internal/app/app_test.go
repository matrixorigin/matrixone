package app

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/datasync/internal/config"
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

type fakeDiscovery struct {
	tables []string
}

func (f fakeDiscovery) ListTables(context.Context, config.Source, string) ([]string, error) {
	return f.tables, nil
}

type fakeRunner struct {
	mode        run.Mode
	tasks       []plan.Task
	failedTasks int
}

func (f *fakeRunner) Run(ctx context.Context, mode run.Mode, runID string, tasks []plan.Task) (report.RunReport, error) {
	f.mode = mode
	f.tasks = tasks
	return report.RunReport{
		RunID: runID,
		Summary: report.Summary{
			TotalTasks:     len(tasks),
			SucceededTasks: len(tasks) - f.failedTasks,
			FailedTasks:    f.failedTasks,
		},
		Tables: []report.TableReport{{
			RunID:          runID,
			SourceName:     "tenant_a",
			SourceDatabase: "src_db",
			SourceTable:    "t1",
			TargetDatabase: "dst_db",
			ExportStatus:   report.StatusSuccess,
			ImportStatus:   report.StatusSkipped,
		}},
	}, nil
}
