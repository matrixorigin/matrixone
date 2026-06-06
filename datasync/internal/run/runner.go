package run

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/datasync/internal/config"
	"github.com/matrixorigin/datasync/internal/plan"
	"github.com/matrixorigin/datasync/internal/report"
	"github.com/matrixorigin/datasync/internal/retry"
)

type Mode string

const (
	ModeSync   Mode = "sync"
	ModeExport Mode = "export"
	ModeImport Mode = "import"
)

type Runner struct {
	Config   *config.Config
	Mode     Mode
	Executor Executor
	DB       DB
}

type Executor interface {
	MoDump(context.Context, MoDumpRequest) error
	MySQLSource(context.Context, MySQLSourceRequest) error
}

type DB interface {
	CountSourceRows(context.Context, plan.Task) (int64, error)
	EnsureTargetDatabase(context.Context, string) error
	CountTargetRows(context.Context, string, string) (int64, error)
}

type MoDumpRequest struct {
	Binary  string
	Task    plan.Task
	WorkDir string
	SQLFile string
}

type MySQLSourceRequest struct {
	Binary   string
	Target   config.Endpoint
	Database string
	SQLFile  string
}

func ParseMode(value string) (Mode, error) {
	switch Mode(value) {
	case "", ModeSync:
		return ModeSync, nil
	case ModeExport:
		return ModeExport, nil
	case ModeImport:
		return ModeImport, nil
	default:
		return "", fmt.Errorf("invalid mode %q, want sync, export, or import", value)
	}
}

func (r Runner) Run(ctx context.Context, runID string, tasks []plan.Task) (report.RunReport, error) {
	start := time.Now()
	if r.Executor == nil {
		r.Executor = LocalExecutor{}
	}
	if r.Config.Parallelism < 1 {
		r.Config.Parallelism = 1
	}

	rows := make([]report.TableReport, len(tasks))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < r.Config.Parallelism; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				rows[idx] = r.runTask(ctx, runID, tasks[idx])
			}
		}()
	}
	for i := range tasks {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	summary := report.Summary{TotalTasks: len(rows), Duration: time.Since(start)}
	for _, row := range rows {
		summary.TotalSourceRows += row.SourceRows
		summary.TotalTargetRows += row.TargetRows
		exportOK := row.ExportStatus == report.StatusSuccess || row.ExportStatus == report.StatusSkipped
		if row.Error == "" && exportOK && row.ImportStatus != report.StatusFailed {
			summary.SucceededTasks++
		} else {
			summary.FailedTasks++
		}
	}
	return report.RunReport{RunID: runID, Summary: summary, Tables: rows}, nil
}

func (r Runner) runTask(ctx context.Context, runID string, task plan.Task) report.TableReport {
	row := report.TableReport{
		RunID:          runID,
		SourceName:     task.SourceName,
		SourceHost:     task.SourceHost,
		SourcePort:     task.SourcePort,
		SourceDatabase: task.SourceDatabase,
		SourceTable:    task.SourceTable,
		TargetDatabase: task.TargetDatabase,
		ImportStatus:   report.StatusPending,
	}
	tableDir, sqlFile, csvFile := r.taskPaths(runID, task)
	row.SQLFile = sqlFile
	row.CSVFile = csvFile

	retryCfg := retry.Config{MaxAttempts: r.Config.Retry.MaxAttempts, Backoff: r.Config.Retry.Backoff}
	var err error
	if r.effectiveMode() == ModeImport {
		row.ExportStatus = report.StatusSkipped
	} else {
		sourceRows, err := r.DB.CountSourceRows(ctx, task)
		if err != nil {
			row.ExportStatus = report.StatusFailed
			row.Error = err.Error()
			return row
		}
		row.SourceRows = sourceRows

		row.ExportStartedAt = time.Now()
		err = retry.Do(ctx, retryCfg, func(ctx context.Context, attempt int) error {
			row.ExportAttempts = attempt
			if err := os.RemoveAll(tableDir); err != nil {
				return err
			}
			if err := os.MkdirAll(tableDir, 0o755); err != nil {
				return err
			}
			return r.Executor.MoDump(ctx, MoDumpRequest{
				Binary:  r.Config.MoDumpPath,
				Task:    task,
				WorkDir: tableDir,
				SQLFile: sqlFile,
			})
		})
		row.ExportFinishedAt = time.Now()
		row.ExportDuration = row.ExportFinishedAt.Sub(row.ExportStartedAt)
		if err != nil {
			row.ExportStatus = report.StatusFailed
			row.Error = err.Error()
			return row
		}
		row.ExportStatus = report.StatusSuccess
	}
	if _, err := requireFile(sqlFile); err != nil {
		row.Error = err.Error()
		if row.ExportStatus == report.StatusSkipped {
			row.ImportStatus = report.StatusFailed
		} else {
			row.ExportStatus = report.StatusFailed
		}
		return row
	}
	csvInfo, err := requireFile(csvFile)
	if err != nil {
		row.Error = err.Error()
		if row.ExportStatus == report.StatusSkipped {
			row.ImportStatus = report.StatusFailed
		} else {
			row.ExportStatus = report.StatusFailed
		}
		return row
	}
	row.CSVFileSize = csvInfo.Size()

	if r.effectiveMode() == ModeExport {
		row.ImportStatus = report.StatusSkipped
		return row
	}

	if err := r.DB.EnsureTargetDatabase(ctx, task.TargetDatabase); err != nil {
		row.ImportStatus = report.StatusFailed
		row.Error = err.Error()
		return row
	}
	row.ImportStartedAt = time.Now()
	err = retry.Do(ctx, retryCfg, func(ctx context.Context, attempt int) error {
		row.ImportAttempts = attempt
		return r.Executor.MySQLSource(ctx, MySQLSourceRequest{
			Binary:   r.Config.MySQLPath,
			Target:   r.Config.Target,
			Database: task.TargetDatabase,
			SQLFile:  sqlFile,
		})
	})
	row.ImportFinishedAt = time.Now()
	row.ImportDuration = row.ImportFinishedAt.Sub(row.ImportStartedAt)
	if err != nil {
		row.ImportStatus = report.StatusFailed
		row.Error = err.Error()
		return row
	}
	targetRows, err := r.DB.CountTargetRows(ctx, task.TargetDatabase, task.SourceTable)
	if err != nil {
		row.ImportStatus = report.StatusFailed
		row.Error = err.Error()
		return row
	}
	row.TargetRows = targetRows
	row.ImportStatus = report.StatusSuccess
	if row.ExportStatus != report.StatusSkipped && row.SourceRows != row.TargetRows {
		row.ImportStatus = report.StatusFailed
		row.Error = fmt.Sprintf("row count mismatch: source=%d target=%d", row.SourceRows, row.TargetRows)
	}
	return row
}

func (r Runner) effectiveMode() Mode {
	if r.Mode == "" {
		return ModeSync
	}
	return r.Mode
}

func (r Runner) taskPaths(runID string, task plan.Task) (string, string, string) {
	tableDir := filepath.Join(r.Config.OutputDir, runID, "exports", task.SourceName, task.SourceDatabase, task.SourceTable)
	sqlFile := filepath.Join(tableDir, task.SourceTable+".sql")
	csvFile := filepath.Join(tableDir, task.SourceDatabase+"_"+task.SourceTable+".csv")
	return tableDir, sqlFile, csvFile
}

func requireFile(path string) (os.FileInfo, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory", path)
	}
	return info, nil
}

type LocalExecutor struct{}

func (LocalExecutor) MoDump(ctx context.Context, req MoDumpRequest) error {
	out, err := os.Create(req.SQLFile)
	if err != nil {
		return err
	}
	defer out.Close()

	cmd := exec.CommandContext(ctx, req.Binary,
		"-u", req.Task.SourceUser,
		"-p", req.Task.SourcePassword,
		"-h", req.Task.SourceHost,
		"-P", strconv.Itoa(req.Task.SourcePort),
		"-db", req.Task.SourceDatabase,
		"-tbl", req.Task.SourceTable,
		"-csv",
		"--local-infile=true",
	)
	cmd.Dir = req.WorkDir
	cmd.Stdout = out
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (LocalExecutor) MySQLSource(ctx context.Context, req MySQLSourceRequest) error {
	cmd := exec.CommandContext(ctx, req.Binary,
		"--local-infile=1",
		"-h", req.Target.Host,
		"-P", strconv.Itoa(req.Target.Port),
		"-u", req.Target.User,
		"-p"+req.Target.Password,
		req.Database,
		"-e", "source "+req.SQLFile,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
