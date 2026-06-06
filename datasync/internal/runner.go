package datasync

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Mode string

const (
	ModeSync   Mode = "sync"
	ModeExport Mode = "export"
	ModeImport Mode = "import"
)

type Runner struct {
	Config   *Config
	Mode     Mode
	Options  Options
	Executor Executor
	DB       DB
	Progress io.Writer
}

type Options struct {
	CleanupExportAfterImport bool
}

type Executor interface {
	MoDump(context.Context, MoDumpRequest) error
	MySQLSource(context.Context, MySQLSourceRequest) error
}

type DB interface {
	CountSourceRows(context.Context, Task) (int64, error)
	EnsureTargetDatabase(context.Context, Task) error
	CountTargetRows(context.Context, Task) (int64, error)
}

type MoDumpRequest struct {
	Binary  string
	Task    Task
	WorkDir string
	SQLFile string
}

type MySQLSourceRequest struct {
	Binary   string
	Target   Endpoint
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

func (r Runner) Run(ctx context.Context, runID string, tasks []Task) (RunReport, error) {
	start := time.Now()
	progress := newProgressLogger(r.Progress)
	if r.Executor == nil {
		r.Executor = LocalExecutor{}
	}
	if r.Config.Parallelism < 1 {
		r.Config.Parallelism = 1
	}

	rows := make([]TableReport, len(tasks))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var completedMu sync.Mutex
	completed := 0
	for worker := 0; worker < r.Config.Parallelism; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				rows[idx] = r.runTaskWithProgress(ctx, runID, tasks[idx], progress)
				completedMu.Lock()
				completed++
				current := completed
				completedMu.Unlock()
				progress.Printf("datasync: completed %d/%d table task(s)", current, len(tasks))
			}
		}()
	}
	for i := range tasks {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	summary := Summary{TotalTasks: len(rows), Duration: time.Since(start)}
	for _, row := range rows {
		summary.TotalSourceRows += row.SourceRows
		summary.TotalTargetRows += row.TargetRows
		exportOK := row.ExportStatus == StatusSuccess || row.ExportStatus == StatusSkipped
		if row.Error == "" && exportOK && row.ImportStatus != StatusFailed {
			summary.SucceededTasks++
		} else {
			summary.FailedTasks++
		}
	}
	return RunReport{RunID: runID, Summary: summary, Tables: rows}, nil
}

func (r Runner) runTask(ctx context.Context, runID string, task Task) TableReport {
	return r.runTaskWithProgress(ctx, runID, task, newProgressLogger(r.Progress))
}

func (r Runner) runTaskWithProgress(ctx context.Context, runID string, task Task, progress *progressLogger) TableReport {
	label := taskProgressLabel(task)
	row := TableReport{
		RunID:          runID,
		SourceName:     task.SourceName,
		SourceHost:     task.SourceHost,
		SourcePort:     task.SourcePort,
		SourceDatabase: task.SourceDatabase,
		SourceTable:    task.SourceTable,
		SourceRows:     task.SourceRows,
		TargetName:     task.TargetName,
		TargetHost:     task.TargetHost,
		TargetPort:     task.TargetPort,
		TargetUser:     task.TargetUser,
		TargetDatabase: task.TargetDatabase,
		ImportStatus:   StatusPending,
	}
	tableDir, sqlFile, csvFile := r.taskPaths(runID, task)
	row.SQLFile = sqlFile
	row.CSVFile = csvFile

	retryCfg := RetryPolicy{MaxAttempts: r.Config.Retry.MaxAttempts, Backoff: r.Config.Retry.Backoff}
	var err error
	if r.effectiveMode() == ModeImport {
		row.ExportStatus = StatusSkipped
		progress.Printf("datasync: [%s] export skipped; using existing files", label)
	} else {
		progress.Printf("datasync: [%s] export started", label)
		sourceRows, err := r.DB.CountSourceRows(ctx, task)
		if err != nil {
			row.ExportStatus = StatusFailed
			row.Error = err.Error()
			progress.Printf("datasync: [%s] export failed: %s", label, row.Error)
			return row
		}
		row.SourceRows = sourceRows

		row.ExportStartedAt = time.Now()
		err = Do(ctx, retryCfg, func(ctx context.Context, attempt int) error {
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
			row.ExportStatus = StatusFailed
			row.Error = err.Error()
			progress.Printf("datasync: [%s] export failed attempts=%d: %s", label, row.ExportAttempts, row.Error)
			return row
		}
		row.ExportStatus = StatusSuccess
		progress.Printf("datasync: [%s] export succeeded rows=%d attempts=%d", label, row.SourceRows, row.ExportAttempts)
	}
	if _, err := requireFile(sqlFile); err != nil {
		row.Error = err.Error()
		if row.ExportStatus == StatusSkipped {
			row.ImportStatus = StatusFailed
		} else {
			row.ExportStatus = StatusFailed
		}
		progress.Printf("datasync: [%s] %s failed: %s", label, failedPhase(row), row.Error)
		return row
	}
	csvInfo, err := requireFile(csvFile)
	if err != nil {
		row.Error = err.Error()
		if row.ExportStatus == StatusSkipped {
			row.ImportStatus = StatusFailed
		} else {
			row.ExportStatus = StatusFailed
		}
		progress.Printf("datasync: [%s] %s failed: %s", label, failedPhase(row), row.Error)
		return row
	}
	row.CSVFileSize = csvInfo.Size()

	if r.effectiveMode() == ModeExport {
		row.ImportStatus = StatusSkipped
		progress.Printf("datasync: [%s] import skipped; export mode", label)
		return row
	}

	progress.Printf("datasync: [%s] import started", label)
	if err := r.DB.EnsureTargetDatabase(ctx, task); err != nil {
		row.ImportStatus = StatusFailed
		row.Error = err.Error()
		progress.Printf("datasync: [%s] import failed: %s", label, row.Error)
		return row
	}
	row.ImportStartedAt = time.Now()
	err = Do(ctx, retryCfg, func(ctx context.Context, attempt int) error {
		row.ImportAttempts = attempt
		if err := r.Executor.MySQLSource(ctx, MySQLSourceRequest{
			Binary:   r.Config.MySQLPath,
			Target:   task.TargetEndpoint(),
			Database: task.TargetDatabase,
			SQLFile:  sqlFile,
		}); err != nil {
			return err
		}
		targetRows, err := r.DB.CountTargetRows(ctx, task)
		if err != nil {
			return err
		}
		row.TargetRows = targetRows
		if row.SourceRows != row.TargetRows {
			return fmt.Errorf("row count mismatch: source=%d target=%d", row.SourceRows, row.TargetRows)
		}
		return nil
	})
	row.ImportFinishedAt = time.Now()
	row.ImportDuration = row.ImportFinishedAt.Sub(row.ImportStartedAt)
	if err != nil {
		row.ImportStatus = StatusFailed
		row.Error = err.Error()
		progress.Printf("datasync: [%s] import failed attempts=%d: %s", label, row.ImportAttempts, row.Error)
		return row
	}
	row.ImportStatus = StatusSuccess
	progress.Printf("datasync: [%s] import succeeded rows=%d attempts=%d", label, row.TargetRows, row.ImportAttempts)
	if r.shouldCleanupExportAfterImport(row) {
		if err := os.RemoveAll(tableDir); err != nil {
			row.ImportStatus = StatusFailed
			row.Error = err.Error()
			progress.Printf("datasync: [%s] cleanup failed: %s", label, row.Error)
		} else {
			progress.Printf("datasync: [%s] cleaned exported files", label)
		}
	}
	return row
}

func failedPhase(row TableReport) string {
	if row.ImportStatus == StatusFailed {
		return "import"
	}
	return "export"
}

func (r Runner) shouldCleanupExportAfterImport(row TableReport) bool {
	if !r.Options.CleanupExportAfterImport {
		return false
	}
	if r.effectiveMode() != ModeSync {
		return false
	}
	return row.ExportStatus == StatusSuccess && row.ImportStatus == StatusSuccess
}

func (r Runner) effectiveMode() Mode {
	if r.Mode == "" {
		return ModeSync
	}
	return r.Mode
}

func (r Runner) taskPaths(runID string, task Task) (string, string, string) {
	tableDir := filepath.Join(
		r.Config.OutputDir,
		runID,
		"exports",
		task.SourceName,
		task.SourceDatabase,
		task.SourceTable,
		task.TargetName,
		task.TargetDatabase,
		task.TargetHost,
		strconv.Itoa(task.TargetPort),
		task.TargetUser,
	)
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
