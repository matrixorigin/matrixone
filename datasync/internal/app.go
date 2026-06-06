package datasync

import (
	"context"
	"fmt"
	"path/filepath"
	"time"
)

type App struct {
	Config    *Config
	Mode      Mode
	Options   Options
	Discovery Discovery
	Runner    TaskRunner
}

type Discovery interface {
	ListTables(context.Context, DatabaseEndpoint, string) ([]string, error)
}

type TaskRunner interface {
	Run(context.Context, Mode, string, []Task) (RunReport, error)
}

type dbClient interface {
	Close() error
	ListOrdinaryTables(context.Context, string) ([]Table, error)
	CountRows(context.Context, string, string) (int64, error)
	EnsureDatabase(context.Context, string) error
}

var openDB = func(ctx context.Context, endpoint DBEndpoint, database string) (dbClient, error) {
	return Open(ctx, endpoint, database)
}

type Result struct {
	RunID        string
	PlannedTasks int
	Report       RunReport
}

func (a App) Run(ctx context.Context, runID string) (Result, error) {
	tasks, err := a.buildTasks(ctx, runID)
	if err != nil {
		return Result{}, err
	}

	runner := a.Runner
	if runner == nil {
		runner = MatrixOneRunner{Config: a.Config, Options: a.Options}
	}
	runReport, err := runner.Run(ctx, a.Mode, runID, tasks)
	if err != nil {
		return Result{}, err
	}

	runDir := filepath.Join(a.Config.OutputDir, runID)
	writtenReport, err := Write(runDir, runReport)
	result := Result{RunID: runID, PlannedTasks: len(tasks), Report: writtenReport}
	if err != nil {
		return result, err
	}
	if writtenReport.Summary.FailedTasks > 0 {
		return result, fmt.Errorf("%d table tasks failed", writtenReport.Summary.FailedTasks)
	}
	return result, nil
}

func (a App) buildTasks(ctx context.Context, runID string) ([]Task, error) {
	if a.Mode == ModeImport {
		return tasksFromReport(filepath.Join(a.Config.OutputDir, runID, "report.json"), a.Config)
	}
	if a.Discovery == nil {
		a.Discovery = MatrixOneDiscovery{}
	}

	discovered := make(map[DatabaseKey][]string)
	for _, database := range a.Config.Databases {
		tables, err := a.Discovery.ListTables(ctx, database.Source, database.Source.Database)
		if err != nil {
			return nil, err
		}
		discovered[DatabaseKey{SourceName: database.Source.Name, Database: database.Source.Database}] = tables
	}
	return BuildTasks(a.Config, discovered), nil
}

func tasksFromReport(path string, cfg *Config) ([]Task, error) {
	runReport, err := Read(path)
	if err != nil {
		return nil, err
	}
	tasks := make([]Task, 0, len(runReport.Tables))
	for _, row := range runReport.Tables {
		password, err := targetPasswordForReportRow(cfg, row)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, Task{
			SourceName:     row.SourceName,
			SourceHost:     row.SourceHost,
			SourcePort:     row.SourcePort,
			SourceDatabase: row.SourceDatabase,
			SourceTable:    row.SourceTable,
			SourceRows:     row.SourceRows,
			TargetName:     row.TargetName,
			TargetHost:     row.TargetHost,
			TargetPort:     row.TargetPort,
			TargetUser:     row.TargetUser,
			TargetPassword: password,
			TargetDatabase: row.TargetDatabase,
		})
	}
	return tasks, nil
}

func targetPasswordForReportRow(cfg *Config, row TableReport) (string, error) {
	for _, database := range cfg.Databases {
		if database.Source.Name == row.SourceName &&
			database.Source.Database == row.SourceDatabase &&
			database.Target.Name == row.TargetName &&
			database.Target.Host == row.TargetHost &&
			database.Target.Port == row.TargetPort &&
			database.Target.User == row.TargetUser &&
			database.Target.Database == row.TargetDatabase {
			return database.Target.Password, nil
		}
	}
	return "", fmt.Errorf("target password not found for report row %s.%s -> %s.%s at %s:%d as %s",
		row.SourceDatabase,
		row.SourceTable,
		row.TargetName,
		row.TargetDatabase,
		row.TargetHost,
		row.TargetPort,
		row.TargetUser,
	)
}

func NewRunID(now time.Time) string {
	return now.Format("20060102-150405")
}

type MatrixOneDiscovery struct{}

func (MatrixOneDiscovery) ListTables(ctx context.Context, source DatabaseEndpoint, database string) ([]string, error) {
	client, err := openDB(ctx, DBEndpoint{
		Host:     source.Host,
		Port:     source.Port,
		User:     source.User,
		Password: source.Password,
	}, database)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	dbTables, err := client.ListOrdinaryTables(ctx, database)
	if err != nil {
		return nil, err
	}
	tables := make([]string, 0, len(dbTables))
	for _, table := range dbTables {
		tables = append(tables, table.Name)
	}
	return tables, nil
}

type MatrixOneRunner struct {
	Config  *Config
	Options Options
}

func (m MatrixOneRunner) Run(ctx context.Context, mode Mode, runID string, tasks []Task) (RunReport, error) {
	return Runner{
		Config:  m.Config,
		Mode:    mode,
		Options: m.Options,
		DB:      MatrixOneRunDB{Config: m.Config},
	}.Run(ctx, runID, tasks)
}

type MatrixOneRunDB struct {
	Config *Config
}

func (m MatrixOneRunDB) CountSourceRows(ctx context.Context, task Task) (int64, error) {
	client, err := openDB(ctx, DBEndpoint{
		Host:     task.SourceHost,
		Port:     task.SourcePort,
		User:     task.SourceUser,
		Password: task.SourcePassword,
	}, task.SourceDatabase)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	return client.CountRows(ctx, task.SourceDatabase, task.SourceTable)
}

func (m MatrixOneRunDB) EnsureTargetDatabase(ctx context.Context, task Task) error {
	client, err := openDB(ctx, DBEndpoint{
		Host:     task.TargetHost,
		Port:     task.TargetPort,
		User:     task.TargetUser,
		Password: task.TargetPassword,
	}, "")
	if err != nil {
		return err
	}
	defer client.Close()
	return client.EnsureDatabase(ctx, task.TargetDatabase)
}

func (m MatrixOneRunDB) CountTargetRows(ctx context.Context, task Task) (int64, error) {
	client, err := openDB(ctx, DBEndpoint{
		Host:     task.TargetHost,
		Port:     task.TargetPort,
		User:     task.TargetUser,
		Password: task.TargetPassword,
	}, task.TargetDatabase)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	return client.CountRows(ctx, task.TargetDatabase, task.SourceTable)
}
