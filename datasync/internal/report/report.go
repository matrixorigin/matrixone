package report

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	StatusPending = "pending"
	StatusSkipped = "skipped"
	StatusSuccess = "success"
	StatusFailed  = "failed"
)

type RunReport struct {
	RunID   string        `json:"run_id"`
	Summary Summary       `json:"summary"`
	Tables  []TableReport `json:"tables"`
}

type Summary struct {
	TotalTasks      int           `json:"total_tasks"`
	SucceededTasks  int           `json:"succeeded_tasks"`
	FailedTasks     int           `json:"failed_tasks"`
	TotalSourceRows int64         `json:"total_source_rows"`
	TotalTargetRows int64         `json:"total_target_rows"`
	Duration        time.Duration `json:"-"`
	JSONReportPath  string        `json:"json_report_path"`
	CSVReportPath   string        `json:"csv_report_path"`
}

type TableReport struct {
	RunID            string        `json:"run_id"`
	SourceName       string        `json:"source_name"`
	SourceHost       string        `json:"source_host"`
	SourcePort       int           `json:"source_port"`
	SourceDatabase   string        `json:"source_database"`
	SourceTable      string        `json:"source_table"`
	TargetName       string        `json:"target_name"`
	TargetHost       string        `json:"target_host"`
	TargetPort       int           `json:"target_port"`
	TargetUser       string        `json:"target_user"`
	TargetDatabase   string        `json:"target_database"`
	SQLFile          string        `json:"sql_file"`
	CSVFile          string        `json:"csv_file"`
	CSVFileSize      int64         `json:"csv_file_size_bytes"`
	SourceRows       int64         `json:"source_rows"`
	TargetRows       int64         `json:"target_rows"`
	ExportStatus     string        `json:"export_status"`
	ImportStatus     string        `json:"import_status"`
	ExportStartedAt  time.Time     `json:"export_started_at,omitempty"`
	ExportFinishedAt time.Time     `json:"export_finished_at,omitempty"`
	ImportStartedAt  time.Time     `json:"import_started_at,omitempty"`
	ImportFinishedAt time.Time     `json:"import_finished_at,omitempty"`
	ExportDuration   time.Duration `json:"-"`
	ImportDuration   time.Duration `json:"-"`
	ExportAttempts   int           `json:"export_attempts"`
	ImportAttempts   int           `json:"import_attempts"`
	Error            string        `json:"error,omitempty"`
}

type jsonRunReport struct {
	RunID   string            `json:"run_id"`
	Summary jsonSummary       `json:"summary"`
	Tables  []jsonTableReport `json:"tables"`
}

type jsonSummary struct {
	TotalTasks      int    `json:"total_tasks"`
	SucceededTasks  int    `json:"succeeded_tasks"`
	FailedTasks     int    `json:"failed_tasks"`
	TotalSourceRows int64  `json:"total_source_rows"`
	TotalTargetRows int64  `json:"total_target_rows"`
	Duration        string `json:"duration"`
	JSONReportPath  string `json:"json_report_path"`
	CSVReportPath   string `json:"csv_report_path"`
}

type jsonTableReport struct {
	RunID            string    `json:"run_id"`
	SourceName       string    `json:"source_name"`
	SourceHost       string    `json:"source_host"`
	SourcePort       int       `json:"source_port"`
	SourceDatabase   string    `json:"source_database"`
	SourceTable      string    `json:"source_table"`
	TargetName       string    `json:"target_name"`
	TargetHost       string    `json:"target_host"`
	TargetPort       int       `json:"target_port"`
	TargetUser       string    `json:"target_user"`
	TargetDatabase   string    `json:"target_database"`
	SQLFile          string    `json:"sql_file"`
	CSVFile          string    `json:"csv_file"`
	CSVFileSize      int64     `json:"csv_file_size_bytes"`
	SourceRows       int64     `json:"source_rows"`
	TargetRows       int64     `json:"target_rows"`
	ExportStatus     string    `json:"export_status"`
	ImportStatus     string    `json:"import_status"`
	ExportStartedAt  time.Time `json:"export_started_at,omitempty"`
	ExportFinishedAt time.Time `json:"export_finished_at,omitempty"`
	ImportStartedAt  time.Time `json:"import_started_at,omitempty"`
	ImportFinishedAt time.Time `json:"import_finished_at,omitempty"`
	ExportDuration   string    `json:"export_duration"`
	ImportDuration   string    `json:"import_duration"`
	ExportAttempts   int       `json:"export_attempts"`
	ImportAttempts   int       `json:"import_attempts"`
	Error            string    `json:"error,omitempty"`
}

func Write(dir string, r RunReport) (RunReport, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return r, err
	}
	r.Summary.JSONReportPath = filepath.Join(dir, "report.json")
	r.Summary.CSVReportPath = filepath.Join(dir, "report.csv")
	if err := writeJSON(r.Summary.JSONReportPath, r); err != nil {
		return r, err
	}
	if err := writeCSV(r.Summary.CSVReportPath, r.Tables); err != nil {
		return r, err
	}
	return r, nil
}

func Read(path string) (RunReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RunReport{}, err
	}
	var r jsonRunReport
	if err := json.Unmarshal(data, &r); err != nil {
		return RunReport{}, err
	}
	return fromJSONReport(r), nil
}

func writeJSON(path string, r RunReport) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(toJSONReport(r))
}

func fromJSONReport(r jsonRunReport) RunReport {
	rows := make([]TableReport, 0, len(r.Tables))
	for _, row := range r.Tables {
		rows = append(rows, TableReport{
			RunID:            row.RunID,
			SourceName:       row.SourceName,
			SourceHost:       row.SourceHost,
			SourcePort:       row.SourcePort,
			SourceDatabase:   row.SourceDatabase,
			SourceTable:      row.SourceTable,
			TargetName:       row.TargetName,
			TargetHost:       row.TargetHost,
			TargetPort:       row.TargetPort,
			TargetUser:       row.TargetUser,
			TargetDatabase:   row.TargetDatabase,
			SQLFile:          row.SQLFile,
			CSVFile:          row.CSVFile,
			CSVFileSize:      row.CSVFileSize,
			SourceRows:       row.SourceRows,
			TargetRows:       row.TargetRows,
			ExportStatus:     row.ExportStatus,
			ImportStatus:     row.ImportStatus,
			ExportStartedAt:  row.ExportStartedAt,
			ExportFinishedAt: row.ExportFinishedAt,
			ImportStartedAt:  row.ImportStartedAt,
			ImportFinishedAt: row.ImportFinishedAt,
			ExportDuration:   parseDuration(row.ExportDuration),
			ImportDuration:   parseDuration(row.ImportDuration),
			ExportAttempts:   row.ExportAttempts,
			ImportAttempts:   row.ImportAttempts,
			Error:            row.Error,
		})
	}
	return RunReport{
		RunID: r.RunID,
		Summary: Summary{
			TotalTasks:      r.Summary.TotalTasks,
			SucceededTasks:  r.Summary.SucceededTasks,
			FailedTasks:     r.Summary.FailedTasks,
			TotalSourceRows: r.Summary.TotalSourceRows,
			TotalTargetRows: r.Summary.TotalTargetRows,
			Duration:        parseDuration(r.Summary.Duration),
			JSONReportPath:  r.Summary.JSONReportPath,
			CSVReportPath:   r.Summary.CSVReportPath,
		},
		Tables: rows,
	}
}

func parseDuration(value string) time.Duration {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return duration
}

func toJSONReport(r RunReport) jsonRunReport {
	rows := make([]jsonTableReport, 0, len(r.Tables))
	for _, row := range r.Tables {
		rows = append(rows, jsonTableReport{
			RunID:            row.RunID,
			SourceName:       row.SourceName,
			SourceHost:       row.SourceHost,
			SourcePort:       row.SourcePort,
			SourceDatabase:   row.SourceDatabase,
			SourceTable:      row.SourceTable,
			TargetName:       row.TargetName,
			TargetHost:       row.TargetHost,
			TargetPort:       row.TargetPort,
			TargetUser:       row.TargetUser,
			TargetDatabase:   row.TargetDatabase,
			SQLFile:          row.SQLFile,
			CSVFile:          row.CSVFile,
			CSVFileSize:      row.CSVFileSize,
			SourceRows:       row.SourceRows,
			TargetRows:       row.TargetRows,
			ExportStatus:     row.ExportStatus,
			ImportStatus:     row.ImportStatus,
			ExportStartedAt:  row.ExportStartedAt,
			ExportFinishedAt: row.ExportFinishedAt,
			ImportStartedAt:  row.ImportStartedAt,
			ImportFinishedAt: row.ImportFinishedAt,
			ExportDuration:   row.ExportDuration.String(),
			ImportDuration:   row.ImportDuration.String(),
			ExportAttempts:   row.ExportAttempts,
			ImportAttempts:   row.ImportAttempts,
			Error:            row.Error,
		})
	}
	return jsonRunReport{
		RunID: r.RunID,
		Summary: jsonSummary{
			TotalTasks:      r.Summary.TotalTasks,
			SucceededTasks:  r.Summary.SucceededTasks,
			FailedTasks:     r.Summary.FailedTasks,
			TotalSourceRows: r.Summary.TotalSourceRows,
			TotalTargetRows: r.Summary.TotalTargetRows,
			Duration:        r.Summary.Duration.String(),
			JSONReportPath:  r.Summary.JSONReportPath,
			CSVReportPath:   r.Summary.CSVReportPath,
		},
		Tables: rows,
	}
}

func writeCSV(path string, rows []TableReport) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	header := []string{
		"run_id",
		"source_name",
		"source_host",
		"source_port",
		"source_database",
		"source_table",
		"target_name",
		"target_host",
		"target_port",
		"target_user",
		"target_database",
		"sql_file",
		"csv_file",
		"csv_file_size_bytes",
		"source_rows",
		"target_rows",
		"export_status",
		"import_status",
		"export_started_at",
		"export_finished_at",
		"import_started_at",
		"import_finished_at",
		"export_duration",
		"import_duration",
		"export_attempts",
		"import_attempts",
		"error",
	}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, row := range rows {
		if err := w.Write([]string{
			row.RunID,
			row.SourceName,
			row.SourceHost,
			strconv.Itoa(row.SourcePort),
			row.SourceDatabase,
			row.SourceTable,
			row.TargetName,
			row.TargetHost,
			strconv.Itoa(row.TargetPort),
			row.TargetUser,
			row.TargetDatabase,
			row.SQLFile,
			row.CSVFile,
			strconv.FormatInt(row.CSVFileSize, 10),
			strconv.FormatInt(row.SourceRows, 10),
			strconv.FormatInt(row.TargetRows, 10),
			row.ExportStatus,
			row.ImportStatus,
			formatTime(row.ExportStartedAt),
			formatTime(row.ExportFinishedAt),
			formatTime(row.ImportStartedAt),
			formatTime(row.ImportFinishedAt),
			row.ExportDuration.String(),
			row.ImportDuration.String(),
			strconv.Itoa(row.ExportAttempts),
			strconv.Itoa(row.ImportAttempts),
			row.Error,
		}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
