package datasync

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	TotalTasks                int           `json:"total_tasks"`
	SucceededTasks            int           `json:"succeeded_tasks"`
	FailedTasks               int           `json:"failed_tasks"`
	TotalSourceRows           int64         `json:"total_source_rows"`
	TotalTargetRows           int64         `json:"total_target_rows"`
	Duration                  time.Duration `json:"-"`
	ExportJSONReportPath      string        `json:"export_json_report_path"`
	ExportCSVReportPath       string        `json:"export_csv_report_path"`
	ExportMarkdownReportPath  string        `json:"export_markdown_report_path"`
	ImportJSONReportPath      string        `json:"import_json_report_path"`
	ImportCSVReportPath       string        `json:"import_csv_report_path"`
	ImportMarkdownReportPath  string        `json:"import_markdown_report_path"`
	SummaryJSONReportPath     string        `json:"summary_json_report_path"`
	SummaryCSVReportPath      string        `json:"summary_csv_report_path"`
	SummaryMarkdownReportPath string        `json:"summary_markdown_report_path"`
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
	TotalTasks                int    `json:"total_tasks"`
	SucceededTasks            int    `json:"succeeded_tasks"`
	FailedTasks               int    `json:"failed_tasks"`
	TotalSourceRows           int64  `json:"total_source_rows"`
	TotalTargetRows           int64  `json:"total_target_rows"`
	Duration                  string `json:"duration"`
	ExportJSONReportPath      string `json:"export_json_report_path"`
	ExportCSVReportPath       string `json:"export_csv_report_path"`
	ExportMarkdownReportPath  string `json:"export_markdown_report_path"`
	ImportJSONReportPath      string `json:"import_json_report_path"`
	ImportCSVReportPath       string `json:"import_csv_report_path"`
	ImportMarkdownReportPath  string `json:"import_markdown_report_path"`
	SummaryJSONReportPath     string `json:"summary_json_report_path"`
	SummaryCSVReportPath      string `json:"summary_csv_report_path"`
	SummaryMarkdownReportPath string `json:"summary_markdown_report_path"`
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
	ExportRetries    int       `json:"export_retries"`
	ImportRetries    int       `json:"import_retries"`
	Error            string    `json:"error,omitempty"`
}

type reportKind string

const (
	reportKindExport  reportKind = "export"
	reportKindImport  reportKind = "import"
	reportKindSummary reportKind = "summary"
)

func Write(dir string, mode Mode, r RunReport) (RunReport, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return r, err
	}
	r.Summary.ExportJSONReportPath = filepath.Join(dir, "export-report.json")
	r.Summary.ExportCSVReportPath = filepath.Join(dir, "export-report.csv")
	r.Summary.ExportMarkdownReportPath = filepath.Join(dir, "export-report.md")
	r.Summary.ImportJSONReportPath = filepath.Join(dir, "import-report.json")
	r.Summary.ImportCSVReportPath = filepath.Join(dir, "import-report.csv")
	r.Summary.ImportMarkdownReportPath = filepath.Join(dir, "import-report.md")
	r.Summary.SummaryJSONReportPath = filepath.Join(dir, "summary-report.json")
	r.Summary.SummaryCSVReportPath = filepath.Join(dir, "summary-report.csv")
	r.Summary.SummaryMarkdownReportPath = filepath.Join(dir, "summary-report.md")

	kinds := []reportKind{reportKindSummary}
	switch effectiveMode(mode) {
	case ModeExport:
		kinds = []reportKind{reportKindExport, reportKindSummary}
	case ModeImport:
		kinds = []reportKind{reportKindImport, reportKindSummary}
	case ModeSync:
		kinds = []reportKind{reportKindExport, reportKindImport, reportKindSummary}
	}
	for _, kind := range kinds {
		if err := writeReportFiles(kind, r); err != nil {
			return r, err
		}
	}
	return r, nil
}

func writeReportFiles(kind reportKind, r RunReport) error {
	jsonPath, csvPath, markdownPath := reportPaths(kind, r.Summary)
	if err := writeJSON(jsonPath, r); err != nil {
		return err
	}
	if err := writeCSV(csvPath, r.Tables); err != nil {
		return err
	}
	return writeMarkdown(markdownPath, kind, r)
}

func reportPaths(kind reportKind, summary Summary) (string, string, string) {
	switch kind {
	case reportKindExport:
		return summary.ExportJSONReportPath, summary.ExportCSVReportPath, summary.ExportMarkdownReportPath
	case reportKindImport:
		return summary.ImportJSONReportPath, summary.ImportCSVReportPath, summary.ImportMarkdownReportPath
	default:
		return summary.SummaryJSONReportPath, summary.SummaryCSVReportPath, summary.SummaryMarkdownReportPath
	}
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
			ExportAttempts:   attemptsFromRetries(row.ExportRetries),
			ImportAttempts:   attemptsFromRetries(row.ImportRetries),
			Error:            row.Error,
		})
	}
	return RunReport{
		RunID: r.RunID,
		Summary: Summary{
			TotalTasks:                r.Summary.TotalTasks,
			SucceededTasks:            r.Summary.SucceededTasks,
			FailedTasks:               r.Summary.FailedTasks,
			TotalSourceRows:           r.Summary.TotalSourceRows,
			TotalTargetRows:           r.Summary.TotalTargetRows,
			Duration:                  parseDuration(r.Summary.Duration),
			ExportJSONReportPath:      r.Summary.ExportJSONReportPath,
			ExportCSVReportPath:       r.Summary.ExportCSVReportPath,
			ExportMarkdownReportPath:  r.Summary.ExportMarkdownReportPath,
			ImportJSONReportPath:      r.Summary.ImportJSONReportPath,
			ImportCSVReportPath:       r.Summary.ImportCSVReportPath,
			ImportMarkdownReportPath:  r.Summary.ImportMarkdownReportPath,
			SummaryJSONReportPath:     r.Summary.SummaryJSONReportPath,
			SummaryCSVReportPath:      r.Summary.SummaryCSVReportPath,
			SummaryMarkdownReportPath: r.Summary.SummaryMarkdownReportPath,
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
			ExportRetries:    retries(row.ExportAttempts),
			ImportRetries:    retries(row.ImportAttempts),
			Error:            row.Error,
		})
	}
	return jsonRunReport{
		RunID: r.RunID,
		Summary: jsonSummary{
			TotalTasks:                r.Summary.TotalTasks,
			SucceededTasks:            r.Summary.SucceededTasks,
			FailedTasks:               r.Summary.FailedTasks,
			TotalSourceRows:           r.Summary.TotalSourceRows,
			TotalTargetRows:           r.Summary.TotalTargetRows,
			Duration:                  r.Summary.Duration.String(),
			ExportJSONReportPath:      r.Summary.ExportJSONReportPath,
			ExportCSVReportPath:       r.Summary.ExportCSVReportPath,
			ExportMarkdownReportPath:  r.Summary.ExportMarkdownReportPath,
			ImportJSONReportPath:      r.Summary.ImportJSONReportPath,
			ImportCSVReportPath:       r.Summary.ImportCSVReportPath,
			ImportMarkdownReportPath:  r.Summary.ImportMarkdownReportPath,
			SummaryJSONReportPath:     r.Summary.SummaryJSONReportPath,
			SummaryCSVReportPath:      r.Summary.SummaryCSVReportPath,
			SummaryMarkdownReportPath: r.Summary.SummaryMarkdownReportPath,
		},
		Tables: rows,
	}
}

func writeMarkdown(path string, kind reportKind, r RunReport) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n", markdownTitle(kind))
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "## 汇总")
	fmt.Fprintf(&b, "- 运行 ID：%s\n", r.RunID)
	fmt.Fprintf(&b, "- 任务总数：%d\n", r.Summary.TotalTasks)
	fmt.Fprintf(&b, "- 成功任务：%d\n", r.Summary.SucceededTasks)
	fmt.Fprintf(&b, "- 失败任务：%d\n", r.Summary.FailedTasks)
	fmt.Fprintf(&b, "- 源端总行数：%d\n", r.Summary.TotalSourceRows)
	fmt.Fprintf(&b, "- 目标端总行数：%d\n", r.Summary.TotalTargetRows)
	fmt.Fprintf(&b, "- 总耗时：%s\n", r.Summary.Duration)
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "## 报告文件")
	fmt.Fprintf(&b, "- 导出报告：%s / %s / %s\n", r.Summary.ExportJSONReportPath, r.Summary.ExportCSVReportPath, r.Summary.ExportMarkdownReportPath)
	fmt.Fprintf(&b, "- 导入报告：%s / %s / %s\n", r.Summary.ImportJSONReportPath, r.Summary.ImportCSVReportPath, r.Summary.ImportMarkdownReportPath)
	fmt.Fprintf(&b, "- 汇总报告：%s / %s / %s\n", r.Summary.SummaryJSONReportPath, r.Summary.SummaryCSVReportPath, r.Summary.SummaryMarkdownReportPath)
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "## 表同步结果")
	fmt.Fprintln(&b, "| 源端 | 目标端 | 源端行数 | 目标端行数 | CSV大小 | 导出耗时 | 导入耗时 | 导出状态 | 导入状态 | 导出重试次数 | 导入重试次数 | 错误 |")
	fmt.Fprintln(&b, "| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |")
	for _, row := range r.Tables {
		fmt.Fprintf(&b, "| %s | %s | %d | %d | %d bytes | %s | %s | %s | %s | %d | %d | %s |\n",
			markdownCell(row.SourceName+"."+row.SourceDatabase+"."+row.SourceTable),
			markdownCell(row.TargetName+"."+row.TargetDatabase+"."+row.SourceTable),
			row.SourceRows,
			row.TargetRows,
			row.CSVFileSize,
			row.ExportDuration,
			row.ImportDuration,
			markdownCell(row.ExportStatus),
			markdownCell(row.ImportStatus),
			retries(row.ExportAttempts),
			retries(row.ImportAttempts),
			markdownCell(row.Error),
		)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func markdownTitle(kind reportKind) string {
	switch kind {
	case reportKindExport:
		return "数据同步导出报告"
	case reportKindImport:
		return "数据同步导入报告"
	default:
		return "数据同步汇总报告"
	}
}

func markdownCell(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.ReplaceAll(value, "|", `\|`)
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
		"export_retries",
		"import_retries",
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
			strconv.Itoa(retries(row.ExportAttempts)),
			strconv.Itoa(retries(row.ImportAttempts)),
			row.Error,
		}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func retries(attempts int) int {
	if attempts <= 1 {
		return 0
	}
	return attempts - 1
}

func attemptsFromRetries(retryCount int) int {
	if retryCount < 0 {
		return 0
	}
	return retryCount + 1
}

func effectiveMode(mode Mode) Mode {
	if mode == "" {
		return ModeSync
	}
	return mode
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
