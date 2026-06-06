package report

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteReports(t *testing.T) {
	dir := t.TempDir()
	r := RunReport{
		RunID: "run1",
		Summary: Summary{
			TotalTasks:      1,
			SucceededTasks:  1,
			TotalSourceRows: 3,
			TotalTargetRows: 3,
			Duration:        10 * time.Millisecond,
		},
		Tables: []TableReport{{
			RunID:          "run1",
			SourceName:     "tenant_a",
			SourceHost:     "127.0.0.1",
			SourcePort:     6001,
			SourceDatabase: "src_db",
			SourceTable:    "t1",
			TargetDatabase: "dst_db",
			SQLFile:        "/tmp/t1.sql",
			CSVFile:        "/tmp/src_db_t1.csv",
			CSVFileSize:    12,
			SourceRows:     3,
			TargetRows:     3,
			ExportStatus:   StatusSuccess,
			ImportStatus:   StatusSuccess,
			ExportAttempts: 1,
			ImportAttempts: 1,
		}},
	}

	written, err := Write(dir, r)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if filepath.Base(written.Summary.JSONReportPath) != "report.json" {
		t.Fatalf("JSONReportPath = %q, want report.json", written.Summary.JSONReportPath)
	}
	if filepath.Base(written.Summary.CSVReportPath) != "report.csv" {
		t.Fatalf("CSVReportPath = %q, want report.csv", written.Summary.CSVReportPath)
	}

	jsonBytes, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	jsonText := string(jsonBytes)
	for _, want := range []string{`"run_id": "run1"`, `"duration": "10ms"`, `"json_report_path":`, `"csv_file_size_bytes": 12`} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("report.json missing %q: %s", want, jsonText)
		}
	}

	csvBytes, err := os.ReadFile(filepath.Join(dir, "report.csv"))
	if err != nil {
		t.Fatal(err)
	}
	csvText := string(csvBytes)
	for _, want := range []string{
		"run_id,source_name,source_host,source_port,source_database,source_table,target_database,sql_file,csv_file,csv_file_size_bytes",
		"run1,tenant_a,127.0.0.1,6001,src_db,t1,dst_db,/tmp/t1.sql,/tmp/src_db_t1.csv,12",
	} {
		if !strings.Contains(csvText, want) {
			t.Fatalf("report.csv missing %q: %s", want, csvText)
		}
	}
	records, err := csv.NewReader(strings.NewReader(csvText)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("CSV record count = %d, want 2", len(records))
	}
	row := records[1]
	if row[12] != StatusSuccess || row[13] != StatusSuccess || row[20] != "1" || row[21] != "1" {
		t.Fatalf("CSV row status/attempt columns = %#v", row)
	}
}
