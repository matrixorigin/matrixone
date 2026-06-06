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
			TargetName:     "target_a",
			TargetHost:     "127.0.0.2",
			TargetPort:     6002,
			TargetUser:     "target:admin",
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
		"run_id,source_name,source_host,source_port,source_database,source_table,target_name,target_host,target_port,target_user,target_database,sql_file,csv_file,csv_file_size_bytes",
		"run1,tenant_a,127.0.0.1,6001,src_db,t1,target_a,127.0.0.2,6002,target:admin,dst_db,/tmp/t1.sql,/tmp/src_db_t1.csv,12",
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
	if row[16] != StatusSuccess || row[17] != StatusSuccess || row[24] != "1" || row[25] != "1" {
		t.Fatalf("CSV row status/attempt columns = %#v", row)
	}
}

func TestWriteReturnsMkdirError(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Write(filepath.Join(file, "report"), RunReport{}); err == nil {
		t.Fatal("Write() error = nil, want mkdir error")
	}
}

func TestReadReport(t *testing.T) {
	dir := t.TempDir()
	written, err := Write(dir, RunReport{
		RunID: "run1",
		Summary: Summary{
			TotalTasks:     1,
			SucceededTasks: 1,
			Duration:       5 * time.Millisecond,
		},
		Tables: []TableReport{{
			RunID:          "run1",
			SourceName:     "tenant_a",
			SourceHost:     "127.0.0.1",
			SourcePort:     6001,
			SourceDatabase: "src_db",
			SourceTable:    "t1",
			TargetName:     "target_a",
			TargetHost:     "127.0.0.2",
			TargetPort:     6002,
			TargetUser:     "target:admin",
			TargetDatabase: "dst_db",
			CSVFileSize:    12,
			ExportDuration: time.Millisecond,
			ImportDuration: 2 * time.Millisecond,
			ExportStatus:   StatusSuccess,
			ImportStatus:   StatusSuccess,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	read, err := Read(written.Summary.JSONReportPath)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if read.RunID != "run1" || read.Summary.Duration != 5*time.Millisecond {
		t.Fatalf("report = %+v, want run1 with 5ms duration", read)
	}
	if len(read.Tables) != 1 {
		t.Fatalf("table count = %d, want 1", len(read.Tables))
	}
	table := read.Tables[0]
	if table.SourceName != "tenant_a" ||
		table.SourceHost != "127.0.0.1" ||
		table.SourcePort != 6001 ||
		table.TargetName != "target_a" ||
		table.TargetHost != "127.0.0.2" ||
		table.TargetPort != 6002 ||
		table.TargetUser != "target:admin" ||
		table.CSVFileSize != 12 ||
		table.ExportDuration != time.Millisecond ||
		table.ImportDuration != 2*time.Millisecond {
		t.Fatalf("table = %+v", table)
	}
}

func TestReadReportReturnsErrors(t *testing.T) {
	if _, err := Read(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("Read() error = nil, want missing file error")
	}

	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); err == nil {
		t.Fatal("Read() error = nil, want invalid JSON error")
	}
}

func TestWriteReturnsJSONCreateError(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Write(file, RunReport{}); err == nil {
		t.Fatal("Write() error = nil, want json create error")
	}
}

func TestWriteCSVReturnsCreateError(t *testing.T) {
	if err := writeCSV(filepath.Join(t.TempDir(), "missing", "report.csv"), nil); err == nil {
		t.Fatal("writeCSV() error = nil, want create error")
	}
}
