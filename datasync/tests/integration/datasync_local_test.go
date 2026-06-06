package integration

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalMatrixOneEndToEnd(t *testing.T) {
	if os.Getenv("DATASYNC_INTEGRATION") != "1" {
		t.Skip("set DATASYNC_INTEGRATION=1 to run local MatrixOne integration test")
	}

	root := repoRoot(t)
	mysql := getenv("DATASYNC_MYSQL", "/usr/local/mysql/bin/mysql")
	moDump := getenv("DATASYNC_MO_DUMP", "/Users/liubo/Workspace/Projects/00-MatrixOrigin/mo_dump/mo-dump")
	outDir := t.TempDir()

	runSQL(t, mysql, "dump", "111", `
drop account if exists dsync_src_a;
drop account if exists dsync_src_b;
drop account if exists dsync_target_a;
drop account if exists dsync_target_b;
create account dsync_src_a admin_name 'admin' identified by '111';
create account dsync_src_b admin_name 'admin' identified by '111';
create account dsync_target_a admin_name 'admin' identified by '111';
create account dsync_target_b admin_name 'admin' identified by '111';
`)
	runSQL(t, mysql, "dsync_src_a:admin", "111", `
create database dsync_a_db1;
create database dsync_a_db2;
create table dsync_a_db1.t_keep(id int primary key, name varchar(50));
insert into dsync_a_db1.t_keep values (1,'alice'),(2,'bob'),(3,'carol');
create table dsync_a_db1.t_skip(id int primary key);
insert into dsync_a_db1.t_skip values (99);
create table dsync_a_db2.orders(id int primary key, item varchar(50));
insert into dsync_a_db2.orders values (10,'book'),(11,'pen');
create table dsync_a_db2.ignored(id int primary key);
insert into dsync_a_db2.ignored values (404);
`)
	runSQL(t, mysql, "dsync_src_b:admin", "111", `
create database dsync_b_db1;
create table dsync_b_db1.events(id int primary key, label varchar(50));
insert into dsync_b_db1.events values (1,'login'),(2,'logout');
create table dsync_b_db1.audit(id int primary key);
insert into dsync_b_db1.audit values (7);
`)
	runSQL(t, mysql, "dsync_target_a:admin", "111", `
create database dsync_target_a1;
create table dsync_target_a1.t_keep(id int primary key, name varchar(50));
insert into dsync_target_a1.t_keep values (100,'old');
`)

	configPath := filepath.Join(outDir, "config.yaml")
	config := []byte(`mo_dump_path: ` + moDump + `
mysql_path: ` + mysql + `
output_dir: ` + outDir + `
parallelism: 2
retry:
  max_attempts: 2
  backoff: 1ms
source:
  host: 127.0.0.1
  port: 6001
  password: "111"
target:
  host: 127.0.0.1
  port: 6001
  password: "111"
databases:
  - source:
      name: tenant_a_db1
      user: dsync_src_a:admin
      database: dsync_a_db1
    target:
      name: target_a
      user: dsync_target_a:admin
      database: dsync_target_a1
    include_tables: [t_keep, t_skip]
    exclude_tables: [t_skip]
  - source:
      name: tenant_a_db2
      user: dsync_src_a:admin
      database: dsync_a_db2
    target:
      name: target_a
      user: dsync_target_a:admin
      database: dsync_target_a2
    include_tables: [orders]
  - source:
      name: tenant_b_db1
      user: dsync_src_b:admin
      database: dsync_b_db1
    target:
      name: target_b
      user: dsync_target_b:admin
      database: dsync_target_b1
    exclude_tables: [audit]
`)
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "run", "./cmd/datasync", "-config", configPath, "-run-id", "itest")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("datasync failed: %v", err)
	}

	assertCount(t, mysql, "dsync_target_a:admin", "111", "dsync_target_a1", "t_keep", "3")
	assertCount(t, mysql, "dsync_target_a:admin", "111", "dsync_target_a2", "orders", "2")
	assertCount(t, mysql, "dsync_target_b:admin", "111", "dsync_target_b1", "events", "2")
	assertTableMissing(t, mysql, "dsync_target_a:admin", "111", "dsync_target_a1", "t_skip")
	assertTableMissing(t, mysql, "dsync_target_a:admin", "111", "dsync_target_a2", "ignored")
	assertTableMissing(t, mysql, "dsync_target_b:admin", "111", "dsync_target_b1", "audit")
	assertReport(t, filepath.Join(outDir, "itest"))

	runSQL(t, mysql, "dsync_target_a:admin", "111", `
drop database dsync_target_a1;
drop database dsync_target_a2;
`)
	runSQL(t, mysql, "dsync_target_b:admin", "111", `
drop database dsync_target_b1;
`)
	importCmd := exec.Command("go", "run", "./cmd/datasync", "-config", configPath, "-mode", "import", "-run-id", "itest")
	importCmd.Dir = root
	importCmd.Stdout = os.Stdout
	importCmd.Stderr = os.Stderr
	if err := importCmd.Run(); err != nil {
		t.Fatalf("datasync import failed: %v", err)
	}

	assertCount(t, mysql, "dsync_target_a:admin", "111", "dsync_target_a1", "t_keep", "3")
	assertCount(t, mysql, "dsync_target_a:admin", "111", "dsync_target_a2", "orders", "2")
	assertCount(t, mysql, "dsync_target_b:admin", "111", "dsync_target_b1", "events", "2")
	assertReport(t, filepath.Join(outDir, "itest"))
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(dir, "../.."))
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func runSQL(t *testing.T, mysql, user, password, sql string) {
	t.Helper()
	cmd := exec.Command(mysql, "-h", "127.0.0.1", "-P", "6001", "-u", user, "-p"+password, "-e", sql)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("mysql failed for %s: %v", user, err)
	}
}

func assertCount(t *testing.T, mysql, user, password, database, table, want string) {
	t.Helper()
	cmd := exec.Command(mysql, "-N", "-h", "127.0.0.1", "-P", "6001", "-u", user, "-p"+password, "-e", "select count(*) from "+database+"."+table)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("count failed for %s.%s: %v", database, table, err)
	}
	got := strings.TrimSpace(string(out))
	if got != want {
		t.Fatalf("%s.%s count = %s, want %s", database, table, got, want)
	}
}

func assertTableMissing(t *testing.T, mysql, user, password, database, table string) {
	t.Helper()
	cmd := exec.Command(mysql, "-N", "-h", "127.0.0.1", "-P", "6001", "-u", user, "-p"+password, "-e", "select count(*) from "+database+"."+table)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatalf("%s.%s exists, want missing", database, table)
	}
}

func assertReport(t *testing.T, runDir string) {
	t.Helper()
	reportPath := filepath.Join(runDir, "summary-report.json")
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Summary struct {
			TotalTasks      int   `json:"total_tasks"`
			SucceededTasks  int   `json:"succeeded_tasks"`
			FailedTasks     int   `json:"failed_tasks"`
			TotalSourceRows int64 `json:"total_source_rows"`
		} `json:"summary"`
		Tables []struct {
			SourceTable    string `json:"source_table"`
			CSVFile        string `json:"csv_file"`
			CSVFileSize    int64  `json:"csv_file_size_bytes"`
			ExportStatus   string `json:"export_status"`
			ImportStatus   string `json:"import_status"`
			TargetRowCount int64  `json:"target_rows"`
			SourceRowCount int64  `json:"source_rows"`
			TargetName     string `json:"target_name"`
			TargetHost     string `json:"target_host"`
			TargetPort     int    `json:"target_port"`
			TargetUser     string `json:"target_user"`
			TargetDatabase string `json:"target_database"`
			SourceDatabase string `json:"source_database"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Summary.TotalTasks != 3 || report.Summary.SucceededTasks != 3 || report.Summary.FailedTasks != 0 {
		t.Fatalf("summary = %+v, want 3 successful tasks", report.Summary)
	}
	if report.Summary.TotalSourceRows != 7 {
		t.Fatalf("total source rows = %d, want 7", report.Summary.TotalSourceRows)
	}
	if len(report.Tables) != 3 {
		t.Fatalf("table count = %d, want 3", len(report.Tables))
	}
	for _, table := range report.Tables {
		if table.CSVFile == "" || table.CSVFileSize <= 0 {
			t.Fatalf("table report missing CSV file size: %+v", table)
		}
		if table.ExportStatus != "success" && table.ExportStatus != "skipped" {
			t.Fatalf("export status = %q, want success or skipped", table.ExportStatus)
		}
		if table.ImportStatus != "success" {
			t.Fatalf("import status = %q, want success", table.ImportStatus)
		}
		if table.TargetRowCount <= 0 {
			t.Fatalf("target row count = %d, want positive", table.TargetRowCount)
		}
		if table.SourceRowCount != table.TargetRowCount {
			t.Fatalf("row counts for %s.%s source=%d target=%d, want equal", table.SourceDatabase, table.SourceTable, table.SourceRowCount, table.TargetRowCount)
		}
		if table.TargetName == "" || table.TargetHost == "" || table.TargetPort == 0 || table.TargetUser == "" {
			t.Fatalf("table report missing target connection fields: %+v", table)
		}
	}
	csvReport, err := os.ReadFile(filepath.Join(runDir, "summary-report.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(csvReport), "csv_file_size_bytes") {
		t.Fatalf("summary-report.csv missing csv_file_size_bytes header: %s", csvReport)
	}
	markdownReport, err := os.ReadFile(filepath.Join(runDir, "summary-report.md"))
	if err != nil {
		t.Fatal(err)
	}
	markdownText := string(markdownReport)
	for _, want := range []string{"# 数据同步汇总报告", "## 汇总", "## 表同步结果", "CSV大小", "导出重试次数", "导入重试次数"} {
		if !strings.Contains(markdownText, want) {
			t.Fatalf("summary-report.md missing %q: %s", want, markdownText)
		}
	}
	for _, file := range []string{"export-report.md", "import-report.md", "summary-report.md"} {
		if _, err := os.Stat(filepath.Join(runDir, file)); err != nil {
			t.Fatalf("%s missing: %v", file, err)
		}
	}
}
