package integration

import (
	"bytes"
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
drop account if exists dsync_target;
create account dsync_src_a admin_name 'admin' identified by '111';
create account dsync_src_b admin_name 'admin' identified by '111';
create account dsync_target admin_name 'admin' identified by '111';
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
`)
	runSQL(t, mysql, "dsync_src_b:admin", "111", `
create database dsync_b_db1;
create table dsync_b_db1.events(id int primary key, label varchar(50));
insert into dsync_b_db1.events values (1,'login'),(2,'logout');
`)
	runSQL(t, mysql, "dsync_target:admin", "111", `
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
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: dsync_target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: dsync_src_a:admin
    password: "111"
    databases:
      - name: dsync_a_db1
        target: dsync_target_a1
        exclude_tables: [t_skip]
      - name: dsync_a_db2
        target: dsync_target_a2
  - name: tenant_b
    host: 127.0.0.1
    port: 6001
    user: dsync_src_b:admin
    password: "111"
    databases:
      - name: dsync_b_db1
        target: dsync_target_b1
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

	assertCount(t, mysql, "dsync_target:admin", "111", "dsync_target_a1", "t_keep", "3")
	assertCount(t, mysql, "dsync_target:admin", "111", "dsync_target_a2", "orders", "2")
	assertCount(t, mysql, "dsync_target:admin", "111", "dsync_target_b1", "events", "2")
	assertTableMissing(t, mysql, "dsync_target:admin", "111", "dsync_target_a1", "t_skip")

	runSQL(t, mysql, "dsync_target:admin", "111", `
drop database dsync_target_a1;
drop database dsync_target_a2;
drop database dsync_target_b1;
`)
	importCmd := exec.Command("go", "run", "./cmd/datasync", "-config", configPath, "-mode", "import", "-run-id", "itest")
	importCmd.Dir = root
	importCmd.Stdout = os.Stdout
	importCmd.Stderr = os.Stderr
	if err := importCmd.Run(); err != nil {
		t.Fatalf("datasync import failed: %v", err)
	}

	assertCount(t, mysql, "dsync_target:admin", "111", "dsync_target_a1", "t_keep", "3")
	assertCount(t, mysql, "dsync_target:admin", "111", "dsync_target_a2", "orders", "2")
	assertCount(t, mysql, "dsync_target:admin", "111", "dsync_target_b1", "events", "2")
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
