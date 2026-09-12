# Datasync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go CLI that exports selected MatrixOne tenant databases table-by-table with `mo-dump -csv`, imports them into mapped target databases with `mysql source`, retries failures, and writes per-table reports.

**Architecture:** `datasync` is a standalone Go command. It loads YAML config, discovers ordinary source tables, builds table tasks, runs export/import steps with retry, and emits JSON/CSV reports under a run directory. `mo-dump` remains an external binary and owns table SQL/CSV generation.

**Tech Stack:** Go 1.23+, `github.com/go-sql-driver/mysql`, `gopkg.in/yaml.v3`, standard `os/exec`, `encoding/json`, `encoding/csv`, and local MatrixOne integration tests.

---

## File Structure

- Create `go.mod`: Go module definition and dependencies.
- Create `cmd/datasync/main.go`: CLI entrypoint and flag parsing.
- Create `internal/config/config.go`: YAML schema, defaulting, validation, environment expansion.
- Create `internal/config/config_test.go`: config unit tests.
- Create `internal/db/db.go`: MatrixOne/MySQL connection helpers, table discovery, row counts, target database creation.
- Create `internal/db/db_test.go`: SQL helper unit tests that do not require MatrixOne.
- Create `internal/plan/plan.go`: table task model and task builder.
- Create `internal/plan/plan_test.go`: mapping and exclude-table tests.
- Create `internal/retry/retry.go`: retry helper.
- Create `internal/retry/retry_test.go`: retry unit tests.
- Create `internal/run/runner.go`: orchestration for export, import, status, concurrency.
- Create `internal/run/runner_test.go`: runner tests using fake command executor and fake database client.
- Create `internal/report/report.go`: JSON/CSV report writer.
- Create `internal/report/report_test.go`: report output tests.
- Create `tests/integration/datasync_local_test.go`: opt-in local MatrixOne integration tests.
- Create `configs/example.yaml`: runnable example config for the local MatrixOne setup.
- Create `README.md`: usage and test instructions.

---

### Task 1: Go Module And CLI Skeleton

**Files:**
- Create: `go.mod`
- Create: `cmd/datasync/main.go`
- Create: `README.md`

- [ ] **Step 1: Write initial module file**

Create `go.mod`:

```go
module github.com/matrixorigin/datasync

go 1.23.0

require (
	github.com/go-sql-driver/mysql v1.8.1
	gopkg.in/yaml.v3 v3.0.1
)
```

- [ ] **Step 2: Write CLI skeleton**

Create `cmd/datasync/main.go`:

```go
package main

import (
	"flag"
	"fmt"
	"os"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to YAML configuration file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "missing required -config")
		os.Exit(2)
	}

	fmt.Fprintf(os.Stderr, "config loading is not implemented yet: %s\n", *configPath)
	os.Exit(1)
}
```

- [ ] **Step 3: Write minimal README**

Create `README.md`:

```markdown
# datasync

`datasync` exports selected MatrixOne tenant tables with `mo-dump -csv` and imports them into mapped target databases with `mysql source`.

## Build

```bash
rtk go build ./cmd/datasync
```

## Run

```bash
rtk go run ./cmd/datasync -config configs/example.yaml
```
```

- [ ] **Step 4: Verify skeleton builds**

Run:

```bash
rtk go mod tidy
rtk go test ./...
rtk go run ./cmd/datasync -version
```

Expected:

```text
dev
```

- [ ] **Step 5: Commit**

```bash
rtk git add go.mod go.sum cmd/datasync/main.go README.md
rtk git commit -m "feat: add datasync cli skeleton"
```

---

### Task 2: Configuration Loading And Validation

**Files:**
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`
- Modify: `cmd/datasync/main.go`
- Create: `configs/example.yaml`

- [ ] **Step 1: Write failing config tests**

Create `internal/config/config_test.go`:

```go
package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaultsAndEnvExpansion(t *testing.T) {
	t.Setenv("DSYNC_PASSWORD", "111")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(`
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
output_dir: ./runs
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: dsync_target:admin
  password: ${DSYNC_PASSWORD}
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: dsync_src_a:admin
    password: "111"
    databases:
      - name: src_db
        exclude_tables: [skip_me]
`), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Parallelism != 1 {
		t.Fatalf("Parallelism = %d, want 1", cfg.Parallelism)
	}
	if cfg.Retry.MaxAttempts != 3 {
		t.Fatalf("Retry.MaxAttempts = %d, want 3", cfg.Retry.MaxAttempts)
	}
	if cfg.Target.Password != "111" {
		t.Fatalf("Target.Password = %q, want 111", cfg.Target.Password)
	}
	if got := cfg.Sources[0].Databases[0].Target; got != "src_db" {
		t.Fatalf("default target = %q, want src_db", got)
	}
}

func TestLoadRejectsMissingEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(`
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: ${MISSING_DSYNC_PASSWORD}
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	_, err = Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want missing environment variable error")
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk go test ./internal/config
```

Expected: FAIL because package `internal/config` does not exist.

- [ ] **Step 3: Implement config package**

Create `internal/config/config.go`:

```go
package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	MoDumpPath  string       `yaml:"mo_dump_path"`
	MySQLPath   string       `yaml:"mysql_path"`
	OutputDir   string       `yaml:"output_dir"`
	Parallelism int          `yaml:"parallelism"`
	Retry       RetryConfig  `yaml:"retry"`
	Target      Endpoint     `yaml:"target"`
	Sources     []Source     `yaml:"sources"`
}

type RetryConfig struct {
	MaxAttempts int           `yaml:"max_attempts"`
	BackoffText string        `yaml:"backoff"`
	Backoff     time.Duration `yaml:"-"`
}

type Endpoint struct {
	Name     string `yaml:"name"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type Source struct {
	Endpoint  `yaml:",inline"`
	Databases []Database `yaml:"databases"`
}

type Database struct {
	Name          string   `yaml:"name"`
	Target        string   `yaml:"target"`
	ExcludeTables []string `yaml:"exclude_tables"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	expanded, err := expandEnvRefs(string(b))
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, err
	}
	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() error {
	if c.OutputDir == "" {
		c.OutputDir = "./runs"
	}
	if c.Parallelism == 0 {
		c.Parallelism = 1
	}
	if c.Retry.MaxAttempts == 0 {
		c.Retry.MaxAttempts = 3
	}
	if c.Retry.Backoff == 0 {
		c.Retry.Backoff = 2 * time.Second
	}
	if c.Retry.BackoffText != "" {
		backoff, err := time.ParseDuration(c.Retry.BackoffText)
		if err != nil {
			return fmt.Errorf("retry.backoff is invalid: %w", err)
		}
		c.Retry.Backoff = backoff
	}
	for si := range c.Sources {
		for di := range c.Sources[si].Databases {
			if c.Sources[si].Databases[di].Target == "" {
				c.Sources[si].Databases[di].Target = c.Sources[si].Databases[di].Name
			}
		}
	}
	return nil
}

func (c *Config) Validate() error {
	if c.MoDumpPath == "" {
		return fmt.Errorf("mo_dump_path is required")
	}
	if c.MySQLPath == "" {
		return fmt.Errorf("mysql_path is required")
	}
	if c.Parallelism < 1 {
		return fmt.Errorf("parallelism must be greater than 0")
	}
	if c.Retry.MaxAttempts < 1 {
		return fmt.Errorf("retry.max_attempts must be greater than 0")
	}
	if err := validateEndpoint("target", c.Target); err != nil {
		return err
	}
	if len(c.Sources) == 0 {
		return fmt.Errorf("sources is required")
	}
	for i, src := range c.Sources {
		if err := validateEndpoint(fmt.Sprintf("sources[%d]", i), src.Endpoint); err != nil {
			return err
		}
		if len(src.Databases) == 0 {
			return fmt.Errorf("sources[%d].databases is required", i)
		}
		for j, db := range src.Databases {
			if db.Name == "" {
				return fmt.Errorf("sources[%d].databases[%d].name is required", i, j)
			}
		}
	}
	return nil
}

func validateEndpoint(prefix string, e Endpoint) error {
	if e.Name == "" {
		return fmt.Errorf("%s.name is required", prefix)
	}
	if e.Host == "" {
		return fmt.Errorf("%s.host is required", prefix)
	}
	if e.Port == 0 {
		return fmt.Errorf("%s.port is required", prefix)
	}
	if e.User == "" {
		return fmt.Errorf("%s.user is required", prefix)
	}
	return nil
}

var envRefPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func expandEnvRefs(input string) (string, error) {
	var missing string
	out := envRefPattern.ReplaceAllStringFunc(input, func(match string) string {
		name := envRefPattern.FindStringSubmatch(match)[1]
		value, ok := os.LookupEnv(name)
		if !ok && missing == "" {
			missing = name
		}
		return value
	})
	if missing != "" {
		return "", fmt.Errorf("environment variable %s is not set", missing)
	}
	return out, nil
}
```

- [ ] **Step 4: Wire config into CLI**

Replace `cmd/datasync/main.go` with:

```go
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/matrixorigin/datasync/internal/config"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to YAML configuration file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "missing required -config")
		os.Exit(2)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "loaded config for %d source tenants\n", len(cfg.Sources))
}
```

- [ ] **Step 5: Add example config**

Create `configs/example.yaml` with the YAML from `docs/superpowers/specs/2026-06-06-datasync-design.md`.

- [ ] **Step 6: Verify config tests**

Run:

```bash
rtk go test ./internal/config
rtk go test ./...
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
rtk git add go.mod go.sum cmd/datasync/main.go internal/config configs/example.yaml
rtk git commit -m "feat: load datasync config"
```

---

### Task 3: MatrixOne Database Client

**Files:**
- Create: `internal/db/db.go`
- Create: `internal/db/db_test.go`

- [ ] **Step 1: Write SQL helper tests**

Create `internal/db/db_test.go`:

```go
package db

import "testing"

func TestQuoteIdent(t *testing.T) {
	if got := QuoteIdent("a`b"); got != "`a``b`" {
		t.Fatalf("QuoteIdent() = %q, want `a``b`", got)
	}
}

func TestDSN(t *testing.T) {
	ep := Endpoint{Host: "127.0.0.1", Port: 6001, User: "acc:admin", Password: "111"}
	got := DSN(ep, "db1")
	want := "acc#admin:111@tcp(127.0.0.1:6001)/db1?allowAllFiles=true&multiStatements=true&parseTime=true"
	if got != want {
		t.Fatalf("DSN() = %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk go test ./internal/db
```

Expected: FAIL because package `internal/db` does not exist.

- [ ] **Step 3: Implement database client**

Create `internal/db/db.go`:

```go
package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Endpoint struct {
	Host     string
	Port     int
	User     string
	Password string
}

type Table struct {
	Name string
	Kind string
}

type Client struct {
	db *sql.DB
}

func DSN(ep Endpoint, database string) string {
	user := strings.ReplaceAll(ep.User, ":", "#")
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?allowAllFiles=true&multiStatements=true&parseTime=true", user, ep.Password, ep.Host, ep.Port, database)
}

func Open(ctx context.Context, ep Endpoint, database string) (*Client, error) {
	conn, err := sql.Open("mysql", DSN(ep, database))
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := conn.PingContext(pingCtx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Client{db: conn}, nil
}

func (c *Client) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

func (c *Client) ListOrdinaryTables(ctx context.Context, database string) ([]Table, error) {
	rows, err := c.db.QueryContext(ctx, "select relname, relkind from mo_catalog.mo_tables where reldatabase = ? and relkind = 'r'", database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var tbl Table
		if err := rows.Scan(&tbl.Name, &tbl.Kind); err != nil {
			return nil, err
		}
		if strings.HasPrefix(tbl.Name, "__mo_") || strings.HasPrefix(tbl.Name, "%!%") {
			continue
		}
		tables = append(tables, tbl)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (c *Client) CountRows(ctx context.Context, database, table string) (int64, error) {
	query := fmt.Sprintf("select count(*) from %s.%s", QuoteIdent(database), QuoteIdent(table))
	var count int64
	if err := c.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Client) EnsureDatabase(ctx context.Context, database string) error {
	_, err := c.db.ExecContext(ctx, "create database if not exists "+QuoteIdent(database))
	return err
}

func QuoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
rtk go test ./internal/db
rtk go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add internal/db go.mod go.sum
rtk git commit -m "feat: add matrixone db client"
```

---

### Task 4: Build Table Task Plan

**Files:**
- Create: `internal/plan/plan.go`
- Create: `internal/plan/plan_test.go`

- [ ] **Step 1: Write failing planner tests**

Create `internal/plan/plan_test.go`:

```go
package plan

import (
	"testing"

	"github.com/matrixorigin/datasync/internal/config"
)

func TestBuildTasksAppliesMappingAndExcludes(t *testing.T) {
	cfg := &config.Config{
		Sources: []config.Source{{
			Endpoint: config.Endpoint{Name: "tenant_a", Host: "127.0.0.1", Port: 6001, User: "a:admin", Password: "111"},
			Databases: []config.Database{{
				Name: "src_db", Target: "target_db", ExcludeTables: []string{"skip"},
			}},
		}},
	}
	tables := map[DatabaseKey][]string{
		{SourceName: "tenant_a", Database: "src_db"}: {"keep", "skip"},
	}

	tasks := BuildTasks(cfg, tables)
	if len(tasks) != 1 {
		t.Fatalf("len(tasks) = %d, want 1", len(tasks))
	}
	task := tasks[0]
	if task.SourceTable != "keep" || task.TargetDatabase != "target_db" {
		t.Fatalf("task = %+v", task)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk go test ./internal/plan
```

Expected: FAIL because package `internal/plan` does not exist.

- [ ] **Step 3: Implement planner**

Create `internal/plan/plan.go`:

```go
package plan

import "github.com/matrixorigin/datasync/internal/config"

type DatabaseKey struct {
	SourceName string
	Database   string
}

type Task struct {
	SourceName     string
	SourceHost     string
	SourcePort     int
	SourceUser     string
	SourcePassword string
	SourceDatabase string
	SourceTable    string
	TargetDatabase string
}

func BuildTasks(cfg *config.Config, tables map[DatabaseKey][]string) []Task {
	var tasks []Task
	for _, src := range cfg.Sources {
		for _, database := range src.Databases {
			excluded := make(map[string]struct{}, len(database.ExcludeTables))
			for _, table := range database.ExcludeTables {
				excluded[table] = struct{}{}
			}
			key := DatabaseKey{SourceName: src.Name, Database: database.Name}
			for _, table := range tables[key] {
				if _, ok := excluded[table]; ok {
					continue
				}
				tasks = append(tasks, Task{
					SourceName:     src.Name,
					SourceHost:     src.Host,
					SourcePort:     src.Port,
					SourceUser:     src.User,
					SourcePassword: src.Password,
					SourceDatabase: database.Name,
					SourceTable:    table,
					TargetDatabase: database.Target,
				})
			}
		}
	}
	return tasks
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
rtk go test ./internal/plan
rtk go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add internal/plan
rtk git commit -m "feat: build datasync table plan"
```

---

### Task 5: Retry Helper

**Files:**
- Create: `internal/retry/retry.go`
- Create: `internal/retry/retry_test.go`

- [ ] **Step 1: Write failing retry tests**

Create `internal/retry/retry_test.go`:

```go
package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoRetriesUntilSuccess(t *testing.T) {
	attempts := 0
	err := Do(context.Background(), Config{MaxAttempts: 3, Backoff: time.Nanosecond}, func(context.Context, int) error {
		attempts++
		if attempts < 2 {
			return errors.New("temporary")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestDoReturnsLastError(t *testing.T) {
	errBoom := errors.New("boom")
	err := Do(context.Background(), Config{MaxAttempts: 2, Backoff: time.Nanosecond}, func(context.Context, int) error {
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("Do() error = %v, want boom", err)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk go test ./internal/retry
```

Expected: FAIL because package `internal/retry` does not exist.

- [ ] **Step 3: Implement retry helper**

Create `internal/retry/retry.go`:

```go
package retry

import (
	"context"
	"time"
)

type Config struct {
	MaxAttempts int
	Backoff     time.Duration
}

func Do(ctx context.Context, cfg Config, fn func(context.Context, int) error) error {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	var last error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if err := fn(ctx, attempt); err != nil {
			last = err
		} else {
			return nil
		}
		if attempt == cfg.MaxAttempts {
			break
		}
		timer := time.NewTimer(cfg.Backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return last
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
rtk go test ./internal/retry
rtk go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add internal/retry
rtk git commit -m "feat: add retry helper"
```

---

### Task 6: Report Writer

**Files:**
- Create: `internal/report/report.go`
- Create: `internal/report/report_test.go`

- [ ] **Step 1: Write failing report tests**

Create `internal/report/report_test.go`:

```go
package report

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteReports(t *testing.T) {
	dir := t.TempDir()
	r := RunReport{
		RunID: "run1",
		Summary: Summary{TotalTasks: 1, SucceededTasks: 1, TotalSourceRows: 3, TotalTargetRows: 3},
		Tables: []TableReport{{
			SourceName: "tenant_a", SourceDatabase: "src_db", SourceTable: "t1", TargetDatabase: "dst_db",
			SQLFile: "/tmp/t1.sql", CSVFile: "/tmp/src_db_t1.csv", SourceRows: 3, TargetRows: 3,
			ExportStatus: "success", ImportStatus: "success",
		}},
	}
	if err := Write(dir, r); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	jsonBytes, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(jsonBytes), `"run_id": "run1"`) {
		t.Fatalf("report.json missing run_id: %s", jsonBytes)
	}
	csvBytes, err := os.ReadFile(filepath.Join(dir, "report.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(csvBytes), "tenant_a,src_db,t1,dst_db") {
		t.Fatalf("report.csv missing row: %s", csvBytes)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk go test ./internal/report
```

Expected: FAIL because package `internal/report` does not exist.

- [ ] **Step 3: Implement report writer**

Create `internal/report/report.go`:

```go
package report

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"time"
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
	Duration        time.Duration `json:"duration"`
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
	TargetDatabase   string        `json:"target_database"`
	SQLFile          string        `json:"sql_file"`
	CSVFile          string        `json:"csv_file"`
	SourceRows       int64         `json:"source_rows"`
	TargetRows       int64         `json:"target_rows"`
	ExportStatus     string        `json:"export_status"`
	ImportStatus     string        `json:"import_status"`
	ExportStartedAt  time.Time     `json:"export_started_at"`
	ExportFinishedAt time.Time     `json:"export_finished_at"`
	ImportStartedAt  time.Time     `json:"import_started_at"`
	ImportFinishedAt time.Time     `json:"import_finished_at"`
	ExportDuration   time.Duration `json:"export_duration"`
	ImportDuration   time.Duration `json:"import_duration"`
	ExportAttempts   int           `json:"export_attempts"`
	ImportAttempts   int           `json:"import_attempts"`
	Error             string        `json:"error,omitempty"`
}

func Write(dir string, r RunReport) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	jsonPath := filepath.Join(dir, "report.json")
	csvPath := filepath.Join(dir, "report.csv")
	r.Summary.JSONReportPath = jsonPath
	r.Summary.CSVReportPath = csvPath
	if err := writeJSON(jsonPath, r); err != nil {
		return err
	}
	return writeCSV(csvPath, r.Tables)
}

func writeJSON(path string, r RunReport) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func writeCSV(path string, rows []TableReport) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	header := []string{"source_name", "source_database", "source_table", "target_database", "source_rows", "target_rows", "export_status", "import_status", "export_attempts", "import_attempts", "sql_file", "csv_file", "error"}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, row := range rows {
		if err := w.Write([]string{
			row.SourceName,
			row.SourceDatabase,
			row.SourceTable,
			row.TargetDatabase,
			strconv.FormatInt(row.SourceRows, 10),
			strconv.FormatInt(row.TargetRows, 10),
			row.ExportStatus,
			row.ImportStatus,
			strconv.Itoa(row.ExportAttempts),
			strconv.Itoa(row.ImportAttempts),
			row.SQLFile,
			row.CSVFile,
			row.Error,
		}); err != nil {
			return err
		}
	}
	return w.Error()
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
rtk go test ./internal/report
rtk go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add internal/report
rtk git commit -m "feat: write datasync reports"
```

---

### Task 7: Runner With Export And Import Execution

**Files:**
- Create: `internal/run/runner.go`
- Create: `internal/run/runner_test.go`

- [ ] **Step 1: Write failing runner tests**

Create `internal/run/runner_test.go`:

```go
package run

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/datasync/internal/config"
	"github.com/matrixorigin/datasync/internal/plan"
)

func TestExportCommandCreatesStableFiles(t *testing.T) {
	dir := t.TempDir()
	task := plan.Task{
		SourceName: "tenant_a", SourceHost: "127.0.0.1", SourcePort: 6001,
		SourceUser: "a:admin", SourcePassword: "111", SourceDatabase: "src_db", SourceTable: "t1", TargetDatabase: "dst_db",
	}
	r := Runner{Config: &config.Config{MoDumpPath: "/bin/mo-dump", MySQLPath: "/bin/mysql", OutputDir: dir}}
	tableDir, sqlFile, csvFile := r.taskPaths("run1", task)
	if !strings.HasSuffix(tableDir, filepath.Join("run1", "exports", "tenant_a", "src_db", "t1")) {
		t.Fatalf("tableDir = %s", tableDir)
	}
	if filepath.Base(sqlFile) != "t1.sql" {
		t.Fatalf("sqlFile = %s", sqlFile)
	}
	if filepath.Base(csvFile) != "src_db_t1.csv" {
		t.Fatalf("csvFile = %s", csvFile)
	}
}

func TestRunTaskRetriesExportAndImports(t *testing.T) {
	dir := t.TempDir()
	exec := &fakeExecutor{}
	db := &fakeDB{sourceRows: 2, targetRows: 2}
	r := Runner{
		Config: &config.Config{
			MoDumpPath: "/bin/mo-dump", MySQLPath: "/bin/mysql", OutputDir: dir,
			Retry: config.RetryConfig{MaxAttempts: 2, Backoff: time.Nanosecond},
			Target: config.Endpoint{Host: "127.0.0.1", Port: 6001, User: "target:admin", Password: "111"},
		},
		Executor: exec,
		DB:       db,
	}
	task := plan.Task{
		SourceName: "tenant_a", SourceHost: "127.0.0.1", SourcePort: 6001,
		SourceUser: "a:admin", SourcePassword: "111", SourceDatabase: "src_db", SourceTable: "t1", TargetDatabase: "dst_db",
	}

	row := r.runTask(context.Background(), "run1", task)
	if row.ExportStatus != "success" || row.ImportStatus != "success" {
		t.Fatalf("row = %+v", row)
	}
	if exec.moDumpCalls != 1 || exec.mysqlCalls != 1 {
		t.Fatalf("moDumpCalls=%d mysqlCalls=%d", exec.moDumpCalls, exec.mysqlCalls)
	}
}

type fakeExecutor struct {
	moDumpCalls int
	mysqlCalls  int
}

func (f *fakeExecutor) MoDump(ctx context.Context, req MoDumpRequest) error {
	f.moDumpCalls++
	if err := os.MkdirAll(req.WorkDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(req.SQLFile, []byte("DROP TABLE IF EXISTS `t1`;"), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(req.WorkDir, "src_db_t1.csv"), []byte("1\n2\n"), 0o644)
}

func (f *fakeExecutor) MySQLSource(ctx context.Context, req MySQLSourceRequest) error {
	f.mysqlCalls++
	return nil
}

type fakeDB struct {
	sourceRows int64
	targetRows int64
}

func (f *fakeDB) CountSourceRows(ctx context.Context, task plan.Task) (int64, error) {
	return f.sourceRows, nil
}

func (f *fakeDB) EnsureTargetDatabase(ctx context.Context, database string) error {
	return nil
}

func (f *fakeDB) CountTargetRows(ctx context.Context, database, table string) (int64, error) {
	return f.targetRows, nil
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk go test ./internal/run
```

Expected: FAIL because package `internal/run` does not exist.

- [ ] **Step 3: Implement runner**

Create `internal/run/runner.go`:

```go
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

type Runner struct {
	Config   *config.Config
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
	Binary string
	Task   plan.Task
	WorkDir string
	SQLFile string
}

type MySQLSourceRequest struct {
	Binary string
	Target config.Endpoint
	Database string
	SQLFile string
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
		if row.Error == "" && row.ExportStatus == "success" && row.ImportStatus == "success" {
			summary.SucceededTasks++
		} else {
			summary.FailedTasks++
		}
	}
	out := report.RunReport{RunID: runID, Summary: summary, Tables: rows}
	return out, nil
}

func (r Runner) runTask(ctx context.Context, runID string, task plan.Task) report.TableReport {
	row := report.TableReport{
		RunID: runID, SourceName: task.SourceName, SourceHost: task.SourceHost, SourcePort: task.SourcePort,
		SourceDatabase: task.SourceDatabase, SourceTable: task.SourceTable, TargetDatabase: task.TargetDatabase,
	}
	tableDir, sqlFile, csvFile := r.taskPaths(runID, task)
	row.SQLFile = sqlFile
	row.CSVFile = csvFile

	sourceRows, err := r.DB.CountSourceRows(ctx, task)
	if err != nil {
		row.ExportStatus = "failed"
		row.Error = err.Error()
		return row
	}
	row.SourceRows = sourceRows

	retryCfg := retry.Config{MaxAttempts: r.Config.Retry.MaxAttempts, Backoff: r.Config.Retry.Backoff}
	row.ExportStartedAt = time.Now()
	err = retry.Do(ctx, retryCfg, func(ctx context.Context, attempt int) error {
		row.ExportAttempts = attempt
		if err := os.RemoveAll(tableDir); err != nil {
			return err
		}
		if err := os.MkdirAll(tableDir, 0o755); err != nil {
			return err
		}
		return r.Executor.MoDump(ctx, MoDumpRequest{Binary: r.Config.MoDumpPath, Task: task, WorkDir: tableDir, SQLFile: sqlFile})
	})
	row.ExportFinishedAt = time.Now()
	row.ExportDuration = row.ExportFinishedAt.Sub(row.ExportStartedAt)
	if err != nil {
		row.ExportStatus = "failed"
		row.Error = err.Error()
		return row
	}
	if _, err := os.Stat(sqlFile); err != nil {
		row.ExportStatus = "failed"
		row.Error = err.Error()
		return row
	}
	if _, err := os.Stat(csvFile); err != nil {
		row.ExportStatus = "failed"
		row.Error = err.Error()
		return row
	}
	row.ExportStatus = "success"

	if err := r.DB.EnsureTargetDatabase(ctx, task.TargetDatabase); err != nil {
		row.ImportStatus = "failed"
		row.Error = err.Error()
		return row
	}
	row.ImportStartedAt = time.Now()
	err = retry.Do(ctx, retryCfg, func(ctx context.Context, attempt int) error {
		row.ImportAttempts = attempt
		return r.Executor.MySQLSource(ctx, MySQLSourceRequest{
			Binary: r.Config.MySQLPath, Target: r.Config.Target, Database: task.TargetDatabase, SQLFile: sqlFile,
		})
	})
	row.ImportFinishedAt = time.Now()
	row.ImportDuration = row.ImportFinishedAt.Sub(row.ImportStartedAt)
	if err != nil {
		row.ImportStatus = "failed"
		row.Error = err.Error()
		return row
	}
	targetRows, err := r.DB.CountTargetRows(ctx, task.TargetDatabase, task.SourceTable)
	if err != nil {
		row.ImportStatus = "failed"
		row.Error = err.Error()
		return row
	}
	row.TargetRows = targetRows
	row.ImportStatus = "success"
	if row.SourceRows != row.TargetRows {
		row.ImportStatus = "failed"
		row.Error = fmt.Sprintf("row count mismatch: source=%d target=%d", row.SourceRows, row.TargetRows)
	}
	return row
}

func (r Runner) taskPaths(runID string, task plan.Task) (string, string, string) {
	tableDir := filepath.Join(r.Config.OutputDir, runID, "exports", task.SourceName, task.SourceDatabase, task.SourceTable)
	sqlFile := filepath.Join(tableDir, task.SourceTable+".sql")
	csvFile := filepath.Join(tableDir, task.SourceDatabase+"_"+task.SourceTable+".csv")
	return tableDir, sqlFile, csvFile
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
	sourceStmt := "source " + req.SQLFile
	cmd := exec.CommandContext(ctx, req.Binary,
		"--local-infile=1",
		"-h", req.Target.Host,
		"-P", strconv.Itoa(req.Target.Port),
		"-u", req.Target.User,
		"-p"+req.Target.Password,
		req.Database,
		"-e", sourceStmt,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
rtk go test ./internal/run
rtk go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add internal/run
rtk git commit -m "feat: run table export and import tasks"
```

---

### Task 8: Orchestrate Discovery, Planning, Running, And Reporting

**Files:**
- Modify: `cmd/datasync/main.go`
- Create: `internal/app/app.go`
- Create: `internal/app/app_test.go`

- [ ] **Step 1: Write app-level fake test**

Create `internal/app/app_test.go`:

```go
package app

import (
	"context"
	"testing"

	"github.com/matrixorigin/datasync/internal/config"
	"github.com/matrixorigin/datasync/internal/plan"
	"github.com/matrixorigin/datasync/internal/report"
)

func TestRunBuildsTasksFromDiscoveredTables(t *testing.T) {
	cfg := &config.Config{
		OutputDir: t.TempDir(),
		Parallelism: 1,
		MoDumpPath: "/bin/mo-dump",
		MySQLPath: "/bin/mysql",
		Target: config.Endpoint{Name: "target", Host: "127.0.0.1", Port: 6001, User: "target:admin", Password: "111"},
		Sources: []config.Source{{
			Endpoint: config.Endpoint{Name: "tenant_a", Host: "127.0.0.1", Port: 6001, User: "a:admin", Password: "111"},
			Databases: []config.Database{{Name: "src_db", Target: "dst_db", ExcludeTables: []string{"skip"}}},
		}},
	}
	app := App{Config: cfg, Discovery: fakeDiscovery{tables: []string{"keep", "skip"}}, Runner: fakeRunner{}}
	result, err := app.Run(context.Background(), "run1")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.PlannedTasks != 1 {
		t.Fatalf("PlannedTasks = %d, want 1", result.PlannedTasks)
	}
}

type fakeDiscovery struct {
	tables []string
}

func (f fakeDiscovery) ListTables(ctx context.Context, source config.Source, database string) ([]string, error) {
	return f.tables, nil
}

type fakeRunner struct{}

func (fakeRunner) Run(ctx context.Context, runID string, tasks []plan.Task) (report.RunReport, error) {
	return report.RunReport{
		RunID: runID,
		Summary: report.Summary{
			TotalTasks:     len(tasks),
			SucceededTasks: len(tasks),
		},
	}, nil
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk go test ./internal/app
```

Expected: FAIL because package `internal/app` does not exist.

- [ ] **Step 3: Implement app orchestration**

Create `internal/app/app.go`:

```go
package app

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/matrixorigin/datasync/internal/config"
	"github.com/matrixorigin/datasync/internal/db"
	"github.com/matrixorigin/datasync/internal/plan"
	"github.com/matrixorigin/datasync/internal/report"
	"github.com/matrixorigin/datasync/internal/run"
)

type App struct {
	Config    *config.Config
	Discovery Discovery
	Runner    TaskRunner
}

type Discovery interface {
	ListTables(context.Context, config.Source, string) ([]string, error)
}

type TaskRunner interface {
	Run(context.Context, string, []plan.Task) (report.RunReport, error)
}

type Result struct {
	RunID        string
	PlannedTasks int
	Report       report.RunReport
}

func (a App) Run(ctx context.Context, runID string) (Result, error) {
	if a.Discovery == nil {
		a.Discovery = MatrixOneDiscovery{}
	}
	discovered := make(map[plan.DatabaseKey][]string)
	for _, source := range a.Config.Sources {
		for _, database := range source.Databases {
			tables, err := a.Discovery.ListTables(ctx, source, database.Name)
			if err != nil {
				return Result{}, err
			}
			discovered[plan.DatabaseKey{SourceName: source.Name, Database: database.Name}] = tables
		}
	}
	tasks := plan.BuildTasks(a.Config, discovered)
	runner := a.Runner
	if runner == nil {
		runner = run.Runner{Config: a.Config, DB: MatrixOneRunDB{Config: a.Config}}
	}
	runReport, err := runner.Run(ctx, runID, tasks)
	if err != nil {
		return Result{}, err
	}
	runDir := filepath.Join(a.Config.OutputDir, runID)
	if err := report.Write(runDir, runReport); err != nil {
		return Result{}, err
	}
	if runReport.Summary.FailedTasks > 0 {
		return Result{RunID: runID, PlannedTasks: len(tasks), Report: runReport}, fmt.Errorf("%d table tasks failed", runReport.Summary.FailedTasks)
	}
	return Result{RunID: runID, PlannedTasks: len(tasks), Report: runReport}, nil
}

func NewRunID(now time.Time) string {
	return now.Format("20060102-150405")
}

type MatrixOneDiscovery struct{}

func (MatrixOneDiscovery) ListTables(ctx context.Context, source config.Source, database string) ([]string, error) {
	client, err := db.Open(ctx, db.Endpoint{Host: source.Host, Port: source.Port, User: source.User, Password: source.Password}, database)
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

type MatrixOneRunDB struct {
	Config *config.Config
}

func (m MatrixOneRunDB) CountSourceRows(ctx context.Context, task plan.Task) (int64, error) {
	client, err := db.Open(ctx, db.Endpoint{Host: task.SourceHost, Port: task.SourcePort, User: task.SourceUser, Password: task.SourcePassword}, task.SourceDatabase)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	return client.CountRows(ctx, task.SourceDatabase, task.SourceTable)
}

func (m MatrixOneRunDB) EnsureTargetDatabase(ctx context.Context, database string) error {
	client, err := db.Open(ctx, db.Endpoint{Host: m.Config.Target.Host, Port: m.Config.Target.Port, User: m.Config.Target.User, Password: m.Config.Target.Password}, "")
	if err != nil {
		return err
	}
	defer client.Close()
	return client.EnsureDatabase(ctx, database)
}

func (m MatrixOneRunDB) CountTargetRows(ctx context.Context, database, table string) (int64, error) {
	client, err := db.Open(ctx, db.Endpoint{Host: m.Config.Target.Host, Port: m.Config.Target.Port, User: m.Config.Target.User, Password: m.Config.Target.Password}, database)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	return client.CountRows(ctx, database, table)
}
```

- [ ] **Step 4: Wire CLI to app**

Replace `cmd/datasync/main.go` with:

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/datasync/internal/app"
	"github.com/matrixorigin/datasync/internal/config"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to YAML configuration file")
	runID := flag.String("run-id", "", "optional run id")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "missing required -config")
		os.Exit(2)
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	if *runID == "" {
		*runID = app.NewRunID(time.Now())
	}
	result, err := app.App{Config: cfg}.Run(context.Background(), *runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "datasync failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("run_id=%s planned_tasks=%d succeeded=%d failed=%d\n", result.RunID, result.PlannedTasks, result.Report.Summary.SucceededTasks, result.Report.Summary.FailedTasks)
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
rtk go test ./internal/app
rtk go test ./...
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add cmd/datasync/main.go internal/app
rtk git commit -m "feat: orchestrate datasync runs"
```

---

### Task 9: Local MatrixOne Integration Test

**Files:**
- Create: `tests/integration/datasync_local_test.go`
- Modify: `configs/example.yaml`
- Modify: `README.md`

- [ ] **Step 1: Write integration test**

Create `tests/integration/datasync_local_test.go`:

```go
package integration

import (
	"os"
	"os/exec"
	"path/filepath"
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
		t.Fatalf("count failed: %v", err)
	}
	if got := string(out[:len(out)-1]); got != want {
		t.Fatalf("%s.%s count = %s, want %s", database, table, got, want)
	}
}

func assertTableMissing(t *testing.T, mysql, user, password, database, table string) {
	t.Helper()
	cmd := exec.Command(mysql, "-N", "-h", "127.0.0.1", "-P", "6001", "-u", user, "-p"+password, "-e", "select count(*) from "+database+"."+table)
	if err := cmd.Run(); err == nil {
		t.Fatalf("%s.%s exists, want missing", database, table)
	}
}
```

- [ ] **Step 2: Document integration test**

Add to `README.md`:

```markdown
## Local MatrixOne Integration Test

The integration test expects MatrixOne on `127.0.0.1:6001` with system user `dump/111`, plus working `mysql` and `mo-dump` binaries.

```bash
rtk DATASYNC_INTEGRATION=1 go test ./tests/integration -v
```
```

- [ ] **Step 3: Run unit tests**

Run:

```bash
rtk go test ./...
```

Expected: PASS, with integration test skipped unless `DATASYNC_INTEGRATION=1`.

- [ ] **Step 4: Run integration test against local MatrixOne**

Run:

```bash
rtk DATASYNC_INTEGRATION=1 go test ./tests/integration -v
```

Expected: PASS. It should create `dsync_src_a`, `dsync_src_b`, `dsync_target`, export three included tables, exclude `t_skip`, and verify target row counts.

- [ ] **Step 5: Commit**

```bash
rtk git add tests/integration README.md configs/example.yaml
rtk git commit -m "test: add local matrixone integration coverage"
```

---

### Task 10: Final Verification And Polish

**Files:**
- Modify as needed: `README.md`
- Modify as needed: `configs/example.yaml`

- [ ] **Step 1: Run formatting**

Run:

```bash
rtk gofmt -w cmd internal tests
```

Expected: command succeeds.

- [ ] **Step 2: Run all unit tests**

Run:

```bash
rtk go test ./...
```

Expected: PASS, with integration test skipped unless enabled.

- [ ] **Step 3: Run local integration test**

Run:

```bash
rtk DATASYNC_INTEGRATION=1 go test ./tests/integration -v
```

Expected: PASS.

- [ ] **Step 4: Build binary**

Run:

```bash
rtk go build ./cmd/datasync
```

Expected: build succeeds and creates `./datasync`.

- [ ] **Step 5: Inspect git status**

Run:

```bash
rtk git status --short
```

Expected: only intentional files are modified or untracked.

- [ ] **Step 6: Commit final polish**

If Step 1 changed files, commit them:

```bash
rtk git add README.md configs/example.yaml cmd internal tests
rtk git commit -m "chore: polish datasync implementation"
```

If Step 1 changed no files, skip this commit.

---

## Self-Review

Spec coverage:

- Multi-tenant source export is covered by Tasks 4, 8, and 9.
- Per-table export is covered by Tasks 4 and 7.
- Config-driven source databases, excluded tables, and target database mapping are covered by Task 2 and Task 4.
- Per-table report fields are covered by Task 6 and consumed by Task 7.
- Export retry and import retry are covered by Task 5 and Task 7.
- Export idempotency through task directory cleanup is covered by Task 7.
- Import idempotency through `source` of SQL containing `DROP TABLE IF EXISTS` is covered by Task 7 and Task 9.
- `mo-dump -csv` and `mysql --local-infile=1` are covered by Task 7 and Task 9.
- Local MatrixOne integration coverage with multiple tenants, multiple databases, excluded table, database mapping, overwrite import, and row counts is covered by Task 9.

Placeholder scan:

- No `TBD`, `TODO`, or open-ended implementation placeholders remain.
- No optional implementation corrections remain in task steps.

Type consistency:

- `config.Config`, `plan.Task`, `report.TableReport`, and `run.Runner` names are consistent across tasks.
- Retry config converts from `config.RetryConfig` to `retry.Config` in the runner.
- MatrixOne connection endpoint is translated from `config.Endpoint` to `db.Endpoint` where database operations happen.
