package datasync

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsAndEnvExpansion(t *testing.T) {
	t.Setenv("DSYNC_PASSWORD", "111")
	cfg := loadConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source:
      name: tenant_a
      host: 127.0.0.1
      port: 6001
      user: dsync_src_a:admin
      password: ${DSYNC_PASSWORD}
      database: src_db
    target:
      name: tenant_target
      host: 127.0.0.2
      port: 6002
      user: dsync_target:admin
      password: ${DSYNC_PASSWORD}
      database: dst_db
    include_tables: [keep, skip_me]
    exclude_tables: [skip_me]
`)

	if cfg.OutputDir != "./runs" {
		t.Fatalf("OutputDir = %q, want ./runs", cfg.OutputDir)
	}
	if cfg.Parallelism != 1 {
		t.Fatalf("Parallelism = %d, want 1", cfg.Parallelism)
	}
	if cfg.Retry.MaxAttempts != 3 {
		t.Fatalf("Retry.MaxAttempts = %d, want 3", cfg.Retry.MaxAttempts)
	}
	if cfg.Retry.BackoffText != "2s" || cfg.Retry.Backoff != 2*time.Second {
		t.Fatalf("Retry backoff = %q/%s, want 2s", cfg.Retry.BackoffText, cfg.Retry.Backoff)
	}
	database := cfg.Databases[0]
	if database.Source.Password != "111" || database.Target.Password != "111" {
		t.Fatalf("passwords source=%q target=%q, want env-expanded 111", database.Source.Password, database.Target.Password)
	}
	if database.Source.Database != "src_db" || database.Target.Database != "dst_db" {
		t.Fatalf("databases source=%q target=%q", database.Source.Database, database.Target.Database)
	}
	if !reflect.DeepEqual(database.IncludeTables, []string{"keep", "skip_me"}) {
		t.Fatalf("IncludeTables = %#v", database.IncludeTables)
	}
	if !reflect.DeepEqual(database.ExcludeTables, []string{"skip_me"}) {
		t.Fatalf("ExcludeTables = %#v", database.ExcludeTables)
	}
}

func TestLoadExpandsEnvAsStringScalar(t *testing.T) {
	t.Setenv("DSYNC_PASSWORD", "abc: def # secret")
	cfg := loadConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source:
      name: tenant_a
      host: 127.0.0.1
      port: 6001
      user: tenant:admin
      password: ${DSYNC_PASSWORD}
      database: src_db
    target:
      name: target
      host: 127.0.0.1
      port: 6001
      user: target:admin
      password: ${DSYNC_PASSWORD}
      database: dst_db
`)

	if cfg.Databases[0].Source.Password != "abc: def # secret" || cfg.Databases[0].Target.Password != "abc: def # secret" {
		t.Fatalf("env-expanded passwords = source %q target %q", cfg.Databases[0].Source.Password, cfg.Databases[0].Target.Password)
	}
}

func TestLoadParsesExplicitRetryBackoff(t *testing.T) {
	cfg := loadConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  max_attempts: 5
  backoff: 1ms
databases:
  - source:
      name: tenant_a
      host: 127.0.0.1
      port: 6001
      user: tenant:admin
      password: "111"
      database: src_db
    target:
      name: target
      host: 127.0.0.1
      port: 6001
      user: target:admin
      password: "111"
      database: dst_db
`)

	if cfg.Retry.MaxAttempts != 5 {
		t.Fatalf("Retry.MaxAttempts = %d, want 5", cfg.Retry.MaxAttempts)
	}
	if cfg.Retry.BackoffText != "1ms" || cfg.Retry.Backoff != time.Millisecond {
		t.Fatalf("Retry backoff = %q/%s, want 1ms", cfg.Retry.BackoffText, cfg.Retry.Backoff)
	}
}

func TestLoadUsesGlobalSourceAndTargetDefaults(t *testing.T) {
	cfg := loadConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
source:
  name: global_source
  host: 127.0.0.1
  port: 6001
  user: global_src:admin
  password: src-pass
target:
  name: global_target
  host: 127.0.0.2
  port: 6002
  user: global_target:admin
  password: target-pass
databases:
  - source:
      database: src_db1
    target:
      database: dst_db1
  - source:
      name: local_source
      host: 127.0.0.3
      port: 6003
      user: local_src:admin
      password: local-src-pass
      database: src_db2
    target:
      name: local_target
      host: 127.0.0.4
      port: 6004
      user: local_target:admin
      password: local-target-pass
      database: dst_db2
`)

	first := cfg.Databases[0]
	if first.Source.Name != "global_source" ||
		first.Source.Host != "127.0.0.1" ||
		first.Source.Port != 6001 ||
		first.Source.User != "global_src:admin" ||
		first.Source.Password != "src-pass" ||
		first.Source.Database != "src_db1" ||
		first.Target.Name != "global_target" ||
		first.Target.Host != "127.0.0.2" ||
		first.Target.Port != 6002 ||
		first.Target.User != "global_target:admin" ||
		first.Target.Password != "target-pass" ||
		first.Target.Database != "dst_db1" {
		t.Fatalf("first database = %+v", first)
	}

	second := cfg.Databases[1]
	if second.Source.Name != "local_source" ||
		second.Source.Host != "127.0.0.3" ||
		second.Source.Port != 6003 ||
		second.Source.User != "local_src:admin" ||
		second.Source.Password != "local-src-pass" ||
		second.Source.Database != "src_db2" ||
		second.Target.Name != "local_target" ||
		second.Target.Host != "127.0.0.4" ||
		second.Target.Port != 6004 ||
		second.Target.User != "local_target:admin" ||
		second.Target.Password != "local-target-pass" ||
		second.Target.Database != "dst_db2" {
		t.Fatalf("second database = %+v", second)
	}
}

func TestLoadSkipsIncompleteDatabaseWithoutGlobalEndpoint(t *testing.T) {
	cfg := loadConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source:
      database: missing_connection_info
    target:
      name: target
      host: 127.0.0.1
      port: 6001
      user: target:admin
      password: "111"
      database: dst_missing
  - source:
      name: tenant_a
      host: 127.0.0.1
      port: 6001
      user: tenant:admin
      password: "111"
      database: src_db
    target:
      name: target
      host: 127.0.0.1
      port: 6001
      user: target:admin
      password: "111"
      database: dst_db
`)

	if len(cfg.Databases) != 1 {
		t.Fatalf("len(Databases) = %d, want 1", len(cfg.Databases))
	}
	if cfg.Databases[0].Source.Database != "src_db" {
		t.Fatalf("remaining database = %+v", cfg.Databases[0])
	}
}

func TestLoadRejectsExplicitZeroDatabasePortWhenGlobalDefaultExists(t *testing.T) {
	path := writeConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
source:
  name: global_source
  host: 127.0.0.1
  port: 6001
  user: global_src:admin
  password: "111"
target:
  name: global_target
  host: 127.0.0.1
  port: 6001
  user: global_target:admin
  password: "111"
databases:
  - source:
      port: 0
      database: src_db
    target:
      database: dst_db
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want explicit zero source port error")
	}
	if !strings.Contains(err.Error(), "databases[0].source.port") {
		t.Fatalf("Load() error = %v, want source port error", err)
	}
}

func TestLoadRejectsMissingEnv(t *testing.T) {
	path := writeConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source:
      name: tenant_a
      host: 127.0.0.1
      port: 6001
      user: tenant:admin
      password: ${MISSING_DSYNC_PASSWORD}
      database: src_db
    target:
      name: target
      host: 127.0.0.1
      port: 6001
      user: target:admin
      password: "111"
      database: dst_db
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want missing environment variable error")
	}
	if !strings.Contains(err.Error(), "MISSING_DSYNC_PASSWORD") {
		t.Fatalf("Load() error = %v, want missing env name", err)
	}
}

func TestLoadRejectsValidationFailures(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{name: "missing mo_dump_path", yaml: `
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`, wantErr: "mo_dump_path"},
		{name: "missing mysql_path", yaml: `
mo_dump_path: /tmp/mo-dump
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`, wantErr: "mysql_path"},
		{name: "unknown top-level key", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
sources: []
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`, wantErr: "sources"},
		{name: "unknown nested key", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
    include: [t1]
`, wantErr: "include"},
		{name: "no databases", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
`, wantErr: "databases"},
		{name: "missing source endpoint field leaves no complete database", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`, wantErr: "databases must contain at least one complete database"},
		{name: "missing target endpoint field leaves no complete database", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, password: "111", database: dst_db}
`, wantErr: "databases must contain at least one complete database"},
		{name: "missing source database leaves no complete database", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111"}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`, wantErr: "databases must contain at least one complete database"},
		{name: "missing target database leaves no complete database", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111"}
`, wantErr: "databases must contain at least one complete database"},
		{name: "zero source port", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 0, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`, wantErr: "databases[0].source.port"},
		{name: "bad target port", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 70000, user: target:admin, password: "111", database: dst_db}
`, wantErr: "databases[0].target.port"},
		{name: "empty include table", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
    include_tables: [""]
`, wantErr: "include_tables"},
		{name: "duplicate include table", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
    include_tables: [t1, t1]
`, wantErr: "include_tables"},
		{name: "empty exclude table", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
    exclude_tables: [""]
`, wantErr: "exclude_tables"},
		{name: "duplicate exclude table", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
    exclude_tables: [t1, t1]
`, wantErr: "exclude_tables"},
		{name: "negative parallelism", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
parallelism: -1
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`, wantErr: "parallelism"},
		{name: "zero retry max attempts", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  max_attempts: 0
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`, wantErr: "retry.max_attempts"},
		{name: "zero retry backoff", yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  backoff: 0s
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`, wantErr: "retry.backoff"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.yaml))
			if err == nil {
				t.Fatal("Load() error = nil, want validation error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Load() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadRejectsDuplicateTopLevelKey(t *testing.T) {
	path := writeConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
output_dir: ./runs-a
output_dir: ./runs-b
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want duplicate key error")
	}
	if !strings.Contains(err.Error(), "duplicate key output_dir") {
		t.Fatalf("Load() error = %v, want duplicate output_dir error", err)
	}
}

func TestLoadRejectsDuplicateRetryKey(t *testing.T) {
	path := writeConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  backoff: 1s
  backoff: 2s
databases:
  - source: {name: tenant_a, host: 127.0.0.1, port: 6001, user: tenant:admin, password: "111", database: src_db}
    target: {name: target, host: 127.0.0.1, port: 6001, user: target:admin, password: "111", database: dst_db}
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want duplicate key error")
	}
	if !strings.Contains(err.Error(), "duplicate key retry.backoff") {
		t.Fatalf("Load() error = %v, want duplicate retry.backoff error", err)
	}
}

func loadConfig(t *testing.T, content string) *Config {
	t.Helper()
	cfg, err := Load(writeConfig(t, content))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	return cfg
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
