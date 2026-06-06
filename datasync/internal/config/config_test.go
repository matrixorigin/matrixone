package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsAndEnvExpansion(t *testing.T) {
	t.Setenv("DSYNC_PASSWORD", "111")
	cfg := loadConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
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
	if cfg.Retry.BackoffText != "2s" {
		t.Fatalf("Retry.BackoffText = %q, want 2s", cfg.Retry.BackoffText)
	}
	if cfg.Retry.Backoff != 2*time.Second {
		t.Fatalf("Retry.Backoff = %s, want 2s", cfg.Retry.Backoff)
	}
	if cfg.Target.Password != "111" {
		t.Fatalf("Target.Password = %q, want 111", cfg.Target.Password)
	}
	if got := cfg.Sources[0].Databases[0].Target; got != "src_db" {
		t.Fatalf("default target = %q, want src_db", got)
	}
}

func TestLoadRejectsMissingEnv(t *testing.T) {
	path := writeConfig(t, `
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
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want missing environment variable error")
	}
	if !strings.Contains(err.Error(), "MISSING_DSYNC_PASSWORD") {
		t.Fatalf("Load() error = %v, want missing env name", err)
	}
}

func TestLoadExpandsEnvAsStringScalar(t *testing.T) {
	t.Setenv("DSYNC_PASSWORD", "abc: def # secret")
	cfg := loadConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: ${DSYNC_PASSWORD}
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`)

	if cfg.Target.Password != "abc: def # secret" {
		t.Fatalf("Target.Password = %q, want env value preserved exactly", cfg.Target.Password)
	}
}

func TestLoadParsesExplicitRetryBackoff(t *testing.T) {
	cfg := loadConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  backoff: 1ms
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
        target: dst_db
`)

	if cfg.Retry.BackoffText != "1ms" {
		t.Fatalf("Retry.BackoffText = %q, want 1ms", cfg.Retry.BackoffText)
	}
	if cfg.Retry.Backoff != time.Millisecond {
		t.Fatalf("Retry.Backoff = %s, want 1ms", cfg.Retry.Backoff)
	}
}

func TestLoadRejectsValidationFailures(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "missing mo_dump_path",
			yaml: `
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "mo_dump_path",
		},
		{
			name: "unknown top-level key",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
unknown_key: true
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "unknown_key",
		},
		{
			name: "unknown nested key",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
        exclude_table: t_skip
`,
			wantErr: "exclude_table",
		},
		{
			name: "missing target field",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "target.user",
		},
		{
			name: "missing mysql_path",
			yaml: `
mo_dump_path: /tmp/mo-dump
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "mysql_path",
		},
		{
			name: "null output dir",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
output_dir: null
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "output_dir",
		},
		{
			name: "empty output dir",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
output_dir: ""
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "output_dir",
		},
		{
			name: "no sources",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
`,
			wantErr: "sources",
		},
		{
			name: "negative parallelism",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
parallelism: -1
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "parallelism",
		},
		{
			name: "zero parallelism",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
parallelism: 0
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "parallelism",
		},
		{
			name: "null parallelism",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
parallelism: null
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "parallelism",
		},
		{
			name: "null retry",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry: null
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "retry",
		},
		{
			name: "negative retry max attempts",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  max_attempts: -1
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "retry.max_attempts",
		},
		{
			name: "zero retry max attempts",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  max_attempts: 0
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "retry.max_attempts",
		},
		{
			name: "zero retry backoff",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  backoff: 0s
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "retry.backoff",
		},
		{
			name: "empty retry backoff",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  backoff: ""
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "retry.backoff",
		},
		{
			name: "null retry backoff empty scalar",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  backoff:
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "retry.backoff",
		},
		{
			name: "null retry backoff",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  backoff: null
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "retry.backoff",
		},
		{
			name: "negative retry backoff",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
retry:
  backoff: -1s
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "retry.backoff",
		},
		{
			name: "negative target port",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: -1
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "target.port",
		},
		{
			name: "zero target port",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 0
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "target.port",
		},
		{
			name: "target port above max",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 70000
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "target.port",
		},
		{
			name: "negative source port",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: -1
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "sources[0].port",
		},
		{
			name: "source port above max",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 70000
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "sources[0].port",
		},
		{
			name: "zero source port",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 0
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "sources[0].port",
		},
		{
			name: "missing source endpoint field",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    password: "111"
    databases:
      - name: src_db
`,
			wantErr: "sources[0].user",
		},
		{
			name: "source with no databases",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
`,
			wantErr: "databases",
		},
		{
			name: "database with empty name",
			yaml: `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - target: dst_db
`,
			wantErr: "database.name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.yaml))
			if err == nil {
				t.Fatal("Load() error = nil, want validation error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Load() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadRejectsOutputDirEnvExpansionToEmpty(t *testing.T) {
	t.Setenv("EMPTY_DSYNC_OUTPUT_DIR", "")
	path := writeConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
output_dir: ${EMPTY_DSYNC_OUTPUT_DIR}
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil, want output_dir validation error")
	}
	if !strings.Contains(err.Error(), "output_dir") {
		t.Fatalf("Load() error = %v, want output_dir validation error", err)
	}
}

func TestLoadRejectsDuplicateTopLevelKey(t *testing.T) {
	path := writeConfig(t, `
mo_dump_path: /tmp/mo-dump
mysql_path: /tmp/mysql
output_dir: ./runs-a
output_dir: ./runs-b
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
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
target:
  name: target
  host: 127.0.0.1
  port: 6001
  user: target:admin
  password: "111"
sources:
  - name: tenant_a
    host: 127.0.0.1
    port: 6001
    user: tenant:admin
    password: "111"
    databases:
      - name: src_db
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
