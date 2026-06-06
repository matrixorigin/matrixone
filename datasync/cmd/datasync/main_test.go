package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestExitCodePrintsVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := exitCode([]string{"-version"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exitCode() = %d, want 0", code)
	}
	if strings.TrimSpace(stdout.String()) != "dev" {
		t.Fatalf("stdout = %q, want dev", stdout.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestExitCodeRejectsMissingConfig(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := exitCode(nil, &stdout, &stderr)

	if code != 2 {
		t.Fatalf("exitCode() = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "missing required -config") {
		t.Fatalf("stderr = %q, want missing config error", stderr.String())
	}
}

func TestExitCodePrintsDetailedHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := exitCode([]string{"-help"}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("exitCode() = %d, want 0", code)
	}
	help := stderr.String()
	for _, want := range []string{
		"Usage: datasync -config <config.yaml> [options]",
		"Configuration:",
		"top-level source and target",
		"connection defaults",
		"Incomplete database entries are ignored",
		"include_tables",
		"exclude_tables",
		"Modes:",
		"-mode sync",
		"-mode export",
		"-mode import",
		"-cleanup-export-after-import",
		"false by default",
		"report.md",
		"Chinese summary",
		"Examples:",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("help missing %q:\n%s", want, help)
		}
	}
	if stdout.String() != "" {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestExitCodeRejectsInvalidMode(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := exitCode([]string{"-config", "configs/example.yaml", "-mode", "copy"}, &stdout, &stderr)

	if code != 2 {
		t.Fatalf("exitCode() = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), `invalid mode "copy"`) {
		t.Fatalf("stderr = %q, want invalid mode error", stderr.String())
	}
}

func TestExitCodeReturnsConfigLoadError(t *testing.T) {
	var stdout, stderr bytes.Buffer

	code := exitCode([]string{"-config", "/does/not/exist.yaml"}, &stdout, &stderr)

	if code != 1 {
		t.Fatalf("exitCode() = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "load config") {
		t.Fatalf("stderr = %q, want load config error", stderr.String())
	}
}
