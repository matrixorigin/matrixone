// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package optools

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestUTAdmissionHolderEvidence(t *testing.T) {
	const start = `{"Time":"2026-09-16T00:00:00Z","Action":"start","Package":"example/dml"}`
	const acquire = `{"Time":"2026-09-16T00:00:01Z","Action":"output","Package":"example/dml","Test":"TestFirst","Output":"MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=admission-acquire duration=1s status=ready wait=1s"}`
	const release = `{"Time":"2026-09-16T00:00:04Z","Action":"output","Package":"example/dml","Output":"MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=admission-release duration=1ms status=ready admission_released=true"}`
	const terminal = `{"Time":"2026-09-16T00:00:09Z","Action":"pass","Package":"example/dml"}`
	for _, tt := range []struct {
		name   string
		events []string
		want   string
		absent string
	}{
		{"explicit", []string{start, acquire, release, terminal}, "explicit=1 process-exit-inferred=0 unresolved=0", "evidence=process-exit-inferred"},
		{"process exit", []string{start, acquire, terminal}, "duration=8.00s evidence=process-exit-inferred package=example/dml", "evidence=explicit"},
		{"cancelled capture", []string{start, acquire, "{truncated"}, "explicit=0 process-exit-inferred=0 unresolved=1", "admission holder duration="},
		{"missing invocation start", []string{acquire, terminal}, "explicit=0 process-exit-inferred=0 unresolved=1", "admission holder duration="},
		{"startless capture after terminated invocation", []string{start, terminal, strings.ReplaceAll(acquire, "00:00:01Z", "00:00:10Z"), strings.ReplaceAll(terminal, "00:00:09Z", "00:00:20Z")}, "explicit=0 process-exit-inferred=0 unresolved=1", "admission holder duration="},
		{"new invocation after terminated invocation", []string{start, terminal, strings.ReplaceAll(start, "00:00:00Z", "00:00:10Z"), strings.ReplaceAll(acquire, "00:00:01Z", "00:00:11Z"), strings.ReplaceAll(terminal, "00:00:09Z", "00:00:20Z")}, "duration=9.00s evidence=process-exit-inferred", "unresolved=1"},
		{"overlapping package invocations", []string{start, acquire, start, terminal}, "explicit=0 process-exit-inferred=0 unresolved=1", "admission holder duration="},
		{"multiple process ids", []string{start, acquire, strings.ReplaceAll(acquire, "pid=10", "pid=11"), terminal}, "explicit=0 process-exit-inferred=0 unresolved=2", "admission holder duration="},
		{"restart generation", []string{start, acquire, release, strings.ReplaceAll(acquire, "00:00:01Z", "00:00:05Z"), terminal}, "explicit=1 process-exit-inferred=1 unresolved=0", "unresolved=1"},
		{"subtest terminal is not process exit", []string{start, acquire, strings.ReplaceAll(terminal, `"Action":"pass"`, `"Action":"pass","Test":"TestFirst"`)}, "explicit=0 process-exit-inferred=0 unresolved=1", "admission holder duration="},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "report.json")
			if err := os.WriteFile(path, []byte(strings.Join(tt.events, "\n")), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("python3", "summarize_ut_setup.py", path)
			cmd.Env = pythonStdlibEnvironment()
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("summary: %v\n%s", err, out)
			}
			text := string(out)
			if !strings.Contains(text, tt.want) || strings.Contains(text, tt.absent) {
				t.Fatalf("want %q, exclude %q:\n%s", tt.want, tt.absent, text)
			}
			if !strings.Contains(text, "wait totals overlap holder work") {
				t.Fatal("missing overlap warning")
			}
		})
	}
}

func TestPythonStdlibEnvironmentUsesConfiguredRuntimeLibraryPath(t *testing.T) {
	t.Setenv("pythonLocation", "/python/toolcache")
	t.Setenv("LD_LIBRARY_PATH", "/matrixone/cgo")
	t.Setenv("PYTHONPATH", "/matrixone/python")
	t.Setenv("PYTHONHOME", "/matrixone/python-home")
	t.Setenv("PYTHONUSERBASE", "/matrixone/user-site")
	t.Setenv("PYTHONSTARTUP", "/matrixone/startup.py")
	t.Setenv("PYTHONINSPECT", "1")
	t.Setenv("MO_UT_ENV_SENTINEL", "preserved")

	env := pythonStdlibEnvironment()
	values := make(map[string]string, len(env))
	for _, entry := range env {
		key, value, _ := strings.Cut(entry, "=")
		values[key] = value
	}
	if values["PATH"] == "" {
		t.Fatal("sanitized Python environment lost PATH")
	}
	if got, want := values["MO_UT_ENV_SENTINEL"], "preserved"; got != want {
		t.Fatalf("sanitized Python environment changed unrelated variable: got %q, want %q", got, want)
	}
	if got, want := values["LD_LIBRARY_PATH"], filepath.Join("/python/toolcache", "lib"); got != want {
		t.Fatalf("Python runtime loader path = %q, want %q", got, want)
	}
	for _, forbidden := range []string{
		"PYTHONPATH", "PYTHONHOME", "PYTHONUSERBASE", "PYTHONSTARTUP", "PYTHONINSPECT",
	} {
		if _, ok := values[forbidden]; ok {
			t.Fatalf("Python stdlib helper inherited %q", forbidden)
		}
	}
}

func TestPythonStdlibEnvironmentDropsNativeLoaderPathWithoutPythonLocation(t *testing.T) {
	t.Setenv("pythonLocation", "")
	t.Setenv("LD_LIBRARY_PATH", "/matrixone/cgo")
	if got := pythonStdlibEnvironment(); strings.Contains(strings.Join(got, "\n"), "LD_LIBRARY_PATH=") {
		t.Fatalf("Python stdlib helper inherited MatrixOne loader path: %q", got)
	}
}

// pythonStdlibEnvironment keeps the configured interpreter's matching runtime
// library available while preventing MatrixOne's native libraries and Python
// import overrides from changing a standard-library-only report parser.
func pythonStdlibEnvironment() []string {
	env := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "LD_LIBRARY_PATH", "PYTHONHOME", "PYTHONPATH", "PYTHONUSERBASE", "PYTHONSTARTUP", "PYTHONINSPECT":
			continue
		default:
			env = append(env, entry)
		}
	}
	if pythonLocation := os.Getenv("pythonLocation"); pythonLocation != "" {
		env = append(env, "LD_LIBRARY_PATH="+filepath.Join(pythonLocation, "lib"))
	}
	return env
}
