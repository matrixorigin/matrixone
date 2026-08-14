// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package optools

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const goUTAnalysisModule = "github.com/matrixorigin/go-ut-analysis@v0.0.0-20250711025253-f31acb12d3b1"

func writeMockGo(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "go")
	script := `#!/bin/bash
count=0
if [[ -f "${MOCK_GO_COUNTER}" ]]; then count=$(<"${MOCK_GO_COUNTER}"); fi
count=$((count + 1))
echo "${count}" > "${MOCK_GO_COUNTER}"
echo "$*" >> "${MOCK_GO_ARGS}"
if (( count >= MOCK_GO_SUCCEED_AFTER )); then exit 0; fi
exit "${MOCK_GO_FAILURE_STATUS}"
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func runInstall(t *testing.T, attempts, succeedAfter, failureStatus int) ([]byte, int, string, string) {
	t.Helper()

	toolsPath, err := filepath.Abs("ut_tools.bash")
	if err != nil {
		t.Fatal(err)
	}
	counter := filepath.Join(t.TempDir(), "attempts")
	arguments := filepath.Join(t.TempDir(), "arguments")
	mockGoDir := writeMockGo(t)
	cmd := exec.Command("bash", "-c", `source "$1"; install_go_ut_analysis "$2" 0`,
		"bash", toolsPath, strconv.Itoa(attempts))
	cmd.Env = append(os.Environ(),
		"PATH="+mockGoDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"MOCK_GO_COUNTER="+counter,
		"MOCK_GO_ARGS="+arguments,
		"MOCK_GO_SUCCEED_AFTER="+strconv.Itoa(succeedAfter),
		"MOCK_GO_FAILURE_STATUS="+strconv.Itoa(failureStatus),
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return output, 0, counter, arguments
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		return output, exitError.ExitCode(), counter, arguments
	}
	t.Fatalf("install go-ut-analysis: %v", err)
	return nil, 0, "", ""
}

func assertAttempts(t *testing.T, counter, arguments string, expected int) {
	t.Helper()

	attempts, err := os.ReadFile(counter)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(attempts)) != strconv.Itoa(expected) {
		t.Fatalf("expected %d attempts, got %q", expected, attempts)
	}
	invocations, err := os.ReadFile(arguments)
	if err != nil {
		t.Fatal(err)
	}
	for lineNumber, invocation := range strings.Split(strings.TrimSpace(string(invocations)), "\n") {
		if invocation != "install "+goUTAnalysisModule {
			t.Fatalf("attempt %d used unexpected arguments %q", lineNumber+1, invocation)
		}
	}
}

func TestInstallGoUTAnalysisRetriesTransientFailures(t *testing.T) {
	output, status, counter, arguments := runInstall(t, 3, 3, 42)
	if status != 0 {
		t.Fatalf("install failed with status %d: %s", status, output)
	}
	assertAttempts(t, counter, arguments, 3)
}

func TestInstallGoUTAnalysisDoesNotRetrySuccess(t *testing.T) {
	output, status, counter, arguments := runInstall(t, 3, 1, 42)
	if status != 0 {
		t.Fatalf("install failed with status %d: %s", status, output)
	}
	assertAttempts(t, counter, arguments, 1)
}

func TestInstallGoUTAnalysisPreservesFinalFailure(t *testing.T) {
	output, status, counter, arguments := runInstall(t, 3, 4, 42)
	if status != 42 {
		t.Fatalf("expected status 42, got %d: %s", status, output)
	}
	assertAttempts(t, counter, arguments, 3)
}

func writeScopeFixture(t *testing.T, root, name, contents string) {
	t.Helper()

	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestListEmbeddedClusterTestPackagesFollowsTransitiveTestDeps(t *testing.T) {
	toolsPath, err := filepath.Abs("ut_tools.bash")
	if err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	fixtures := map[string]string{
		"go.mod": `module example.com/cluster-scope

go 1.24
`,
		"pkg/embed/embed.go": `package embed

const Enabled = true
`,
		"pkg/embed/embed_test.go": `package embed

import "testing"

func TestEmbed(t *testing.T) {}
`,
		"pkg/helper/helper.go": `package helper

import "example.com/cluster-scope/pkg/embed"

var Enabled = embed.Enabled
`,
		"pkg/owner/owner.go": `package owner
`,
		"pkg/owner/owner_test.go": `package owner

import (
	_ "example.com/cluster-scope/pkg/helper"
	"testing"
)

func TestOwner(t *testing.T) {}
`,
		"pkg/light/light.go": `package light
`,
		"pkg/light/light_test.go": `package light

import "testing"

func TestLight(t *testing.T) {}
`,
	}
	for name, contents := range fixtures {
		writeScopeFixture(t, root, name, contents)
	}

	cmd := exec.Command("bash", "-c",
		`source "$1"; list_embedded_cluster_test_packages ./...`,
		"bash", toolsPath)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"GOWORK=off",
		"GOFLAGS=-mod=readonly",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("discover embedded cluster packages: %v\n%s", err, output)
	}

	expected := strings.Join([]string{
		"example.com/cluster-scope/pkg/embed",
		"example.com/cluster-scope/pkg/owner",
	}, "\n")
	if actual := strings.TrimSpace(string(output)); actual != expected {
		t.Fatalf("unexpected embedded cluster packages:\n%s\nexpected:\n%s", actual, expected)
	}

	cmd = exec.Command("bash", "-c",
		`source "$1"; list_embedded_cluster_test_packages ./pkg/owner ./pkg/light`,
		"bash", toolsPath)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"GOWORK=off",
		"GOFLAGS=-mod=readonly",
	)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("discover partial embedded cluster scope: %v\n%s", err, output)
	}
	if actual := strings.TrimSpace(string(output)); actual != "example.com/cluster-scope/pkg/owner" {
		t.Fatalf("partial scope included an unrequested package: %s", actual)
	}
}

func TestListEmbeddedClusterTestPackagesPreservesGoListFailure(t *testing.T) {
	toolsPath, err := filepath.Abs("ut_tools.bash")
	if err != nil {
		t.Fatal(err)
	}
	counter := filepath.Join(t.TempDir(), "attempts")
	arguments := filepath.Join(t.TempDir(), "arguments")
	mockGoDir := writeMockGo(t)
	cmd := exec.Command("bash", "-c",
		`source "$1"; list_embedded_cluster_test_packages ./...`,
		"bash", toolsPath)
	cmd.Env = append(os.Environ(),
		"PATH="+mockGoDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"MOCK_GO_COUNTER="+counter,
		"MOCK_GO_ARGS="+arguments,
		"MOCK_GO_SUCCEED_AFTER=2",
		"MOCK_GO_FAILURE_STATUS=42",
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected go list failure, got success: %s", output)
	}
	exitError, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("unexpected command error: %v", err)
	}
	if exitError.ExitCode() != 42 {
		t.Fatalf("expected status 42, got %d: %s", exitError.ExitCode(), output)
	}
	attempts, err := os.ReadFile(counter)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(attempts)) != "1" {
		t.Fatalf("expected one go list attempt, got %q", attempts)
	}
}
