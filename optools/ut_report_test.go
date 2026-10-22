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
	"strings"
	"testing"
)

func runUTReportValidator(t *testing.T, report string, requireComplete bool) (string, int) {
	t.Helper()
	script, err := filepath.Abs("validate_ut_report.py")
	if err != nil {
		t.Fatal(err)
	}
	reportPath := filepath.Join(t.TempDir(), "ut-report.json")
	if err := os.WriteFile(reportPath, []byte(report), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{script}
	if requireComplete {
		args = append(args, "--require-complete")
	}
	args = append(args, reportPath)
	output, err := exec.Command("python3", args...).CombinedOutput()
	if err == nil {
		return string(output), 0
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		return string(output), exitError.ExitCode()
	}
	t.Fatalf("run UT report validator: %v", err)
	return "", 0
}

func TestValidateUTReportAcceptsPauseContAndRepeatedPackageLifecycles(t *testing.T) {
	report := strings.Join([]string{
		`{"Action":"start","Package":"example.test"}`,
		`{"Action":"run","Package":"example.test","Test":"TestFirst"}`,
		`{"Action":"pause","Package":"example.test","Test":"TestFirst"}`,
		`{"Action":"cont","Package":"example.test","Test":"TestFirst"}`,
		`{"Action":"pass","Package":"example.test","Test":"TestFirst"}`,
		`{"Action":"pass","Package":"example.test"}`,
		// Plan shards may execute different roots from the same package in
		// separate processes. Repeating package lifecycle records is valid.
		`{"Action":"start","Package":"example.test"}`,
		`{"Action":"run","Package":"example.test","Test":"TestSecond"}`,
		`{"Action":"skip","Package":"example.test","Test":"TestSecond"}`,
		`{"Action":"pass","Package":"example.test"}`,
	}, "\n") + "\n"
	output, status := runUTReportValidator(t, report, true)
	if status != 0 {
		t.Fatalf("valid lifecycle rejected with status %d: %s", status, output)
	}
	if !strings.Contains(output, "test_starts=2") || !strings.Contains(output, "incomplete=0") {
		t.Fatalf("lifecycle summary missing: %s", output)
	}
}

func TestValidateUTReportRejectsDuplicateTestRootsAcrossShards(t *testing.T) {
	report := strings.Join([]string{
		`{"Action":"run","Package":"example.test","Test":"TestDuplicate"}`,
		`{"Action":"pass","Package":"example.test","Test":"TestDuplicate"}`,
		`{"Action":"run","Package":"example.test","Test":"TestDuplicate"}`,
		`{"Action":"pass","Package":"example.test","Test":"TestDuplicate"}`,
	}, "\n") + "\n"
	output, status := runUTReportValidator(t, report, true)
	if status == 0 {
		t.Fatalf("duplicate test root was accepted: %s", output)
	}
	if !strings.Contains(output, "duplicate test root example.test:TestDuplicate") {
		t.Fatalf("duplicate diagnostic missing: %s", output)
	}
}

func TestValidateUTReportRejectsTransitionsWithoutAnActiveRoot(t *testing.T) {
	report := strings.Join([]string{
		`{"Action":"cont","Package":"example.test","Test":"TestMissing"}`,
	}, "\n") + "\n"
	output, status := runUTReportValidator(t, report, false)
	if status == 0 {
		t.Fatalf("invalid lifecycle was accepted: %s", output)
	}
	if !strings.Contains(output, "without an active test root") {
		t.Fatalf("missing invalid lifecycle diagnostic: %s", output)
	}
}

func TestValidateUTReportReportsTerminalOnlySubtest(t *testing.T) {
	report := strings.Join([]string{
		`{"Action":"run","Package":"example.test","Test":"TestParent"}`,
		`{"Action":"pass","Package":"example.test","Test":"TestParent/short"}`,
		`{"Action":"pass","Package":"example.test","Test":"TestParent"}`,
	}, "\n") + "\n"
	output, status := runUTReportValidator(t, report, true)
	if status != 0 {
		t.Fatalf("terminal-only short subtest rejected: %s", output)
	}
	if !strings.Contains(output, "terminal-only test roots=1") {
		t.Fatalf("terminal-only diagnostic missing: %s", output)
	}
}

func TestValidateUTReportAllowsTruncatedFailureDiagnostics(t *testing.T) {
	report := strings.Join([]string{
		`{"Action":"output","Package":"example.test","Test":"TestActive","Output":"{\"Action\":\"run\"}"}`,
		`{"Action":"run","Package":"example.test","Test":"TestActive"}`,
		"truncated",
	}, "\n") + "\n"
	output, status := runUTReportValidator(t, report, false)
	if status != 0 {
		t.Fatalf("truncated failure report should remain diagnostic: %s", output)
	}
	if !strings.Contains(output, "malformed=1") || !strings.Contains(output, "incomplete=1") {
		t.Fatalf("truncation summary missing: %s", output)
	}

	output, status = runUTReportValidator(t, report, true)
	if status == 0 {
		t.Fatalf("successful run must reject incomplete report: %s", output)
	}
	if !strings.Contains(output, "report contains 1 malformed JSON line") ||
		!strings.Contains(output, "report ended with 1 incomplete test root") {
		t.Fatalf("strict truncation diagnostics missing: %s", output)
	}
}

func TestValidateUTReportRequiresTestsOnSuccess(t *testing.T) {
	for _, tc := range []struct{ name, report string }{
		{"empty", ""},
		{"unrelated_json", "{}\n"},
		{"package_only", `{"Action":"start","Package":"example.test"}` + "\n" + `{"Action":"pass","Package":"example.test"}` + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, status := runUTReportValidator(t, tc.report, true)
			if status == 0 || !strings.Contains(output, "no test lifecycle events") {
				t.Fatalf("empty selection accepted: status=%d %s", status, output)
			}
			output, status = runUTReportValidator(t, tc.report, false)
			if status != 0 {
				t.Fatalf("empty failure diagnostic rejected: status=%d %s", status, output)
			}
		})
	}
	report := `{"Action":"pass","Package":"example.test","Test":"TestParent/short"}` + "\n"
	output, status := runUTReportValidator(t, report, true)
	if status != 0 {
		t.Fatalf("terminal-only evidence rejected: status=%d %s", status, output)
	}
}

func TestValidateUTReportRequiresPackageCompletion(t *testing.T) {
	report := strings.Join([]string{
		`{"Action":"start","Package":"example.test"}`,
		`{"Action":"run","Package":"example.test","Test":"TestOK"}`,
		`{"Action":"pass","Package":"example.test","Test":"TestOK"}`,
	}, "\n") + "\n"
	output, status := runUTReportValidator(t, report, true)
	if status == 0 || !strings.Contains(output, "incomplete package lifecycle") {
		t.Fatalf("truncated package accepted: status=%d %s", status, output)
	}
	output, status = runUTReportValidator(t, report, false)
	if status != 0 || !strings.Contains(output, "incomplete package lifecycle") {
		t.Fatalf("missing truncated-package diagnostic: status=%d %s", status, output)
	}
	report += `{"Action":"pass","Package":"example.test"}` + "\n"
	output, status = runUTReportValidator(t, report, true)
	if status != 0 {
		t.Fatalf("completed package rejected: status=%d %s", status, output)
	}
}
