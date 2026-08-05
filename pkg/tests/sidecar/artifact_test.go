// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sidecar

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteFailureArtifactRedactsAndMinimizesData(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	report.Case.ID = "artifact/case"
	report.Case.SQL = "SELECT * FROM mysql://visible-user:visible-pass@host/db, s3://bucket/private/path WHERE token=visible-token AND value='literal-secret'"
	report.Case.ArtifactRedactValues = []string{"literal-secret"}
	report.Case.Seed = 42
	report.Case.CapabilitySetHash = "capability-hash"
	report.Case.ReadDigest = "read-digest"
	report.Case.SyntheticPlan = []byte{0x01, 0x02, 0x03}
	report.Native.Rows = []Row{{TextCell("sensitive-row-value")}}
	report.Offloaded.Rows = []Row{{TextCell("different-sensitive-row-value")}}
	report.Native.Schema[0].Name = "literal-secret"
	report.Native.Schema[0].DatabaseType = "VARCHAR-literal-secret"
	report.Offloaded.Schema[0].Name = "literal-secret"
	report.Offloaded.Schema[0].DatabaseType = "VARCHAR-literal-secret"
	report.Offloaded.Error = &SQLError{Code: 1, SQLState: "literal-secret", Class: "literal-secret", Message: "password=visible-password authorization: Bearer visible-bearer"}

	path, err := WriteFailureArtifact(t.TempDir(), report, errors.New("request to https://private.example/path used literal-secret"))
	if err != nil {
		t.Fatalf("WriteFailureArtifact() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, forbidden := range []string{
		"s3://bucket", "visible-token", "literal-secret", "sensitive-row-value",
		"different-sensitive-row-value", "visible-password", "visible-user", "visible-pass",
		"visible-bearer", "private.example",
	} {
		if strings.Contains(text, forbidden) {
			t.Errorf("artifact contains sensitive value %q: %s", forbidden, text)
		}
	}
	for _, required := range []string{
		`"seed": 42`, `"rows_sha256"`, `"synthetic_plan_sha256"`,
		`"backend": "sirius_gpu"`, `<redacted>`, `<redacted-url>`,
	} {
		if !strings.Contains(text, required) {
			t.Errorf("artifact does not contain %q: %s", required, text)
		}
	}
	var metadata artifactMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}
	for _, schema := range [][]Column{metadata.Native.Schema, metadata.Offloaded.Schema} {
		if len(schema) != 1 || schema[0].Name != "<redacted>" || schema[0].DatabaseType != "VARCHAR-<redacted>" {
			t.Fatalf("artifact schema was not redacted: %+v", schema)
		}
	}
	if metadata.Offloaded.Error.SQLState != "<redacted>" || metadata.Offloaded.Error.Class != "<redacted>" {
		t.Fatalf("artifact error identity was not redacted: %+v", metadata.Offloaded.Error)
	}
	if report.Native.Schema[0].Name != "literal-secret" || report.Offloaded.Schema[0].DatabaseType != "VARCHAR-literal-secret" {
		t.Fatalf("artifact redaction mutated source observations: native=%+v offloaded=%+v",
			report.Native.Schema, report.Offloaded.Schema)
	}

	plan, err := os.ReadFile(filepath.Join(filepath.Dir(path), "plan.substrait.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(plan) != string(report.Case.SyntheticPlan) {
		t.Fatalf("synthetic plan = %v, want %v", plan, report.Case.SyntheticPlan)
	}
	assertMode(t, path, 0o600)
	assertMode(t, filepath.Dir(path), 0o700)
}

func TestWriteFailureArtifactIsDeterministic(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	report.Case.Comparison = ComparisonUnordered
	report.Native.Rows = []Row{{TextCell("a")}, {TextCell("b")}}
	report.Offloaded.Rows = []Row{{TextCell("b")}, {TextCell("a")}}
	failure := errors.New("forced failure")
	root := t.TempDir()

	path, err := WriteFailureArtifact(root, report, failure)
	if err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	path, err = WriteFailureArtifact(root, report, failure)
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("artifact changed across identical writes\nfirst: %s\nsecond: %s", first, second)
	}

	var metadata artifactMetadata
	if err := json.Unmarshal(second, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.Native.RowsSHA256 != metadata.Offloaded.RowsSHA256 {
		t.Fatalf("unordered row hashes differ: native=%s offloaded=%s",
			metadata.Native.RowsSHA256, metadata.Offloaded.RowsSHA256)
	}
}

func TestUnorderedArtifactFingerprintPreservesMultiplicity(t *testing.T) {
	t.Parallel()

	left, err := fingerprintRows(ComparisonUnordered, []Row{{TextCell("a")}, {TextCell("a")}, {TextCell("b")}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := fingerprintRows(ComparisonUnordered, []Row{{TextCell("a")}, {TextCell("b")}, {TextCell("b")}})
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatalf("unordered fingerprints match for different duplicate counts: %s", left)
	}
}

func TestWriteFailureArtifactRemovesStaleSyntheticPlan(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	report.Case.SyntheticPlan = []byte("synthetic")
	root := t.TempDir()
	path, err := WriteFailureArtifact(root, report, errors.New("first"))
	if err != nil {
		t.Fatal(err)
	}
	planPath := filepath.Join(filepath.Dir(path), "plan.substrait.bin")
	if _, err := os.Stat(planPath); err != nil {
		t.Fatal(err)
	}

	report.Case.SyntheticPlan = nil
	if _, err := WriteFailureArtifact(root, report, errors.New("second")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(planPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale synthetic plan stat error = %v, want not exist", err)
	}
}

func TestWriteFailureArtifactTightensExistingFilePermissions(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	report.Case.ID = "reused-artifact"
	report.Case.SyntheticPlan = []byte("new synthetic plan")
	root := t.TempDir()
	caseDir := filepath.Join(root, artifactCaseDirectory(report.Case.ID))
	if err := os.MkdirAll(caseDir, 0o700); err != nil {
		t.Fatal(err)
	}
	metadataPath := filepath.Join(caseDir, artifactMetadataName)
	planPath := filepath.Join(caseDir, "plan.substrait.bin")
	for _, path := range []string{metadataPath, planPath} {
		if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	path, err := WriteFailureArtifact(root, report, errors.New("replacement"))
	if err != nil {
		t.Fatalf("WriteFailureArtifact() error = %v", err)
	}
	if path != metadataPath {
		t.Fatalf("artifact path = %q, want %q", path, metadataPath)
	}
	assertMode(t, metadataPath, 0o600)
	assertMode(t, planPath, 0o600)
	plan, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(plan) != string(report.Case.SyntheticPlan) {
		t.Fatalf("synthetic plan = %q, want %q", plan, report.Case.SyntheticPlan)
	}
}

func TestWriteFailureArtifactValidatesInput(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	if _, err := WriteFailureArtifact("", report, errors.New("failure")); err == nil {
		t.Fatal("WriteFailureArtifact() accepted empty root")
	}
	if _, err := WriteFailureArtifact(t.TempDir(), report, nil); err == nil {
		t.Fatal("WriteFailureArtifact() accepted nil failure")
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s permissions = %o, want %o", path, got, want)
	}
}
