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
	"bytes"
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
	report.Case.SQL = "SELECT * FROM mysql://visible-user:visible-pass@host/db, s3://bucket/private/path WHERE token=visible-token AND value='literal-secret'; AWS_ACCESS_KEY_ID=aws-access AWS_SECRET_ACCESS_KEY='aws-secret' AWS_SESSION_TOKEN=aws-session DB_PASSWORD=db-password API_TOKEN=api-token CLIENT_SECRET=client-secret"
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
	report.Offloaded.Error = &SQLError{Code: 1, SQLState: "literal-secret", Class: "literal-secret", Message: `password=visible-password SERVICE_PASSWORD=json-service authorization: Bearer visible-bearer {"AWS_ACCESS_KEY_ID":"json-access","AWS_SECRET_ACCESS_KEY":"json-secret","AWS_SESSION_TOKEN":"json-session"}`}

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
		"visible-bearer", "private.example", "aws-access", "aws-secret", "aws-session",
		"json-access", "json-secret", "json-session", "db-password", "api-token",
		"client-secret", "json-service",
	} {
		if strings.Contains(text, forbidden) {
			t.Errorf("artifact contains sensitive value %q: %s", forbidden, text)
		}
	}
	for _, required := range []string{
		`"seed": 42`, `"rows_sha256"`, `"synthetic_plan_file"`, `"synthetic_plan_sha256"`,
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

	planPath := filepath.Join(filepath.Dir(path), metadata.SyntheticPlanFile)
	plan, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(plan) != string(report.Case.SyntheticPlan) {
		t.Fatalf("synthetic plan = %v, want %v", plan, report.Case.SyntheticPlan)
	}
	if metadata.SyntheticPlanFile != artifactPlanName(metadata.SyntheticPlanSHA256) ||
		metadata.SyntheticPlanSHA256 != sha256Hex(plan) {
		t.Fatalf("synthetic plan reference does not match content: %+v", metadata)
	}
	assertMode(t, path, 0o600)
	assertMode(t, planPath, 0o600)
	assertMode(t, filepath.Dir(path), 0o700)
}

func TestWriteFailureArtifactUsesOpaqueCaseDirectory(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	report.Case.ID = "case-secret123"
	report.Case.ArtifactRedactValues = []string{"secret123"}
	root := t.TempDir()
	path, err := WriteFailureArtifact(root, report, errors.New("failure"))
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Base(filepath.Dir(path))
	if strings.Contains(directory, report.Case.ID) || strings.Contains(directory, "secret123") {
		t.Fatalf("artifact directory exposes case ID: %q", directory)
	}
	if directory != artifactCaseDirectory(report.Case.ID) || len(directory) != len("case-")+64 {
		t.Fatalf("artifact directory = %q, want deterministic SHA-256 name", directory)
	}
	other := artifactCaseDirectory("case-other")
	if directory == other {
		t.Fatalf("different case IDs share artifact directory %q", directory)
	}
}

func TestWriteFailureArtifactRedactsCredentialNames(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	report.Case.SQL = "SELECT 1 /* OPENAI_API_KEY=alpha-4271; PGPASSWORD=bravo-5832; TLS_PRIVATE_KEY=charlie-6943 */"
	path, err := WriteFailureArtifact(t.TempDir(), report, errors.New("forced failure"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, credential := range []string{"alpha-4271", "bravo-5832", "charlie-6943"} {
		if strings.Contains(text, credential) {
			t.Errorf("artifact contains credential value %q: %s", credential, text)
		}
	}
	for _, name := range []string{"OPENAI_API_KEY", "PGPASSWORD", "TLS_PRIVATE_KEY"} {
		if !strings.Contains(text, name+"=<redacted>") {
			t.Errorf("artifact did not redact %s assignment: %s", name, text)
		}
	}
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

	left := fingerprintRows(ComparisonUnordered, []Row{{TextCell("a")}, {TextCell("a")}, {TextCell("b")}})
	right := fingerprintRows(ComparisonUnordered, []Row{{TextCell("a")}, {TextCell("b")}, {TextCell("b")}})
	if left == right {
		t.Fatalf("unordered fingerprints match for different duplicate counts: %s", left)
	}
}

func TestWriteFailureArtifactPersistsMalformedObservations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cell Cell
	}{
		{name: "invalid kind", cell: Cell{Kind: CellInvalid, Data: []byte("adapter-bug")}},
		{name: "null with data", cell: Cell{Kind: CellNull, Data: []byte("not-null")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			report := successfulReport()
			report.Offloaded.Rows[0][0] = test.cell
			failure := Compare(report)
			if failure == nil {
				t.Fatal("Compare() accepted malformed observation")
			}

			path, err := WriteFailureArtifact(t.TempDir(), report, failure)
			if err != nil {
				t.Fatalf("WriteFailureArtifact() error = %v", err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), string(test.cell.Data)) {
				t.Fatalf("artifact exposed malformed cell data: %s", data)
			}
			var metadata artifactMetadata
			if err := json.Unmarshal(data, &metadata); err != nil {
				t.Fatal(err)
			}
			if metadata.Offloaded.RowCount != 1 || metadata.Offloaded.RowsSHA256 == "" {
				t.Fatalf("malformed observation was not fingerprinted: %+v", metadata.Offloaded)
			}
		})
	}
}

func TestWriteFailureArtifactOmitsPlanReferenceForPlanlessGeneration(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	report.Case.SyntheticPlan = []byte("synthetic")
	root := t.TempDir()
	path, err := WriteFailureArtifact(root, report, errors.New("first"))
	if err != nil {
		t.Fatal(err)
	}
	first := readArtifactMetadata(t, path)
	planPath := filepath.Join(filepath.Dir(path), first.SyntheticPlanFile)
	if _, err := os.Stat(planPath); err != nil {
		t.Fatal(err)
	}

	report.Case.SyntheticPlan = nil
	path, err = WriteFailureArtifact(root, report, errors.New("second"))
	if err != nil {
		t.Fatal(err)
	}
	second := readArtifactMetadata(t, path)
	if second.SyntheticPlanFile != "" || second.SyntheticPlanSHA256 != "" {
		t.Fatalf("planless generation references a synthetic plan: %+v", second)
	}
	// Prior plans are immutable. Removing one here could break a concurrent
	// writer whose metadata publication has not happened yet.
	if _, err := os.Stat(planPath); err != nil {
		t.Fatalf("previous immutable plan was removed: %v", err)
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
	planPath := filepath.Join(caseDir, artifactPlanName(sha256Hex(report.Case.SyntheticPlan)))
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

func TestWriteFailureArtifactPublishesConsistentConcurrentGeneration(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	slow := successfulReport()
	slow.Case.ID = "same-case"
	slow.Case.Seed = 1
	slow.Case.SyntheticPlan = []byte("slow plan")
	fast := successfulReport()
	fast.Case.ID = slow.Case.ID
	fast.Case.Seed = 2
	fast.Case.SyntheticPlan = []byte("fast plan")

	slowAtPublication := make(chan struct{})
	releaseSlow := make(chan struct{})
	slowResult := make(chan error, 1)
	go func() {
		_, err := writeFailureArtifact(root, slow, errors.New("slow failure"), func(path string, data []byte) error {
			close(slowAtPublication)
			<-releaseSlow
			return writePrivateFile(path, data)
		})
		slowResult <- err
	}()

	select {
	case <-slowAtPublication:
	case err := <-slowResult:
		t.Fatalf("slow writer returned before metadata publication: %v", err)
	}
	if _, err := WriteFailureArtifact(root, fast, errors.New("fast failure")); err != nil {
		close(releaseSlow)
		t.Fatalf("fast WriteFailureArtifact() error = %v", err)
	}
	close(releaseSlow)
	if err := <-slowResult; err != nil {
		t.Fatalf("slow WriteFailureArtifact() error = %v", err)
	}

	metadataPath := filepath.Join(root, artifactCaseDirectory(slow.Case.ID), artifactMetadataName)
	metadata := readArtifactMetadata(t, metadataPath)
	if metadata.Seed != slow.Case.Seed {
		t.Fatalf("published seed = %d, want slow writer seed %d", metadata.Seed, slow.Case.Seed)
	}
	assertPublishedPlan(t, metadataPath, metadata, slow.Case.SyntheticPlan)
}

func TestWriteFailureArtifactPreservesPublishedGenerationWhenReplacementStops(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		plan []byte
	}{
		{name: "different plan", plan: []byte("replacement plan")},
		{name: "no plan"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			published := successfulReport()
			published.Case.ID = "replacement-case"
			published.Case.Seed = 1
			published.Case.SyntheticPlan = []byte("published plan")
			metadataPath, err := WriteFailureArtifact(root, published, errors.New("published failure"))
			if err != nil {
				t.Fatal(err)
			}

			replacement := successfulReport()
			replacement.Case.ID = published.Case.ID
			replacement.Case.Seed = 2
			replacement.Case.SyntheticPlan = test.plan
			publicationStopped := errors.New("metadata publication stopped")
			_, err = writeFailureArtifact(root, replacement, errors.New("replacement failure"), func(string, []byte) error {
				return publicationStopped
			})
			if !errors.Is(err, publicationStopped) {
				t.Fatalf("writeFailureArtifact() error = %v, want %v", err, publicationStopped)
			}

			metadata := readArtifactMetadata(t, metadataPath)
			if metadata.Seed != published.Case.Seed {
				t.Fatalf("published seed = %d, want original seed %d", metadata.Seed, published.Case.Seed)
			}
			assertPublishedPlan(t, metadataPath, metadata, published.Case.SyntheticPlan)
		})
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

func readArtifactMetadata(t *testing.T, path string) artifactMetadata {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var metadata artifactMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func assertPublishedPlan(t *testing.T, metadataPath string, metadata artifactMetadata, want []byte) {
	t.Helper()
	if metadata.SyntheticPlanFile != artifactPlanName(metadata.SyntheticPlanSHA256) {
		t.Fatalf("synthetic plan name/hash mismatch: %+v", metadata)
	}
	plan, err := os.ReadFile(filepath.Join(filepath.Dir(metadataPath), metadata.SyntheticPlanFile))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plan, want) {
		t.Fatalf("synthetic plan = %q, want %q", plan, want)
	}
	if metadata.SyntheticPlanSHA256 != sha256Hex(plan) {
		t.Fatalf("synthetic plan hash = %q, want %q", metadata.SyntheticPlanSHA256, sha256Hex(plan))
	}
}
