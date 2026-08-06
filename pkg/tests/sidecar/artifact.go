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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const artifactMetadataName = "failure.json"

var (
	artifactURLPattern           = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s"'<>]+`)
	artifactAuthorizationPattern = regexp.MustCompile(`(?i)\bauthorization\b(\s*[:=]\s*)[^\r\n]+`)
	artifactSecretPattern        = regexp.MustCompile(`(?i)\b((?:[a-z0-9]+_)*(?:username|user|password|passwd|pwd|pgpassword|token|secret|read_ref|api_key|private_key|access_key|secret_key|key_id|session_token))\b(["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;]+)`)
)

type artifactObservation struct {
	Schema     []Column          `json:"schema,omitempty"`
	RowCount   int               `json:"row_count"`
	RowsSHA256 string            `json:"rows_sha256"`
	Error      *artifactSQLError `json:"error,omitempty"`
	Evidence   artifactEvidence  `json:"evidence"`
}

type artifactSQLError struct {
	Code     uint16 `json:"code,omitempty"`
	SQLState string `json:"sql_state,omitempty"`
	Class    string `json:"class,omitempty"`
	Message  string `json:"message,omitempty"`
}

type artifactEvidence struct {
	Backend  string `json:"backend"`
	Outcome  string `json:"outcome"`
	Fallback bool   `json:"fallback"`
}

type artifactMetadata struct {
	CaseID               string              `json:"case_id"`
	SQL                  string              `json:"sql"`
	Comparison           string              `json:"comparison"`
	Seed                 uint64              `json:"seed"`
	CapabilitySetHash    string              `json:"capability_set_hash,omitempty"`
	ReadDigest           string              `json:"read_digest,omitempty"`
	SyntheticPlanSHA256  string              `json:"synthetic_plan_sha256,omitempty"`
	Failure              string              `json:"failure"`
	NativeExpectation    artifactEvidence    `json:"native_expectation"`
	OffloadedExpectation artifactEvidence    `json:"offloaded_expectation"`
	Native               artifactObservation `json:"native"`
	Offloaded            artifactObservation `json:"offloaded"`
}

// WriteFailureArtifact writes deterministic, data-minimized diagnostics for a
// failed comparison. Raw rows are fingerprinted, never emitted. A raw plan is
// emitted only when the case explicitly supplied SyntheticPlan.
func WriteFailureArtifact(root string, report Report, failure error) (string, error) {
	if root == "" {
		return "", moerr.NewInvalidInputNoCtx("sidecar failure artifact root is empty")
	}
	if failure == nil {
		return "", moerr.NewInvalidInputNoCtx("sidecar failure artifact requires a failure")
	}
	if err := validateCase(report.Case); err != nil {
		return "", err
	}

	caseDir := filepath.Join(root, artifactCaseDirectory(report.Case.ID))
	if err := os.MkdirAll(caseDir, 0o700); err != nil {
		return "", errors.Join(moerr.NewInternalErrorNoCtx("create sidecar failure artifact directory"), err)
	}
	if err := os.Chmod(caseDir, 0o700); err != nil {
		return "", errors.Join(moerr.NewInternalErrorNoCtx("secure sidecar failure artifact directory"), err)
	}

	redact := func(value string) string {
		return redactArtifactText(value, report.Case.ArtifactRedactValues)
	}
	native, err := makeArtifactObservation(report.Native, report.Case.Comparison, redact)
	if err != nil {
		return "", errors.Join(moerr.NewInvalidInputNoCtx("encode native failure artifact"), err)
	}
	offloaded, err := makeArtifactObservation(report.Offloaded, report.Case.Comparison, redact)
	if err != nil {
		return "", errors.Join(moerr.NewInvalidInputNoCtx("encode offloaded failure artifact"), err)
	}

	metadata := artifactMetadata{
		CaseID:               redact(report.Case.ID),
		SQL:                  redact(report.Case.SQL),
		Comparison:           report.Case.Comparison.String(),
		Seed:                 report.Case.Seed,
		CapabilitySetHash:    redact(report.Case.CapabilitySetHash),
		ReadDigest:           redact(report.Case.ReadDigest),
		Failure:              redact(failure.Error()),
		NativeExpectation:    makeArtifactEvidence(ExecutionEvidence(report.Case.NativeExpectation)),
		OffloadedExpectation: makeArtifactEvidence(ExecutionEvidence(report.Case.OffloadedExpectation)),
		Native:               native,
		Offloaded:            offloaded,
	}
	if len(report.Case.SyntheticPlan) != 0 {
		metadata.SyntheticPlanSHA256 = sha256Hex(report.Case.SyntheticPlan)
		planPath := filepath.Join(caseDir, "plan.substrait.bin")
		if err := writePrivateFile(planPath, report.Case.SyntheticPlan); err != nil {
			return "", errors.Join(moerr.NewInternalErrorNoCtx("write synthetic sidecar plan artifact"), err)
		}
	} else {
		planPath := filepath.Join(caseDir, "plan.substrait.bin")
		if err := os.Remove(planPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", errors.Join(moerr.NewInternalErrorNoCtx("remove stale synthetic sidecar plan artifact"), err)
		}
	}

	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
		return "", errors.Join(moerr.NewInternalErrorNoCtx("marshal sidecar failure artifact"), err)
	}
	metadataPath := filepath.Join(caseDir, artifactMetadataName)
	if err := writePrivateFile(metadataPath, data.Bytes()); err != nil {
		return "", errors.Join(moerr.NewInternalErrorNoCtx("write sidecar failure artifact"), err)
	}
	return metadataPath, nil
}

// writePrivateFile replaces path atomically with a newly created 0600 file.
// Creating a fresh inode is important: os.WriteFile's mode is ignored when the
// destination already exists, so rewriting a retained artifact would otherwise
// preserve accidentally widened permissions.
func writePrivateFile(path string, data []byte) (err error) {
	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		if temporary != nil {
			_ = temporary.Close()
		}
		if err != nil {
			_ = os.Remove(temporaryPath)
		}
	}()

	if err = temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err = temporary.Write(data); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	temporary = nil
	if err = os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return nil
}

func makeArtifactObservation(observation Observation, mode ComparisonMode, redact func(string) string) (artifactObservation, error) {
	rowsFingerprint, err := fingerprintRows(mode, observation.Rows)
	if err != nil {
		return artifactObservation{}, err
	}
	schema := make([]Column, len(observation.Schema))
	copy(schema, observation.Schema)
	for i := range schema {
		schema[i].Name = redact(schema[i].Name)
		schema[i].DatabaseType = redact(schema[i].DatabaseType)
	}
	result := artifactObservation{
		Schema:     schema,
		RowCount:   len(observation.Rows),
		RowsSHA256: rowsFingerprint,
		Evidence:   makeArtifactEvidence(observation.Evidence),
	}
	if observation.Error != nil {
		result.Error = &artifactSQLError{
			Code:     observation.Error.Code,
			SQLState: redact(observation.Error.SQLState),
			Class:    redact(observation.Error.Class),
			Message:  redact(observation.Error.Message),
		}
	}
	return result, nil
}

func fingerprintRows(mode ComparisonMode, rows []Row) (string, error) {
	hash := sha256.New()
	_, _ = hash.Write([]byte{byte(mode)})
	var rowCount [8]byte
	binary.BigEndian.PutUint64(rowCount[:], uint64(len(rows)))
	_, _ = hash.Write(rowCount[:])

	if mode == ComparisonOrdered {
		for i, row := range rows {
			encoded, err := encodeRow(row)
			if err != nil {
				return "", errors.Join(moerr.NewInvalidInputNoCtxf("row %d", i), err)
			}
			_, _ = hash.Write(encoded)
		}
		return hex.EncodeToString(hash.Sum(nil)), nil
	}

	// Combine fixed-size row digests instead of sorting copied row bodies. Sum
	// preserves multiplicity, XOR strengthens the diagnostic fingerprint, and
	// memory remains constant even when a failed test returned many rows.
	var sum, xor [sha256.Size]byte
	for i, row := range rows {
		encoded, err := encodeRow(row)
		if err != nil {
			return "", errors.Join(moerr.NewInvalidInputNoCtxf("row %d", i), err)
		}
		rowDigest := sha256.Sum256(encoded)
		carry := uint16(0)
		for j := sha256.Size - 1; j >= 0; j-- {
			value := uint16(sum[j]) + uint16(rowDigest[j]) + carry
			sum[j] = byte(value)
			carry = value >> 8
			xor[j] ^= rowDigest[j]
		}
	}
	_, _ = hash.Write(sum[:])
	_, _ = hash.Write(xor[:])
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func makeArtifactEvidence(evidence ExecutionEvidence) artifactEvidence {
	return artifactEvidence{
		Backend:  evidence.Backend.String(),
		Outcome:  evidence.Outcome.String(),
		Fallback: evidence.Fallback,
	}
}

func artifactCaseDirectory(caseID string) string {
	digest := sha256.Sum256([]byte(caseID))
	return "case-" + hex.EncodeToString(digest[:])
}

func redactArtifactText(value string, explicit []string) string {
	redacted := value
	values := append([]string(nil), explicit...)
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	for _, secret := range values {
		if secret != "" {
			redacted = strings.ReplaceAll(redacted, secret, "<redacted>")
		}
	}
	redacted = artifactURLPattern.ReplaceAllString(redacted, "<redacted-url>")
	redacted = artifactAuthorizationPattern.ReplaceAllString(redacted, "authorization$1<redacted>")
	redacted = artifactSecretPattern.ReplaceAllString(redacted, "$1$2<redacted>")
	return redacted
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
