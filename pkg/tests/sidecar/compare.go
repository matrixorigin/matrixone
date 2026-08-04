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
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// MismatchError identifies the first semantic difference between two paths.
type MismatchError struct {
	Field  string
	Detail string
}

func (e *MismatchError) Error() string {
	return fmt.Sprintf("sidecar differential mismatch in %s: %s", e.Field, e.Detail)
}

// Run executes native and offloaded paths and compares their observations.
func Run(ctx context.Context, runner Runner, testCase Case) (Report, error) {
	report := Report{Case: testCase}
	if runner == nil {
		return report, moerr.NewInvalidInputNoCtx("sidecar differential runner is nil")
	}
	if err := validateCase(testCase); err != nil {
		return report, err
	}

	var err error
	report.Native, err = runner.Run(ctx, testCase, ModeNative)
	if err != nil {
		return report, errors.Join(
			moerr.NewInternalErrorNoCtxf("run sidecar case %q in %s mode", testCase.ID, ModeNative), err)
	}
	report.Offloaded, err = runner.Run(ctx, testCase, ModeOffloaded)
	if err != nil {
		return report, errors.Join(
			moerr.NewInternalErrorNoCtxf("run sidecar case %q in %s mode", testCase.ID, ModeOffloaded), err)
	}

	return report, Compare(report)
}

// Compare verifies execution evidence before comparing SQL-visible results.
func Compare(report Report) error {
	if err := validateCase(report.Case); err != nil {
		return err
	}
	if err := validateObservation(ModeNative, report.Native, report.Case.NativeExpectation); err != nil {
		return err
	}
	if err := validateObservation(ModeOffloaded, report.Offloaded, report.Case.OffloadedExpectation); err != nil {
		return err
	}

	if report.Native.Evidence.Outcome != report.Offloaded.Evidence.Outcome {
		return mismatch("outcome", "%s path is %s, %s path is %s",
			ModeNative, report.Native.Evidence.Outcome, ModeOffloaded, report.Offloaded.Evidence.Outcome)
	}

	if report.Native.Evidence.Outcome != OutcomeSucceeded {
		return compareErrors(report.Native.Error, report.Offloaded.Error)
	}
	if !columnsEqual(report.Native.Schema, report.Offloaded.Schema) {
		return mismatch("schema", "native=%v offloaded=%v", report.Native.Schema, report.Offloaded.Schema)
	}

	return compareRows(report.Case.Comparison, report.Native.Rows, report.Offloaded.Rows)
}

func validateCase(testCase Case) error {
	if testCase.ID == "" {
		return moerr.NewInvalidInputNoCtx("sidecar differential case ID is empty")
	}
	if testCase.SQL == "" {
		return moerr.NewInvalidInputNoCtxf("sidecar differential case %q has empty SQL", testCase.ID)
	}
	if testCase.Comparison != ComparisonOrdered && testCase.Comparison != ComparisonUnordered {
		return moerr.NewInvalidInputNoCtxf(
			"sidecar differential case %q has invalid comparison mode %d", testCase.ID, testCase.Comparison)
	}
	if err := validateExpectation(ModeNative, testCase.NativeExpectation); err != nil {
		return errors.Join(moerr.NewInvalidInputNoCtxf("sidecar differential case %q", testCase.ID), err)
	}
	if err := validateExpectation(ModeOffloaded, testCase.OffloadedExpectation); err != nil {
		return errors.Join(moerr.NewInvalidInputNoCtxf("sidecar differential case %q", testCase.ID), err)
	}
	return nil
}

func validateExpectation(mode Mode, expectation Expectation) error {
	if !validBackend(expectation.Backend) {
		return moerr.NewInvalidInputNoCtxf("%s expectation has invalid backend %d", mode, expectation.Backend)
	}
	if !validOutcome(expectation.Outcome) {
		return moerr.NewInvalidInputNoCtxf("%s expectation has invalid outcome %d", mode, expectation.Outcome)
	}
	return nil
}

func validateObservation(mode Mode, observation Observation, expectation Expectation) error {
	actual := Expectation(observation.Evidence)
	if actual != expectation {
		return mismatch(mode.String()+" execution evidence", "expected=%s/%s/fallback=%t actual=%s/%s/fallback=%t",
			expectation.Backend, expectation.Outcome, expectation.Fallback,
			actual.Backend, actual.Outcome, actual.Fallback)
	}

	if observation.Evidence.Outcome == OutcomeSucceeded && observation.Error != nil {
		return mismatch(mode.String()+" outcome", "successful execution returned SQL error %s", stableErrorIdentity(observation.Error))
	}
	if observation.Evidence.Outcome != OutcomeSucceeded && observation.Error == nil {
		return mismatch(mode.String()+" outcome", "%s execution has no SQL error", observation.Evidence.Outcome)
	}
	if observation.Error != nil && !hasStableErrorIdentity(observation.Error) {
		return mismatch(mode.String()+" error", "error has no stable code, SQLSTATE, or class")
	}

	for rowIndex, row := range observation.Rows {
		if len(row) != len(observation.Schema) {
			return mismatch(mode.String()+" row width", "row %d has %d cells for %d columns",
				rowIndex, len(row), len(observation.Schema))
		}
		for columnIndex, cell := range row {
			if err := validateCell(columnIndex, cell); err != nil {
				return mismatch(mode.String()+" row", "row %d: %v", rowIndex, err)
			}
		}
	}
	return nil
}

func compareErrors(native, offloaded *SQLError) error {
	if native == nil || offloaded == nil {
		return mismatch("error", "native=%s offloaded=%s", stableErrorIdentity(native), stableErrorIdentity(offloaded))
	}
	if native.Code != offloaded.Code || native.SQLState != offloaded.SQLState || native.Class != offloaded.Class {
		return mismatch("error identity", "native=%s offloaded=%s", stableErrorIdentity(native), stableErrorIdentity(offloaded))
	}
	return nil
}

func compareRows(mode ComparisonMode, native, offloaded []Row) error {
	if len(native) != len(offloaded) {
		return mismatch("row count", "native=%d offloaded=%d", len(native), len(offloaded))
	}

	if mode == ComparisonOrdered {
		for i := range native {
			nativeRow, err := encodeRow(native[i])
			if err != nil {
				return mismatch("native rows", "row %d: %v", i, err)
			}
			offloadedRow, err := encodeRow(offloaded[i])
			if err != nil {
				return mismatch("offloaded rows", "row %d: %v", i, err)
			}
			if !bytes.Equal(nativeRow, offloadedRow) {
				return mismatch("row data", "row %d differs", i)
			}
		}
		return nil
	}

	counts := make(map[string]int, len(native))
	for i, row := range native {
		encoded, err := encodeRow(row)
		if err != nil {
			return mismatch("native rows", "row %d: %v", i, err)
		}
		counts[string(encoded)]++
	}
	for i, row := range offloaded {
		encoded, err := encodeRow(row)
		if err != nil {
			return mismatch("offloaded rows", "row %d: %v", i, err)
		}
		key := string(encoded)
		count := counts[key]
		if count == 0 {
			return mismatch("row data", "offloaded row %d has no native multiset match", i)
		}
		if count == 1 {
			delete(counts, key)
		} else {
			counts[key] = count - 1
		}
	}
	return nil
}

func encodeRow(row Row) ([]byte, error) {
	encoded := make([]byte, 8, 8+len(row)*9)
	binary.BigEndian.PutUint64(encoded, uint64(len(row)))
	var length [8]byte
	for columnIndex, cell := range row {
		if err := validateCell(columnIndex, cell); err != nil {
			return nil, err
		}
		encoded = append(encoded, byte(cell.Kind))
		binary.BigEndian.PutUint64(length[:], uint64(len(cell.Data)))
		encoded = append(encoded, length[:]...)
		encoded = append(encoded, cell.Data...)
	}
	return encoded, nil
}

func validateCell(columnIndex int, cell Cell) error {
	if cell.Kind < CellNull || cell.Kind > CellBinary {
		return moerr.NewInvalidInputNoCtxf("column %d has invalid cell kind %d", columnIndex, cell.Kind)
	}
	if cell.Kind == CellNull && len(cell.Data) != 0 {
		return moerr.NewInvalidInputNoCtxf("column %d has NULL cell with data", columnIndex)
	}
	return nil
}

func columnsEqual(left, right []Column) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		leftColumn, rightColumn := left[i], right[i]
		if leftColumn.Name != rightColumn.Name || leftColumn.DatabaseType != rightColumn.DatabaseType ||
			leftColumn.NullableKnown != rightColumn.NullableKnown ||
			leftColumn.LengthKnown != rightColumn.LengthKnown ||
			leftColumn.DecimalSizeKnown != rightColumn.DecimalSizeKnown {
			return false
		}
		if leftColumn.NullableKnown && leftColumn.Nullable != rightColumn.Nullable {
			return false
		}
		if leftColumn.LengthKnown && leftColumn.Length != rightColumn.Length {
			return false
		}
		if leftColumn.DecimalSizeKnown &&
			(leftColumn.Precision != rightColumn.Precision || leftColumn.Scale != rightColumn.Scale) {
			return false
		}
	}
	return true
}

func validBackend(backend Backend) bool {
	return backend >= BackendMatrixOneNative && backend <= BackendDuckDBCPU
}

func validOutcome(outcome Outcome) bool {
	return outcome >= OutcomeSucceeded && outcome <= OutcomeCancelled
}

func hasStableErrorIdentity(sqlError *SQLError) bool {
	return sqlError.Code != 0 || sqlError.SQLState != "" || sqlError.Class != ""
}

func stableErrorIdentity(sqlError *SQLError) string {
	if sqlError == nil {
		return "<nil>"
	}
	return fmt.Sprintf("code=%d state=%q class=%q", sqlError.Code, sqlError.SQLState, sqlError.Class)
}

func mismatch(field, format string, args ...any) error {
	return &MismatchError{Field: field, Detail: fmt.Sprintf(format, args...)}
}
