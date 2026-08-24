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

// Package sidecar provides the typed, backend-aware differential-test contract
// used by MatrixOne and Sirius integration tests.
package sidecar

import "context"

// Mode selects the execution path used by a Runner.
type Mode uint8

const (
	ModeUnknown Mode = iota
	ModeNative
	ModeOffloaded
)

func (m Mode) String() string {
	switch m {
	case ModeNative:
		return "native"
	case ModeOffloaded:
		return "offloaded"
	default:
		return "unknown"
	}
}

// ComparisonMode controls whether row order is significant.
type ComparisonMode uint8

const (
	ComparisonUnknown ComparisonMode = iota
	ComparisonOrdered
	ComparisonUnordered
)

func (m ComparisonMode) String() string {
	switch m {
	case ComparisonOrdered:
		return "ordered"
	case ComparisonUnordered:
		return "unordered"
	default:
		return "unknown"
	}
}

// Backend is the engine that actually started executing a request.
type Backend uint8

const (
	BackendUnknown Backend = iota
	BackendMatrixOneNative
	BackendSiriusGPU
	BackendDuckDBCPU
)

func (b Backend) String() string {
	switch b {
	case BackendMatrixOneNative:
		return "matrixone_native"
	case BackendSiriusGPU:
		return "sirius_gpu"
	case BackendDuckDBCPU:
		return "duckdb_cpu"
	default:
		return "unknown"
	}
}

// Outcome is the terminal state of an execution attempt.
type Outcome uint8

const (
	OutcomePending Outcome = iota
	OutcomeSucceeded
	OutcomeFailed
	OutcomeCancelled
)

func (o Outcome) String() string {
	switch o {
	case OutcomeSucceeded:
		return "succeeded"
	case OutcomeFailed:
		return "failed"
	case OutcomeCancelled:
		return "cancelled"
	default:
		return "pending"
	}
}

// ExecutionEvidence records what actually executed, rather than what was only
// planned. Fallback is true when execution moved to a different backend.
type ExecutionEvidence struct {
	Backend  Backend
	Outcome  Outcome
	Fallback bool
}

// Expectation is matched exactly against one observation's execution evidence.
type Expectation ExecutionEvidence

// CellKind keeps NULL, text, and binary values distinct.
type CellKind uint8

const (
	CellInvalid CellKind = iota
	CellNull
	CellText
	CellBinary
)

// Cell contains the driver-visible representation of one value. Text remains
// unparsed so decimal, timestamp, and floating-point spellings stay observable.
type Cell struct {
	Kind CellKind
	Data []byte
}

func NullCell() Cell {
	return Cell{Kind: CellNull}
}

func TextCell(value string) Cell {
	return Cell{Kind: CellText, Data: []byte(value)}
}

func BinaryCell(value []byte) Cell {
	return Cell{Kind: CellBinary, Data: append([]byte(nil), value...)}
}

// Row is one result row.
type Row []Cell

// Column is the schema metadata visible through the database driver.
type Column struct {
	Name             string
	DatabaseType     string
	Nullable         bool
	NullableKnown    bool
	Length           int64
	LengthKnown      bool
	Precision        int64
	Scale            int64
	DecimalSizeKnown bool
}

// SQLError separates stable error identity from diagnostic message text.
type SQLError struct {
	Code     uint16
	SQLState string
	Class    string
	Message  string
}

// Observation is the complete externally visible result of one execution.
// Schema may be known before execution starts. Rows retain anything delivered
// after a backend starts and before a terminal failure or cancellation; Compare
// checks that partial result before the error identity.
type Observation struct {
	Schema   []Column
	Rows     []Row
	Error    *SQLError
	Evidence ExecutionEvidence
}

// Case defines one native-versus-offloaded comparison. SyntheticPlan is the
// only plan body eligible for failure-artifact output; production plan payloads
// intentionally have no representation here.
type Case struct {
	ID                   string
	SQL                  string
	Comparison           ComparisonMode
	NativeExpectation    Expectation
	OffloadedExpectation Expectation
	Seed                 uint64
	SyntheticPlan        []byte
	CapabilitySetHash    string
	ReadDigest           string
	ArtifactRedactValues []string
}

// Runner executes one case through the selected path.
type Runner interface {
	Run(context.Context, Case, Mode) (Observation, error)
}

// Report retains both observations for assertions and failure artifacts.
type Report struct {
	Case      Case
	Native    Observation
	Offloaded Observation
}
