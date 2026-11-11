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

package api

import (
	"errors"
	"fmt"
	"math"
)

// MemoryPhase identifies a fixed accounting operation, never untrusted text.
type MemoryPhase uint8

const (
	MemoryReservation MemoryPhase = iota
	MemoryInitialAdmission
	MemoryEncoderOutput
	MemoryEncoderGrowth
)

func (p MemoryPhase) String() string {
	switch p {
	case MemoryInitialAdmission:
		return "initial-admission"
	case MemoryEncoderOutput:
		return "encoder-output"
	case MemoryEncoderGrowth:
		return "encoder-growth"
	default:
		return "reservation"
	}
}

// MemoryLimitError contains only accounting numbers and fixed identifiers.
type MemoryLimitError struct {
	Used, Requested, Limit int64
	Phase                  MemoryPhase
	DML                    bool
	Encoder                *MemoryLimitError
}

func NewDMLMemoryLimitError(used, requested, limit int64, phase MemoryPhase) *MemoryLimitError {
	return &MemoryLimitError{Used: used, Requested: requested, Limit: limit, Phase: phase, DML: true}
}
func NewEncoderMemoryLimitError(used, requested, limit int64, phase MemoryPhase) *MemoryLimitError {
	return &MemoryLimitError{Used: used, Requested: requested, Limit: limit, Phase: phase}
}
func (e *MemoryLimitError) Error() string {
	msg := fmt.Sprintf("%s: Iceberg memory limit exceeded [used_bytes=%d, requested_bytes=%d, limit_bytes=%d, phase=%s", ErrPlanningLimitExceeded, e.Used, e.Requested, e.Limit, e.Phase)
	if e.DML {
		msg += ", config=iceberg.write.dml_max_memory, toml=dml-max-memory"
	}
	if e.Encoder != nil {
		msg += fmt.Sprintf(", encoder_used_bytes=%d, encoder_requested_bytes=%d, encoder_limit_bytes=%d, encoder_phase=%s", e.Encoder.Used, e.Encoder.Requested, e.Encoder.Limit, e.Encoder.Phase)
	}
	return msg + "]"
}
func IsMemoryLimit(err error) bool { var e *MemoryLimitError; return errors.As(err, &e) && e != nil }

// DMLMemoryEncoderError relates local encoder accounting to the enclosing budget.
func DMLMemoryEncoderError(err error, used, scratch, limit int64) error {
	var encoder *MemoryLimitError
	if !errors.As(err, &encoder) || encoder == nil || encoder.DML {
		return err
	}
	e := NewDMLMemoryLimitError(memoryAdd(memoryAdd(used, scratch), encoder.Used), encoder.Requested, limit, encoder.Phase)
	e.Encoder = encoder
	return e
}

func memoryAdd(a, b int64) int64 {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}
