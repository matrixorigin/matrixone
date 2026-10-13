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

package substrait

import (
	"errors"
	"fmt"
)

// EligibilityReason identifies a normal, side-effect-free reason why Sirius
// cannot execute a query. Callers may fall back to MatrixOne execution only
// when IsNotEligible reports true; every other error is operational or signals
// malformed planner state and must be surfaced.
type EligibilityReason string

const (
	EligibilityPlanShape   EligibilityReason = "plan-shape"
	EligibilityOperator    EligibilityReason = "operator"
	EligibilityExpression  EligibilityReason = "expression"
	EligibilityType        EligibilityReason = "type"
	EligibilityTransaction EligibilityReason = "transaction"
	EligibilitySnapshot    EligibilityReason = "snapshot"
)

// EligibilityError is returned only before a durable lease is published.
type EligibilityError struct {
	reason EligibilityReason
	detail string
}

func (e *EligibilityError) Error() string {
	return fmt.Sprintf("substrait: not eligible (%s): %s", e.reason, e.detail)
}

func (e *EligibilityError) Reason() EligibilityReason { return e.reason }

// NotEligible constructs a normal Sirius eligibility decline.
func NotEligible(reason EligibilityReason, detail string) error {
	return &EligibilityError{reason: reason, detail: detail}
}

func notEligiblef(reason EligibilityReason, format string, args ...any) error {
	return NotEligible(reason, fmt.Sprintf(format, args...))
}

// IsNotEligible reports whether err is a normal Sirius eligibility decline.
func IsNotEligible(err error) bool {
	var target *EligibilityError
	return errors.As(err, &target)
}

// NotEligibleReason returns the stable reason for an eligibility decline.
func NotEligibleReason(err error) (EligibilityReason, bool) {
	var target *EligibilityError
	if !errors.As(err, &target) {
		return "", false
	}
	return target.reason, true
}
