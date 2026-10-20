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

package process

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/system"
)

var testingServiceMemoryLedgers sync.Map

func serviceMemoryAdmissionBudget(service string) *system.MemoryAdmissionBudget {
	if value, ok := testingServiceMemoryLedgers.Load(service); ok {
		return value.(*system.MemoryAdmissionBudget)
	}
	return system.DefaultMemoryAdmissionBudget()
}

// SetServiceMemoryAdmissionBudgetForTesting installs a deterministic physical
// owner for an embedded SQL test, before starting its uniquely named CN. It is
// not a production configuration: all other services retain the process-wide
// real-domain ledger. The caller must stop the CN before invoking cleanup.
func SetServiceMemoryAdmissionBudgetForTesting(service string, ledger *system.MemoryAdmissionBudget) (func(), error) {
	if service == "" || ledger == nil {
		return nil, ErrExecutionResourceInvalid
	}
	if _, loaded := executionResourceCNBudgets.Load(service); loaded {
		return nil, ErrExecutionResourceInvalid
	}
	if _, loaded := testingServiceMemoryLedgers.LoadOrStore(service, ledger); loaded {
		return nil, ErrExecutionResourceInvalid
	}
	return func() {
		testingServiceMemoryLedgers.Delete(service)
		executionResourceCNBudgets.Delete(service)
	}, nil
}
