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

// BorrowViewSchemaResources installs the parent's existing statement budget in
// a fresh isolated binding process. Only the parent owns closing this generation.
func (proc *Process) BorrowViewSchemaResources(parent *Process, generation *ExecutionResourceGeneration) error {
	if proc == nil || proc.Base == nil || parent == nil || parent.Base == nil || proc.Base == parent.Base || generation == nil || generation.Closed() {
		return &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Message: "invalid view schema resource owner"}
	}
	parent.Base.executionResourceBudgetMu.Lock()
	current := parent.Base.executionResourceBudget
	parent.Base.executionResourceBudgetMu.Unlock()
	if current != generation {
		return &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Message: "view schema execution generation changed"}
	}
	proc.Base.executionResourceBudgetMu.Lock()
	defer proc.Base.executionResourceBudgetMu.Unlock()
	if proc.Base.executionResourceBudget != nil {
		return &ExecutionResourceError{Kind: ExecutionResourceErrorInvalid, Message: "view schema child already has a budget"}
	}
	proc.Base.executionResourceBudget = generation
	proc.Base.executionResourceBudgetBorrowed = true
	proc.Base.Lim = parent.Base.Lim
	return nil
}

// UsesExecutionResourceGeneration checks without opening a replacement generation.
func (proc *Process) UsesExecutionResourceGeneration(generation *ExecutionResourceGeneration) bool {
	if proc == nil || proc.Base == nil {
		return false
	}
	proc.Base.executionResourceBudgetMu.Lock()
	defer proc.Base.executionResourceBudgetMu.Unlock()
	return proc.Base.executionResourceBudget == generation
}
