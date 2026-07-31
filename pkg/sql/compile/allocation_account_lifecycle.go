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

package compile

import (
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type executionAllocationAccountOwner interface {
	AllocationAccountEnabled() bool
	SetAllocationAccount(*mpool.AllocationAccount) error
	ClearAllocationAccount(*mpool.AllocationAccount) error
}

// statementAllocationAttempt owns one local execution generation. The
// MessageBoard pointer is captured at open so prepared/retry Reset cannot make
// terminal cleanup drain a newer board.
type statementAllocationAttempt struct {
	registry *mpool.AllocationAccountRegistry
	account  *mpool.AllocationAccount
	board    *message.MessageBoard
	exporter func(mpool.AllocationAccountTerminalSnapshot)
	owners   []executionAllocationAccountOwner

	once     sync.Once
	snapshot mpool.AllocationAccountTerminalSnapshot
	err      error
}

func (c *Compile) beginAllocationAccountAttempt() (
	*statementAllocationAttempt,
	error,
) {
	if c == nil || c.allocationAccountRegistry == nil {
		return nil, nil
	}
	if c.proc == nil || c.MessageBoard == nil || c.allocationAttempt != nil ||
		c.allocationTerminalExporter == nil {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	var controller mpool.AllocationCapacityController
	var err error
	if c.allocationControllerProvider != nil {
		controller, err = c.allocationControllerProvider()
		if err != nil {
			return nil, err
		}
		if controller == nil {
			return nil, mpool.ErrAllocationAccountInvariant
		}
	}
	account, err := c.allocationAccountRegistry.OpenWithController(
		c.allocationAccountLimit,
		controller,
	)
	if err != nil {
		return nil, err
	}
	owners, err := configureAllocationAccountOwners(c.scopes, account)
	if err != nil {
		snapshot, first, finalizeErr := c.allocationAccountRegistry.
			CompleteTerminalWithError(account, err)
		if first {
			c.allocationTerminalExporter(snapshot)
		}
		if finalizeErr != nil {
			return nil, finalizeErr
		}
		return nil, err
	}
	attempt := &statementAllocationAttempt{
		registry: c.allocationAccountRegistry,
		account:  account,
		board:    c.MessageBoard,
		exporter: c.allocationTerminalExporter,
		owners:   owners,
	}
	c.allocationAttempt = attempt
	return attempt, nil
}

func configureAllocationAccountOwners(
	scopes []*Scope,
	account *mpool.AllocationAccount,
) ([]executionAllocationAccountOwner, error) {
	var configured []executionAllocationAccountOwner
	isConfigured := func(candidate executionAllocationAccountOwner) bool {
		for _, owner := range configured {
			if owner == candidate {
				return true
			}
		}
		return false
	}
	rollback := func(cause error) error {
		for i := len(configured) - 1; i >= 0; i-- {
			cause = errors.Join(
				cause,
				configured[i].ClearAllocationAccount(account),
			)
		}
		return cause
	}
	var configure func(*Scope) error
	configure = func(scope *Scope) error {
		if scope == nil {
			return nil
		}
		if err := vm.HandleAllOp(
			scope.RootOp,
			func(_ vm.Operator, op vm.Operator) error {
				if owner, ok := op.(executionAllocationAccountOwner); ok &&
					owner.AllocationAccountEnabled() {
					if isConfigured(owner) {
						return nil
					}
					if err := owner.SetAllocationAccount(account); err != nil {
						return err
					}
					configured = append(configured, owner)
				}
				return nil
			},
		); err != nil {
			return err
		}
		for _, preScope := range scope.PreScopes {
			if err := configure(preScope); err != nil {
				return err
			}
		}
		return nil
	}
	for _, scope := range scopes {
		if err := configure(scope); err != nil {
			return nil, rollback(err)
		}
	}
	return configured, nil
}

func hasAllocationAccountOwner(scopes []*Scope) bool {
	var inspect func(*Scope) bool
	inspect = func(scope *Scope) bool {
		if scope == nil {
			return false
		}
		found := false
		_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if owner, ok := op.(executionAllocationAccountOwner); ok &&
				owner.AllocationAccountEnabled() {
				found = true
			}
			return nil
		})
		if found {
			return true
		}
		for _, preScope := range scope.PreScopes {
			if inspect(preScope) {
				return true
			}
		}
		return false
	}
	for _, scope := range scopes {
		if inspect(scope) {
			return true
		}
	}
	return false
}

// ensureAllocationAccountLifecycle activates accounting only when the physical
// plan contains a complete migrated owner. Legacy plans never open a registry
// slot or initialize the HashBuild budget.
func (c *Compile) ensureAllocationAccountLifecycle(
	exporter func(mpool.AllocationAccountTerminalSnapshot),
) error {
	if c == nil {
		return nil
	}
	if !hasAllocationAccountOwner(c.scopes) {
		if c.allocationLifecycleAutomatic {
			c.allocationAccountRegistry = nil
			c.allocationAccountLimit = 0
			c.allocationControllerProvider = nil
			c.allocationTerminalExporter = nil
			c.allocationLifecycleAutomatic = false
		}
		return nil
	}
	if c.allocationAccountRegistry != nil && !c.allocationLifecycleAutomatic {
		return nil
	}
	if exporter == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if c.proc == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	budget, err := c.proc.GetHashBuildBudget()
	if err != nil {
		return err
	}
	registry, err := budget.AllocationAccountRegistry()
	if err != nil {
		return err
	}
	limit := budget.Snapshot().Cap
	if limit == 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	c.ConfigureAllocationAccountLifecycleWithController(
		registry,
		limit,
		func() (mpool.AllocationCapacityController, error) {
			if budget.Closed() {
				return nil, process.ErrHashBuildBudgetClosed
			}
			return budget, nil
		},
		exporter,
	)
	c.allocationLifecycleAutomatic = true
	return nil
}

func (a *statementAllocationAttempt) finish() (
	mpool.AllocationAccountTerminalSnapshot,
	error,
) {
	if a == nil {
		return mpool.AllocationAccountTerminalSnapshot{}, nil
	}
	a.once.Do(func() {
		// Scope.Run/MergeRun and remote notifier barriers must have returned
		// before this point. Draining the board first releases queued JoinMap
		// and spill payload ownership through their normal Destroy methods.
		a.board.CloseAndDrain()
		for i := len(a.owners) - 1; i >= 0; i-- {
			a.err = errors.Join(
				a.err,
				a.owners[i].ClearAllocationAccount(a.account),
			)
		}
		a.owners = nil
		var first bool
		var terminalErr error
		a.snapshot, first, terminalErr = a.registry.CompleteTerminalWithError(
			a.account,
			a.err,
		)
		a.err = terminalErr
		if first && a.exporter != nil {
			a.exporter(a.snapshot)
		}
	})
	return a.snapshot, a.err
}

func (c *Compile) finishAllocationAccountAttempt() error {
	if c == nil || c.allocationAttempt == nil {
		return nil
	}
	attempt := c.allocationAttempt
	c.allocationAttempt = nil
	_, err := attempt.finish()
	return err
}

func (c *Compile) copyAllocationAccountLifecycleTo(dst *Compile) {
	if c == nil || dst == nil {
		return
	}
	dst.ConfigureAllocationAccountLifecycle(
		c.allocationAccountRegistry,
		c.allocationAccountLimit,
		c.allocationTerminalExporter,
	)
	dst.allocationControllerProvider = c.allocationControllerProvider
	dst.allocationLifecycleAutomatic = c.allocationLifecycleAutomatic
}
