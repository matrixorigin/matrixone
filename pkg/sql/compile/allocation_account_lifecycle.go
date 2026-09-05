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
	"reflect"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func allocationLifecycleCall(call func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(
				err,
				mpool.ErrAllocationAccountInvariant,
				moerr.NewInternalErrorNoCtxf(
					"allocation lifecycle panic: %v", recovered,
				),
			)
		}
	}()
	return call()
}

// joinAllocationLifecycleErrors keeps a lone failure's concrete type intact
// and avoids rejoining the same terminal failure. errors.Join wraps even one
// non-nil error, which would turn a statement *moerr.Error into a generic Go
// error before it crosses the pipeline wire.
func joinAllocationLifecycleErrors(primary, secondary error) error {
	if primary == nil {
		return secondary
	}
	if secondary == nil {
		return primary
	}
	if reflect.TypeOf(primary).Comparable() && primary == secondary {
		return primary
	}
	return errors.Join(primary, secondary)
}

type executionAllocationAccountOwner interface {
	SetAllocationAccount(*mpool.AllocationAccount) error
	ClearAllocationAccount(*mpool.AllocationAccount) error
}

type executionAllocationAccountActivationPolicy interface {
	ActivatesAllocationAccountLifecycle() bool
}

func hasAllocationAccountActivator(
	owners []executionAllocationAccountOwner,
) bool {
	for _, owner := range owners {
		policy, ok := owner.(executionAllocationAccountActivationPolicy)
		if !ok || policy.ActivatesAllocationAccountLifecycle() {
			return true
		}
	}
	return false
}

// statementAllocationAttempt owns one local execution generation. The
// MessageBoard pointer is captured at open so prepared/retry Reset cannot make
// terminal cleanup drain a newer board.
type statementAllocationAttempt struct {
	registry *mpool.AllocationAccountRegistry
	account  *mpool.AllocationAccount
	board    *message.MessageBoard
	exporter func(mpool.AllocationAccountTerminalSnapshot)

	ownersMu sync.Mutex
	owners   []executionAllocationAccountOwner
	ownerSet map[executionAllocationAccountOwner]struct{}
	closing  bool

	prepareOnce  sync.Once
	completeOnce sync.Once
	snapshot     mpool.AllocationAccountTerminalSnapshot
	prepareErr   error
	completeErr  error
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
	owners := c.allocationAccountOwners
	var err error
	if owners == nil {
		owners, err = collectAllocationAccountOwners(c.scopes)
		if err != nil {
			return nil, err
		}
	}
	c.allocationAccountOwners = nil
	if c.allocationControllerProvider == nil {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	controller, err := c.allocationControllerProvider()
	if err != nil {
		return nil, err
	}
	if controller == nil {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	account, err := c.allocationAccountRegistry.OpenWithController(
		c.allocationAccountLimit,
		controller,
	)
	if err != nil {
		return nil, err
	}
	owners, err = configureAllocationAccountOwners(owners, account)
	if err != nil {
		var snapshot mpool.AllocationAccountTerminalSnapshot
		var first bool
		finalizeErr := allocationLifecycleCall(func() error {
			var terminalErr error
			snapshot, first, terminalErr = c.allocationAccountRegistry.
				CompleteTerminalWithError(account, err)
			return terminalErr
		})
		if first {
			finalizeErr = joinAllocationLifecycleErrors(
				finalizeErr,
				allocationLifecycleCall(func() error {
					c.allocationTerminalExporter(snapshot)
					return nil
				}),
			)
		}
		if finalizeErr != nil {
			return nil, joinAllocationLifecycleErrors(err, finalizeErr)
		}
		return nil, err
	}
	attempt := &statementAllocationAttempt{
		registry: c.allocationAccountRegistry,
		account:  account,
		board:    c.MessageBoard,
		exporter: c.allocationTerminalExporter,
		owners:   owners,
		ownerSet: make(map[executionAllocationAccountOwner]struct{}, len(owners)),
	}
	for _, owner := range owners {
		attempt.ownerSet[owner] = struct{}{}
	}
	c.allocationAttempt = attempt
	return attempt, nil
}

// attachRuntimeOwners binds operators cloned after runOnce starts to the same
// attempt. Parallel scan/load workers are execution-local and do not exist
// when the template scopes are collected.
func (a *statementAllocationAttempt) attachRuntimeOwners(scopes []*Scope) error {
	if a == nil || a.account == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	owners, err := collectAllocationAccountOwners(scopes)
	if err != nil || len(owners) == 0 {
		return err
	}

	a.ownersMu.Lock()
	defer a.ownersMu.Unlock()
	if a.closing {
		return mpool.ErrAllocationAccountInvariant
	}
	newOwners := make([]executionAllocationAccountOwner, 0, len(owners))
	for _, owner := range owners {
		if _, exists := a.ownerSet[owner]; !exists {
			newOwners = append(newOwners, owner)
		}
	}
	configured, err := configureAllocationAccountOwners(newOwners, a.account)
	if err != nil {
		return err
	}
	for _, owner := range configured {
		a.ownerSet[owner] = struct{}{}
	}
	a.owners = append(a.owners, configured...)
	return nil
}

func (c *Compile) attachRuntimeAllocationOwners(scopes []*Scope) error {
	if c == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if c.allocationAttempt == nil {
		owners, err := collectAllocationAccountOwners(scopes)
		if err != nil {
			return err
		}
		if !hasAllocationAccountActivator(owners) {
			return nil
		}
		return mpool.ErrAllocationAccountInvariant
	}
	return c.allocationAttempt.attachRuntimeOwners(scopes)
}

func configureAllocationAccountOwners(
	owners []executionAllocationAccountOwner,
	account *mpool.AllocationAccount,
) ([]executionAllocationAccountOwner, error) {
	configured := make([]executionAllocationAccountOwner, 0, len(owners))
	rollback := func(cause error) error {
		for i := len(configured) - 1; i >= 0; i-- {
			cause = joinAllocationLifecycleErrors(
				cause,
				allocationLifecycleCall(func() error {
					return configured[i].ClearAllocationAccount(account)
				}),
			)
		}
		return cause
	}
	for _, owner := range owners {
		if err := allocationLifecycleCall(func() error {
			return owner.SetAllocationAccount(account)
		}); err != nil {
			return nil, rollback(err)
		}
		configured = append(configured, owner)
	}
	return configured, nil
}

func collectAllocationAccountOwners(
	scopes []*Scope,
) ([]executionAllocationAccountOwner, error) {
	owners := make([]executionAllocationAccountOwner, 0)
	seen := make(map[executionAllocationAccountOwner]struct{})
	var inspect func(*Scope) error
	inspect = func(scope *Scope) error {
		if scope == nil {
			return nil
		}
		if err := vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if owner, ok := op.(executionAllocationAccountOwner); ok {
				if _, exists := seen[owner]; !exists {
					seen[owner] = struct{}{}
					owners = append(owners, owner)
				}
			}
			return nil
		}); err != nil {
			return err
		}
		for _, preScope := range scope.PreScopes {
			if err := inspect(preScope); err != nil {
				return err
			}
		}
		return nil
	}
	for _, scope := range scopes {
		if err := inspect(scope); err != nil {
			return nil, err
		}
	}
	return owners, nil
}

// ensureAllocationAccountLifecycle installs one account whenever the physical
// plan contains an activating allocation owner. Implementing the owner
// contract is the boundary: there is no per-owner activation switch.
func (c *Compile) ensureAllocationAccountLifecycle(
	exporter func(mpool.AllocationAccountTerminalSnapshot),
) error {
	if c == nil {
		return nil
	}
	owners, err := collectAllocationAccountOwners(c.scopes)
	if err != nil {
		return err
	}
	c.allocationAccountOwners = owners
	if !hasAllocationAccountActivator(owners) && len(c.materializedSources) == 0 {
		c.allocationAccountOwners = nil
		if c.allocationControllerProvider != nil {
			c.allocationAccountRegistry = nil
			c.allocationAccountLimit = 0
			c.allocationControllerProvider = nil
			c.allocationTerminalExporter = nil
		}
		return nil
	}
	if exporter == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if c.proc == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	budget, err := c.proc.GetExecutionResourceBudget()
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
	c.allocationAccountRegistry = registry
	c.allocationAccountLimit = limit
	c.allocationControllerProvider = func() (mpool.AllocationCapacityController, error) {
		if budget.Closed() {
			return nil, process.ErrExecutionResourceClosed
		}
		return budget, nil
	}
	c.allocationTerminalExporter = exporter
	return nil
}

func (a *statementAllocationAttempt) finish() (
	mpool.AllocationAccountTerminalSnapshot,
	error,
) {
	if a == nil {
		return mpool.AllocationAccountTerminalSnapshot{}, nil
	}
	a.prepareTerminal(true)
	return a.completeTerminal()
}

// prepareTerminal closes the operator-owned part of an attempt after every
// scope producer has quiesced. A coordinator-owned board can be closed here.
// Remote fragments share one board on a CN, so their statement group closes it
// only after every expected fragment has reached this boundary.
func (a *statementAllocationAttempt) prepareTerminal(closeBoard bool) error {
	if a == nil {
		return nil
	}
	a.prepareOnce.Do(func() {
		if closeBoard {
			a.prepareErr = joinAllocationLifecycleErrors(
				a.prepareErr,
				allocationLifecycleCall(func() error {
					a.board.CloseAndDrain()
					return nil
				}),
			)
		}
		a.ownersMu.Lock()
		a.closing = true
		owners := a.owners
		a.owners = nil
		a.ownerSet = nil
		a.ownersMu.Unlock()
		for i := len(owners) - 1; i >= 0; i-- {
			a.prepareErr = joinAllocationLifecycleErrors(
				a.prepareErr,
				allocationLifecycleCall(func() error {
					return owners[i].ClearAllocationAccount(a.account)
				}),
			)
		}
	})
	return a.prepareErr

}

func (a *statementAllocationAttempt) completeTerminal() (
	mpool.AllocationAccountTerminalSnapshot,
	error,
) {
	if a == nil {
		return mpool.AllocationAccountTerminalSnapshot{}, nil
	}
	a.completeOnce.Do(func() {
		prepareErr := a.prepareTerminal(false)
		var first bool
		a.completeErr = allocationLifecycleCall(func() error {
			var terminalErr error
			a.snapshot, first, terminalErr = a.registry.CompleteTerminalWithError(
				a.account,
				prepareErr,
			)
			return terminalErr
		})
		if first && a.exporter != nil {
			a.completeErr = joinAllocationLifecycleErrors(
				a.completeErr,
				allocationLifecycleCall(func() error {
					a.exporter(a.snapshot)
					return nil
				}),
			)
		}
	})
	return a.snapshot, a.completeErr
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
	dst.allocationAccountRegistry = c.allocationAccountRegistry
	dst.allocationAccountLimit = c.allocationAccountLimit
	dst.allocationTerminalExporter = c.allocationTerminalExporter
	dst.allocationControllerProvider = c.allocationControllerProvider
}
