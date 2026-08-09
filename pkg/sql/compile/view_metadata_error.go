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

package compile

import (
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type viewRefreshIdentityChangedError struct{ cause error }

func (failure *viewRefreshIdentityChangedError) Error() string { return failure.cause.Error() }
func (failure *viewRefreshIdentityChangedError) Unwrap() error { return failure.cause }

type viewRefreshDependencyUnavailableError struct{ cause error }

func (failure *viewRefreshDependencyUnavailableError) Error() string { return failure.cause.Error() }
func (failure *viewRefreshDependencyUnavailableError) Unwrap() error { return failure.cause }

type viewRefreshFailureCode uint32

const (
	viewRefreshFailureNone viewRefreshFailureCode = iota
	viewRefreshFailurePermanentlyInvalid
	viewRefreshFailureDependencyUnavailable
	viewRefreshFailureIdentityChanged
	viewRefreshFailurePlannerIncompatible
	viewRefreshFailureTxnConflict
	viewRefreshFailureCanceled
	viewRefreshFailureInfrastructure
)

type viewRefreshDisposition uint8

const (
	viewRefreshRollbackDDL viewRefreshDisposition = iota
	viewRefreshRetry
	viewRefreshMarkInvalid
)

type viewRefreshFailure struct {
	code        viewRefreshFailureCode
	disposition viewRefreshDisposition
	cause       error
}

func (failure *viewRefreshFailure) Error() string { return failure.cause.Error() }
func (failure *viewRefreshFailure) Unwrap() error { return failure.cause }

func classifyViewRefreshFailure(err error) *viewRefreshFailure {
	if err == nil {
		return &viewRefreshFailure{code: viewRefreshFailureNone}
	}
	var identityChanged *viewRefreshIdentityChangedError
	if errors.As(err, &identityChanged) {
		return &viewRefreshFailure{viewRefreshFailureIdentityChanged, viewRefreshMarkInvalid, err}
	}
	var dependencyUnavailable *viewRefreshDependencyUnavailableError
	if errors.As(err, &dependencyUnavailable) {
		return &viewRefreshFailure{viewRefreshFailureDependencyUnavailable, viewRefreshRetry, err}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted) {
		return &viewRefreshFailure{viewRefreshFailureCanceled, viewRefreshRetry, err}
	}
	if moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
		return &viewRefreshFailure{viewRefreshFailureTxnConflict, viewRefreshRetry, err}
	}
	if moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) ||
		moerr.IsMoErrCode(err, moerr.ErrBadDB) {
		return &viewRefreshFailure{viewRefreshFailureDependencyUnavailable, viewRefreshRetry, err}
	}
	if moerr.IsMoErrCode(err, moerr.ErrParseError) {
		return &viewRefreshFailure{viewRefreshFailurePlannerIncompatible, viewRefreshMarkInvalid, err}
	}
	if moerr.IsMoErrCode(err, moerr.ErrBadView) ||
		moerr.IsMoErrCode(err, moerr.ErrViewWrongList) {
		return &viewRefreshFailure{viewRefreshFailurePermanentlyInvalid, viewRefreshMarkInvalid, err}
	}
	if moerr.IsMoErrCode(err, moerr.ErrRPCTimeout) || moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
		return &viewRefreshFailure{viewRefreshFailureInfrastructure, viewRefreshRetry, err}
	}
	return &viewRefreshFailure{viewRefreshFailureInfrastructure, viewRefreshRetry, err}
}
