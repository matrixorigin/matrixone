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

package message

import (
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// JoinMapResultKind identifies the terminal state of a join-map dependency.
//
// A result is deliberately not represented by a (*JoinMap, error) pair.  A
// nil JoinMap is a valid successful result (an empty build), while a
// JoinMapBuildError is a failed build.  Keeping the state explicit prevents a
// failed producer from being observed as an empty build by one of the
// broadcast consumers.
type JoinMapResultKind uint8

const (
	JoinMapResultUnfinalized JoinMapResultKind = iota
	JoinMapResultSuccess
	JoinMapResultBuildError
)

// JoinMapBuildError is the immutable, typed error published by a failed
// HashBuild. The error is snapshotted when it is constructed: consumers get a
// fresh moerr clone for substantive failures, while cancellation consumers
// share only immutable context sentinels and a copied diagnostic string.
type JoinMapBuildError struct {
	err        *moerr.Error
	contextErr error
	message    string
}

var _ error = new(JoinMapBuildError)

// NewJoinMapBuildError snapshots err as a typed build error. Substantive
// MatrixOne errors retain their stable wire code. Pure context cancellation
// errors retain their identity so scope aggregation can recover the causal
// execution error from the canceled pipeline context. Other non-moerr errors
// are converted to a stable internal MatrixOne error.
func NewJoinMapBuildError(err error) *JoinMapBuildError {
	if err == nil {
		err = moerr.NewInternalErrorNoCtx("hash build failed without an error")
	}
	if contextErr, ok := snapshotContextCancellation(err); ok {
		buildErr := &JoinMapBuildError{
			contextErr: contextErr,
			message:    err.Error(),
		}
		// Preserve stable moerr detail for compatibility even though cancellation
		// identity, rather than the moerr itself, owns error unwrapping.
		var me *moerr.Error
		if errors.As(err, &me) {
			buildErr.err = cloneMoErr(me)
		}
		return buildErr
	}
	if me := firstSubstantiveMoErr(err); me != nil {
		return &JoinMapBuildError{err: cloneMoErr(me)}
	}
	return &JoinMapBuildError{err: moerr.NewInternalErrorNoCtx(err.Error())}
}

type contextCancellationKind uint8

const (
	contextCanceled contextCancellationKind = 1 << iota
	contextDeadlineExceeded
)

// snapshotContextCancellation recognizes only error trees whose every leaf is
// cancellation-shaped. A joined substantive failure must not be downgraded to
// cleanup fallout merely because another leaf is context.Canceled.
func snapshotContextCancellation(err error) (error, bool) {
	kind, ok := contextCancellationTreeKind(err)
	if !ok {
		return nil, false
	}
	if kind&contextDeadlineExceeded != 0 {
		if kind&contextCanceled != 0 {
			return errors.Join(context.Canceled, context.DeadlineExceeded), true
		}
		return context.DeadlineExceeded, true
	}
	return context.Canceled, true
}

func contextCancellationTreeKind(err error) (contextCancellationKind, bool) {
	if err == nil {
		return 0, false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return 0, false
		}
		var kind contextCancellationKind
		for _, child := range children {
			childKind, ok := contextCancellationTreeKind(child)
			if !ok {
				return 0, false
			}
			kind |= childKind
		}
		return kind, true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if child := wrapped.Unwrap(); child != nil {
			return contextCancellationTreeKind(child)
		}
	}
	var me *moerr.Error
	if errors.As(err, &me) {
		return contextCancellationMoErrKind(me)
	}

	var kind contextCancellationKind
	if errors.Is(err, context.Canceled) {
		kind |= contextCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		kind |= contextDeadlineExceeded
	}
	return kind, kind != 0
}

func contextCancellationMoErrKind(err *moerr.Error) (contextCancellationKind, bool) {
	if err == nil {
		return 0, false
	}
	switch err.ErrorCode() {
	case moerr.ErrQueryInterrupted:
		return contextCanceled, true
	case moerr.ErrQueryTimeout:
		return contextDeadlineExceeded, true
	default:
		return 0, false
	}
}

// firstSubstantiveMoErr finds a MatrixOne error that is not itself a
// cancellation result. A cancellation-shaped moerr must not mask a generic or
// MatrixOne execution error that appears later in the same joined tree.
func firstSubstantiveMoErr(err error) *moerr.Error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if me := firstSubstantiveMoErr(child); me != nil {
				return me
			}
		}
		return nil
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if child := wrapped.Unwrap(); child != nil {
			return firstSubstantiveMoErr(child)
		}
	}
	var me *moerr.Error
	if errors.As(err, &me) {
		if _, cancellation := contextCancellationMoErrKind(me); !cancellation {
			return me
		}
	}
	return nil
}

func cloneMoErr(src *moerr.Error) *moerr.Error {
	if src == nil {
		return moerr.NewInternalErrorNoCtx("hash build failed without an error")
	}
	data, err := src.MarshalBinary()
	if err != nil {
		return moerr.NewInternalErrorNoCtx(src.Error())
	}
	dst := new(moerr.Error)
	if err := dst.UnmarshalBinary(data); err != nil {
		return moerr.NewInternalErrorNoCtx(src.Error())
	}
	// moerr's binary form intentionally omits Detail.  Keep it as part of the
	// immutable snapshot because memory-admission errors often put useful,
	// safe metadata there.
	dst.SetDetail(src.Detail())
	return dst
}

func (e *JoinMapBuildError) Error() string {
	if e == nil {
		return "hash build failed"
	}
	if e.contextErr != nil {
		if e.message != "" {
			return e.message
		}
		return e.contextErr.Error()
	}
	if e.err == nil {
		return "hash build failed"
	}
	return e.err.Error()
}

// Unwrap lets errors.As/errors.Is inspect either the MatrixOne error or the
// preserved cancellation identity while keeping the published BuildError as
// the outer, typed terminal value.
func (e *JoinMapBuildError) Unwrap() error {
	if e != nil && e.contextErr != nil {
		if errors.Is(e.contextErr, context.DeadlineExceeded) {
			if errors.Is(e.contextErr, context.Canceled) {
				// errors.Join exposes its backing child slice through Unwrap. Return a
				// fresh value so one broadcast consumer cannot mutate another's view.
				return errors.Join(context.Canceled, context.DeadlineExceeded)
			}
			return context.DeadlineExceeded
		}
		return context.Canceled
	}
	return e.AsMoErr()
}

// As preserves the pre-existing errors.As contract for MatrixOne errors while
// Unwrap carries context identity for cancellation arbitration. Each caller
// receives an independent moerr snapshot.
func (e *JoinMapBuildError) As(target any) bool {
	me, ok := target.(**moerr.Error)
	if !ok {
		return false
	}
	*me = e.AsMoErr()
	return true
}

func (e *JoinMapBuildError) ErrorCode() uint16 {
	if e == nil {
		return moerr.ErrInternal
	}
	if e.contextErr != nil {
		if errors.Is(e.contextErr, context.DeadlineExceeded) {
			return moerr.ErrQueryTimeout
		}
		return moerr.ErrQueryInterrupted
	}
	if e.err == nil {
		return moerr.ErrInternal
	}
	return e.err.ErrorCode()
}

func (e *JoinMapBuildError) Detail() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Detail()
}

// AsMoErr returns an independent typed moerr snapshot for the caller.
func (e *JoinMapBuildError) AsMoErr() *moerr.Error {
	if e == nil {
		return moerr.NewInternalErrorNoCtx("hash build failed")
	}
	if e.contextErr != nil {
		var me *moerr.Error
		if errors.Is(e.contextErr, context.DeadlineExceeded) {
			me = moerr.NewQueryTimeout(context.Background())
		} else {
			me = moerr.NewQueryInterrupted(context.Background())
		}
		me.SetDetail(e.Detail())
		return me
	}
	if e.err == nil {
		return moerr.NewInternalErrorNoCtx(e.Error())
	}
	return cloneMoErr(e.err)
}

// AsError returns the immutable cancellation wrapper when cancellation
// identity must survive errors.Is, or a fresh moerr clone for substantive
// failures. Execution operators should propagate this value to their scope.
func (e *JoinMapBuildError) AsError() error {
	if e != nil && e.contextErr != nil {
		return e
	}
	return e.AsMoErr()
}

// JoinMapResult is the immutable terminal dependency value shared by all
// consumers of a broadcast JoinMap.  Its zero value is intentionally
// unfinalized and must never be used as a terminal result.
type JoinMapResult struct {
	kind     JoinMapResultKind
	joinMap  *JoinMap
	buildErr *JoinMapBuildError
}

// NewJoinMapResult creates a successful terminal result.  A nil map means a
// true empty build and remains distinct from a BuildError result.
func NewJoinMapResult(jm *JoinMap) JoinMapResult {
	return JoinMapResult{kind: JoinMapResultSuccess, joinMap: jm}
}

// NewJoinMapBuildErrorResult creates a failed terminal result.
func NewJoinMapBuildErrorResult(err error) JoinMapResult {
	return JoinMapResult{kind: JoinMapResultBuildError, buildErr: NewJoinMapBuildError(err)}
}

func (r JoinMapResult) Kind() JoinMapResultKind {
	return r.kind
}

func (r JoinMapResult) Finalized() bool {
	return r.kind == JoinMapResultSuccess || r.kind == JoinMapResultBuildError
}

func (r JoinMapResult) IsSuccess() bool {
	return r.kind == JoinMapResultSuccess
}

func (r JoinMapResult) IsEmpty() bool {
	return r.kind == JoinMapResultSuccess && r.joinMap == nil
}

func (r JoinMapResult) IsBuildError() bool {
	return r.kind == JoinMapResultBuildError && r.buildErr != nil
}

func (r JoinMapResult) JoinMap() *JoinMap {
	if !r.IsSuccess() {
		return nil
	}
	return r.joinMap
}

func (r JoinMapResult) BuildError() *JoinMapBuildError {
	if !r.IsBuildError() {
		return nil
	}
	return r.buildErr
}

// Err returns the typed terminal error, or nil for successful results.
func (r JoinMapResult) Err() error {
	if e := r.BuildError(); e != nil {
		return e
	}
	return nil
}
