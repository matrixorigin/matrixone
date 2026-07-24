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
// HashBuild.  The error is snapshotted when it is constructed; consumers get a
// fresh moerr clone through AsMoErr so a caller cannot mutate the value seen by
// another broadcast consumer.
type JoinMapBuildError struct {
	err *moerr.Error
}

var _ error = new(JoinMapBuildError)

// NewJoinMapBuildError snapshots err as a typed build error.  Non-moerr
// errors are converted to an internal MatrixOne error so callers still get a
// stable error code from every consumer.
func NewJoinMapBuildError(err error) *JoinMapBuildError {
	if err == nil {
		err = moerr.NewInternalErrorNoCtx("hash build failed without an error")
	}
	var me *moerr.Error
	if !errors.As(err, &me) {
		me = moerr.NewInternalErrorNoCtx(err.Error())
	}
	return &JoinMapBuildError{err: cloneMoErr(me)}
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
	if e == nil || e.err == nil {
		return "hash build failed"
	}
	return e.err.Error()
}

// Unwrap lets errors.As/errors.Is inspect the MatrixOne error while keeping
// the published BuildError as the outer, typed terminal value.
func (e *JoinMapBuildError) Unwrap() error {
	return e.AsMoErr()
}

func (e *JoinMapBuildError) ErrorCode() uint16 {
	if e == nil || e.err == nil {
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
	return cloneMoErr(e.err)
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
