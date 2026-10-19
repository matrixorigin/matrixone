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

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

const (
	DefaultMaxDigestLength = 1024
	MaximumMaxDigestLength = 1 << 20
)

// ResolveMaxDigestLengthWithError resolves max_digest_length at most once for
// one statement. Child processes share BaseProcess, and remote processes
// receive the explicit value/presence pair through SessionInfo, so every
// fragment of a statement observes the same value. Zero is valid and
// therefore must never be used as the unset sentinel.
//
// A resolver error or an invalid value is returned to the caller instead of
// being replaced with the default. Silently changing the token budget is not
// safe: the same statement could produce a different digest depending on
// whether it runs locally or after a remote hop. A process without a resolver
// is a remote/background process and uses the captured snapshot or the
// bounded default for legacy payloads.
func ResolveMaxDigestLengthWithError(proc *Process) (int, error) {
	if proc == nil || proc.Base == nil {
		return DefaultMaxDigestLength, nil
	}

	proc.Base.statementSettingsMu.Lock()
	defer proc.Base.statementSettingsMu.Unlock()

	if proc.Base.SessionInfo.MaxDigestLengthSet {
		length, err := maxDigestLengthValueWithError(proc.Base.SessionInfo.MaxDigestLength)
		if err != nil {
			return 0, err
		}
		proc.Base.SessionInfo.MaxDigestLength = int64(length)
		return length, nil
	}

	length := DefaultMaxDigestLength
	if resolver := proc.GetResolveVariableFunc(); resolver != nil {
		value, err := resolver("max_digest_length", true, true)
		if err != nil {
			return 0, moerr.NewInternalErrorNoCtxf("resolve max_digest_length: %v", err)
		}
		length, err = maxDigestLengthValueWithError(value)
		if err != nil {
			return 0, err
		}
	}
	proc.Base.SessionInfo.MaxDigestLength = int64(length)
	proc.Base.SessionInfo.MaxDigestLengthSet = true
	return length, nil
}

// resolveMaxDigestLengthForProcessInfo returns the value that can be carried
// by a ProcessInfo payload. A digest-aware remote sender resolves the setting
// strictly in validateRemoteExpressionPipelineProtocol before this method is
// called, so a captured snapshot is always validated and never replaced. For
// ordinary queries, keep the historical best-effort behavior: their resolver
// is not required to implement an unrelated digest setting.
func resolveMaxDigestLengthForProcessInfo(proc *Process) (int, error) {
	if proc == nil || proc.Base == nil {
		return DefaultMaxDigestLength, nil
	}

	proc.Base.statementSettingsMu.Lock()
	if proc.Base.SessionInfo.MaxDigestLengthSet {
		length, err := maxDigestLengthValueWithError(proc.Base.SessionInfo.MaxDigestLength)
		if err != nil {
			proc.Base.statementSettingsMu.Unlock()
			return 0, err
		}
		proc.Base.SessionInfo.MaxDigestLength = int64(length)
		proc.Base.statementSettingsMu.Unlock()
		return length, nil
	}
	proc.Base.statementSettingsMu.Unlock()

	// No digest snapshot has been captured. BuildProcessInfo is also used for
	// ordinary remote queries, whose resolver may legitimately not expose this
	// digest-only variable. Preserve the compatibility fallback in that case.
	return ResolveMaxDigestLength(proc), nil
}

// ResolveMaxDigestLength is retained for callers that cannot propagate an
// error yet. New execution and digest protocol paths must use
// ResolveMaxDigestLengthWithError so a failed or invalid setting cannot be
// silently changed into a different digest contract. This compatibility helper
// deliberately does not cache its fallback: a later strict caller must still
// be able to observe and report the original resolution failure.
func ResolveMaxDigestLength(proc *Process) int {
	length, err := ResolveMaxDigestLengthWithError(proc)
	if err == nil {
		return length
	}
	return DefaultMaxDigestLength
}

// ResetMaxDigestLengthSnapshot starts a new statement generation. Frontend
// multi-statement execution reuses a Process, so its statement boundary must
// clear both the presence bit and the old value before the next lazy resolve.
func (proc *Process) ResetMaxDigestLengthSnapshot() {
	if proc == nil || proc.Base == nil {
		return
	}
	proc.Base.statementSettingsMu.Lock()
	proc.Base.SessionInfo.MaxDigestLength = 0
	proc.Base.SessionInfo.MaxDigestLengthSet = false
	proc.Base.statementSettingsMu.Unlock()
}

func maxDigestLengthValueWithError(value any) (int, error) {
	switch n := value.(type) {
	case int64:
		return normalizeMaxDigestLengthWithError(n)
	case uint64:
		if n > MaximumMaxDigestLength {
			return 0, invalidMaxDigestLength(value)
		}
		return int(n), nil
	case int:
		return normalizeMaxDigestLengthWithError(int64(n))
	case nil:
		// A nil value is how a resolver reports an unavailable optional value;
		// preserve the documented bounded default in that case.
		return DefaultMaxDigestLength, nil
	default:
		return 0, invalidMaxDigestLength(value)
	}
}

func invalidMaxDigestLength(value any) error {
	return moerr.NewInternalErrorNoCtxf("invalid max_digest_length value (%T): %v", value, value)
}

func normalizeMaxDigestLengthWithError(length int64) (int, error) {
	if length < 0 || length > MaximumMaxDigestLength {
		return 0, invalidMaxDigestLength(length)
	}
	return int(length), nil
}
