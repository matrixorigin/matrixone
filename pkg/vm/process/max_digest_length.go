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

const (
	DefaultMaxDigestLength = 1024
	MaximumMaxDigestLength = 1 << 20
)

// ResolveMaxDigestLength resolves max_digest_length at most once for one
// statement. Child processes share BaseProcess, and remote processes receive
// the explicit value/presence pair through SessionInfo, so every fragment of a
// statement observes the same value. Zero is valid and therefore must never be
// used as the unset sentinel.
func ResolveMaxDigestLength(proc *Process) int {
	if proc == nil || proc.Base == nil {
		return DefaultMaxDigestLength
	}

	proc.Base.statementSettingsMu.Lock()
	defer proc.Base.statementSettingsMu.Unlock()

	if proc.Base.SessionInfo.MaxDigestLengthSet {
		length := normalizeMaxDigestLength(proc.Base.SessionInfo.MaxDigestLength)
		proc.Base.SessionInfo.MaxDigestLength = int64(length)
		return length
	}

	length := DefaultMaxDigestLength
	if resolver := proc.GetResolveVariableFunc(); resolver != nil {
		if value, err := resolver("max_digest_length", true, true); err == nil {
			length = maxDigestLengthValue(value)
		}
	}
	proc.Base.SessionInfo.MaxDigestLength = int64(length)
	proc.Base.SessionInfo.MaxDigestLengthSet = true
	return length
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

func maxDigestLengthValue(value any) int {
	switch n := value.(type) {
	case int64:
		return normalizeMaxDigestLength(n)
	case uint64:
		if n > MaximumMaxDigestLength {
			return DefaultMaxDigestLength
		}
		return int(n)
	case int:
		return normalizeMaxDigestLength(int64(n))
	default:
		return DefaultMaxDigestLength
	}
}

func normalizeMaxDigestLength(length int64) int {
	if length < 0 || length > MaximumMaxDigestLength {
		return DefaultMaxDigestLength
	}
	return int(length)
}
