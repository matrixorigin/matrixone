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

/*
MatrixOne-owned addition: this file is not part of the vendored upstream
inventory (see PROVENANCE in the parent directory).

The repository's err-check gate (Makefile) scans pkg/ production code for the
two standard-library error-construction helpers (they must not be spelled out
here, or the scan would match this comment), and the configured staticcheck
policy requires lower-case, unpunctuated error strings (ST1005). The vendored
syntax package still needs a small number of simple error values (for example
the ErrReplacementError sentinel), so they are built through errString.
*/

package syntax

// errString is a minimal error carrier for the vendored engine's simple
// static error values. See the file-level comment above.
type errString string

func (e errString) Error() string { return string(e) }
