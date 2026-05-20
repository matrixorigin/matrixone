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

package iscp

// DefaultResolveVariable is the system-variable resolver attached to
// the *process.Process spawned by ProcessInitSQL. ProcessInitSQL has
// no frontend session, so the proc would otherwise get a nil resolver
// — and table functions like cagra_create / ivfpq_create that consult
// session variables (kmeans_train_percent, etc.) would silently skip
// reads and build with degenerate config.
//
// pkg/frontend's init() wires this to a closure that reads
// gSysVarsDefs[name].Default. Tests that don't blank-import
// pkg/frontend will see nil here; ProcessInitSQL nil-checks before
// calling WithResolveVariableFunc(...), preserving today's
// nil-resolver behaviour as the fallback.
//
// Defaults-only by design: SET GLOBAL overrides are NOT honoured here.
// Per-index admin-tuned values are expected to ride along in the
// captured-vars sqlexec.Metadata that the idxcron task carries.
var DefaultResolveVariable func(
	varName string, isSystemVar, isGlobalVar bool,
) (any, error)
