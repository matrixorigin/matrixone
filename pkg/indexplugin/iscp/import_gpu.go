//go:build gpu

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

// GPU-only ISCP-hook wiring. CAGRA and IVF-PQ have cuvs-backed table
// functions (cagra_create / ivfpq_create) implemented only under the
// gpu tag, so their iscp Hooks (which import the GPU-only sync
// objects) live in build-tag-gated sub-packages. Gating the
// registration here keeps CPU binaries from registering hooks they
// can never invoke.

package iscp

import (
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/iscp"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/iscp"
)
