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

// GPU-only vector-index plugins. CAGRA and IVF-PQ have CUDA-backed
// table functions (cagra_create / ivfpq_create) implemented only under
// the gpu tag, so registering them on a CPU binary would let CREATE
// INDEX proceed until the BUILD SQL fails mid-flight — by which point
// hidden tables have been created and DELETEs run. Gating the
// registration here makes plan-build at pkg/sql/plan/build_ddl.go's
// vectorplugin.Get dispatch return "unsupported index type: cagra" /
// "unsupported index type: ivfpq" on CPU binaries, before any DDL
// side effects.

package all

import (
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin"
)
