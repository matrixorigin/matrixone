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

//go:build !gpu

package plan

// The cagra and ivfpq plugins are registered only in the gpu build
// (pkg/indexplugin/all/all_gpu.go). These imports register them in the cpu
// test build, so the planner tests see the plugin set of the gpu build.
import (
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin"
)
