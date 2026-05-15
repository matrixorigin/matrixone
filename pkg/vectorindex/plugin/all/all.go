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

// Package all blank-imports every vector index plugin so that their init()
// registrations run. Import this once from cmd/mo-service/main.go (or any
// other entrypoint that needs vector indexes).
package all

import (
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin"
	// HNSW / IVF-FLAT / CAGRA shim plugins are added in Phase 3.
)
