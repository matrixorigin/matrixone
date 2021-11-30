// Copyright 2021 Matrix Origin
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

package guest

import "github.com/matrixorigin/matrixone/pkg/vm/mmu/host"

// Mmu is container for a query execution
type Mmu struct {
	// size, current usage of memory
	size int64
	// Limit, maximum memory can be used in this query execution
	Limit int64
	// Mmu,
	Mmu *host.Mmu
}
