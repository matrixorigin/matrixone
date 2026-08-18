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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// mergeEqualRuntimeStringDomain makes provenance merging for equal aggregate
// candidates commutative and associative. RuntimeStringBinary and
// RuntimeStringText both carry explicit information, while Inherit does not;
// Binary wins the otherwise ambiguous explicit conflict.
func mergeEqualRuntimeStringDomain(
	destination *vector.Vector,
	destinationRow int,
	source *vector.Vector,
	sourceRow int,
	mp *mpool.MPool,
) error {
	domain := destination.GetRuntimeStringDomainAt(destinationRow)
	if candidate := source.GetRuntimeStringDomainAt(sourceRow); candidate > domain {
		domain = candidate
	}
	return destination.SetRuntimeStringDomainAtWithMP(destinationRow, domain, mp)
}
