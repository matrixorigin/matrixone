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
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func requireLegacyAggregateType(typ types.Type) error {
	return plan.RequireLegacyCollations(typ.PlanType())
}

// Generic vector codecs preserve structurally valid future metadata. Aggregate
// state is executable: its inner type must be admitted before decoding can
// repack, merge, or publish it. The caller retains ownership on either outcome.
func unmarshalAggregateVector(vec *vector.Vector, reader io.Reader, mp *mpool.MPool) error {
	if err := vec.UnmarshalWithReader(reader, mp); err != nil {
		return err
	}
	return requireLegacyAggregateType(*vec.GetType())
}
