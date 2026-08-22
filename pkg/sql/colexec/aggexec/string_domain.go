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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// mergeEqualRuntimeStringDomain makes provenance merging for equal aggregate
// candidates commutative and associative. The contributing-values policy is
// defined over effective domains, not runtime enum ordinals: an inherited
// statically-binary value therefore dominates an explicit Text override.
func mergeEqualRuntimeStringDomain(
	destination *vector.Vector,
	destinationRow int,
	source *vector.Vector,
	sourceRow int,
	mp *mpool.MPool,
) error {
	destinationType := *destination.GetType()
	sourceType := *source.GetType()
	if types.StaticStringDomain(destinationType) == types.StringDomainNone {
		return nil
	}
	destinationState, err := types.NewStringSemanticState(
		destinationType,
		destination.GetRuntimeStringDomainAt(destinationRow),
		types.StringSourceExpression,
		types.StringLiteralNone,
		types.StringConversionString,
		types.StringNullKindForType(destinationType, destination.IsNull(uint64(destinationRow))),
	)
	if err != nil {
		return err
	}
	sourceState, err := types.NewStringSemanticState(
		sourceType,
		source.GetRuntimeStringDomainAt(sourceRow),
		types.StringSourceExpression,
		types.StringLiteralNone,
		types.StringConversionString,
		types.StringNullKindForType(sourceType, source.IsNull(uint64(sourceRow))),
	)
	if err != nil {
		return err
	}
	merged, err := types.MergeStringSemanticStates(
		types.StringMergeContributingValues,
		destinationType,
		destinationState,
		sourceState,
	)
	if err != nil {
		return err
	}
	if err := destination.SetRuntimeStringDomainAtWithMP(destinationRow, merged.RuntimeDomain(), mp); err != nil {
		return err
	}
	mergedSource, err := types.MergeStringSources(
		destination.GetStringSourceAt(destinationRow), source.GetStringSourceAt(sourceRow))
	if err != nil {
		return err
	}
	return destination.SetStringSourceAtWithMP(destinationRow, mergedSource, mp)
}
