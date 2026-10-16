// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func builtInInternalTableCollation(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	input := vector.GenerateFunctionStrParameter(parameters[0])
	output := vector.MustFunctionResult[types.Varlena](result)
	for row := uint64(0); row < uint64(length); row++ {
		data, null := input.GetStrValue(row)
		if null || functionRowSkipped(selectList, row) {
			if err := output.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		var extra api.SchemaExtra
		if err := extra.Unmarshal(data); err != nil {
			return err
		}
		identity := extra.DefaultCharset
		if identity == uint32(collation.LegacyIdentity) {
			// Historical tables have no persisted table-default field. Keep
			// their pre-upgrade metadata presentation without inventing a row.
			identity = uint32(collation.UTF8MB4GeneralCIIdentity)
		}
		// Unmarshal validates the closed physical-format domain. Rendering a
		// table default is not admission to execute that physical index format.
		revision := extra.CollationVersion
		if extra.KeyFormat == collation.KeyFormatV1 && revision == uint32(collation.RevisionV1) && identity <= uint32(collation.UTF8MB4GeneralCIIdentity) {
			// Native UNIQUE retains a schema-level V1 marker even when the
			// independent table default belongs to a legacy character domain.
			revision = uint32(collation.RevisionLegacy)
		}
		if err := collation.RequireLegacy(identity, revision, collation.KeyFormatLegacy); err != nil {
			return err
		}
		definition, err := collation.EffectiveDefinition(identity, revision)
		if err != nil {
			return err
		}
		if err := output.AppendBytes([]byte(definition.Name), false); err != nil {
			return err
		}
	}
	return nil
}
