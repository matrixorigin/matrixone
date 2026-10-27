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

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"strings"
)

// ValidateM0BirthProperties distinguishes the ordinary catalog properties that
// PlanDefToCstrDef puts in StreamConfigsDef from an actual connector/config.
// Planner, CN and TN use the same predicate so legal unsupported tables choose
// COPY before catalog mutation, rather than failing only during TN commit.
func ValidateM0BirthProperties(properties []*plan.Property) error {
	invalid := func() error { return moerr.NewInternalErrorNoCtx("M0: unsupported birth-fill table properties") }
	for _, property := range properties {
		if property == nil {
			return invalid()
		}
		switch strings.ToLower(property.Key) {
		case SystemRelAttr_Kind:
			if property.Value != SystemOrdinaryRel {
				return invalid()
			}
		case SystemRelAttr_CreateSQL, SystemRelAttr_Comment:
		default:
			return invalid()
		}
	}
	return nil
}

func ValidateM0BirthTableProperties(table *plan.TableDef) error {
	for _, def := range table.Defs {
		if properties := def.GetProperties(); properties != nil {
			if err := ValidateM0BirthProperties(properties.Properties); err != nil {
				return err
			}
		}
	}
	return nil
}
