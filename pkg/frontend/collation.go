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

package frontend

import "github.com/matrixorigin/matrixone/pkg/util/sysview"

type Collation struct {
	collationName string
	charset       string
	id            int64
	isDefault     string
	isCompiled    string
	sortLen       int32
	padAttribute  string
}

// Collations is the executable capability list exposed by SHOW COLLATION.
// It is derived from the canonical information-schema definitions so SHOW
// COLLATION and COLLATION_CHARACTER_SET_APPLICABILITY cannot drift apart.
var Collations = func() []*Collation {
	collations := make([]*Collation, 0, len(sysview.SupportedCollationDefinitions))
	for _, definition := range sysview.SupportedCollationDefinitions {
		if !definition.Advertised {
			continue
		}
		collations = append(collations, &Collation{
			collationName: definition.Name,
			charset:       definition.Charset,
			id:            definition.ID,
			isDefault:     definition.IsDefault,
			isCompiled:    definition.IsCompiled,
			sortLen:       definition.SortLen,
			padAttribute:  definition.PadAttribute,
		})
	}
	return collations
}()
