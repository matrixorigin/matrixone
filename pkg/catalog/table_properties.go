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

package catalog

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// AppendStreamConfigProperties appends persisted properties except metadata
// projected from mo_tables columns. Re-importing those keys would duplicate
// them on every schema rewrite and could override current catalog values.
// Other properties retain their order and duplicates; configs is not modified.
func AppendStreamConfigProperties(properties, configs []*plan.Property) []*plan.Property {
	for _, config := range configs {
		switch strings.ToLower(config.GetKey()) {
		case SystemRelAttr_Kind, SystemRelAttr_Comment, SystemRelAttr_CreateSQL:
			continue
		}
		properties = append(properties, config)
	}
	return properties
}
