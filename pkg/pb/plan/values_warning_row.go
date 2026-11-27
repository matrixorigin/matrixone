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

package plan

import (
	"strconv"
	"strings"
)

const ValuesWarningOrdinalColumn = "_values_ordinal"
const ValuesWarningOrdinalScan = "values-warning-ordinal-scan"
const valuesWarningRowPrefix = "values-warning-row:"

// ValuesWarningRowOption marks the singleton projection that owns a VALUES tuple.
func ValuesWarningRowOption(row uint64) string {
	return valuesWarningRowPrefix + strconv.FormatUint(row, 10)
}

// ValuesWarningRow is one-based. Zero denotes an ordinary projection.
func (n *Node) ValuesWarningRow() uint64 {
	if n == nil || n.NodeType != Node_PROJECT {
		return 0
	}
	if value, ok := strings.CutPrefix(n.ExtraOptions, valuesWarningRowPrefix); ok {
		row, err := strconv.ParseUint(value, 10, 32)
		if err == nil {
			return row
		}
	}
	return 0
}
