// Copyright 2022 Matrix Origin
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

package fileservice

import "math"

type IOVector struct {
	// path to file, '/' separated
	FilePath string
	// io entries
	// empty entry not allowed
	Entries []IOEntry
}

func (i IOVector) offsetRange() (
	min int,
	max int,
	readToEnd bool,
) {
	min = math.MaxInt
	max = 0
	for _, entry := range i.Entries {
		if entry.Offset < min {
			min = entry.Offset
		}
		if entry.Size < 0 {
			entry.Size = 0
			readToEnd = true
		}
		if end := entry.Offset + entry.Size; end > max {
			max = end
		}
	}
	return
}
