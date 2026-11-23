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

package engine

const (
	MaxPointSampleCandidates = 4
	MaxPointSampleColumns    = 4
	PointSampleRows          = 256
)

// IndexPointKey binds a direct INT equality to its base-table column.
type IndexPointKey struct {
	Seqnum uint16
	Value  int32
}

// IndexPointSample counts joint matches over the same Observed rows for every
// candidate. The other PointSampleRows-Observed preselected rows are unknown.
type IndexPointSample struct {
	Matches  [MaxPointSampleCandidates]int
	Observed int
}
