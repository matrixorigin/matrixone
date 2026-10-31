//go:build mo_m0

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

package jobs

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func init() {
	activeM0WriteSchemaVersion = m0WriteSchemaVersion
}

func m0WriteSchemaVersion(schema *catalog.Schema, seqnums []uint16, fallback uint32) uint32 {
	if schema == nil {
		return fallback
	}
	version, err := catalog.M0DerivedAppendVersion(schema)
	if err != nil {
		return fallback
	}
	next := schema.Extra.NextColSeqnum
	if len(seqnums) < int(next) {
		return fallback
	}
	for seq := uint32(0); seq < next; seq++ {
		if seqnums[seq] != uint16(seq) {
			return fallback
		}
	}
	return version
}
