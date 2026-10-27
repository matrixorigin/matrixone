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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

var activeM0WriteSchemaVersion = func(
	_ *catalog.Schema,
	_ []uint16,
	fallback uint32,
) uint32 {
	return fallback
}

func validateM0CapturedWrite(schema *catalog.Schema, seqnums []uint16, capturedVersion uint32) error {
	if schema == nil || schema.Extra == nil || schema.Extra.AppendMissingState == nil {
		return nil
	}
	return objectio.ValidateM0WritePrefix(schema.Extra.AppendMissingState, schema.Version, capturedVersion, seqnums)
}
