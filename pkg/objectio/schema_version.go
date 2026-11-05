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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// ObjectSchemaVersion reads the existing version independently of a writer
// table ID. The caller is responsible for the semantic meaning of this physical field.
func ObjectSchemaVersion(h Header) (uint32, error) {
	if len(h) < HeaderSize || h.Magic() != uint64(Magic) {
		return 0, moerr.NewInternalErrorNoCtx("objectio: invalid object header")
	}
	return types.DecodeUint32(h[8+2+ExtentSize : 8+2+ExtentSize+4]), nil
}
