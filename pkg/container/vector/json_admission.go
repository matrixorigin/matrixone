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

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// validateJSONPayload is the raw-byte admission boundary for T_json. Vector
// framing alone proves only that bytes are addressable, not that descendants
// are valid. Copies/unions of admitted, immutable vectors preserve this invariant.
func validateJSONPayload(typ types.Type, raw []byte) error {
	if typ.Oid != types.T_json {
		return nil
	}
	if len(raw) == 0 {
		return moerr.NewInvalidInputNoCtx("invalid JSON vector payload")
	}
	return validateJSONValue(typ, bytejson.ByteJson{Type: raw[0], Data: raw[1:]})
}

func validateJSONValue(typ types.Type, value bytejson.ByteJson) error {
	if typ.Oid == types.T_json && !bytejson.IsValidByteJson(value) {
		return moerr.NewInvalidInputNoCtx("invalid JSON vector payload")
	}
	return nil
}
