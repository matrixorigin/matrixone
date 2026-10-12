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

package plan

import (
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// FunctionalJSONSemanticsKey identifies the bounded JSON text-key contract in
// the existing persisted index parameters. Index metadata survives column
// pruning, unlike a generated expression referenced only by an unused key.
const FunctionalJSONSemanticsKey = "functional_json_semantics"

// JSONFunctionalIndexSemantics validates the known semantic revision without
// treating ordinary JSON expressions as a new functional-index capability.
func JSONFunctionalIndexSemantics(index *IndexDef) (bool, error) {
	if index == nil || !strings.Contains(index.IndexAlgoParams, FunctionalJSONSemanticsKey) {
		return false, nil
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal([]byte(index.IndexAlgoParams), &values); err != nil {
		return false, moerr.NewInvalidInputNoCtx("invalid JSON functional index semantics metadata")
	}
	value, present := values[FunctionalJSONSemanticsKey]
	if !present {
		return false, nil
	}
	var revision string
	if err := json.Unmarshal(value, &revision); err != nil || revision != "1" {
		return false, moerr.NewInvalidInputNoCtx("unsupported JSON functional index semantics revision")
	}
	return true, nil
}
