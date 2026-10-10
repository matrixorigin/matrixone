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

package bytejson

import (
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/internal/bytejsonvalidate"
)

const storedJSONValidationMinimumWork = 1024

// ValidateStoredJSONDocument validates a binary JSON document before any
// operation can follow its offsets. It combines ordinary scalar/structural
// validation with canonical ranges, sorted UTF-8 keys and UTF-8 strings in the
// existing bounded traversal. Depth is capped before descending; traversal
// retains no width-dependent child/frame slices and charges both encoded bytes
// and stored nodes/entries. It does not establish mutable-vector ownership.
func ValidateStoredJSONDocument(document ByteJson) error {
	workLimit := uint64(len(document.Data))
	if workLimit < storedJSONValidationMinimumWork {
		workLimit = storedJSONValidationMinimumWork
	}
	if workLimit <= ^uint64(0)/4 {
		workLimit *= 4
	} else {
		workLimit = ^uint64(0)
	}

	if document.Type == TpCodeArray || document.Type == TpCodeObject {
		valid, depthExceeded := bytejsonvalidate.StoredContainer(document.Type, document.Data, validStoredJSONScalar, workLimit)
		if depthExceeded {
			return newJSONDocumentDepthError(JSONDocumentMaxNestingDepth)
		}
		if !valid {
			return invalidStoredJSONDocument()
		}
	} else {
		work := uint64(0)
		if err := chargeStoredJSONValidationWork(&work, workLimit, 1); err != nil {
			return err
		}
		if !validStoredJSONScalar(document.Type, document.Data) {
			return invalidStoredJSONDocument()
		}
	}
	return nil
}

func validStoredJSONScalar(tp byte, data []byte) bool {
	// Keep decimal parsing, binary subtype/fallback behavior, shortest uvarints,
	// literal/number sizes and finite floats with their existing scalar owner.
	if !validByteJsonScalar(tp, data) {
		return false
	}
	if tp == TpCodeString {
		payload, _ := bytejsonvalidate.UvarintPayload(data)
		return utf8.Valid(payload)
	}
	return true
}

func chargeStoredJSONValidationWork(work *uint64, limit, amount uint64) error {
	if *work > limit || amount > limit-*work {
		return invalidStoredJSONDocument()
	}
	*work += amount
	return nil
}

func invalidStoredJSONDocument() error {
	return moerr.NewInvalidInputNoCtx("invalid binary JSON document")
}
