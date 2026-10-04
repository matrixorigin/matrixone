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
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const JSONDocumentMaxNestingDepth = maxJSONMergeNestingDepth

func ParseFromByteSliceWithDepthLimit(data []byte, maxDepth int) (ByteJson, error) {
	if len(data) == 0 {
		return Null, moerr.NewInvalidInputNoCtx("json text is empty")
	}
	if maxDepth <= 0 {
		return Null, moerr.NewInvalidInputNoCtx("json document max depth must be positive")
	}
	var document ByteJson
	if err := document.unmarshalJSONWithDepthLimit(data, maxDepth); err != nil {
		return Null, err
	}
	return document, nil
}

func ValidateJSONDocumentDepth(document ByteJson) error {
	if err := ValidateJSONMergeDocument(document); err != nil {
		return newJSONDocumentDepthError(JSONDocumentMaxNestingDepth)
	}
	return nil
}

type jsonDocumentDepthError struct {
	cause *moerr.Error
}

func (err *jsonDocumentDepthError) Error() string {
	return err.cause.Error()
}

func (err *jsonDocumentDepthError) Unwrap() error {
	return err.cause
}

func newJSONDocumentDepthError(maxDepth int) error {
	return &jsonDocumentDepthError{
		cause: moerr.NewInvalidInputNoCtxf("json document nesting depth exceeds %d", maxDepth),
	}
}

func IsJSONDocumentDepthError(err error) bool {
	var depthErr *jsonDocumentDepthError
	return errors.As(err, &depthErr)
}
