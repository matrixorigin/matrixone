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

package jsonvalue

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// TableSpec is the immutable, versioned FUNCTION_SCAN payload. Column positions
// refer to the full dynamic table schema, even when projection prunes columns.
type TableSpec struct {
	Version  int           `json:"version"`
	RootPath string        `json:"root_path"`
	Columns  []TableColumn `json:"columns"`
}

type TableResponse struct {
	Action  string `json:"action"`
	Default string `json:"default,omitempty"`
}

type TableColumn struct {
	Name       string        `json:"name,omitempty"`
	OriginName string        `json:"origin_name,omitempty"`
	Kind       string        `json:"kind"`
	Type       types.Type    `json:"type"`
	Path       string        `json:"path,omitempty"`
	OnEmpty    TableResponse `json:"on_empty"`
	OnError    TableResponse `json:"on_error"`
	Children   []TableColumn `json:"children,omitempty"`
}

// ConvertTableScalarWithContext admits only the lossy conversions whose SQL
// policy and diagnostic are implemented by the serial table consumer. Keep the
// shared foundation conversion unchanged for its existing callers.
func ConvertTableScalarWithContext(ctx context.Context, value bytejson.ByteJson, target types.Type, options ConversionOptions) Result {
	result := ConvertScalarWithContext(ctx, value, target, options)
	if result.Status == StatusTruncated && isTextConversionTarget(target.Oid) {
		return Result{Status: StatusStatementError, Err: moerr.NewNotSupported(ctx, "JSON_TABLE lossy character conversion requires the scalar compatibility gate")}
	}
	if result.Status == StatusSuccess && target.IsIntOrUint() {
		text, err := safeScalarText(value)
		if err != nil {
			return Result{Status: StatusStatementError, Err: err}
		}
		_, truncated, known, _ := canonicalDecimalInput(strings.ReplaceAll(text, "E", "e"), types.New(types.T_decimal128, 38, 0))
		if known && truncated {
			return Result{Status: StatusStatementError, Err: moerr.NewNotSupported(ctx, "JSON_TABLE fractional integer conversion requires the scalar compatibility gate")}
		}
	}
	return result
}
