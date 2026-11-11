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

package frontend

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ENUM/SET fields publish STRING plus the declaration flag. Their execution
// vectors and text/binary row encoders continue to carry string labels.
func setEnumSetResultMetadata(ctx context.Context, source *plan.Type, charset uint8, col *MysqlColumn) error {
	var width int32
	var flag uint32
	switch {
	case source.Id == int32(types.T_enum):
		flag = defines.ENUM_FLAG
		for label := range strings.SplitSeq(source.Enumvalues, ",") {
			width = max(width, int32(utf8.RuneCountInString(label)))
		}
	case source.Id == int32(types.T_uint64) && source.Enumvalues != "":
		flag = defines.SET_FLAG
		labels := types.DecodeSetValues(source.Enumvalues)
		width = int32(len(labels) - 1) // separators between selected members
		for _, label := range labels {
			width += int32(utf8.RuneCountInString(label))
		}
	default:
		return moerr.NewInternalError(ctx, "invalid ENUM/SET result domain")
	}
	// Encoding belongs to the display expression, not the ordinal/bitmap type.
	col.SetColumnType(defines.MYSQL_TYPE_STRING)
	col.SetLength(mysqlStringColumnLength(width, mysqlTextMaxBytesPerCharacter(charset)))
	col.SetDecimal(0)
	col.SetFlag(col.Flag() | uint16(flag))
	return nil
}
