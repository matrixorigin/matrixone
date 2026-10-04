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

package incrservice

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestAutoColumnVisibilityAndOffsetValidation(t *testing.T) {
	def := &plan.TableDef{TblId: 7, Cols: []*plan.ColDef{
		{Name: "id", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
		{Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "__mo_fake_pk_col", Hidden: true, Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
	}}

	require.Equal(t, []string{"id", "__mo_fake_pk_col"}, autoColumnNames(GetAutoColumnFromDef(def)))
	require.Equal(t, []string{"id"}, autoColumnNames(GetUserAutoColumnFromDef(def)))
	require.Equal(t, []string{"__mo_fake_pk_col"}, autoColumnNames(GetInternalAutoColumnFromDef(def)))

	require.NoError(t, ValidateAutoColumnOffset(context.Background(), types.T_uint8, math.MaxUint8))
	err := ValidateAutoColumnOffset(context.Background(), types.T_uint8, math.MaxUint8+1)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
}

func autoColumnNames(cols []AutoColumn) []string {
	names := make([]string, len(cols))
	for i := range cols {
		names[i] = cols[i].ColName
	}
	return names
}
