// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestM0BirthCopyExitQuotedRename(t *testing.T) {
	sql := m0CopyExitRenameSQL("d`b", &plan.AlterTableName{OldName: "o`ld", NewName: "n`ew"})
	require.Equal(t, "alter table `d``b`.`o``ld` rename to `n``ew`", sql)
}

func TestM0BirthCopyExitOrdinarySourceNoChange(t *testing.T) {
	require.NoError(t, validateM0CopyExitTarget(&plan.TableDef{}, nil))
	source := &plan.TableDef{AppendMissingState: &plan.AppendMissingState{FormatVersion: 99}}
	require.Error(t, validateM0CopyExitTarget(source, &plan.TableDef{}))
}

func TestM0BirthCopyExitTargetLayout(t *testing.T) {
	key := &plan.ColDef{Name: "__mo_cpkey_col", ColId: 1, Seqnum: 0, Hidden: true, Primary: true, Typ: plan.Type{Id: 20}}
	suffix := &plan.ColDef{Name: "flag", ColId: 2, Seqnum: 1, Typ: plan.Type{Id: 10}, Default: &plan.Default{NullAbility: true}}
	rowid := &plan.ColDef{Name: "__mo_rowid", ColId: 3, Seqnum: 2, Hidden: true}
	source := &plan.TableDef{Version: 1, Pkey: &plan.PrimaryKeyDef{CompPkeyCol: key}, Cols: []*plan.ColDef{key, suffix, rowid},
		AppendMissingState: &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"), BaseWidth: 1, PkSeqnum: 0,
			Columns: []*plan.ColumnBirthFill{{Seqnum: 1, BirthAppendOrdinal: 1, Type: suffix.Typ, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1}}}}
	require.NoError(t, plan.ValidateAppendMissingTable(source))
	target := &plan.TableDef{Cols: []*plan.ColDef{{Name: "flag", ColId: 1, Seqnum: 0}}}
	require.Error(t, validateM0CopyExitTarget(source, target))
	target.Cols = append(target.Cols, &plan.ColDef{Name: "__mo_rowid", Hidden: true})
	require.NoError(t, validateM0CopyExitTarget(source, target))
	target.Version = 1
	require.Error(t, validateM0CopyExitTarget(source, target))
	target.Version = 0
	target.Cols[0].Seqnum = 1
	require.Error(t, validateM0CopyExitTarget(source, target))
	target.Cols[0].Seqnum = 0
	target.AppendMissingState = source.AppendMissingState
	require.Error(t, validateM0CopyExitTarget(source, target))
}
