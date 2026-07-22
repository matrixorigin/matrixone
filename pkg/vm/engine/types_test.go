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

package engine

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPlanDefToCstrDefPersistsCheckConstraintsOutsideUserProperties(t *testing.T) {
	check := &plan.CheckDef{
		Name:            "__mo_chk_1",
		IsGeneratedName: true,
		NotEnforced:     true,
		Check: &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: true},
			}},
		},
	}
	userProperty := &plan.Property{
		Key:   "__mo_check_constraints",
		Value: "mo_check_constraints_v1:<user-value>",
	}
	tableDef := &plan.TableDef{
		Checks: []*plan.CheckDef{check},
		Defs: []*plan.TableDef_DefType{{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: []*plan.Property{userProperty}},
			},
		}},
	}

	cstr, err := PlanDefToCstrDef(tableDef)
	require.NoError(t, err)
	require.Len(t, cstr.Cts, 2)
	require.Equal(t, userProperty, cstr.Cts[0].(*StreamConfigsDef).Configs[0])
	require.Equal(t, check, cstr.Cts[1].(*CheckConstraintsDef).Checks[0])

	data, err := cstr.MarshalBinary()
	require.NoError(t, err)
	decoded := &ConstraintDef{}
	require.NoError(t, decoded.UnmarshalBinary(data))
	require.Len(t, decoded.Cts, 2)
	require.Equal(t, userProperty, decoded.Cts[0].(*StreamConfigsDef).Configs[0])
	decodedCheck := decoded.Cts[1].(*CheckConstraintsDef).Checks[0]
	require.Equal(t, check.Name, decodedCheck.Name)
	require.True(t, decodedCheck.IsGeneratedName)
	require.True(t, decodedCheck.NotEnforced)
	require.NotNil(t, decodedCheck.Check)
}

func TestCheckConstraintsDefPBConversion(t *testing.T) {
	def := &CheckConstraintsDef{Checks: []*plan.CheckDef{{Name: "chk"}}}
	pbDef := def.ToPBVersion()
	require.Equal(t, def, pbDef.FromPBVersion())
}

func TestCheckConstraintsDefCorruptionFailsClosed(t *testing.T) {
	data := []byte{byte(CheckConstraint)}
	data = binary.BigEndian.AppendUint64(data, 1)
	data = binary.BigEndian.AppendUint64(data, 10)
	data = append(data, 0xff)

	err := (&ConstraintDef{}).UnmarshalBinary(data)
	require.True(t, errors.Is(err, io.ErrUnexpectedEOF))

	hugeCount := []byte{byte(CheckConstraint)}
	hugeCount = binary.BigEndian.AppendUint64(hugeCount, ^uint64(0))
	err = (&ConstraintDef{}).UnmarshalBinary(hugeCount)
	require.True(t, errors.Is(err, io.ErrUnexpectedEOF))
}
