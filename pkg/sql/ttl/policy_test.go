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

package ttl

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestTTLDurationMatrix(t *testing.T) {
	for _, tc := range []struct {
		unit   string
		factor uint64
	}{{"SECOND", 1}, {"MINUTE", 60}, {"HOUR", 3600}, {"DAY", 86400}, {"WEEK", 604800}} {
		for _, amount := range []uint64{0, 1, 2, 17, MaxDurationSeconds / tc.factor, MaxDurationSeconds/tc.factor + 1, math.MaxUint64} {
			t.Run(fmt.Sprintf("%s_%d", tc.unit, amount), func(t *testing.T) {
				seconds, err := Duration(amount, tc.unit)
				if amount == 0 || amount > MaxDurationSeconds/tc.factor {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, amount*tc.factor, seconds)
				}
			})
		}
	}
	for _, unit := range []string{"MONTH", "YEAR", "QUARTER", "MICROSECOND", "DAY_SECOND", "", "BOGUS"} {
		_, err := Duration(1, unit)
		require.Error(t, err)
	}
	seconds, err := Duration(3, "day")
	require.NoError(t, err)
	require.Equal(t, uint64(259200), seconds)
}
func TestTTLMetadataPersistenceAndValidation(t *testing.T) {
	p := &Policy{Version: 1, Column: "created_at", Seqnum: 1, Seconds: 86400}
	def := &plan.TableDef{Cols: []*plan.ColDef{{Name: "created_at", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_datetime)}}}}
	Set(def, p)
	got, err := Get(def)
	require.NoError(t, err)
	require.Equal(t, p, got)
	require.NoError(t, ValidateColumn(def, got))
	wire, err := def.Marshal()
	require.NoError(t, err)
	var decoded plan.TableDef
	require.NoError(t, decoded.Unmarshal(wire))
	got, err = Get(&decoded)
	require.NoError(t, err)
	require.Equal(t, p, got)
	c := engine.PlanDefToCstrDef(def)
	data, err := c.MarshalBinary()
	require.NoError(t, err)
	got, err = PolicyFromConstraint(data)
	require.NoError(t, err)
	require.Equal(t, p, got)
	Set(def, nil)
	got, err = Get(def)
	require.NoError(t, err)
	require.Nil(t, got)
	def.Cols[0].Seqnum = 2
	require.Error(t, ValidateColumn(def, p))
	def.Cols[0].Seqnum = 1
	def.Cols[0].Hidden = true
	require.Error(t, ValidateColumn(def, p))
	for _, metadata := range []string{"", "null", "{}", "[]", "not json", `{"version":2,"column":"a","seconds":1}`, `{"version":1,"column":"a","seconds":0}`, `{"version":1,"column":"a","seconds":3153600001}`, `{"version":1,"column":"a","seconds":1,"seqnum":65536}`, `{"version":1,"column":"a","seconds":1,"where":"id=1"}`, `{"version":1,"column":"a","seconds":1} {}`} {
		_, err = Decode(metadata)
		require.Error(t, err, metadata)
	}
	_, err = FromProperties([]*plan.Property{{Key: PropertyKey, Value: Encode(p)}, {Key: PropertyKey, Value: Encode(p)}})
	require.Error(t, err)
	_, err = PolicyFromConstraint([]byte{byte(engine.StreamConfig)})
	require.Error(t, err)
}
func TestTTLDeleteSQLBoundaryAndQuoting(t *testing.T) {
	now := time.Date(2026, 3, 8, 1, 0, 0, 123456000, time.UTC)
	p := &Policy{Version: 1, Column: "a`b", Seconds: 1}
	sql, err := DeleteSQL("db`name", "tbl`name", p, now, 19)
	require.NoError(t, err)
	require.Equal(t, "DELETE FROM `db``name`.`tbl``name` WHERE CAST(`a``b` AS DATETIME(6)) <= CAST('2026-03-08 00:59:59.123456' AS DATETIME(6)) LIMIT 19", sql)
	local := now.In(time.FixedZone("offset", 8*3600))
	sql2, err := DeleteSQL("db`name", "tbl`name", p, local, 19)
	require.NoError(t, err)
	require.Equal(t, sql, sql2)
	_, err = DeleteSQL("d", "t", p, now, 0)
	require.Error(t, err)
	_, err = DeleteSQL("d", "t", p, time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), 1)
	require.Error(t, err)
}

// A deterministic independent arithmetic oracle verifies the cutoff rewrite
// across 20,000 randomized row ages, durations and microsecond boundaries.
func TestTTLCutoffRandomizedOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	now := time.Date(2026, 10, 13, 4, 0, 0, 123456000, time.UTC)
	for i := 0; i < 20000; i++ {
		seconds := uint64(1 + rng.Int63n(int64(MaxDurationSeconds)))
		cutoff := now.Add(-time.Duration(seconds) * time.Second)
		delta := time.Duration(rng.Int63n(2000001)-1000000) * time.Microsecond
		row := cutoff.Add(delta)
		expected := !row.Add(time.Duration(seconds) * time.Second).After(now)
		actual := !row.After(cutoff)
		require.Equal(t, expected, actual, "case %d", i)
		_, err := DeleteSQL("d", "t", &Policy{Version: 1, Column: "ts", Seconds: seconds}, now, 1)
		require.NoError(t, err)
	}
}

func TestTTLConstraintMalformedMatrix(t *testing.T) {
	rng := rand.New(rand.NewSource(2026))
	for i := 0; i < 10000; i++ {
		data := make([]byte, rng.Intn(256))
		_, err := rng.Read(data)
		require.NoError(t, err)
		policy, err := PolicyFromConstraint(data)
		if err == nil && policy != nil {
			require.NoError(t, policy.Validate())
		}
	}
	for _, kind := range []engine.ConstraintType{engine.Index, engine.RefChildTable, engine.ForeignKey, engine.PrimaryKey, engine.StreamConfig} {
		huge := []byte{byte(kind), 255, 255, 255, 255, 255, 255, 255, 255}
		_, err := PolicyFromConstraint(huge)
		require.Error(t, err)
	}
}
func FuzzTTLConstraintDecode(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{byte(engine.StreamConfig)})
	f.Add([]byte{byte(engine.StreamConfig), 255, 255, 255, 255, 255, 255, 255, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		policy, err := PolicyFromConstraint(data)
		if err == nil && policy != nil {
			require.NoError(t, policy.Validate())
		}
	})
}
