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

package productl2

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// stubSql records the statements the operator runs and answers a COUNT(*) with count.
func stubSql(t *testing.T, mp *mpool.MPool, count int64, sqls *[]string) func() {
	old := productl2_runSql
	productl2_runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		*sqls = append(*sqls, sql)
		if !strings.HasPrefix(sql, "SELECT COUNT(*)") {
			return executor.Result{}, nil
		}
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], count, false, mp))
		bat.SetRowCount(1)
		return executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}, nil
	}
	return func() { productl2_runSql = old }
}

// probeBatch returns a batch of an int32 primary key and a vecf32(1) column.
func probeBatch(t *testing.T, mp *mpool.MPool, pks []int32, vecs [][]float32, nulls []bool) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 1, 0))
	for i := range pks {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], pks[i], false, mp))
		require.NoError(t, vector.AppendArray(bat.Vecs[1], vecs[i], nulls[i], mp))
	}
	bat.SetRowCount(len(pks))
	return bat
}

func TestLeaveOutRecordsUnstorableRows(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	var sqls []string
	defer stubSql(t, mp, 2, &sqls)()

	ap := &Productl2{Ignored: &Ignored{
		Storage: ivfflat.NewStorage(types.T_array_float32, "float16", 0, 0, false),
		Db:      "db", Table: "ign", Pk: 0, Limit: 2,
	}}
	ctr := &ap.ctr
	ctr.sqlproc = sqlexec.NewSqlProcess(proc)
	ctr.inBat = probeBatch(t, mp, []int32{1, 2, 3, 4}, [][]float32{{1}, {70000}, {0}, {-80000}}, []bool{false, false, true, false})

	out, err := ctr.leaveOut(ap, proc, ctr.inBat.Vecs[1])
	require.NoError(t, err)
	require.Equal(t, []bool{false, true, false, true}, out, "the NULL row 3 is not checked")
	require.Len(t, sqls, 1)
	require.Contains(t, sqls[0], "REPLACE INTO `db`.`ign`")
	require.Contains(t, sqls[0], "(2, 20348, NULL, 0), (4, 20348, NULL, 0)")
	require.Equal(t, int64(2), ctr.ignored)

	require.NoError(t, ctr.endIgnored(ap), "2 rows within the limit of 2")
	require.Equal(t, int64(0), ctr.ignored)
	require.Contains(t, sqls[1], "SELECT COUNT(*) FROM `db`.`ign`")
}

func TestLeaveOutPastTheLimit(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	var sqls []string
	defer stubSql(t, mp, 3, &sqls)()

	ap := &Productl2{Ignored: &Ignored{
		Storage: ivfflat.NewStorage(types.T_array_float32, "float16", 0, 0, false),
		Db:      "db", Table: "ign", Pk: 0, Limit: 2,
	}}
	ctr := &ap.ctr
	ctr.sqlproc = sqlexec.NewSqlProcess(proc)
	ctr.inBat = probeBatch(t, mp, []int32{7}, [][]float32{{70000}}, []bool{false})
	_, err := ctr.leaveOut(ap, proc, ctr.inBat.Vecs[1])
	require.NoError(t, err)
	err = ctr.endIgnored(ap)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrIndexTooManyBadRecords), "%v", err)
	require.Contains(t, err.Error(), "row 7")
}

// Without an ignored table, or a primary key to record, an unstorable row fails the operator
// with its reason; a storable batch is not touched.
func TestLeaveOutWithoutTable(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	var sqls []string
	defer stubSql(t, mp, 0, &sqls)()

	storage := ivfflat.NewStorage(types.T_array_float32, "float16", 0, 0, false)
	for _, ig := range []*Ignored{{Storage: storage, Pk: 0}, {Storage: storage, Db: "db", Table: "ign", Pk: -1}} {
		ap := &Productl2{Ignored: ig}
		ctr := &ap.ctr
		ctr.sqlproc = sqlexec.NewSqlProcess(proc)
		ctr.inBat = probeBatch(t, mp, []int32{1, 2}, [][]float32{{1}, {70000}}, []bool{false, false})
		_, err := ctr.leaveOut(ap, proc, ctr.inBat.Vecs[1])
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrIndexVectorOutOfRange), "%v", err)
	}

	ap := &Productl2{Ignored: &Ignored{Storage: storage, Db: "db", Table: "ign", Pk: 0}}
	ap.ctr.sqlproc = sqlexec.NewSqlProcess(proc)
	ap.ctr.inBat = probeBatch(t, mp, []int32{1}, [][]float32{{1}}, []bool{false})
	out, err := ap.ctr.leaveOut(ap, proc, ap.ctr.inBat.Vecs[1])
	require.NoError(t, err)
	require.Nil(t, out)
	require.NoError(t, ap.ctr.endIgnored(ap))
	require.Empty(t, sqls, "nothing recorded, nothing counted")

	out, err = (&container{}).leaveOut(&Productl2{}, proc, ap.ctr.inBat.Vecs[1])
	require.NoError(t, err)
	require.Nil(t, out, "no index on the build side")
}

func TestPkLiteral(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, []byte{0x01, 'x', '\''}, false, mp))
	lit, err := (&Ignored{Serial: true}).pkLiteral(proc, vec, 0)
	require.NoError(t, err)
	require.Equal(t, "X'017827'", lit)
	lit, err = (&Ignored{}).pkLiteral(proc, vec, 0)
	require.NoError(t, err)
	require.Equal(t, `'\u0001x\''`, strings.ReplaceAll(lit, "\x01", `\u0001`))

	// a composite key reads as the hex literal of its serial
	p := types.NewPacker()
	defer p.Close()
	p.EncodeInt64(1)
	p.EncodeStringType([]byte("x"))
	serial := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(serial, p.GetBuf(), false, mp))
	ig := &Ignored{Serial: true}
	lit, err = ig.pkLiteral(proc, serial, 0)
	require.NoError(t, err)
	require.Equal(t, vectorindex.SerialPkString(p.GetBuf()), lit)
	require.Equal(t, lit, ig.pkString(serial, 0, lit))
	require.Equal(t, "7", (&Ignored{}).pkString(serial, 0, "7"))

	// an ENUM key is its stored number, whether the probe carries the value or its serial
	ig = &Ignored{Enum: true, EnumValues: "a,b,c"}
	enum := vector.NewVec(types.T_enum.ToType())
	require.NoError(t, vector.AppendFixed(enum, types.Enum(2), false, mp))
	require.NoError(t, vector.AppendFixed(enum, types.Enum(0), false, mp))
	lit, err = ig.pkLiteral(proc, enum, 0)
	require.NoError(t, err)
	require.Equal(t, "SERIAL_EXTRACT(X'503d1502', 0 AS ENUM('a','b','c'))", lit)
	require.Equal(t, "b", ig.pkString(enum, 0, lit), "users read the member")
	require.Equal(t, "0", ig.pkString(enum, 1, ""), "no such member")
	p.Reset()
	p.EncodeEnum(types.Enum(3))
	enumSerial := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(enumSerial, p.GetBuf(), false, mp))
	lit, err = ig.pkLiteral(proc, enumSerial, 0)
	require.NoError(t, err)
	require.Equal(t, "SERIAL_EXTRACT(X'503d1503', 0 AS ENUM('a','b','c'))", lit)
	_, err = ig.pkLiteral(proc, serial, 0)
	require.Error(t, err, "a serial of other parts")
	_, err = ig.pkLiteral(proc, vec, 0)
	require.Error(t, err, "no serial")
	for _, v := range []*vector.Vector{vec, serial, enum, enumSerial} {
		v.Free(mp)
	}
}

func TestEncodeIgnored(t *testing.T) {
	s, err := EncodeIgnored(nil)
	require.NoError(t, err)
	require.Empty(t, s)
	ig, err := DecodeIgnored("")
	require.NoError(t, err)
	require.Nil(t, ig)

	in := &Ignored{Storage: ivfflat.NewStorage(types.T_array_float64, "int8", -1, 1, true), Db: "db", Table: "t", Pk: 3, Serial: true, Limit: 5}
	s, err = EncodeIgnored(in)
	require.NoError(t, err)
	out, err := DecodeIgnored(s)
	require.NoError(t, err)
	require.Equal(t, in, out)

	_, err = DecodeIgnored("{")
	require.Error(t, err)
}
