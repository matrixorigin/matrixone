// Copyright 2022 Matrix Origin
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

package sqlexec

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

// mockMessage is a mock Message type to test continue branch
type mockMessage struct {
	tag int32
}

func (m mockMessage) Serialize() []byte {
	return nil
}

func (m mockMessage) Deserialize([]byte) message.Message {
	return nil
}

func (m mockMessage) NeedBlock() bool {
	return false
}

func (m mockMessage) GetMsgTag() int32 {
	return m.tag
}

func (m mockMessage) GetReceiverAddr() message.MessageAddress {
	return message.AddrBroadCastOnCurrentCN()
}

func (m mockMessage) DebugString() string {
	return "mock message"
}

func (m mockMessage) Destroy() {
}

func TestWaitBloomFilterForTableFunction(t *testing.T) {
	t.Run("return nil when RuntimeFilterSpecs is empty", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		sqlproc := NewSqlProcess(proc)
		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{}
		result, err := WaitBloomFilter(sqlproc)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("return nil when Proc is nil", func(t *testing.T) {
		sqlproc := &SqlProcess{}
		result, err := WaitBloomFilter(sqlproc)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("return nil when UseBloomFilter is false", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		sqlproc := NewSqlProcess(proc)
		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
			{
				Tag:            1,
				UseBloomFilter: false,
			},
		}
		result, err := WaitBloomFilter(sqlproc)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("return data when BLOOMFILTER message found", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		mb := message.NewMessageBoard()
		proc.SetMessageBoard(mb)
		sqlproc := NewSqlProcess(proc)

		tag := int32(100)
		expectedData := []byte{1, 2, 3, 4, 5}

		rtMsg := message.RuntimeFilterMessage{
			Tag:  tag,
			Typ:  message.RuntimeFilter_BLOOMFILTER,
			Data: expectedData,
		}
		message.SendMessage(rtMsg, mb)

		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
			{
				Tag:            tag,
				UseBloomFilter: true,
			},
		}

		done := make(chan bool)
		var result []byte
		var err error
		go func() {
			result, err = WaitBloomFilter(sqlproc)
			done <- true
		}()

		<-done

		require.NoError(t, err)
		require.Equal(t, expectedData, result)
	})

	t.Run("return nil when no matching message found", func(t *testing.T) {
		testProc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		mb := message.NewMessageBoard()
		testProc.SetMessageBoard(mb)
		sqlproc := NewSqlProcess(testProc)

		tag := int32(200)

		// Send a message with different tag
		rtMsg := message.RuntimeFilterMessage{
			Tag:  tag + 1, // different tag
			Typ:  message.RuntimeFilter_BLOOMFILTER,
			Data: []byte{1, 2, 3},
		}
		message.SendMessage(rtMsg, mb)

		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
			{
				Tag:            tag,
				UseBloomFilter: true,
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		testProc.Ctx = ctx

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		done := make(chan bool)
		var result []byte
		var err error
		go func() {
			result, err = WaitBloomFilter(sqlproc)
			done <- true
		}()

		<-done
		cancel()

		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("continue when message is not RuntimeFilterMessage", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		mb := message.NewMessageBoard()
		proc.SetMessageBoard(mb)
		sqlproc := NewSqlProcess(proc)

		tag := int32(300)
		expectedData := []byte{6, 7, 8, 9, 10}

		// Send a mock message (not RuntimeFilterMessage)
		mockMsg := mockMessage{tag: tag}
		message.SendMessage(mockMsg, mb)

		// Also send a valid BLOOMFILTER message
		rtMsg := message.RuntimeFilterMessage{
			Tag:  tag,
			Typ:  message.RuntimeFilter_BLOOMFILTER,
			Data: expectedData,
		}
		message.SendMessage(rtMsg, mb)

		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
			{
				Tag:            tag,
				UseBloomFilter: true,
			},
		}

		done := make(chan bool)
		var result []byte
		var err error
		go func() {
			result, err = WaitBloomFilter(sqlproc)
			done <- true
		}()

		<-done

		// Should skip the mock message and return the BLOOMFILTER data
		require.NoError(t, err)
		require.Equal(t, expectedData, result)
	})

	t.Run("skip non-BLOOMFILTER runtime filter types", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		mb := message.NewMessageBoard()
		proc.SetMessageBoard(mb)
		sqlproc := NewSqlProcess(proc)

		tag := int32(700)
		expectedData := []byte{11, 12, 13}

		// Send a PASS type message first (should be skipped)
		passMsg := message.RuntimeFilterMessage{
			Tag: tag,
			Typ: message.RuntimeFilter_PASS,
		}
		message.SendMessage(passMsg, mb)

		// Then send a valid BLOOMFILTER message
		rtMsg := message.RuntimeFilterMessage{
			Tag:  tag,
			Typ:  message.RuntimeFilter_BLOOMFILTER,
			Data: expectedData,
		}
		message.SendMessage(rtMsg, mb)

		sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{
			{
				Tag:            tag,
				UseBloomFilter: true,
			},
		}

		done := make(chan bool)
		var result []byte
		var err error
		go func() {
			result, err = WaitBloomFilter(sqlproc)
			done <- true
		}()

		<-done

		// Should skip the PASS message and return the BLOOMFILTER data
		require.NoError(t, err)
		require.Equal(t, expectedData, result)
	})
}

func TestBuildExactPkFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	ctx := context.Background()

	t.Run("int64 values", func(t *testing.T) {
		vec := vector.NewVec(types.New(types.T_int64, 8, 0))
		vector.AppendFixed(vec, int64(10), false, proc.Mp())
		vector.AppendFixed(vec, int64(20), false, proc.Mp())
		vector.AppendFixed(vec, int64(30), false, proc.Mp())

		result, err := BuildExactPkFilter(ctx, vec)
		require.NoError(t, err)
		require.Equal(t, "10,20,30", result)
	})

	t.Run("empty vector", func(t *testing.T) {
		vec := vector.NewVec(types.New(types.T_int64, 8, 0))

		result, err := BuildExactPkFilter(ctx, vec)
		require.NoError(t, err)
		require.Equal(t, "", result)
	})

	t.Run("single value", func(t *testing.T) {
		vec := vector.NewVec(types.New(types.T_int64, 8, 0))
		vector.AppendFixed(vec, int64(99), false, proc.Mp())

		result, err := BuildExactPkFilter(ctx, vec)
		require.NoError(t, err)
		require.Equal(t, "99", result)
	})

	t.Run("varchar values", func(t *testing.T) {
		vec := vector.NewVec(types.New(types.T_varchar, 128, 0))
		vector.AppendBytes(vec, []byte("hello"), false, proc.Mp())
		vector.AppendBytes(vec, []byte("world"), false, proc.Mp())

		result, err := BuildExactPkFilter(ctx, vec)
		require.NoError(t, err)
		require.Equal(t, "'hello','world'", result)
	})

	t.Run("with null values", func(t *testing.T) {
		vec := vector.NewVec(types.New(types.T_int64, 8, 0))
		vector.AppendFixed(vec, int64(10), false, proc.Mp())
		vector.AppendFixed(vec, int64(0), true, proc.Mp()) // null
		vector.AppendFixed(vec, int64(30), false, proc.Mp())

		result, err := BuildExactPkFilter(ctx, vec)
		require.NoError(t, err)
		require.Equal(t, "10,30", result)
	})
}

func TestAppendUint64(t *testing.T) {
	buf := []byte("prefix:")
	buf = AppendUint64(buf, 12345)
	require.Equal(t, "prefix:12345", string(buf))
}

func TestAppendInt64(t *testing.T) {
	buf := AppendInt64(nil, -42)
	require.Equal(t, "-42", string(buf))

	buf = AppendInt64(nil, 0)
	require.Equal(t, "0", string(buf))
}

func TestAppendFloat64(t *testing.T) {
	cases := []struct {
		val      float64
		bitSize  int
		expected string
	}{
		{1.23, 64, "1.23"},
		{math.Inf(1), 64, "+Infinity"},
		{math.Inf(-1), 64, "-Infinity"},
		{3.1415926, 32, "3.1415925"}, // float32 precision
	}

	for _, c := range cases {
		buf := AppendFloat64(nil, c.val, c.bitSize)
		require.Equal(t, c.expected, string(buf))
	}
}

func TestAppendHex(t *testing.T) {
	buf := AppendHex(nil, []byte{0x01, 0x02, 0xAF})
	require.Equal(t, "x'0102af'", string(buf))
}

func TestAppendVectorSQLLiteral(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	ctx := context.Background()

	t.Run("bool", func(t *testing.T) {
		vec := testutil.MakeBoolVector([]bool{true, false}, nil, proc.Mp())
		buf, err := AppendVectorSQLLiteral(ctx, vec, 0, nil)
		require.NoError(t, err)
		require.Equal(t, "true", string(buf))

		buf, err = AppendVectorSQLLiteral(ctx, vec, 1, nil)
		require.NoError(t, err)
		require.Equal(t, "false", string(buf))
	})

	t.Run("bit", func(t *testing.T) {
		vec := vector.NewVec(types.New(types.T_bit, 10, 0))
		vector.AppendFixed(vec, uint64(0x3FF), false, proc.Mp())
		buf, err := AppendVectorSQLLiteral(ctx, vec, 0, nil)
		require.NoError(t, err)
		// 10 bits is 2 bytes. 0x3FF -> [0x03, 0xFF] (big endian encoded)
		// EncodeUint64 gives [0xFF, 0x03, 0x00...] so slice to 2 bytes is [0xFF, 0x03], reversed is [0x03, 0xFF]
		require.Equal(t, "'\x03\xff'", string(buf))
	})

	t.Run("ints", func(t *testing.T) {
		vec8 := testutil.MakeInt8Vector([]int8{-8}, nil, proc.Mp())
		buf, _ := AppendVectorSQLLiteral(ctx, vec8, 0, nil)
		require.Equal(t, "-8", string(buf))

		vec16 := testutil.MakeInt16Vector([]int16{-16}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vec16, 0, nil)
		require.Equal(t, "-16", string(buf))

		vec32 := testutil.MakeInt32Vector([]int32{-32}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vec32, 0, nil)
		require.Equal(t, "-32", string(buf))

		vec64 := testutil.MakeInt64Vector([]int64{-64}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vec64, 0, nil)
		require.Equal(t, "-64", string(buf))
	})

	t.Run("uints", func(t *testing.T) {
		vec8 := testutil.MakeUint8Vector([]uint8{8}, nil, proc.Mp())
		buf, _ := AppendVectorSQLLiteral(ctx, vec8, 0, nil)
		require.Equal(t, "8", string(buf))

		vec16 := testutil.MakeUint16Vector([]uint16{16}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vec16, 0, nil)
		require.Equal(t, "16", string(buf))

		vec32 := testutil.MakeUint32Vector([]uint32{32}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vec32, 0, nil)
		require.Equal(t, "32", string(buf))

		vec64 := testutil.MakeUint64Vector([]uint64{64}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vec64, 0, nil)
		require.Equal(t, "64", string(buf))
	})

	t.Run("floats", func(t *testing.T) {
		vec32 := testutil.MakeFloat32Vector([]float32{1.5}, nil, proc.Mp())
		buf, _ := AppendVectorSQLLiteral(ctx, vec32, 0, nil)
		require.Equal(t, "1.5", string(buf))

		vec64 := testutil.MakeFloat64Vector([]float64{2.5}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vec64, 0, nil)
		require.Equal(t, "2.5", string(buf))
	})

	t.Run("binary", func(t *testing.T) {
		vec := vector.NewVec(types.T_binary.ToType())
		vector.AppendBytes(vec, []byte{0xAB, 0xCD}, false, proc.Mp())
		buf, _ := AppendVectorSQLLiteral(ctx, vec, 0, nil)
		require.Equal(t, "x'abcd'", string(buf))
	})

	t.Run("string types", func(t *testing.T) {
		vec := testutil.MakeVarcharVector([]string{"it's a \\ test"}, nil, proc.Mp())
		buf, _ := AppendVectorSQLLiteral(ctx, vec, 0, nil)
		// escape ' to \' and \ to \\
		require.Equal(t, "'it\\'s a \\\\ test'", string(buf))
	})

	t.Run("date types", func(t *testing.T) {
		vec := testutil.MakeDateVector([]string{"2023-01-01"}, nil, proc.Mp())
		buf, _ := AppendVectorSQLLiteral(ctx, vec, 0, nil)
		require.Equal(t, "'2023-01-01'", string(buf))

		vecDt := testutil.MakeDatetimeVector([]string{"2023-01-01 12:00:00"}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vecDt, 0, nil)
		require.Equal(t, "'2023-01-01 12:00:00'", string(buf))

		vecTm := testutil.MakeTimeVector([]string{"2023-01-01 12:00:00"}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vecTm, 0, nil)
		require.Equal(t, "'12:00:00'", string(buf))

		vecTs := testutil.MakeTimestampVector([]string{"2023-01-01 12:00:00"}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vecTs, 0, nil)
		require.Equal(t, "'2023-01-01 12:00:00'", string(buf))
	})

	t.Run("decimal", func(t *testing.T) {
		d64, _ := types.Decimal64FromFloat64(1.23, 10, 2)
		vec64 := vector.NewVec(types.New(types.T_decimal64, 10, 2))
		vector.AppendFixed(vec64, d64, false, proc.Mp())
		buf, _ := AppendVectorSQLLiteral(ctx, vec64, 0, nil)
		require.Equal(t, "'1.23'", string(buf))

		d128, _ := types.Decimal128FromFloat64(1.23, 10, 2)
		vec128 := vector.NewVec(types.New(types.T_decimal128, 10, 2))
		vector.AppendFixed(vec128, d128, false, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vec128, 0, nil)
		require.Equal(t, "'1.23'", string(buf))
	})

	t.Run("uuid and others", func(t *testing.T) {
		uid, _ := types.ParseUuid("12345678-1234-5678-1234-567812345678")
		vecUuid := testutil.MakeUUIDVector([]types.Uuid{uid}, nil, proc.Mp())
		buf, _ := AppendVectorSQLLiteral(ctx, vecUuid, 0, nil)
		require.Equal(t, "'12345678-1234-5678-1234-567812345678'", string(buf))

		rowId := types.Rowid{}
		rowId[0] = 1
		vecRowId := testutil.MakeRowIdVector([]types.Rowid{rowId}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vecRowId, 0, nil)
		require.Equal(t, "'01000000-0000-0000-0000-000000000000-0-0-0'", string(buf))

		blockId := types.Blockid{}
		blockId[0] = 2
		vecBlockId := testutil.MakeBlockIdVector([]types.Blockid{blockId}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vecBlockId, 0, nil)
		require.Equal(t, "'02000000-0000-0000-0000-000000000000-0-0'", string(buf))

		ts := types.TS{}
		ts[4] = 3
		vecTs := testutil.MakeTSVector([]types.TS{ts}, nil, proc.Mp())
		buf, _ = AppendVectorSQLLiteral(ctx, vecTs, 0, nil)
		require.Equal(t, "'3-0'", string(buf))
	})

	t.Run("enum", func(t *testing.T) {
		vec := vector.NewVec(types.T_enum.ToType())
		vector.AppendFixed(vec, types.Enum(1), false, proc.Mp())
		buf, _ := AppendVectorSQLLiteral(ctx, vec, 0, nil)
		require.Equal(t, "'1'", string(buf))
	})

	t.Run("unsupported", func(t *testing.T) {
		vec := vector.NewVec(types.T_any.ToType())
		buf, err := AppendVectorSQLLiteral(ctx, vec, 0, nil)
		require.Error(t, err)
		require.Nil(t, buf)
	})
}
