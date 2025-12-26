// Copyright 2025 Matrix Origin
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
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFormatValIntoString_StringEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := "a'b\"c\\\n\t\r\x1a\x00"
	require.NoError(t, formatValIntoString(ses, val, types.New(types.T_varchar, 0, 0), &buf))
	require.Equal(t, `'a\'b"c\\\n\t\r\Z\0'`, buf.String())
}

func TestFormatValIntoString_ByteEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := []byte{'x', 0x00, '\\', 0x07, '\''}
	require.NoError(t, formatValIntoString(ses, val, types.New(types.T_varbinary, 0, 0), &buf))
	require.Equal(t, `'x\0\\\x07\''`, buf.String())
}

func TestFormatValIntoString_Time(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val, err := types.ParseTime("12:34:56.123456", 6)
	require.NoError(t, err)

	require.NoError(t, formatValIntoString(ses, val, types.New(types.T_time, 0, 6), &buf))
	require.Equal(t, `'12:34:56.123456'`, buf.String())
}

func TestFormatValIntoString_JSONEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := `{"k":"` + string([]byte{0x01, '\n'}) + `"}`
	require.NoError(t, formatValIntoString(ses, val, types.New(types.T_json, 0, 0), &buf))
	require.Equal(t, `'{"k":"\\u0001\\u000a"}'`, buf.String())
}

func TestFormatValIntoString_JSONByteJson(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	bj, err := types.ParseStringToByteJson(`{"a":1}`)
	require.NoError(t, err)

	require.NoError(t, formatValIntoString(ses, bj, types.New(types.T_json, 0, 0), &buf))
	require.Equal(t, `'{"a": 1}'`, buf.String())
}

func TestFormatValIntoString_Nil(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	require.NoError(t, formatValIntoString(ses, nil, types.New(types.T_varchar, 0, 0), &buf))
	require.Equal(t, "NULL", buf.String())
}

func TestFormatValIntoString_UnsupportedType(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	err := formatValIntoString(ses, true, types.New(types.T_Rowid, 0, 0), &buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not support type")
}

func TestCompareSingleValInVector_AllTypes(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name  string
		build func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int)
	}{
		{
			name: "json",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_json.ToType()
				leftVec := vector.NewVec(typ)
				rightVec := vector.NewVec(typ)
				leftVal, err := types.ParseStringToByteJson(`{"k":1}`)
				require.NoError(t, err)
				rightVal, err := types.ParseStringToByteJson(`{"k":2}`)
				require.NoError(t, err)
				leftBytes, err := leftVal.Marshal()
				require.NoError(t, err)
				rightBytes, err := rightVal.Marshal()
				require.NoError(t, err)
				require.NoError(t, vector.AppendBytes(leftVec, leftBytes, false, mp))
				require.NoError(t, vector.AppendBytes(rightVec, rightBytes, false, mp))
				expected := types.CompareValue(types.DecodeJson(leftBytes), types.DecodeJson(rightBytes))
				return leftVec, rightVec, expected
			},
		},
		{
			name: "bool",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_bool.ToType()
				leftVal, rightVal := false, true
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "bit",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_bit.ToType()
				leftVal, rightVal := uint64(1), uint64(3)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int8",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int8.ToType()
				leftVal, rightVal := int8(1), int8(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint8",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint8.ToType()
				leftVal, rightVal := uint8(1), uint8(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int16",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int16.ToType()
				leftVal, rightVal := int16(1), int16(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint16",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint16.ToType()
				leftVal, rightVal := uint16(1), uint16(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int32.ToType()
				leftVal, rightVal := int32(1), int32(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint32.ToType()
				leftVal, rightVal := uint32(1), uint32(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int64.ToType()
				leftVal, rightVal := int64(1), int64(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint64.ToType()
				leftVal, rightVal := uint64(1), uint64(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "float32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_float32.ToType()
				leftVal, rightVal := float32(1.1), float32(2.2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "float64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_float64.ToType()
				leftVal, rightVal := float64(1.1), float64(2.2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "char",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_char, 8, 0)
				leftVal, rightVal := []byte("a"), []byte("b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "varchar",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_varchar, 16, 0)
				leftVal, rightVal := []byte("alpha"), []byte("beta")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "blob",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_blob.ToType()
				leftVal, rightVal := []byte("blob-a"), []byte("blob-b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "text",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_text.ToType()
				leftVal, rightVal := []byte("text-a"), []byte("text-b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "binary",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_binary, 4, 0)
				leftVal, rightVal := []byte{0x00, 0x01}, []byte{0x00, 0x02}
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "varbinary",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_varbinary, 8, 0)
				leftVal, rightVal := []byte{0x01, 0x02}, []byte{0x02, 0x03}
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "datalink",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_datalink.ToType()
				leftVal, rightVal := []byte("link-a"), []byte("link-b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "array_float32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_array_float32, 2, 0)
				leftVal := []float32{1, 2}
				rightVal := []float32{1, 3}
				leftVec := buildArrayVector(t, mp, typ, [][]float32{leftVal})
				rightVec := buildArrayVector(t, mp, typ, [][]float32{rightVal})
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "array_float64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_array_float64, 2, 0)
				leftVal := []float64{1, 2}
				rightVal := []float64{2, 3}
				leftVec := buildArrayVector(t, mp, typ, [][]float64{leftVal})
				rightVec := buildArrayVector(t, mp, typ, [][]float64{rightVal})
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "date",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_date.ToType()
				leftVal, rightVal := types.Date(1), types.Date(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "datetime",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_datetime.ToType()
				leftVal, rightVal := types.Datetime(1), types.Datetime(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "time",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_time.ToType()
				leftVal, rightVal := types.Time(1), types.Time(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "timestamp",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_timestamp.ToType()
				leftVal, rightVal := types.Timestamp(1), types.Timestamp(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "decimal64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_decimal64, 18, 2)
				leftVal, err := types.Decimal64FromFloat64(12.34, 18, 2)
				require.NoError(t, err)
				rightVal, err := types.Decimal64FromFloat64(23.45, 18, 2)
				require.NoError(t, err)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "decimal128",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_decimal128, 38, 4)
				leftVal, err := types.Decimal128FromFloat64(12.34, 38, 4)
				require.NoError(t, err)
				rightVal, err := types.Decimal128FromFloat64(23.45, 38, 4)
				require.NoError(t, err)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uuid",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uuid.ToType()
				var leftVal, rightVal types.Uuid
				copy(leftVal[:], []byte("aaaaaaaaaaaaaaaa"))
				copy(rightVal[:], []byte("bbbbbbbbbbbbbbbb"))
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "rowid",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_Rowid.ToType()
				var blk types.Blockid
				copy(blk[:], []byte("block-identifier"))
				leftVal := types.NewRowid(&blk, 1)
				rightVal := types.NewRowid(&blk, 2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "blockid",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_Blockid.ToType()
				var leftVal, rightVal types.Blockid
				copy(leftVal[:], []byte("block-00000000001"))
				copy(rightVal[:], []byte("block-00000000002"))
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "ts",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_TS.ToType()
				leftVal := types.BuildTS(1, 0)
				rightVal := types.BuildTS(2, 0)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "enum",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_enum.ToType()
				leftVal, rightVal := types.Enum(1), types.Enum(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leftVec, rightVec, expected := tc.build(t, mp)
			defer leftVec.Free(mp)
			defer rightVec.Free(mp)

			cmp, err := compareSingleValInVector(ctx, ses, 0, 0, leftVec, rightVec)
			require.NoError(t, err)
			require.Equal(t, expected, cmp)
		})
	}
}

func TestCompareSingleValInVector_ConstVectors(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	typ := types.T_int32.ToType()
	leftVec := buildFixedVector(t, mp, typ, int32(5))
	rightVec, err := vector.NewConstFixed[int32](typ, int32(7), 3, mp)
	require.NoError(t, err)
	defer leftVec.Free(mp)
	defer rightVec.Free(mp)

	cmp, err := compareSingleValInVector(ctx, ses, 0, 2, leftVec, rightVec)
	require.NoError(t, err)
	require.Equal(t, types.CompareValue(int32(5), int32(7)), cmp)
}

func buildFixedVector[T any](t *testing.T, mp *mpool.MPool, typ types.Type, vals ...T) *vector.Vector {
	vec := vector.NewVec(typ)
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	return vec
}

func buildBytesVector(t *testing.T, mp *mpool.MPool, typ types.Type, vals ...[]byte) *vector.Vector {
	vec := vector.NewVec(typ)
	for _, v := range vals {
		require.NoError(t, vector.AppendBytes(vec, v, false, mp))
	}
	return vec
}

func buildArrayVector[T types.RealNumbers](t *testing.T, mp *mpool.MPool, typ types.Type, vals [][]T) *vector.Vector {
	vec := vector.NewVec(typ)
	require.NoError(t, vector.AppendArrayList(vec, vals, nil, mp))
	return vec
}

func newValidateSession(t *testing.T) *Session {
	t.Helper()

	proc := testutil.NewProcess(t)
	service := "validate-output-dir"

	InitServerLevelVars(service)
	setPu(service, &config.ParameterUnit{
		SV:          &config.FrontendParameters{},
		FileService: proc.Base.FileService,
	})

	return &Session{
		feSessionImpl: feSessionImpl{service: service},
		proc:          proc,
	}
}

func TestValidateOutputDirPath(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)

	t.Run("empty path", func(t *testing.T) {
		require.NoError(t, validateOutputDirPath(ctx, ses, ""))
	})

	t.Run("local directory exists", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, validateOutputDirPath(ctx, ses, dir))
	})

	t.Run("local directory missing", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "missing")
		err := validateOutputDirPath(ctx, ses, dir)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("local path is file", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "file")
		require.NoError(t, err)
		require.NoError(t, f.Close())

		err = validateOutputDirPath(ctx, ses, f.Name())
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "not a directory")
	})

	t.Run("stage path through cache", func(t *testing.T) {
		stageDir := t.TempDir()
		stageURL, err := url.Parse("file://" + stageDir)
		require.NoError(t, err)
		stageName := "stage_local"

		cache := ses.proc.GetStageCache()
		cache.Set(stageName, stage.StageDef{
			Name: stageName,
			Url:  stageURL,
		})

		err = validateOutputDirPath(ctx, ses, fmt.Sprintf("stage://%s", stageName))
		require.NoError(t, err)
	})

	t.Run("shared fileservice directory exists", func(t *testing.T) {
		fs := ses.proc.Base.FileService
		dirPath := fmt.Sprintf("%s:/exists", defines.SharedFileServiceName)
		filePath := fmt.Sprintf("%s/file.txt", dirPath)

		write := fileservice.IOVector{
			FilePath: filePath,
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   int64(len("x")),
				Data:   []byte("x"),
			}},
		}
		require.NoError(t, fs.Write(ctx, write))

		require.NoError(t, validateOutputDirPath(ctx, ses, dirPath))
	})

	t.Run("shared fileservice missing directory", func(t *testing.T) {
		dirPath := fmt.Sprintf("%s:/not-exist", defines.SharedFileServiceName)

		err := validateOutputDirPath(ctx, ses, dirPath)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	})

	t.Run("shared fileservice path is file", func(t *testing.T) {
		fs := ses.proc.Base.FileService
		filePath := fmt.Sprintf("%s:/just-file", defines.SharedFileServiceName)

		write := fileservice.IOVector{
			FilePath: filePath,
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   int64(len("abc")),
				Data:   []byte("abc"),
			}},
		}
		require.NoError(t, fs.Write(ctx, write))

		err := validateOutputDirPath(ctx, ses, filePath)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "not a directory")
	})

	t.Run("invalid path format", func(t *testing.T) {
		err := validateOutputDirPath(ctx, ses, string([]byte{0x00, ':'}))
		require.Error(t, err)
	})

	t.Run("service argument error", func(t *testing.T) {
		err := validateOutputDirPath(ctx, ses, "s3,bad:/bucket")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	})

	t.Run("service root unreachable", func(t *testing.T) {
		err := validateOutputDirPath(ctx, ses, "s3-opts,endpoint=http://127.0.0.1:65535,region=us-east-1,bucket=b,key=k,secret=s,prefix=tmp:")
		require.Error(t, err)
	})
}
