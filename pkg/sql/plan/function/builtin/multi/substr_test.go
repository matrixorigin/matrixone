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

package multi

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestSubStr(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  []byte
		wantScalar bool
	}{
		{
			name:       "TEST01",
			vecs:       makeSubStrVectors("abcdefghijklmn", 5, 0, false),
			proc:       procs,
			wantBytes:  []byte("efghijklmn"),
			wantScalar: true,
		},
		{
			name:       "TEST02",
			vecs:       makeSubStrVectors("abcdefghijklmn", 7, 0, false),
			proc:       procs,
			wantBytes:  []byte("ghijklmn"),
			wantScalar: true,
		},
		{
			name:       "TEST03",
			vecs:       makeSubStrVectors("abcdefghijklmn", 11, 0, false),
			proc:       procs,
			wantBytes:  []byte("klmn"),
			wantScalar: true,
		},
		{
			name:       "TEST04",
			vecs:       makeSubStrVectors("abcdefghijklmn", 16, 0, false),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST05",
			vecs:       makeSubStrVectors("abcdefghijklmn", 5, 6, true),
			proc:       procs,
			wantBytes:  []byte("efghij"),
			wantScalar: true,
		},
		{
			name:       "TEST06",
			vecs:       makeSubStrVectors("abcdefghijklmn", 5, 10, true),
			proc:       procs,
			wantBytes:  []byte("efghijklmn"),
			wantScalar: true,
		},
		{
			name:       "TEST07",
			vecs:       makeSubStrVectors("abcdefghijklmn", 5, 0, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST08",
			vecs:       makeSubStrVectors("abcdefghijklmn", 6, -8, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST09",
			vecs:       makeSubStrVectors("abcdefghijklmn", 6, -9, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST09",
			vecs:       makeSubStrVectors("abcdefghijklmn", 6, -4, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST10",
			vecs:       makeSubStrVectors("abcdefghijklmn", 6, -1, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test11",
			vecs:       makeSubStrVectors("abcdefghijklmn", -4, 0, false),
			proc:       procs,
			wantBytes:  []byte("klmn"),
			wantScalar: true,
		},
		{
			name:       "Test12",
			vecs:       makeSubStrVectors("abcdefghijklmn", -14, 0, false),
			proc:       procs,
			wantBytes:  []byte("abcdefghijklmn"),
			wantScalar: true,
		},
		{
			name:       "Test13",
			vecs:       makeSubStrVectors("abcdefghijklmn", -16, 0, false),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test14",
			vecs:       makeSubStrVectors("abcdefghijklmn", -4, 3, true),
			proc:       procs,
			wantBytes:  []byte("klm"),
			wantScalar: true,
		},
		{
			name:       "Test15",
			vecs:       makeSubStrVectors("abcdefghijklmn", -14, 10, true),
			proc:       procs,
			wantBytes:  []byte("abcdefghij"),
			wantScalar: true,
		},
		{
			name:       "Test16",
			vecs:       makeSubStrVectors("abcdefghijklmn", -14, 15, true),
			proc:       procs,
			wantBytes:  []byte("abcdefghijklmn"),
			wantScalar: true,
		},
		{
			name:       "Test17",
			vecs:       makeSubStrVectors("abcdefghijklmn", -16, 10, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test18",
			vecs:       makeSubStrVectors("abcdefghijklmn", -16, 20, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test19",
			vecs:       makeSubStrVectors("abcdefghijklmn", -16, 2, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test20",
			vecs:       makeSubStrVectors("abcdefghijklmn", -12, 2, true),
			proc:       procs,
			wantBytes:  []byte("cd"),
			wantScalar: true,
		},
		{
			name:       "Test21",
			vecs:       makeSubStrVectors("abcdefghijklmn", -12, 14, true),
			proc:       procs,
			wantBytes:  []byte("cdefghijklmn"),
			wantScalar: true,
		},
		{
			name:       "Test22",
			vecs:       makeSubStrVectors("abcdefghijklmn", -12, 0, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test23",
			vecs:       makeSubStrVectors("abcdefghijklmn", -6, -5, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test24",
			vecs:       makeSubStrVectors("abcdefghijklmn", -6, -10, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test25",
			vecs:       makeSubStrVectors("abcdefghijklmn", -6, -1, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			substr, err := Substring(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, substr.GetBytes(0))
			require.Equal(t, c.wantScalar, substr.IsScalar())
		})
	}
}

func TestSubStrUTF(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  []byte
		wantScalar bool
	}{
		{
			name:       "TEST01",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 5, 0, false),
			proc:       procs,
			wantBytes:  []byte("cdef我爱你中国"),
			wantScalar: true,
		},
		{
			name:       "TEST02",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 7, 0, false),
			proc:       procs,
			wantBytes:  []byte("ef我爱你中国"),
			wantScalar: true,
		},
		{
			name:       "TEST03",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 11, 0, false),
			proc:       procs,
			wantBytes:  []byte("你中国"),
			wantScalar: true,
		},
		{
			name:       "TEST04",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 16, 0, false),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST05",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 5, 6, true),
			proc:       procs,
			wantBytes:  []byte("cdef我爱"),
			wantScalar: true,
		},
		{
			name:       "TEST06",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 5, 10, true),
			proc:       procs,
			wantBytes:  []byte("cdef我爱你中国"),
			wantScalar: true,
		},
		{
			name:       "TEST07",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 5, 0, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST08",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 6, -8, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST09",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 6, -9, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST09",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 6, -4, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST10",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", 6, -1, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test11",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -4, 0, false),
			proc:       procs,
			wantBytes:  []byte("爱你中国"),
			wantScalar: true,
		},
		{
			name:       "Test12",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -13, 0, false),
			proc:       procs,
			wantBytes:  []byte("明天abcdef我爱你中国"),
			wantScalar: true,
		},
		{
			name:       "Test13",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -16, 0, false),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test14",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -4, 3, true),
			proc:       procs,
			wantBytes:  []byte("爱你中"),
			wantScalar: true,
		},
		{
			name:       "Test15",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -13, 10, true),
			proc:       procs,
			wantBytes:  []byte("明天abcdef我爱"),
			wantScalar: true,
		},
		{
			name:       "Test16",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -13, 15, true),
			proc:       procs,
			wantBytes:  []byte("明天abcdef我爱你中国"),
			wantScalar: true,
		},
		{
			name:       "Test17",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -16, 10, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test18",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -16, 20, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test19",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -16, 2, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test20",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -12, 2, true),
			proc:       procs,
			wantBytes:  []byte("天a"),
			wantScalar: true,
		},
		{
			name:       "Test21",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -12, 14, true),
			proc:       procs,
			wantBytes:  []byte("天abcdef我爱你中国"),
			wantScalar: true,
		},
		{
			name:       "Test22",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -12, 0, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test23",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -6, -5, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test24",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -6, -10, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test25",
			vecs:       makeSubStrVectors("明天abcdef我爱你中国", -6, -1, true),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			substr, err := Substring(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, substr.GetBytes(0))
			require.Equal(t, c.wantScalar, substr.IsScalar())
		})
	}
}

func TestSubStrBlob(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  []byte
		wantScalar bool
	}{
		{
			name:       "TEST01",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 5, 0, false, procs),
			proc:       procs,
			wantBytes:  []byte("efghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"),
			wantScalar: true,
		},
		{
			name:       "TEST02",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 27, 0, false, procs),
			proc:       procs,
			wantBytes:  []byte("qwertyuiopasdfghjklzxcvbnm"),
			wantScalar: true,
		},
		{
			name:       "TEST03",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 50, 0, false, procs),
			proc:       procs,
			wantBytes:  []byte("bnm"),
			wantScalar: true,
		},
		{
			name:       "TEST04",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 53, 0, false, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST05",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 5, 6, true, procs),
			proc:       procs,
			wantBytes:  []byte("efghij"),
			wantScalar: true,
		},
		{
			name:       "TEST06",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 5, 10, true, procs),
			proc:       procs,
			wantBytes:  []byte("efghijklmn"),
			wantScalar: true,
		},
		{
			name:       "TEST07",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 5, 0, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST08",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 6, -8, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST09",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 6, -9, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST09",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 6, -4, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "TEST10",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), 6, -1, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test11",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -4, 0, false, procs),
			proc:       procs,
			wantBytes:  []byte("vbnm"),
			wantScalar: true,
		},
		{
			name:       "Test12",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -14, 0, false, procs),
			proc:       procs,
			wantBytes:  []byte("dfghjklzxcvbnm"),
			wantScalar: true,
		},
		{
			name:       "Test13",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -50, 0, false, procs),
			proc:       procs,
			wantBytes:  []byte("cdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"),
			wantScalar: true,
		},
		{
			name:       "Test14",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -4, 3, true, procs),
			proc:       procs,
			wantBytes:  []byte("vbn"),
			wantScalar: true,
		},
		{
			name:       "Test15",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -26, 10, true, procs),
			proc:       procs,
			wantBytes:  []byte("qwertyuiop"),
			wantScalar: true,
		},
		{
			name:       "Test16",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -14, 15, true, procs),
			proc:       procs,
			wantBytes:  []byte("dfghjklzxcvbnm"),
			wantScalar: true,
		},
		{
			name:       "Test17",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -53, 10, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test18",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -53, 20, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test19",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -53, 2, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test20",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -50, 2, true, procs),
			proc:       procs,
			wantBytes:  []byte("cd"),
			wantScalar: true,
		},
		{
			name:       "Test21",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -12, 14, true, procs),
			proc:       procs,
			wantBytes:  []byte("ghjklzxcvbnm"),
			wantScalar: true,
		},
		{
			name:       "Test22",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -12, 0, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test23",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -6, -5, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test24",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -6, -10, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
		{
			name:       "Test25",
			vecs:       makeSubStrBlobVectors([]byte("abcdefghijklmnopqrstuvwxyzqwertyuiopasdfghjklzxcvbnm"), -6, 0, true, procs),
			proc:       procs,
			wantBytes:  []byte(""),
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			substr, err := Substring(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, substr.GetBytes(0))
			require.Equal(t, c.wantScalar, substr.IsScalar())
		})
	}
}

// Construct vector parameter of substring function
func makeSubStrVectors(src string, start int64, length int64, withLength bool) []*vector.Vector {
	vec := make([]*vector.Vector, 2)
	vec[0] = vector.NewConstString(types.T_varchar.ToType(), 10, src, testutil.TestUtilMp)
	vec[1] = vector.NewConstFixed(types.T_int64.ToType(), 10, start, testutil.TestUtilMp)
	if withLength {
		lvec := vector.NewConstFixed(types.T_int64.ToType(), 10, length, testutil.TestUtilMp)
		vec = append(vec, lvec)
	}
	return vec
}

func makeSubStrBlobVectors(src []byte, start int64, length int64, withLength bool, procs *process.Process) []*vector.Vector {
	inputVector := make([]*vector.Vector, 2)
	inputType := types.New(types.T_blob, 0, 0)
	inputVector[0] = vector.NewConst(inputType, 1)
	inputVector[0].Append(src, false, procs.Mp())
	inputVector[1] = vector.NewConstFixed(types.T_int64.ToType(), 10, start, testutil.TestUtilMp)
	if withLength {
		lvec := vector.NewConstFixed(types.T_int64.ToType(), 10, length, testutil.TestUtilMp)
		inputVector = append(inputVector, lvec)
	}
	return inputVector
}
