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

package plan

import (
	"bytes"
	"compress/zlib"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// The fixture expressions were bound and serialized by predecessor efc8c79,
// before the compatibility-only lowering was removed. Replay them through the
// production executor to retain support for their persisted representation.
func TestPersistedJSONConsumersReplayPredecessorPlan(t *testing.T) {
	names := []string{"md5", "sha", "sha1", "sha2", "compress", "quote", "soundex"}
	fixtures := map[string]string{
		"compress": "0a04084640013a360a112080808080b0304208636f6d707265737312210a04084740033a190a0f208080808080024206636f6e63617412060a02083e2a00",
		"md5":      "0a06083d202040033a310a0c2080808080800e42036d643512210a04084740033a190a0f208080808080024206636f6e63617412060a02083e2a00",
		"quote":    "0a04084740023a330a0e2080808080c02c420571756f746512210a04084740033a190a0f208080808080024206636f6e63617412060a02083e2a00",
		"sha":      "0a06083d202840033a310a0c20808080809018420373686112210a04084740033a190a0f208080808080024206636f6e63617412060a02083e2a00",
		"sha1":     "0a06083d202840033a320a0d2080808080901842047368613112210a04084740033a190a0f208080808080024206636f6e63617412060a02083e2a00",
		"sha2":     "0a07083d20800140033a3f0a0d2080808080a01842047368613212210a04084740033a190a0f208080808080024206636f6e63617412060a02083e2a00120b0a04081710011203288002",
		"soundex":  "0a0a084720ffffffff0740033a350a102080808080d02c4207736f756e64657812210a04084740033a190a0f208080808080024206636f6e63617412060a02083e2a00",
	}
	proc := newPlanTestProcess(t)
	defer proc.Free()
	b := batch.NewWithSize(1)
	defer b.Clean(proc.Mp())
	b.Vecs[0] = vector.NewVec(types.T_json.ToType())
	docs := []string{`"a\nb"`, `["` + strings.Repeat("a", 65536) + `"]`}
	for _, doc := range docs {
		bj, err := types.ParseStringToByteJson(doc)
		require.NoError(t, err)
		data, err := bj.Marshal()
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(b.Vecs[0], data, false, proc.Mp()))
	}
	require.NoError(t, vector.AppendBytes(b.Vecs[0], nil, true, proc.Mp()))
	b.SetRowCount(3)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			data, err := hex.DecodeString(fixtures[name])
			require.NoError(t, err)
			var expr planpb.Expr
			require.NoError(t, expr.Unmarshal(data))
			require.Equal(t, "concat", expr.GetF().Args[0].GetF().Func.ObjName)
			result, release, err := colexec.GetReadonlyResultFromExpression(proc, &expr, []*batch.Batch{b})
			if release != nil {
				defer release()
			}
			require.NoError(t, err)
			if name == "quote" {
				require.False(t, result.IsNull(2))
				require.Equal(t, "NULL", string(result.GetBytesAt(2)))
			} else {
				require.True(t, result.IsNull(2), "SQL NULL must stay NULL")
			}
			for i, doc := range docs {
				require.False(t, result.IsNull(uint64(i)))
				got := result.GetBytesAt(i)
				switch name {
				case "md5":
					sum := md5.Sum([]byte(doc))
					require.Equal(t, hex.EncodeToString(sum[:]), string(got))
				case "sha", "sha1":
					sum := sha1.Sum([]byte(doc))
					require.Equal(t, hex.EncodeToString(sum[:]), string(got))
				case "sha2":
					sum := sha256.Sum256([]byte(doc))
					require.Equal(t, hex.EncodeToString(sum[:]), string(got))
				case "compress":
					require.GreaterOrEqual(t, len(got), 4)
					require.Equal(t, uint32(len(doc)), binary.LittleEndian.Uint32(got))
					zr, err := zlib.NewReader(bytes.NewReader(got[4:]))
					require.NoError(t, err)
					decoded, err := io.ReadAll(zr)
					require.NoError(t, err)
					require.NoError(t, zr.Close())
					require.Equal(t, []byte(doc), decoded)
				case "quote":
					expected := "'" + strings.ReplaceAll(strings.ReplaceAll(doc, `\`, `\\`), "'", `\'`) + "'"
					require.Equal(t, expected, string(got))
				case "soundex":
					expected := "A510"
					if i == 1 {
						expected = "A000"
					}
					require.Equal(t, expected, string(got))
				}
			}
		})
	}
}
