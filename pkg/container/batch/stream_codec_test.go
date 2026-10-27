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

package batch

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type shortCodecWriter struct{}

func (shortCodecWriter) Write(p []byte) (int, error) { return 0, nil }

type failingCodecWriter struct {
	left int
	err  error
}

func (w *failingCodecWriter) Write(p []byte) (int, error) {
	if w.left < len(p) {
		return 0, w.err
	}
	w.left -= len(p)
	return len(p), nil
}

func TestStreamCodecCompatibilityAndFailures(t *testing.T) {
	m := mpool.MustNew("stream-codec")
	defer mpool.DeleteMPool(m)
	b := New([]string{"id", "wide"})
	defer b.Clean(m)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	b.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixedList(b.Vecs[0], []int64{1, 2, 3}, nil, m))
	require.NoError(t, vector.AppendBytesList(b.Vecs[1], [][]byte{[]byte("inlined"), []byte(strings.Repeat("w", 131072)), nil}, []bool{false, false, true}, m))
	b.SetRowCount(3)
	b.ExtraBuf = []byte("extra")
	b.Recursive = 2
	b.ShuffleIDX = 9
	cases := []*Batch{b, New([]string{"constant", "null"})}
	defer cases[1].Clean(m)
	var err error
	cases[1].Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(strings.Repeat("c", 32)), 5, m)
	require.NoError(t, err)
	cases[1].Vecs[1] = vector.NewConstNull(types.T_int64.ToType(), 5, m)
	cases[1].SetRowCount(5)
	// Frozen source3c7eb8de7f encoder outputs, captured before consolidation.
	// They cover every field without maintaining a second encoder in tests.
	goldens := []struct {
		size   int64
		sha256 string
	}{
		{131323, "d3034c6d0aabdd24ed56214b0f3d78325bce842497ccd1e7f3c5045a21c23fb9"},
		{180, "c099d7641f251e480978aea19d04c735d9e6fc2f50ab6366f698217deb418ef7"},
	}
	for i, bat := range cases {
		t.Run(fmt.Sprintf("rows-%d", bat.RowCount()), func(t *testing.T) {
			for _, streaming := range []bool{false, true} {
				var actual bytes.Buffer
				var writer io.Writer = &actual
				if streaming {
					writer = struct{ io.Writer }{&actual}
				}
				require.NoError(t, bat.MarshalBinaryTo(writer))
				require.Equal(t, goldens[i].sha256, fmt.Sprintf("%x", sha256.Sum256(actual.Bytes())), "streaming=%t", streaming)
				require.Equal(t, goldens[i].size, int64(actual.Len()))
			}
			require.Equal(t, goldens[i].size, bat.MarshalBinarySize())
			require.ErrorIs(t, bat.MarshalBinaryTo(shortCodecWriter{}), io.ErrShortWrite)
			boom := errors.New("write failed")
			require.ErrorIs(t, bat.MarshalBinaryTo(&failingCodecWriter{left: 64, err: boom}), boom)
		})
	}
}
